import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


# =====================================================
# CDC Processor (No Primary Key, Row-based CDC)
# =====================================================
class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None

        self.ins_count = 0
        self.upd_count = 0
        self.del_count = 0

        logger.info(f"CDC initialized | bucket={bucket} table={table}")

    # -------------------------------------------------
    # Path helpers
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_ts=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                name = parts[-1]
                if add_ts:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    name = name.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [name])
        raise ValueError("Invalid CDC path")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file | {src} → {dst}")

    # -------------------------------------------------
    # S3 IO
    # -------------------------------------------------
    def load_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(f"Loaded {key} | rows={tbl.num_rows}")
        return tbl

    def write_arrow(self, key, tbl):
        buf = tbl.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"LOAD written | s3://{self.bucket}/{key}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for p in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in p.get("Contents", []):
                k = o["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        files.sort()
        logger.info(f"CDC files discovered: {files}")
        return files

    # -------------------------------------------------
    # Schema handling
    # -------------------------------------------------
    def infer_schema(self, load_tbl, cdc_tbl):
        fields = []
        for f in load_tbl.schema:
            if pa.types.is_null(f.type):
                if f.name in cdc_tbl.column_names:
                    fields.append(pa.field(f.name, cdc_tbl.schema.field(f.name).type))
                else:
                    fields.append(pa.field(f.name, pa.string()))
            else:
                fields.append(f)
        return pa.schema(fields)

    def upgrade_load(self, tbl, schema):
        cols = []
        for f in schema:
            col = tbl[f.name]
            if pa.types.is_null(col.type):
                cols.append(pa.array([None] * tbl.num_rows, type=f.type))
            else:
                cols.append(col)
        return pa.Table.from_arrays(cols, schema=schema)

    def align_schema(self, tbl, schema, op_col):
        cols = {}
        for f in schema:
            if f.name in tbl.column_names:
                c = tbl[f.name]
                if c.type != f.type:
                    try:
                        c = pc.cast(c, f.type)
                    except Exception:
                        c = pa.array([None] * tbl.num_rows, type=f.type)
                cols[f.name] = c
            else:
                cols[f.name] = pa.array([None] * tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + schema.names
        )

    # -------------------------------------------------
    # Row-based DELETE (no PK)
    # -------------------------------------------------
    def delete_matching_rows(self, base, del_tbl):
        if del_tbl.num_rows == 0 or base.num_rows == 0:
            return base

        base_df = base.to_pandas()
        del_df = del_tbl.to_pandas()

        before = len(base_df)
        merged = base_df.merge(del_df.drop_duplicates(), how="left", indicator=True)
        base_df = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

        removed = before - len(base_df)
        self.del_count += removed

        logger.info(f"DELETE applied | rows_removed={removed}")
        return pa.Table.from_pandas(base_df, preserve_index=False)

    # -------------------------------------------------
    # Main CDC logic
    # -------------------------------------------------
    def apply_cdc_file(self, cdc_tbl, op_col):
        op = pc.utf8_upper(pc.cast(cdc_tbl[op_col], pa.string()))

        ins = cdc_tbl.filter(pc.equal(op, "I")).remove_column(0)
        upd = cdc_tbl.filter(pc.equal(op, "U")).remove_column(0)
        dele = cdc_tbl.filter(pc.equal(op, "D")).remove_column(0)

        if dele.num_rows:
            self.df = self.delete_matching_rows(self.df, dele)

        if upd.num_rows:
            self.df = self.delete_matching_rows(self.df, upd)
            self.df = pa.concat_tables([self.df, upd])
            self.upd_count += upd.num_rows

        if ins.num_rows:
            self.df = pa.concat_tables([self.df, ins])
            self.ins_count += ins.num_rows

    # -------------------------------------------------
    # Processor
    # -------------------------------------------------
    def process(self, trigger_key):
        start = datetime.utcnow()
        prefix = self.get_load_prefix(trigger_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow(load_key)
        initial_rows = self.df.num_rows

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found")
            return

        first_cdc = self.load_arrow(cdc_files[0])
        op_col = first_cdc.column_names[0]

        schema = self.infer_schema(self.df, first_cdc)
        self.df = self.upgrade_load(self.df, schema)

        for f in cdc_files:
            logger.info(f"Processing CDC file: {f}")
            raw = self.load_arrow(f)
            aligned = self.align_schema(raw, schema, op_col)
            self.apply_cdc_file(aligned, op_col)
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        self.write_arrow(load_key, self.df)

        logger.info(json.dumps({
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": self.df.num_rows,
            "inserts": self.ins_count,
            "updates": self.upd_count,
            "deletes": self.del_count,
            "time_sec": (datetime.utcnow() - start).total_seconds()
        }))


# =====================================================
# Lambda handler
# =====================================================
def lambda_handler(event, context):
    logger.info(json.dumps(event))

    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "/processed/" in key or "LOAD" in key or not key.endswith(".csv"):
                continue

            table = key.split("/")[-2]
            CDCProcessorArrow(bucket, table).process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error("CDC failed", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None

        logger.info(f"CDC Processor initialized → bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helper paths
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_ts=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                fname = parts[-1]
                if add_ts:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    fname = fname.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [fname])
        raise ValueError("Invalid directory structure")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, Key=dst,
                       CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved {src} → {dst}")

    # -------------------------------------------------
    # S3 / Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"],
                           parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded {key} → rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Wrote updated LOAD → s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema alignment
    # -------------------------------------------------
    def align_schema(self, cdc_tbl, target_schema, op_col):
        cols = {}
        for f in target_schema:
            if f.name in cdc_tbl.column_names:
                col = cdc_tbl[f.name]
                if col.type != f.type:
                    col = pc.cast(col, f.type, safe=False)
                cols[f.name] = col
            else:
                cols[f.name] = pa.array([None] * cdc_tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [cdc_tbl[op_col]] + list(cols.values()),
            names=[op_col] + target_schema.names
        )

    # -------------------------------------------------
    # Row Signature (NO PRIMARY KEY)
    # -------------------------------------------------
    def row_signature(self, tbl):
        """
        Vectorized hash of full row (all columns)
        """
        return pc.hash(
            pc.struct(*[tbl[c] for c in tbl.column_names])
        )

    # -------------------------------------------------
    # Vectorized CDC Engine (FAST)
    # -------------------------------------------------
    def apply_cdc(self, base_tbl, cdc_tbl, op_col):
        start_rows = base_tbl.num_rows

        base_sig = self.row_signature(base_tbl)
        cdc_sig = self.row_signature(cdc_tbl.remove_column(0))

        ops = pc.utf8_upper(pc.cast(cdc_tbl[op_col], pa.string()))

        ins_mask = pc.equal(ops, "I")
        upd_mask = pc.equal(ops, "U")
        del_mask = pc.equal(ops, "D")

        inserts = cdc_tbl.filter(ins_mask).remove_column(0)
        updates = cdc_tbl.filter(upd_mask).remove_column(0)
        deletes = cdc_tbl.filter(del_mask).remove_column(0)

        # DELETE → remove ALL matching rows
        if deletes.num_rows > 0:
            del_sig = self.row_signature(deletes)
            keep_mask = pc.invert(pc.is_in(base_sig, del_sig))
            base_tbl = base_tbl.filter(keep_mask)

        # UPDATE → delete old rows + insert new
        if updates.num_rows > 0:
            upd_sig = self.row_signature(updates)
            keep_mask = pc.invert(pc.is_in(self.row_signature(base_tbl), upd_sig))
            base_tbl = base_tbl.filter(keep_mask)
            base_tbl = pa.concat_tables([base_tbl, updates])

        # INSERT
        if inserts.num_rows > 0:
            base_tbl = pa.concat_tables([base_tbl, inserts])

        return base_tbl, inserts.num_rows, updates.num_rows, deletes.num_rows

    # -------------------------------------------------
    # Main Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow_table(load_key)
        schema = self.df.schema

        paginator = s3.get_paginator("list_objects_v2")
        cdc_files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                k = o["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    cdc_files.append(k)

        cdc_files = sorted(cdc_files)
        logger.info(f"CDC files discovered: {cdc_files}")

        total_i = total_u = total_d = 0

        for f in cdc_files:
            raw = self.load_arrow_table(f)
            op_col = raw.column_names[0]
            aligned = self.align_schema(raw, schema, op_col)

            logger.info(f"Applying CDC file → {f}")
            self.df, i, u, d = self.apply_cdc(self.df, aligned, op_col)

            total_i += i
            total_u += u
            total_d += d

            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        out = self.write_arrow_table(load_key, self.df)

        logger.info(json.dumps({
            "status": "success",
            "final_rows": self.df.num_rows,
            "inserted": total_i,
            "updated": total_u,
            "deleted": total_d,
            "output": out,
            "time_sec": (datetime.utcnow() - start).total_seconds()
        }))


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info("Lambda triggered")
    logger.info(json.dumps(event))

    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "LOAD" in key or "/processed/" in key or not key.endswith(".csv"):
                continue

            table = key.split("/")[-2]
            CDCProcessorArrow(bucket, table).process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error("CDC Lambda failed", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

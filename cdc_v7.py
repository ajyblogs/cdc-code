import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
from io import BytesIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None
        self.pk_col: str | None = None

    # ------------------------
    # S3 helpers
    # ------------------------
    def read_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"[READ] {key} rows={tbl.num_rows}")
        return tbl

    def write_arrow(self, key, table):
        buf = BytesIO()
        csv.write_csv(table, buf, write_options=csv.WriteOptions(delimiter="|"))
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        logger.info(f"[WRITE] {key} rows={table.num_rows}")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": src}, Key=dst)
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"[ARCHIVE] {src} → {dst}")

    def get_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_ts=False):
        parts = key.split("/")
        name = parts[-1]
        if add_ts:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            name = name.replace(".csv", f"_{ts}.csv")
        for i, p in enumerate(parts):
            if p.upper().startswith("DSET"):
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [name])
        raise ValueError("DSET folder not found")

    def list_cdc_files(self, prefix):
        files = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        files.sort()
        logger.info(f"[DISCOVER] CDC files={files}")
        return files

    # ------------------------
    # Schema inference from CDC
    # ------------------------
    def infer_load_schema(self, load_tbl, sample_cdc_tbls):
        fields = []
        for f in load_tbl.schema:
            if pa.types.is_null(f.type):
                inferred_type = None
                for cdc_tbl in sample_cdc_tbls:
                    if f.name in cdc_tbl.column_names:
                        cdc_type = cdc_tbl.schema.field(f.name).type
                        if not pa.types.is_null(cdc_type):
                            inferred_type = cdc_type
                            break
                fields.append(pa.field(f.name, inferred_type or pa.string()))
            else:
                fields.append(f)
        return pa.schema(fields)

    # ------------------------
    # Schema alignment
    # ------------------------
    def align_schema(self, cdc_tbl, target_schema, op_col):
        arrays = [cdc_tbl[op_col]]
        for f in target_schema:
            if f.name in cdc_tbl.column_names:
                col = cdc_tbl[f.name]
                if col.type != f.type:
                    col = pc.cast(col, f.type, safe=False)
            else:
                col = pa.nulls(cdc_tbl.num_rows, type=f.type)
            arrays.append(col)
        return pa.Table.from_arrays(arrays, names=[op_col]+[f.name for f in target_schema])

    # ------------------------
    # Vectorised DELETE (full-row match, ChunkedArray safe)
    # ------------------------
    def apply_delete(self, df_del):
        if df_del.num_rows == 0:
            return
        mask = pa.array([True] * self.df.num_rows)
        for col in self.df.column_names:
            df_col = pa.chunked_array(self.df[col])
            del_col = pa.chunked_array(df_del[col])
            col_mask = pc.invert(pc.is_in(df_col, del_col))
            mask = pc.and_(mask, col_mask)
        self.df = self.df.filter(mask)
        logger.info(f"[DELETE] applied, LOAD rows={self.df.num_rows}")

    # ------------------------
    # Vectorised UPDATE (PK-based, ChunkedArray safe)
    # ------------------------
    def apply_update(self, df_upd):
        if df_upd.num_rows == 0:
            return
        pk_values = pa.chunked_array(df_upd[self.pk_col])
        mask = pc.invert(pc.is_in(pa.chunked_array(self.df[self.pk_col]), pk_values))
        remaining = self.df.filter(mask)
        self.df = pa.concat_tables([remaining, df_upd])
        logger.info(f"[UPDATE] applied, LOAD rows={self.df.num_rows}")

    # ------------------------
    # Vectorised INSERT
    # ------------------------
    def apply_insert(self, df_ins):
        if df_ins.num_rows == 0:
            return
        self.df = pa.concat_tables([self.df, df_ins])
        logger.info(f"[INSERT] applied, LOAD rows={self.df.num_rows}")

    # ------------------------
    # Main CDC processor
    # ------------------------
    def process(self, trigger_key):
        prefix = self.get_prefix(trigger_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # Load base table
        self.df = self.read_arrow(load_key)
        self.pk_col = self.df.column_names[0]
        initial_rows = self.df.num_rows
        logger.info(f"[LOAD] initial rows={initial_rows}")

        # Discover CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[SKIP] no CDC files")
            return

        # Sample first 5 CDC files for schema inference
        sample_cdc = [self.read_arrow(f) for f in cdc_files[:5]]
        inferred_schema = self.infer_load_schema(self.df, sample_cdc)
        if inferred_schema != self.df.schema:
            self.df = self.df.cast(inferred_schema)
            logger.info("[SCHEMA] LOAD schema upgraded from CDC inference")

        # Operation column
        op_col = sample_cdc[0].column_names[0]
        total_ins = total_upd = total_del = 0

        # Process each CDC file sequentially
        for f in cdc_files:
            cdc_tbl = self.read_arrow(f)
            aligned = self.align_schema(cdc_tbl, self.df.schema, op_col)

            ops = pc.utf8_upper(pc.cast(aligned[op_col], pa.string()))
            df_ins = aligned.filter(pc.equal(ops, "I")).remove_column(0)
            df_upd = aligned.filter(pc.equal(ops, "U")).remove_column(0)
            df_del = aligned.filter(pc.equal(ops, "D")).remove_column(0)

            total_ins += df_ins.num_rows
            total_upd += df_upd.num_rows
            total_del += df_del.num_rows

            # Apply operations
            self.apply_delete(df_del)
            self.apply_update(df_upd)
            self.apply_insert(df_ins)

            # Archive CDC file
            self.move_file(f, self.get_processed_path(f))

        # Archive old LOAD
        self.move_file(load_key, self.get_processed_path(load_key, add_ts=True))
        self.write_arrow(load_key, self.df)

        final_rows = self.df.num_rows
        logger.info(f"[STATS] INSERT={total_ins}, UPDATE={total_upd}, DELETE={total_del}, FINAL_LOAD={final_rows}, net_change={final_rows-initial_rows:+d}")


# ------------------------
# Lambda handler
# ------------------------
def lambda_handler(event, context):
    logger.info(json.dumps(event))
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]
        if "LOAD" in key or "/processed/" in key:
            continue
        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)
    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

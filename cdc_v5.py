import json
import boto3
import polars as pl
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")


class CDCProcessorPolarsVectorized:
    def __init__(self, bucket: str, table: str):
        self.bucket = bucket
        self.table = table
        self.df: pl.DataFrame | None = None
        self.pk_col: str | None = None
        self.op_col: str | None = None

    # ------------------------
    # Helpers
    # ------------------------
    def extract_table_name(self, key):
        for part in key.split("/"):
            if part.startswith("DSET"):
                return part
        return "unknown"

    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, part in enumerate(parts):
            if part.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[: i + 1] + ["processed"] + parts[i + 1 : -1] + [filename])
        raise ValueError("Invalid key")

    def move_file(self, source, target):
        s3.copy_object(CopySource={"Bucket": self.bucket, "Key": source}, Bucket=self.bucket, Key=target)
        s3.delete_object(Bucket=self.bucket, Key=source)
        logger.info(f"Moved: {source} -> {target}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    files.append(key)
        return sorted(files)

    # ------------------------
    # Load data
    # ------------------------
    def load_base_file(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        self.df = pl.read_csv(obj["Body"], separator="|")
        if self.df.width > 0:
            self.pk_col = self.df.columns[0]
        logger.info(f"Loaded base with {self.df.height} rows")

    # ------------------------
    # Write result
    # ------------------------
    def write_csv(self, key):
        csv_bytes = self.df.write_csv(stream=True, separator="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_bytes)
        return f"s3://{self.bucket}/{key}"

    # ------------------------
    # Vectorized CDC Logic
    # ------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()

        base_prefix = self.get_load_prefix(cdc_key)
        load_key = f"{base_prefix}LOAD00000001.csv"

        # Load base table
        self.load_base_file(load_key)
        initial_rows = self.df.height

        # Collect CDC files
        cdc_files = self.list_cdc_files(base_prefix)
        if not cdc_files:
            logger.info("No CDC files found")
            return

        logger.info(f"{len(cdc_files)} CDC files found")

        # Load all CDC files into one DF
        df_list = []
        for key in cdc_files:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
            df_cdc = pl.read_csv(obj["Body"], separator="|")
            df_list.append(df_cdc)

        cdc_df = pl.concat(df_list, how="vertical")
        self.op_col = cdc_df.columns[0]   # first column is op (I/U/D)

        # Separate by operation
        df_ins = cdc_df.filter(pl.col(self.op_col).str.to_uppercase().is_in(["I", "INSERT"]))
        df_upd = cdc_df.filter(pl.col(self.op_col).str.to_uppercase().is_in(["U", "UPDATE"]))
        df_del = cdc_df.filter(pl.col(self.op_col).str.to_uppercase().is_in(["D", "DELETE"]))

        # Drop op column to align with base
        df_ins = df_ins.drop(self.op_col, strict=False)
        df_upd = df_upd.drop(self.op_col, strict=False)
        df_del = df_del.drop(self.op_col, strict=False)

        # Ensure CDC columns align with base table
        cdc_columns = df_ins.columns | df_upd.columns | df_del.columns
        for col in cdc_columns:
            if col not in self.df.columns:
                self.df = self.df.with_columns(pl.lit(None).alias(col))

        untouched_cols = [c for c in self.df.columns if c not in cdc_columns]

        df_ins = df_ins.with_columns([pl.lit(None).alias(c) for c in untouched_cols if c not in df_ins.columns])
        df_upd = df_upd.with_columns([pl.lit(None).alias(c) for c in untouched_cols if c not in df_upd.columns])
        df_del = df_del.with_columns([pl.lit(None).alias(c) for c in untouched_cols if c not in df_del.columns])

        # ------------------------
        # VECTORIZED DELETE (anti-join full row match)
        # ------------------------
        if df_del.height > 0:
            join_cols = df_del.columns
            before = self.df.height
            self.df = self.df.join(df_del, on=join_cols, how="anti")
            logger.info(f"DELETE removed {before - self.df.height} rows")

        # ------------------------
        # VECTORIZED UPDATE (PK-based)
        #   1. remove PKs from base
        #   2. append updated rows
        # ------------------------
        if df_upd.height > 0:
            pk = self.pk_col
            base_before = self.df.height

            # Remove existing PK rows
            self.df = self.df.join(df_upd.select(pk), on=pk, how="anti")

            # Add updated rows
            self.df = pl.concat([self.df, df_upd], how="vertical")

            logger.info(f"UPDATE affected {df_upd.height} rows")

        # ------------------------
        # VECTORIZED INSERT
        # ------------------------
        if df_ins.height > 0:
            self.df = pl.concat([self.df, df_ins], how="vertical")
            logger.info(f"INSERT added {df_ins.height} rows")

        # Move CDC files
        for key in cdc_files:
            self.move_file(key, self.get_processed_path(key))

        # Move LOAD file
        load_processed = self.get_processed_path(load_key, add_timestamp=True)
        self.move_file(load_key, load_processed)

        # Write updated base
        output = self.write_csv(load_key)

        logger.info({
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": self.df.height,
            "row_change": self.df.height - initial_rows,
            "inserts": df_ins.height,
            "updates": df_upd.height,
            "deletes": df_del.height,
            "output": output,
            "processing_time_seconds": (datetime.utcnow() - start).total_seconds(),
        })


# ------------------------
# Lambda handler
# ------------------------
def lambda_handler(event, context):
    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if not key.endswith(".csv") or "LOAD" in key or "/processed/" in key:
                continue

            table_name = key.split("/")[-2]
            processor = CDCProcessorPolarsVectorized(bucket, table_name)

            processor.process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(str(e), exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

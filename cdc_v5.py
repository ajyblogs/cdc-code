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


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None
        self.pk_col = None
        self.op_col = None

    # -----------------------------
    # Helper functions
    # -----------------------------
    def extract_table_name(self, key):
        for part in key.split("/"):
            if part.startswith("DSET"):
                return part
        return "unknown"

    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [filename])
        raise ValueError("Invalid path structure")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved: {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    out.append(key)
        return sorted(out)

    # -----------------------------
    # Load and write Arrow tables
    # -----------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        return csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        return f"s3://{self.bucket}/{key}"

    # -----------------------------
    # Vectorized CDC Processor
    # -----------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        prefix = self.get_load_prefix(cdc_key)
        load_file = f"{prefix}LOAD00000001.csv"

        # Load base table
        self.df = self.load_arrow_table(load_file)
        initial_rows = self.df.num_rows
        self.pk_col = self.df.column_names[0]

        # Collect CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found")
            return

        logger.info(f"CDC files found: {len(cdc_files)}")

        # Load all CDC files into one table (concatenate)
        cdc_tables = [self.load_arrow_table(f) for f in cdc_files]
        cdc_all: pa.Table = pa.concat_tables(cdc_tables)

        self.op_col = cdc_all.column_names[0]

        # Split into INSERT, UPDATE, DELETE
        op = pc.utf8_upper(cdc_all[self.op_col])

        df_ins = cdc_all.filter(pc.equal(op, pa.scalar("I")))
        df_upd = cdc_all.filter(pc.equal(op, pa.scalar("U")))
        df_del = cdc_all.filter(pc.equal(op, pa.scalar("D")))

        # Drop op column for alignment
        df_ins = df_ins.remove_column(df_ins.schema.get_field_index(self.op_col))
        df_upd = df_upd.remove_column(df_upd.schema.get_field_index(self.op_col))
        df_del = df_del.remove_column(df_del.schema.get_field_index(self.op_col))

        # -----------------------------
        # DELETE (Vectorized Anti-Join)
        # -----------------------------
        if df_del.num_rows > 0:
            del_pk_set = set(df_del[self.pk_col].to_pylist())
            mask = pc.invert(pc.is_in(self.df[self.pk_col], value_set=del_pk_set))
            before = self.df.num_rows
            self.df = self.df.filter(mask)
            logger.info(f"DELETE removed {before - self.df.num_rows} rows")

        # -----------------------------
        # UPDATE (Vectorized)
        #   1. remove matching PKs
        #   2. append updated records
        # -----------------------------
        if df_upd.num_rows > 0:
            upd_pk_set = set(df_upd[self.pk_col].to_pylist())
            mask = pc.invert(pc.is_in(self.df[self.pk_col], value_set=upd_pk_set))
            self.df = self.df.filter(mask)
            self.df = pa.concat_tables([self.df, df_upd])
            logger.info(f"UPDATE applied to {df_upd.num_rows} rows")

        # -----------------------------
        # INSERT (Vectorized)
        # -----------------------------
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"INSERT added {df_ins.num_rows} rows")

        # Move CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Move LOAD file
        load_p = self.get_processed_path(load_file, add_timestamp=True)
        self.move_file(load_file, load_p)

        # Write updated LOAD
        out = self.write_arrow_table(load_file, self.df)

        # Log final summary
        logger.info({
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": self.df.num_rows,
            "row_change": self.df.num_rows - initial_rows,
            "inserts": df_ins.num_rows,
            "updates": df_upd.num_rows,
            "deletes": df_del.num_rows,
            "output": out,
            "processing_time_seconds": (datetime.utcnow() - start).total_seconds()
        })


# -----------------------------
# Lambda Handler
# -----------------------------
def lambda_handler(event, context):
    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "/processed/" in key or not key.endswith(".csv") or "LOAD" in key:
                continue

            table_name = key.split("/")[-2]
            processor = CDCProcessorArrow(bucket, table_name)
            processor.process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(f"Error in CDC processing: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

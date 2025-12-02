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
        logger.info(f"Initialized CDCProcessorArrow for table={table}, bucket={bucket}")

    # -----------------------------
    # Helper functions
    # -----------------------------
    def extract_table_name(self, key):
        for part in key.split("/"):
            if part.startswith("DSET"):
                return part
        return "unknown"

    def get_load_prefix(self, key):
        prefix = "/".join(key.split("/")[:-1]) + "/"
        logger.info(f"Load prefix resolved: {prefix}")
        return prefix

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                processed_path = "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [filename])
                logger.info(f"Processed path resolved: {key} → {processed_path}")
                return processed_path
        raise ValueError("Invalid path structure")

    def move_file(self, src, dst):
        logger.info(f"Moving file from {src} to {dst}")
        s3.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved: {src} → {dst}")

    def list_cdc_files(self, prefix):
        logger.info(f"Listing CDC files under prefix: {prefix}")
        paginator = s3.get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    out.append(key)
        logger.info(f"CDC files discovered: {out}")
        return sorted(out)

    # -----------------------------
    # Load and write Arrow tables
    # -----------------------------
    def load_arrow_table(self, key):
        logger.info(f"Loading Arrow table from S3: {key}")
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded table: {key} → {tbl.num_rows} rows, {tbl.num_columns} columns")
        return tbl

    def write_arrow_table(self, key, table):
        logger.info(f"Writing updated Arrow table to S3: {key}, rows={table.num_rows}")
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Write complete: s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -----------------------------
    # Vectorized CDC Processor
    # -----------------------------
    def process(self, cdc_key):
        logger.info(f"Starting CDC processing for key: {cdc_key}")
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_file = f"{prefix}LOAD00000001.csv"

        logger.info(f"Loading base dataset: {load_file}")
        self.df = self.load_arrow_table(load_file)
        initial_rows = self.df.num_rows
        logger.info(f"Initial base table rows: {initial_rows}")

        self.pk_col = self.df.column_names[0]
        logger.info(f"Primary key column detected: {self.pk_col}")

        # Collect CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found. Exiting.")
            return

        logger.info(f"Total CDC files to ingest: {len(cdc_files)}")

        # Load all CDC files into one table
        logger.info("Loading all CDC files into Arrow tables")
        cdc_tables = [self.load_arrow_table(f) for f in cdc_files]
        cdc_all: pa.Table = pa.concat_tables(cdc_tables)
        logger.info(f"Combined CDC table rows: {cdc_all.num_rows}")

        self.op_col = cdc_all.column_names[0]
        logger.info(f"CDC operation column detected: {self.op_col}")

        # Split into INSERT, UPDATE, DELETE
        logger.info("Splitting CDC dataset into I/U/D operations")
        op = pc.utf8_upper(cdc_all[self.op_col])

        df_ins = cdc_all.filter(pc.equal(op, pa.scalar("I")))
        df_upd = cdc_all.filter(pc.equal(op, pa.scalar("U")))
        df_del = cdc_all.filter(pc.equal(op, pa.scalar("D")))

        logger.info(f"CDC breakdown: INSERT={df_ins.num_rows}, UPDATE={df_upd.num_rows}, DELETE={df_del.num_rows}")

        # Drop op column for alignment
        df_ins = df_ins.remove_column(df_ins.schema.get_field_index(self.op_col))
        df_upd = df_upd.remove_column(df_upd.schema.get_field_index(self.op_col))
        df_del = df_del.remove_column(df_del.schema.get_field_index(self.op_col))

        # -----------------------------
        # DELETE
        # -----------------------------
        if df_del.num_rows > 0:
            logger.info("Processing DELETE operations")
            del_pk_set = set(df_del[self.pk_col].to_pylist())
            logger.info(f"DELETE PK count: {len(del_pk_set)}")

            mask = pc.invert(pc.is_in(self.df[self.pk_col], value_set=del_pk_set))
            before = self.df.num_rows
            self.df = self.df.filter(mask)
            logger.info(f"DELETE removed {before - self.df.num_rows} rows")

        # -----------------------------
        # UPDATE
        # -----------------------------
        if df_upd.num_rows > 0:
            logger.info("Processing UPDATE operations")
            upd_pk_set = set(df_upd[self.pk_col].to_pylist())
            logger.info(f"UPDATE PK count: {len(upd_pk_set)}")

            mask = pc.invert(pc.is_in(self.df[self.pk_col], value_set=upd_pk_set))
            before = self.df.num_rows
            self.df = self.df.filter(mask)
            logger.info(f"UPDATE removed old rows: {before - self.df.num_rows}")

            self.df = pa.concat_tables([self.df, df_upd])
            logger.info(f"UPDATE added new rows: {df_upd.num_rows}")

        # -----------------------------
        # INSERT
        # -----------------------------
        if df_ins.num_rows > 0:
            logger.info("Processing INSERT operations")
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"INSERT added rows: {df_ins.num_rows}")

        # Move CDC files
        logger.info("Archiving CDC files to /processed/")
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Move LOAD file
        logger.info("Archiving previous LOAD file")
        load_p = self.get_processed_path(load_file, add_timestamp=True)
        self.move_file(load_file, load_p)

        # Write updated LOAD
        logger.info("Writing updated LOAD file")
        out = self.write_arrow_table(load_file, self.df)

        # Log final summary
        final_rows = self.df.num_rows
        logger.info({
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": final_rows,
            "row_change": final_rows - initial_rows,
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
    logger.info("Lambda triggered with event:")
    logger.info(json.dumps(event))

    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            logger.info(f"Processing S3 event object: bucket={bucket}, key={key}")

            if "/processed/" in key or not key.endswith(".csv") or "LOAD" in key:
                logger.info(f"Skipping file (not CDC eligible): {key}")
                continue

            table_name = key.split("/")[-2]
            processor = CDCProcessorArrow(bucket, table_name)
            processor.process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(f"Error in CDC processing: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

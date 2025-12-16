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

        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0

        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Path Helpers
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                fname = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    fname = fname.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [fname])
        raise ValueError("Invalid CDC directory")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved {src} → {dst}")

    # -------------------------------------------------
    # S3 + Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(f"Loaded {key} → rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Updated LOAD written: s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # CDC File Discovery
    # -------------------------------------------------
    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        logger.info(f"CDC files discovered: {files}")
        return sorted(files)

    # -------------------------------------------------
    # Schema Alignment
    # -------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col):
        cols = []
        for f in target_schema:
            if f.name in tbl.column_names:
                col = tbl[f.name]
                if col.type != f.type:
                    col = pc.cast(col, f.type, safe=False)
            else:
                col = pa.array([None] * tbl.num_rows, type=f.type)
            cols.append(col)

        return pa.Table.from_arrays(
            [tbl[op_col]] + cols,
            names=[op_col] + target_schema.names
        )

    # -------------------------------------------------
    # DELETE matching rows across ALL columns
    # -------------------------------------------------
    def delete_matching_rows(self, base, rows):
        mask = pa.scalar(True)
        for col in base.column_names:
            mask = pc.and_(
                mask,
                pc.is_in(base[col], value_set=rows[col])
            )

        to_keep = pc.invert(mask)
        deleted = pc.sum(pc.cast(mask, pa.int64())).as_py()
        return base.filter(to_keep)

    # -------------------------------------------------
    # DELETE matching rows across partial columns
    # -------------------------------------------------
    def delete_partial_matching_rows(self, base, row):
        mask = pa.scalar(True)
        for col in base.column_names:
            val = row[col][0].as_py()
            if val is not None:
                mask = pc.and_(
                    mask,
                    pc.equal(base[col], pa.scalar(val))
                )
        to_keep = pc.invert(mask)
        deleted = pc.sum(pc.cast(mask, pa.int64())).as_py()
    
        return base.filter(to_keep)

    # -------------------------------------------------
    # Main CDC Application (Order Preserving)
    # -------------------------------------------------
    def apply_cdc(self, base, cdc, op_col):
        for i in range(cdc.num_rows):
            op = str(cdc[op_col][i].as_py()).upper()
            row = cdc.slice(i, 1).remove_column(0)

            if op == "D":
                base = self.delete_matching_rows(base, row)
                self.delete_count += 1

            elif op == "U":
                base, deleted = self.delete_partial_matching_rows(base, row)
                base = pa.concat_tables([base, row])
                self.update_count += 1

            elif op == "I":
                base = pa.concat_tables([base, row])
                self.insert_count += 1

        return base

    # -------------------------------------------------
    # Main Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow_table(load_key)
        schema = self.df.schema

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found")
            return

        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col = first_cdc.column_names[0]

        for f in cdc_files:
            logger.info(f"Processing CDC file: {f}")
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, schema, op_col)

            self.df = self.apply_cdc(self.df, aligned, op_col)
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        out = self.write_arrow_table(load_key, self.df)

        logger.info(json.dumps({
            "status": "success",
            "final_rows": self.df.num_rows,
            "inserts": self.insert_count,
            "updates": self.update_count,
            "deletes": self.delete_count,
            "time_sec": (datetime.utcnow() - start).total_seconds(),
            "output": out
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

            if "/processed/" in key or not key.endswith(".csv") or "LOAD" in key:
                continue

            table = key.split("/")[-2]
            CDCProcessorArrow(bucket, table).process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error("CDC Lambda failed", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

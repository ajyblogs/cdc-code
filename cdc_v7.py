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
    def _init_(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None
        self.pk_col = None
        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helper functions
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i,p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [filename])
        raise ValueError("Invalid CDC structure")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, Key=dst,
                       CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file → {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    out.append(key)
        return sorted(out)

    # -------------------------------------------------
    # Load / Write functions
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded table: {key} → rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        # Efficient writer
        csv_buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_buf)
        logger.info(f"Written updated LOAD → {key}")

    # -------------------------------------------------
    # Align schema (fix type mismatches, missing cols)
    # -------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col_name):
        cols = {}
        for field in target_schema:
            name = field.name
            if name in tbl.column_names:
                col = tbl[name]
                if col.type != field.type:
                    try:
                        col = pc.cast(col, field.type)
                    except:
                        col = pc.cast(col, pa.string())
                cols[name] = col
            else:
                cols[name] = pa.array([None] * tbl.num_rows, type=field.type)

        return pa.Table.from_arrays(
            [tbl[op_col_name]] + list(cols.values()),
            names=[op_col_name] + [f.name for f in target_schema]
        )

    # -------------------------------------------------
    # SAFE uppercase
    # -------------------------------------------------
    def safe_upper(self, arr):
        try:
            arr = arr.combine_chunks()
            if pa.types.is_string(arr.type):
                return pc.utf8_upper(arr)
            return pc.utf8_upper(pc.cast(arr, pa.string()))
        except:
            return arr

    # -------------------------------------------------
    # MAIN PROCESSOR
    # -------------------------------------------------
    def process(self, cdc_key):
        prefix = self.get_load_prefix(cdc_key)
        load_file = f"{prefix}LOAD00000001.csv"

        # Load base load
        self.df = self.load_arrow_table(load_file)
        self.pk_col = self.df.column_names[0]

        # Find CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found.")
            return

        # Fetch first file for OP column name
        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col_name = first_cdc.column_names[0]

        # Align all CDC files
        aligned_tables = []
        for f in cdc_files:
            raw_tbl = self.load_arrow_table(f)
            aligned_tbl = self.align_schema(raw_tbl, self.df.schema, op_col_name)
            aligned_tables.append(aligned_tbl)

        # Combine all CDC
        cdc_all = pa.concat_tables(aligned_tables)
        op_arr = self.safe_upper(cdc_all[op_col_name])

        # Split CDC
        df_ins = cdc_all.filter(pc.equal(op_arr, pa.scalar("I"))).remove_column(0)
        df_upd = cdc_all.filter(pc.equal(op_arr, pa.scalar("U"))).remove_column(0)
        df_del = cdc_all.filter(pc.equal(op_arr, pa.scalar("D"))).remove_column(0)

        # DELETE
        if df_del.num_rows > 0:
            del_keys = df_del[self.pk_col].combine_chunks()
            del_mask = pc.is_in(self.df[self.pk_col], value_set=del_keys)
            self.df = self.df.filter(pc.invert(del_mask))

        # UPDATE
        if df_upd.num_rows > 0:
            upd_keys = df_upd[self.pk_col].combine_chunks()
            upd_mask = pc.is_in(self.df[self.pk_col], value_set=upd_keys)
            self.df = self.df.filter(pc.invert(upd_mask))
            self.df = pa.concat_tables([self.df, df_upd])

        # INSERT
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])

        # Move all CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # -------------------------------------------------
        # SAFE LOAD ROTATION (TEMP → ARCHIVE → PROMOTE)
        # -------------------------------------------------
        tmp_load = load_file + ".tmp"
        self.write_arrow_table(tmp_load, self.df)

        # Archive only if it exists
        try:
            s3.head_object(Bucket=self.bucket, Key=load_file)
            archive_key = self.get_processed_path(load_file, add_timestamp=True)
            self.move_file(load_file, archive_key)
        except:
            logger.warning("No existing LOAD to archive.")

        # Promote temp → final LOAD
        self.move_file(tmp_load, load_file)


# -------------------------------------------------
# Lambda handler
# -------------------------------------------------
def lambda_handler(event, context):
    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "/processed/" in key or "LOAD" in key or not key.endswith(".csv"):
                continue

            table_name = key.split("/")[-2]
            CDCProcessorArrow(bucket, table_name).process(key)

        return {"statusCode": 200, "body": "success"}

    except Exception as e:
        logger.error(str(e), exc_info=True)
        return {"statusCode": 500, "body": str(e)}

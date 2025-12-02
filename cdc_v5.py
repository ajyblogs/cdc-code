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

        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helper functions
    # -------------------------------------------------
    def get_load_prefix(self, key):
        prefix = "/".join(key.split("/")[:-1]) + "/"
        return prefix

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [filename])
        raise ValueError("Invalid CDC directory structure")

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

        logger.info(f"CDC files discovered: {out}")
        return sorted(out)

    # -------------------------------------------------
    # Load / Write Arrow tables
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(f"Loaded table: {key} → rows={tbl.num_rows}, cols={tbl.num_columns}")
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Updated LOAD written to s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # FIX: Schema Alignment
    # -------------------------------------------------
    def align_schema(self, tbl, target_schema):
        """
        Ensures CDC table has same schema as LOAD.
        - Missing columns → added as null column
        - Type mismatches → cast to LOAD type (fallback: cast to string)
        """
        cols = {}

        for field in target_schema:
            name = field.name

            if name in tbl.column_names:
                col = tbl[name]

                if col.type != field.type:
                    logger.info(
                        f"Type mismatch in column '{name}' → {col.type} vs {field.type}, casting..."
                    )

                    try:
                        col = pc.cast(col, field.type)
                    except Exception:
                        # last fallback: cast to string
                        logger.info(
                            f"Failed to cast column '{name}' to {field.type}, forcing STRING type"
                        )
                        col = pc.cast(col, pa.string())

                cols[name] = col
            else:
                # missing column
                logger.info(f"Missing column '{name}' in CDC → filling nulls")
                cols[name] = pa.array([None] * tbl.num_rows, type=field.type)

        return pa.Table.from_arrays(
            list(cols.values()), names=[f.name for f in target_schema]
        )

    # -------------------------------------------------
    # Main CDC Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        logger.info(f"Processing CDC file: {cdc_key}")
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_file = f"{prefix}LOAD00000001.csv"
        logger.info(f"Loading base LOAD file: {load_file}")

        # LOAD base dataset
        self.df = self.load_arrow_table(load_file)
        initial_rows = self.df.num_rows
        self.pk_col = self.df.column_names[0]

        logger.info(f"Primary key column: {self.pk_col}")

        # Discover CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found.")
            return

        target_schema = self.df.schema
        cdc_tables = []

        logger.info("Aligning CDC schemas to LOAD schema")

        # Load and fix schema for each CDC file
        for f in cdc_files:
            raw_tbl = self.load_arrow_table(f)
            aligned = self.align_schema(raw_tbl, target_schema)
            cdc_tables.append(aligned)

        # Merge all CDC rows
        cdc_all = pa.concat_tables(cdc_tables)
        logger.info(f"Combined CDC rows after alignment: {cdc_all.num_rows}")

        self.op_col = cdc_all.column_names[0]

        # Split operations
        op = pc.utf8_upper(cdc_all[self.op_col])
        df_ins = cdc_all.filter(pc.equal(op, pa.scalar("I")))
        df_upd = cdc_all.filter(pc.equal(op, pa.scalar("U")))
        df_del = cdc_all.filter(pc.equal(op, pa.scalar("D")))

        logger.info(f"CDC Ops → INSERT={df_ins.num_rows}, UPDATE={df_upd.num_rows}, DELETE={df_del.num_rows}")

        # Drop op column
        df_ins = df_ins.remove_column(0)
        df_upd = df_upd.remove_column(0)
        df_del = df_del.remove_column(0)

        # -------------------------------------------------
        # DELETE
        # -------------------------------------------------
        if df_del.num_rows > 0:
            del_pk_set = set(df_del[self.pk_col].to_pylist())
            mask = pc.invert(pc.is_in(self.df[self.pk_col], value_set=del_pk_set))

            before = self.df.num_rows
            self.df = self.df.filter(mask)
            logger.info(f"DELETE removed rows: {before - self.df.num_rows}")

        # -------------------------------------------------
        # UPDATE
        # -------------------------------------------------
        if df_upd.num_rows > 0:
            upd_pk_set = set(df_upd[self.pk_col].to_pylist())
            mask = pc.invert(pc.is_in(self.df[self.pk_col], value_set=upd_pk_set))

            before = self.df.num_rows
            self.df = self.df.filter(mask)
            logger.info(f"UPDATE removed old rows: {before - self.df.num_rows}")

            self.df = pa.concat_tables([self.df, df_upd])
            logger.info(f"UPDATE inserted new rows: {df_upd.num_rows}")

        # -------------------------------------------------
        # INSERT
        # -------------------------------------------------
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"INSERT rows added: {df_ins.num_rows}")

        # Archive CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Archive old LOAD
        new_load_archive = self.get_processed_path(load_file, add_timestamp=True)
        self.move_file(load_file, new_load_archive)

        # Write updated LOAD
        out = self.write_arrow_table(load_file, self.df)

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
            "time_sec": (datetime.utcnow() - start).total_seconds()
        })


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info("Lambda triggered:")
    logger.info(json.dumps(event))

    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "/processed/" in key or not key.endswith(".csv") or "LOAD" in key:
                logger.info(f"Skipping file: {key}")
                continue

            table_name = key.split("/")[-2]
            proc = CDCProcessorArrow(bucket, table_name)
            proc.process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(f"Error in CDC Lambda: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({
            "status": "error",
            "message": str(e)
        })}

import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging

# --------------------------------------------------
# Logging
# --------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------
# AWS
# --------------------------------------------------
s3 = boto3.client("s3")


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None

        logger.info(
            f"CDC Processor initialized for bucket={bucket}, table={table}"
        )

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
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

                return "/".join(
                    parts[: i + 1]
                    + ["processed"]
                    + parts[i + 1 : -1]
                    + [filename]
                )

        raise ValueError("Invalid directory structure")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src},
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file: {src} -> {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        out = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if (
                    key.endswith(".csv")
                    and "LOAD" not in key
                    and "/processed/" not in key
                ):
                    out.append(key)

        logger.info(f"CDC files discovered: {out}")
        return sorted(out)

    # --------------------------------------------------
    # Arrow IO
    # --------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|"),
        )
        logger.info(
            f"Loaded {key} -> rows={tbl.num_rows}, cols={tbl.num_columns}"
        )
        return tbl

    def write_arrow_table(self, key, table):
        buf = (
            table.to_pandas()
            .to_csv(index=False, sep="|")
            .encode()
        )

        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Wrote updated LOAD -> s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # --------------------------------------------------
    # Schema alignment
    # --------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col):
        cols = {}

        for f in target_schema:
            if f.name in tbl.column_names:
                col = tbl[f.name]
                if col.type != f.type:
                    try:
                        col = pc.cast(col, f.type)
                    except Exception:
                        col = pa.array(
                            [None] * tbl.num_rows, type=f.type
                        )
                cols[f.name] = col
            else:
                cols[f.name] = pa.array(
                    [None] * tbl.num_rows, type=f.type
                )

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + target_schema.names,
        )

    # --------------------------------------------------
    # Pure Arrow Sequential CDC (NO PK)
    # --------------------------------------------------
    def apply_cdc_no_pk(self, base_tbl, cdc_tbl, op_col):
        schema = base_tbl.schema
        cols = schema.names

        base_rows = list(
            zip(*[base_tbl[c].to_pylist() for c in cols])
        )

        cdc_data = {
            name: cdc_tbl[name].to_pylist()
            for name in cdc_tbl.column_names
        }

        for i in range(cdc_tbl.num_rows):
            op = str(cdc_data[op_col][i]).upper()
            row = tuple(cdc_data[c][i] for c in cols)

            if op == "I":
                base_rows.append(row)

            elif op == "D":
                before = len(base_rows)
                base_rows = [r for r in base_rows if r != row]
                logger.info(
                    f"DELETE removed {before - len(base_rows)} rows"
                )

            elif op == "U":
                before = len(base_rows)
                base_rows = [r for r in base_rows if r != row]
                base_rows.append(row)
                logger.info(
                    f"UPDATE replaced {before - len(base_rows)} rows"
                )

        arrays = {
            c: pa.array(
                [r[i] for r in base_rows],
                type=schema.field(c).type,
            )
            for i, c in enumerate(cols)
        }

        return pa.Table.from_pydict(arrays, schema=schema)

    # --------------------------------------------------
    # Main Processor
    # --------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        logger.info(f"Processing CDC trigger file: {cdc_key}")
        logger.info(f"Base LOAD file: {load_key}")

        self.df = self.load_arrow_table(load_key)
        initial_rows = self.df.num_rows

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found")
            return

        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col = first_cdc.column_names[0]
        schema = self.df.schema

        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, schema, op_col)

            logger.info(f"Applying CDC file sequentially: {f}")
            self.df = self.apply_cdc_no_pk(
                self.df, aligned, op_col
            )

            self.move_file(f, self.get_processed_path(f))

        self.move_file(
            load_key,
            self.get_processed_path(load_key, True),
        )

        out = self.write_arrow_table(load_key, self.df)

        logger.info(
            json.dumps(
                {
                    "status": "success",
                    "initial_rows": initial_rows,
                    "final_rows": self.df.num_rows,
                    "row_change": self.df.num_rows - initial_rows,
                    "output": out,
                    "time_sec": (
                        datetime.utcnow() - start
                    ).total_seconds(),
                }
            )
        )


# --------------------------------------------------
# Lambda Handler
# --------------------------------------------------
def lambda_handler(event, context):
    logger.info("Lambda triggered")
    logger.info(json.dumps(event))

    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if (
                "/processed/" in key
                or "LOAD" in key
                or not key.endswith(".csv")
            ):
                logger.info(f"Skipping file: {key}")
                continue

            table = key.split("/")[-2]
            CDCProcessorArrow(bucket, table).process(key)

        return {
            "statusCode": 200,
            "body": json.dumps({"status": "success"}),
        }

    except Exception as e:
        logger.error("CDC Lambda failed", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }

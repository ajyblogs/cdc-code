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
        self.pk_col = None

        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helper functions
    # -------------------------------------------------
    def get_load_prefix(self, key):
        prefix = "/".join(key.split("/")[:-1]) + "/"
        logger.info(f"Derived LOAD prefix: {prefix}")
        return prefix

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                dst = "/".join(parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [filename])
                logger.info(f"Processed path resolved: {dst}")
                return dst
        raise ValueError("Invalid CDC directory structure")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
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

        out = sorted(out)
        logger.info(f"CDC files discovered: {out}")
        return out

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(
            f"Loaded table: {key} → rows={tbl.num_rows}, cols={tbl.num_columns}"
        )
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Updated LOAD written to s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema Handling
    # -------------------------------------------------
    def infer_unified_schema(self, load_table, cdc_tables):
        logger.info("Inferring unified schema from LOAD and CDC samples")
        fields = []

        for f in load_table.schema:
            if pa.types.is_null(f.type):
                logger.info(f"Column '{f.name}' is NULL in LOAD, inferring from CDC")
                inferred = None
                for t in cdc_tables:
                    if f.name in t.column_names:
                        ttype = t.schema.field(f.name).type
                        if not pa.types.is_null(ttype):
                            inferred = ttype
                            logger.info(
                                f"Inferred type for '{f.name}': {inferred}"
                            )
                            break
                fields.append(pa.field(f.name, inferred or pa.string()))
            else:
                fields.append(f)

        schema = pa.schema(fields)
        logger.info(f"Unified schema resolved: {schema}")
        return schema

    def upgrade_load_schema(self, load_table, unified_schema):
        logger.info("Upgrading LOAD table to unified schema")
        cols = []

        for f in unified_schema:
            col = load_table[f.name]
            if pa.types.is_null(col.type):
                logger.info(
                    f"Upgrading LOAD column '{f.name}' from NULL to {f.type}"
                )
                cols.append(pa.array([None] * load_table.num_rows, type=f.type))
            else:
                cols.append(col)

        return pa.Table.from_arrays(cols, schema=unified_schema)

    def align_schema(self, tbl, target_schema, op_col):
        logger.info("Aligning CDC schema with LOAD schema")
        cols = {}

        for f in target_schema:
            if f.name in tbl.column_names:
                col = tbl[f.name]
                if col.type != f.type:
                    logger.info(
                        f"Type mismatch for column '{f.name}', casting "
                        f"{col.type} → {f.type}"
                    )
                    try:
                        col = pc.cast(col, f.type)
                    except Exception as e:
                        logger.warning(
                            f"Cast failed for '{f.name}', filling NULLs: {e}"
                        )
                        col = pa.array([None] * tbl.num_rows, type=f.type)
                cols[f.name] = col
            else:
                logger.info(
                    f"Missing column '{f.name}' in CDC, filling NULLs"
                )
                cols[f.name] = pa.array([None] * tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + target_schema.names
        )

    # -------------------------------------------------
    # Pure Arrow Sequential CDC
    # -------------------------------------------------
    def apply_cdc_sequential_arrow(self, base_tbl, cdc_tbl, op_col):
        logger.info(
            f"Applying sequential CDC → rows={cdc_tbl.num_rows}"
        )

        pk = self.pk_col
        schema = base_tbl.schema

        arrays = {
            name: base_tbl[name].to_pylist()
            for name in schema.names
        }

        row_map = {
            arrays[pk][i]: i
            for i in range(len(arrays[pk]))
            if arrays[pk][i] is not None
        }

        tombstones = set()
        row_count = len(arrays[pk])

        cdc_cols = {
            name: cdc_tbl[name].to_pylist()
            for name in cdc_tbl.column_names
        }

        ins = upd = dele = 0

        for i in range(cdc_tbl.num_rows):
            op = str(cdc_cols[op_col][i]).upper()
            key = cdc_cols[pk][i]

            if key is None:
                continue

            if op == "D":
                if key in row_map:
                    tombstones.add(row_map[key])
                    del row_map[key]
                    dele += 1

            elif op in ("I", "U"):
                if key in row_map:
                    idx = row_map[key]
                    upd += 1
                else:
                    idx = row_count
                    row_map[key] = idx
                    row_count += 1
                    for c in schema.names:
                        arrays[c].append(None)
                    ins += 1

                for c in schema.names:
                    arrays[c][idx] = cdc_cols[c][i]

        logger.info(
            f"CDC batch applied → INSERT={ins}, UPDATE={upd}, DELETE={dele}"
        )

        keep_idx = [i for i in range(row_count) if i not in tombstones]

        final = {
            c: pa.array(
                [arrays[c][i] for i in keep_idx],
                type=schema.field(c).type
            )
            for c in schema.names
        }

        logger.info(
            f"Post-CDC row count: {len(keep_idx)}"
        )

        return pa.Table.from_pydict(final, schema=schema)

    # -------------------------------------------------
    # Main Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        logger.info(f"Processing CDC file trigger: {cdc_key}")
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        logger.info(f"Loading base LOAD file: {load_key}")
        self.df = self.load_arrow_table(load_key)
        initial_rows = self.df.num_rows

        self.pk_col = self.df.column_names[0]
        logger.info(f"Primary key column detected: {self.pk_col}")

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found. Exiting.")
            return

        logger.info(f"Processing {len(cdc_files)} CDC files")

        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col = first_cdc.column_names[0]
        logger.info(f"Operation column detected: {op_col}")

        schema = self.infer_unified_schema(self.df, [first_cdc])
        self.df = self.upgrade_load_schema(self.df, schema)

        for f in cdc_files:
            logger.info(f"Applying CDC file: {f}")
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, schema, op_col)
            self.df = self.apply_cdc_sequential_arrow(
                self.df, aligned, op_col
            )
            self.move_file(f, self.get_processed_path(f))

        archive_load = self.get_processed_path(load_key, add_timestamp=True)
        self.move_file(load_key, archive_load)

        out = self.write_arrow_table(load_key, self.df)

        logger.info(json.dumps({
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": self.df.num_rows,
            "row_change": self.df.num_rows - initial_rows,
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

            if "/processed/" in key or "LOAD" in key or not key.endswith(".csv"):
                logger.info(f"Skipping file: {key}")
                continue

            table = key.split("/")[-2]
            CDCProcessorArrow(bucket, table).process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error("Error in CDC Lambda", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }

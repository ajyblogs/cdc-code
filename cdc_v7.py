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
        logger.info(f"CDC files discovered ({len(out)}): {out}")
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
        logger.info(f"Loaded table: {key} → rows={tbl.num_rows}, cols={tbl.num_columns}")
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
        logger.info("Inferring unified schema")
        fields = []
        for field in load_table.schema:
            if pa.types.is_null(field.type):
                inferred = None
                for t in cdc_tables:
                    if field.name in t.column_names:
                        ttype = t.schema.field(field.name).type
                        if not pa.types.is_null(ttype):
                            inferred = ttype
                            break
                fields.append(pa.field(field.name, inferred or pa.string()))
                logger.info(f"Inferred column '{field.name}' → {inferred or 'string'}")
            else:
                fields.append(field)
        schema = pa.schema(fields)
        logger.info(f"Unified schema: {schema}")
        return schema

    def upgrade_load_schema(self, load_table, unified_schema):
        logger.info("Upgrading LOAD schema")
        cols = []
        for f in unified_schema:
            col = load_table[f.name]
            if pa.types.is_null(col.type):
                cols.append(pa.array([None] * load_table.num_rows, type=f.type))
            else:
                cols.append(col)
        return pa.Table.from_arrays(cols, schema=unified_schema)

    def align_schema(self, tbl, target_schema, op_col):
        logger.info("Aligning CDC schema to LOAD schema")
        cols = {}
        for f in target_schema:
            if f.name in tbl.column_names:
                col = tbl[f.name]
                if col.type != f.type:
                    logger.info(f"Casting column '{f.name}' from {col.type} → {f.type}")
                    try:
                        col = pc.cast(col, f.type)
                    except Exception as e:
                        logger.warning(f"Cast failed for '{f.name}': {e}")
                        col = pa.array([None] * tbl.num_rows, type=f.type)
                cols[f.name] = col
            else:
                logger.info(f"Missing column '{f.name}' in CDC → filling nulls")
                cols[f.name] = pa.array([None] * tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + target_schema.names
        )

    # -------------------------------------------------
    # PURE PYARROW SEQUENTIAL CDC
    # -------------------------------------------------
    def apply_cdc_sequential_arrow(self, base_tbl, cdc_tbl, op_col):
        logger.info("Applying CDC sequentially (pure PyArrow)")
        pk = self.pk_col
        schema = base_tbl.schema

        arrays = {c: base_tbl[c].to_pylist() for c in schema.names}
        row_map = {arrays[pk][i]: i for i in range(len(arrays[pk])) if arrays[pk][i] is not None}
        tombstones = set()
        row_count = len(arrays[pk])

        cdc_cols = {c: cdc_tbl[c].to_pylist() for c in cdc_tbl.column_names}

        for i in range(cdc_tbl.num_rows):
            op = str(cdc_cols[op_col][i]).upper()
            key = cdc_cols[pk][i]

            logger.debug(f"CDC row {i}: op={op}, pk={key}")

            if key is None:
                continue

            if op == "D":
                if key in row_map:
                    tombstones.add(row_map[key])
                    del row_map[key]

            elif op in ("I", "U"):
                if key in row_map:
                    idx = row_map[key]
                else:
                    idx = row_count
                    row_map[key] = idx
                    row_count += 1
                    for c in schema.names:
                        arrays[c].append(None)

                for c in schema.names:
                    arrays[c][idx] = cdc_cols[c][i]

        keep_idx = [i for i in range(row_count) if i not in tombstones]

        final = {
            c: pa.array([arrays[c][i] for i in keep_idx], type=schema.field(c).type)
            for c in schema.names
        }

        logger.info(f"CDC applied. Final row count: {len(keep_idx)}")
        return pa.Table.from_pydict(final, schema=schema)

    # -------------------------------------------------
    # Main Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        logger.info(f"Processing CDC trigger file: {cdc_key}")

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow_table(load_key)
        initial_rows = self.df.num_rows
        self.pk_col = self.df.column_names[0]
        logger.info(f"Primary key column detected: {self.pk_col}")

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found. Exiting.")
            return

        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col = first_cdc.column_names[0]
        logger.info(f"Operation column detected: {op_col}")

        schema = self.infer_unified_schema(self.df, [first_cdc])
        self.df = self.upgrade_load_schema(self.df, schema)

        # ---- LOAD ALL CDC FILES IN ORDER ----
        aligned_tables = []
        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, schema, op_col)
            aligned_tables.append(aligned)

        combined_cdc = pa.concat_tables(aligned_tables)
        logger.info(f"Total CDC rows combined: {combined_cdc.num_rows}")

        # ---- APPLY CDC ONCE ----
        self.df = self.apply_cdc_sequential_arrow(self.df, combined_cdc, op_col)

        # ---- ARCHIVE FILES ----
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, add_timestamp=True))
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
        logger.error("CDC Lambda failed", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

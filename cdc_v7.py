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
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [filename])
        raise ValueError("Invalid CDC directory structure")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})
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
    # Arrow I/O
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        return csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema helpers
    # -------------------------------------------------
    def infer_unified_schema(self, load_table, cdc_tables):
        fields = []
        for field in load_table.schema:
            if pa.types.is_null(field.type):
                inferred = None
                for t in cdc_tables:
                    if field.name in t.column_names:
                        t_type = t.schema.field(field.name).type
                        if not pa.types.is_null(t_type):
                            inferred = t_type
                            break
                fields.append(pa.field(field.name, inferred or pa.string()))
            else:
                fields.append(field)
        return pa.schema(fields)

    def upgrade_load_schema(self, load_table, schema):
        cols = []
        for f in schema:
            if pa.types.is_null(load_table[f.name].type):
                cols.append(pa.array([None] * load_table.num_rows, type=f.type))
            else:
                cols.append(load_table[f.name])
        return pa.Table.from_arrays(cols, schema=schema)

    def align_schema(self, tbl, schema, op_col):
        cols = {}
        for f in schema:
            if f.name in tbl.column_names:
                col = tbl[f.name]
                if col.type != f.type:
                    try:
                        col = pc.cast(col, f.type)
                    except Exception:
                        col = pa.array([None] * tbl.num_rows, type=f.type)
                cols[f.name] = col
            else:
                cols[f.name] = pa.array([None] * tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + [f.name for f in schema]
        )

    # -------------------------------------------------
    # CDC COLLAPSE (🔥 KEY FIX 🔥)
    # -------------------------------------------------
    def collapse_cdc_by_pk(self, tbl, op_col, pk_col):
        logger.info("Collapsing CDC records per primary key")

        row_id = pa.array(range(tbl.num_rows), type=pa.int64())
        tbl = tbl.append_column("__row_id__", row_id)

        sorted_tbl = tbl.sort_by([
            (pk_col, "ascending"),
            ("__row_id__", "descending")
        ])

        dedup = sorted_tbl.group_by(pk_col).aggregate([
            ("__row_id__", "min")
        ])

        final = dedup.join(
            sorted_tbl,
            keys=pk_col,
            right_keys=pk_col,
            join_type="inner"
        ).drop(["__row_id__"])

        logger.info(f"CDC collapsed {tbl.num_rows} → {final.num_rows}")
        return final

    def safe_upper(self, arr):
        return pc.utf8_upper(pc.cast(arr, pa.string()))

    # -------------------------------------------------
    # MAIN PROCESS
    # -------------------------------------------------
    def process(self, cdc_key):
        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow_table(load_key)
        self.pk_col = self.df.column_names[0]
        initial_rows = self.df.num_rows

        cdc_files = self.list_cdc_files(prefix)
        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col = first_cdc.column_names[0]

        schema = self.infer_unified_schema(self.df, [first_cdc])
        self.df = self.upgrade_load_schema(self.df, schema)

        inserts, updates, deletes = [], [], []

        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, schema, op_col)

            # 🔥 collapse CDC here
            aligned = self.collapse_cdc_by_pk(aligned, op_col, self.pk_col)

            op = self.safe_upper(aligned[op_col])

            inserts.append(aligned.filter(pc.equal(op, "I")).remove_column(0))
            updates.append(aligned.filter(pc.equal(op, "U")).remove_column(0))
            deletes.append(aligned.filter(pc.equal(op, "D")).remove_column(0))

        df_ins = pa.concat_tables(inserts) if inserts else pa.table({}, schema=schema)
        df_upd = pa.concat_tables(updates) if updates else pa.table({}, schema=schema)
        df_del = pa.concat_tables(deletes) if deletes else pa.table({}, schema=schema)

        # ---------------- DELETE ----------------
        if df_del.num_rows:
            del_keys = set(df_del[self.pk_col].to_pylist())
            keep = [i for i, k in enumerate(self.df[self.pk_col].to_pylist()) if k not in del_keys]
            self.df = self.df.take(keep) if keep else pa.table({}, schema=self.df.schema)

        # ---------------- UPDATE ----------------
        if df_upd.num_rows:
            upd_keys = set(df_upd[self.pk_col].to_pylist())
            keep = [i for i, k in enumerate(self.df[self.pk_col].to_pylist()) if k not in upd_keys]
            self.df = self.df.take(keep) if keep else pa.table({}, schema=self.df.schema)
            self.df = pa.concat_tables([self.df, df_upd])

        # ---------------- INSERT ----------------
        if df_ins.num_rows:
            self.df = pa.concat_tables([self.df, df_ins])

        # Archive CDC
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Archive LOAD
        self.move_file(load_key, self.get_processed_path(load_key, True))

        # Write new LOAD
        out = self.write_arrow_table(load_key, self.df)

        logger.info(json.dumps({
            "initial_rows": initial_rows,
            "final_rows": self.df.num_rows,
            "insert": df_ins.num_rows,
            "update": df_upd.num_rows,
            "delete": df_del.num_rows,
            "output": out
        }))


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "LOAD" in key or "/processed/" in key or not key.endswith(".csv"):
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200}

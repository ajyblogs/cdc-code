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
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src},
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file → {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    files.append(key)
        return sorted(files)

    # -------------------------------------------------
    # Arrow I/O
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded {key} rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Wrote updated LOAD → {key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema alignment (CDC → LOAD)
    # -------------------------------------------------
    def align_schema(self, cdc_tbl, load_schema, op_col):
        cols = {}

        for field in load_schema:
            name = field.name
            if name in cdc_tbl.column_names:
                col = cdc_tbl[name]
                if col.type != field.type:
                    try:
                        col = pc.cast(col, field.type)
                    except Exception:
                        col = pa.array([None] * cdc_tbl.num_rows, type=field.type)
                cols[name] = col
            else:
                cols[name] = pa.array([None] * cdc_tbl.num_rows, type=field.type)

        return pa.Table.from_arrays(
            [cdc_tbl[op_col]] + list(cols.values()),
            names=[op_col] + [f.name for f in load_schema],
        )

    # -------------------------------------------------
    # CDC COLLAPSE (🔥 core fix)
    # -------------------------------------------------
    def collapse_cdc_by_pk(self, tbl, pk_col):
        logger.info("Collapsing CDC records by PK")

        row_id = pa.array(range(tbl.num_rows), type=pa.int64())
        tbl = tbl.append_column("__row_id__", row_id)

        sorted_tbl = tbl.sort_by(
            [(pk_col, "ascending"), ("__row_id__", "descending")]
        )

        dedup = sorted_tbl.group_by(pk_col).aggregate(
            [("__row_id__", "min")]
        )

        final = (
            dedup.join(
                sorted_tbl,
                keys=pk_col,
                right_keys=pk_col,
                join_type="inner",
            )
            .drop(["__row_id__"])
        )

        logger.info(f"CDC collapsed {tbl.num_rows} → {final.num_rows}")
        return final

    def safe_upper(self, arr):
        return pc.utf8_upper(pc.cast(arr, pa.string()))

    # -------------------------------------------------
    # Main process
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.load_arrow_table(load_key)
        self.pk_col = self.df.column_names[0]
        load_schema = self.df.schema
        initial_rows = self.df.num_rows

        # CDC files
        cdc_files = self.list_cdc_files(prefix)
        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col = first_cdc.column_names[0]

        inserts, updates, deletes = [], [], []

        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, load_schema, op_col)

            # 🔥 collapse per PK BEFORE touching LOAD
            aligned = self.collapse_cdc_by_pk(aligned, self.pk_col)

            op = self.safe_upper(aligned[op_col])

            inserts.append(aligned.filter(pc.equal(op, "I")).remove_column(0))
            updates.append(aligned.filter(pc.equal(op, "U")).remove_column(0))
            deletes.append(aligned.filter(pc.equal(op, "D")).remove_column(0))

        df_ins = pa.concat_tables(inserts) if inserts else pa.table({}, schema=load_schema)
        df_upd = pa.concat_tables(updates) if updates else pa.table({}, schema=load_schema)
        df_del = pa.concat_tables(deletes) if deletes else pa.table({}, schema=load_schema)

        logger.info(
            f"CDC summary INSERT={df_ins.num_rows}, UPDATE={df_upd.num_rows}, DELETE={df_del.num_rows}"
        )

        # ---------------- DELETE ----------------
        if df_del.num_rows:
            del_keys = set(df_del[self.pk_col].to_pylist())
            keep_idx = [
                i
                for i, k in enumerate(self.df[self.pk_col].to_pylist())
                if k not in del_keys
            ]
            self.df = self.df.take(keep_idx) if keep_idx else pa.table({}, schema=load_schema)

        # ---------------- UPDATE ----------------
        if df_upd.num_rows:
            upd_keys = set(df_upd[self.pk_col].to_pylist())
            keep_idx = [
                i
                for i, k in enumerate(self.df[self.pk_col].to_pylist())
                if k not in upd_keys
            ]
            self.df = self.df.take(keep_idx) if keep_idx else pa.table({}, schema=load_schema)
            self.df = pa.concat_tables([self.df, df_upd])

        # ---------------- INSERT ----------------
        if df_ins.num_rows:
            self.df = pa.concat_tables([self.df, df_ins])

        # Archive CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Archive old LOAD
        self.move_file(load_key, self.get_processed_path(load_key, True))

        # Write new LOAD
        out = self.write_arrow_table(load_key, self.df)

        logger.info(
            json.dumps(
                {
                    "initial_rows": initial_rows,
                    "final_rows": self.df.num_rows,
                    "insert": df_ins.num_rows,
                    "update": df_upd.num_rows,
                    "delete": df_del.num_rows,
                    "output": out,
                    "time_sec": (datetime.utcnow() - start).total_seconds(),
                }
            )
        )


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info(json.dumps(event))

    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "LOAD" in key or "/processed/" in key or not key.endswith(".csv"):
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

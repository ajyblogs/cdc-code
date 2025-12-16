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
        self.df = None
        self.pk_col = None

        logger.info(f"[INIT] bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helpers
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
                return "/".join(parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [fname])
        raise ValueError("Invalid CDC path")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, Key=dst,
                       CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"[ARCHIVE] {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        return sorted(files)

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"],
                           parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"[IO] Loaded {key} rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"[IO] Written LOAD rows={table.num_rows}")

    # -------------------------------------------------
    # Schema alignment
    # -------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col):
        cols = {}
        for field in target_schema:
            if field.name in tbl.column_names:
                col = tbl[field.name]
                if col.type != field.type:
                    try:
                        col = pc.cast(col, field.type)
                    except Exception:
                        col = pa.array([None] * tbl.num_rows, type=field.type)
                cols[field.name] = col
            else:
                cols[field.name] = pa.array([None] * tbl.num_rows, type=field.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + [f.name for f in target_schema]
        )

    def safe_upper(self, arr):
        return pc.utf8_upper(pc.cast(arr, pa.string()))

    # -------------------------------------------------
    # ⭐ GLOBAL CDC COLLAPSE (FIX #1)
    # -------------------------------------------------
    def collapse_all_cdc(self, cdc_tbl, op_col):
        logger.info(f"[CDC-COLLAPSE] Global collapse rows={cdc_tbl.num_rows}")

        rows = cdc_tbl.to_pylist()
        data_cols = [c for c in cdc_tbl.column_names if c != op_col]

        state = {}      # partial-key → row
        deletes = []    # full deletes

        def partial_key(r):
            return tuple((c, r[c]) for c in data_cols if r[c] is not None)

        def full_key(r):
            return tuple(r[c] for c in data_cols)

        for r in rows:
            op = str(r[op_col]).upper()
            pkey = partial_key(r)

            if op in ("I", "U"):
                state[pkey] = r
                logger.info(f"[CDC-COLLAPSE] {op} kept | key={pkey}")

            elif op == "D":
                if pkey in state:
                    del state[pkey]
                    logger.info(f"[CDC-COLLAPSE] DELETE removed staged row | key={pkey}")
                else:
                    deletes.append(r)
                    logger.info(f"[CDC-COLLAPSE] DELETE retained (standalone)")

        final_rows = list(state.values()) + deletes
        logger.info(f"[CDC-COLLAPSE] Final CDC rows={len(final_rows)}")

        return pa.Table.from_pylist(final_rows, schema=cdc_tbl.schema)

    # -------------------------------------------------
    # MAIN PROCESS
    # -------------------------------------------------
    def process(self, cdc_key):
        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.load_arrow_table(load_key)
        self.pk_col = self.df.column_names[0]

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            return

        # Load & align ALL CDC first
        first = self.load_arrow_table(cdc_files[0])
        op_col = first.column_names[0]

        aligned_tables = []
        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, self.df.schema, op_col)
            aligned_tables.append(aligned)

        all_cdc = pa.concat_tables(aligned_tables)

        # ⭐ FIX #1: collapse globally
        collapsed = self.collapse_all_cdc(all_cdc, op_col)
        op_arr = self.safe_upper(collapsed[op_col])

        df_ins = collapsed.filter(pc.equal(op_arr, "I")).remove_column(0)
        df_upd = collapsed.filter(pc.equal(op_arr, "U")).remove_column(0)
        df_del = collapsed.filter(pc.equal(op_arr, "D")).remove_column(0)

        logger.info(
            f"[CDC] After collapse I={df_ins.num_rows}, "
            f"U={df_upd.num_rows}, D={df_del.num_rows}"
        )

        # -------------------------------------------------
        # APPLY DELETE (FIX #2: FULL ROW MATCH)
        # -------------------------------------------------
        if df_del.num_rows > 0:
            del_rows = {
                tuple(r.values()) for r in df_del.to_pylist()
            }

            keep_idx = [
                i for i, r in enumerate(self.df.to_pylist())
                if tuple(r.values()) not in del_rows
            ]

            self.df = self.df.take(keep_idx) if keep_idx else self.df.slice(0, 0)
            logger.info(f"[APPLY] DELETE applied")

        # -------------------------------------------------
        # APPLY UPDATE
        # -------------------------------------------------
        if df_upd.num_rows > 0:
            upd_keys = set(df_upd[self.pk_col].to_pylist())

            keep_idx = [
                i for i, pk in enumerate(self.df[self.pk_col].to_pylist())
                if pk not in upd_keys
            ]

            self.df = self.df.take(keep_idx) if keep_idx else self.df.slice(0, 0)
            self.df = pa.concat_tables([self.df, df_upd])
            logger.info(f"[APPLY] UPDATE applied")

        # -------------------------------------------------
        # APPLY INSERT
        # -------------------------------------------------
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"[APPLY] INSERT applied")

        # -------------------------------------------------
        # ARCHIVE + WRITE
        # -------------------------------------------------
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        self.write_arrow_table(load_key, self.df)

        logger.info(f"[DONE] Final LOAD rows={self.df.num_rows}")


# -------------------------------------------------
# Lambda handler
# -------------------------------------------------
def lambda_handler(event, context):
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "/processed/" in key or "LOAD" in key:
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

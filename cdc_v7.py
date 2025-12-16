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

        logger.info(
            f"[INIT] CDCProcessorArrow | bucket={bucket}, table={table}"
        )

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
                return "/".join(
                    parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [fname]
                )
        raise ValueError("Invalid CDC directory structure")

    def move_file(self, src, dst):
        logger.info(f"[ARCHIVE] {src} → {dst}")
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if (
                    key.endswith(".csv")
                    and "LOAD" not in key
                    and "/processed/" not in key
                ):
                    files.append(key)
        logger.info(f"[DISCOVERY] CDC files={files}")
        return sorted(files)

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        logger.info(f"[IO] Loading {key}")
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(f"[IO] Loaded rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        logger.info(f"[IO] Writing LOAD rows={table.num_rows}")
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)

    # -------------------------------------------------
    # Schema
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
                cols[field.name] = pa.array(
                    [None] * tbl.num_rows, type=field.type
                )

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + [f.name for f in target_schema]
        )

    def safe_upper(self, arr):
        return pc.utf8_upper(pc.cast(arr, pa.string()))

    # -------------------------------------------------
    # ⭐ CDC COLLAPSE
    # -------------------------------------------------
    def collapse_cdc_rows(self, cdc_tbl, op_col):
        logger.info(
            f"[CDC-COLLAPSE] Input rows={cdc_tbl.num_rows}"
        )

        rows = cdc_tbl.to_pylist()
        data_cols = [c for c in cdc_tbl.column_names if c != op_col]

        state = {}
        final_rows = []

        def partial_key(r):
            return tuple((c, r[c]) for c in data_cols if r[c] is not None)

        for idx, r in enumerate(rows):
            op = str(r[op_col]).upper()
            pkey = partial_key(r)

            if op == "I":
                logger.info(
                    f"[CDC-COLLAPSE] INSERT staged | key={pkey}"
                )
                state[pkey] = r

            elif op == "U":
                logger.info(
                    f"[CDC-COLLAPSE] UPDATE overwrite | key={pkey}"
                )
                state[pkey] = r

            elif op == "D":
                if pkey in state:
                    logger.info(
                        f"[CDC-COLLAPSE] DELETE removed staged row | key={pkey}"
                    )
                    del state[pkey]
                else:
                    logger.info(
                        f"[CDC-COLLAPSE] DELETE retained | key={pkey}"
                    )
                    final_rows.append(r)

        final_rows.extend(state.values())

        logger.info(
            f"[CDC-COLLAPSE] Output rows={len(final_rows)}"
        )

        return pa.Table.from_pylist(final_rows, schema=cdc_tbl.schema)

    # -------------------------------------------------
    # MAIN PROCESS
    # -------------------------------------------------
    def process(self, cdc_key):
        logger.info(f"[PROCESS] Start | CDC={cdc_key}")

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.load_arrow_table(load_key)
        self.pk_col = self.df.column_names[0]

        logger.info(
            f"[PROCESS] LOAD rows={self.df.num_rows}, pk={self.pk_col}"
        )

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[PROCESS] No CDC files")
            return

        first = self.load_arrow_table(cdc_files[0])
        op_col = first.column_names[0]

        inserts, updates, deletes = [], [], []

        # ---------------------------
        # CDC PROCESSING
        # ---------------------------
        for f in cdc_files:
            logger.info(f"[PROCESS] CDC file={f}")

            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, self.df.schema, op_col)
            collapsed = self.collapse_cdc_rows(aligned, op_col)

            op_arr = self.safe_upper(collapsed[op_col])

            ins = collapsed.filter(pc.equal(op_arr, "I")).remove_column(0)
            upd = collapsed.filter(pc.equal(op_arr, "U")).remove_column(0)
            dele = collapsed.filter(pc.equal(op_arr, "D")).remove_column(0)

            logger.info(
                f"[PROCESS] Ops after collapse | "
                f"I={ins.num_rows}, U={upd.num_rows}, D={dele.num_rows}"
            )

            if ins.num_rows:
                inserts.append(ins)
            if upd.num_rows:
                updates.append(upd)
            if dele.num_rows:
                deletes.append(dele)

        # ---------------------------
        # APPLY DELETE (full row match)
        # ---------------------------
        if deletes:
            df_del = pa.concat_tables(deletes)
            before = self.df.num_rows

            del_rows = set(
                tuple(r.values()) for r in df_del.to_pylist()
            )

            keep_idx = [
                i for i, r in enumerate(self.df.to_pylist())
                if tuple(r.values()) not in del_rows
            ]

            self.df = self.df.take(keep_idx) if keep_idx else self.df.slice(0, 0)

            logger.info(
                f"[APPLY] DELETE removed={before - self.df.num_rows}"
            )

        # ---------------------------
        # APPLY UPDATE (PK match)
        # ---------------------------
        if updates:
            df_upd = pa.concat_tables(updates)
            before = self.df.num_rows

            upd_keys = set(df_upd[self.pk_col].to_pylist())

            keep_idx = [
                i for i, pk in enumerate(self.df[self.pk_col].to_pylist())
                if pk not in upd_keys
            ]

            self.df = self.df.take(keep_idx) if keep_idx else self.df.slice(0, 0)
            self.df = pa.concat_tables([self.df, df_upd])

            logger.info(
                f"[APPLY] UPDATE replaced={before - len(keep_idx)}, "
                f"added={df_upd.num_rows}"
            )

        # ---------------------------
        # APPLY INSERT
        # ---------------------------
        if inserts:
            df_ins = pa.concat_tables(inserts)
            self.df = pa.concat_tables([self.df, df_ins])

            logger.info(
                f"[APPLY] INSERT added={df_ins.num_rows}"
            )

        # ---------------------------
        # ARCHIVE + WRITE
        # ---------------------------
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        self.move_file(
            load_key,
            self.get_processed_path(load_key, add_timestamp=True)
        )

        self.write_arrow_table(load_key, self.df)

        logger.info(
            f"[PROCESS] Completed | final_rows={self.df.num_rows}"
        )


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info("[LAMBDA] Triggered")
    logger.info(json.dumps(event))

    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "/processed/" in key or "LOAD" in key:
            logger.info(f"[LAMBDA] Skipped {key}")
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success"})
    }

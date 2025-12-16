import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging
from io import BytesIO

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
        self.df = None
        self.pk_col = None

        logger.info(f"[INIT] bucket={bucket}, table={table}")

    # --------------------------------------------------
    # Path helpers
    # --------------------------------------------------
    def get_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_ts=False):
        parts = key.split("/")
        fname = parts[-1]
        if add_ts:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            filename = filename.replace(".csv", f"_{ts}.csv")
    
        for i, p in enumerate(parts):
            if p.upper().startswith("DSET"):
                return "/".join(
                    parts[: i + 1] +
                    ["processed"] +
                    parts[i + 1 : -1] +
                    [filename]
                )
    
        raise ValueError(f"DSET folder not found in path: {key}")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src},
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"[ARCHIVE] {src} → {dst}")

    # --------------------------------------------------
    # S3 listing
    # --------------------------------------------------
    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if (
                    k.endswith(".csv")
                    and "LOAD" not in k
                    and "/processed/" not in k
                ):
                    files.append(k)

        files = sorted(files)
        logger.info(f"[DISCOVER] CDC files={files}")
        return files

    # --------------------------------------------------
    # Arrow IO
    # --------------------------------------------------
    def read_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|"),
        )
        logger.info(f"[READ] {key} rows={tbl.num_rows}")
        return tbl

    def write_arrow(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"[WRITE] LOAD rows={table.num_rows}")

    # --------------------------------------------------
    # Schema alignment
    # --------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col):
        arrays = [tbl[op_col]]

        for field in target_schema:
            if field.name in tbl.column_names:
                col = tbl[field.name]
                if col.type != field.type:
                    try:
                        col = pc.cast(col, field.type)
                    except Exception:
                        col = pa.array([None] * tbl.num_rows, type=field.type)
            else:
                col = pa.array([None] * tbl.num_rows, type=field.type)

            arrays.append(col)

        names = [op_col] + [f.name for f in target_schema]
        return pa.Table.from_arrays(arrays, names=names)

    # --------------------------------------------------
    # ⭐ CORRECT CDC COLLAPSE (PK-based UPDATE)
    # --------------------------------------------------
    def collapse_all_cdc(self, cdc_tbl, op_col):
        logger.info(f"[CDC-COLLAPSE] Start rows={cdc_tbl.num_rows}")

        rows = cdc_tbl.to_pylist()
        data_cols = [c for c in cdc_tbl.column_names if c != op_col]

        staged = {}     # pk_value -> row (INSERT / UPDATE)
        deletes = []    # full-row DELETEs

        for r in rows:
            op = str(r[op_col]).upper()
            pk_val = r[self.pk_col]

            if op == "I":
                logger.info(f"[CDC-COLLAPSE] INSERT staged | pk={pk_val}")
                staged[pk_val] = r

            elif op == "U":
                logger.info(f"[CDC-COLLAPSE] UPDATE overwrote | pk={pk_val}")
                staged[pk_val] = r

            elif op == "D":
                if pk_val in staged:
                    logger.info(f"[CDC-COLLAPSE] DELETE removed staged | pk={pk_val}")
                    del staged[pk_val]
                else:
                    logger.info(f"[CDC-COLLAPSE] DELETE retained (full row)")
                    deletes.append(r)

        final_rows = list(staged.values()) + deletes

        logger.info(
            f"[CDC-COLLAPSE] Done | final_rows={len(final_rows)}"
        )

        return pa.Table.from_pylist(final_rows, schema=cdc_tbl.schema)

    # --------------------------------------------------
    # MAIN PROCESS
    # --------------------------------------------------
    def process(self, trigger_key):
        prefix = self.get_prefix(trigger_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.read_arrow(load_key)
        self.pk_col = self.df.column_names[0]

        # CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[SKIP] No CDC files found")
            return

        # Read & align CDC
        first = self.read_arrow(cdc_files[0])
        op_col = first.column_names[0]

        aligned = []
        for f in cdc_files:
            raw = self.read_arrow(f)
            aligned.append(self.align_schema(raw, self.df.schema, op_col))

        all_cdc = pa.concat_tables(aligned)

        # Collapse CDC (FIXED)
        collapsed = self.collapse_all_cdc(all_cdc, op_col)

        op_arr = pc.utf8_upper(pc.cast(collapsed[op_col], pa.string()))

        df_ins = collapsed.filter(pc.equal(op_arr, "I")).remove_column(0)
        df_upd = collapsed.filter(pc.equal(op_arr, "U")).remove_column(0)
        df_del = collapsed.filter(pc.equal(op_arr, "D")).remove_column(0)

        logger.info(
            f"[CDC] After collapse | I={df_ins.num_rows}, "
            f"U={df_upd.num_rows}, D={df_del.num_rows}"
        )

        # --------------------------------------------------
        # APPLY DELETE (FULL ROW MATCH)
        # --------------------------------------------------
        if df_del.num_rows > 0:
            del_rows = {
                tuple(r.values()) for r in df_del.to_pylist()
            }

            keep_idx = [
                i
                for i, r in enumerate(self.df.to_pylist())
                if tuple(r.values()) not in del_rows
            ]

            self.df = (
                self.df.take(keep_idx)
                if keep_idx
                else self.df.slice(0, 0)
            )

            logger.info(f"[APPLY] DELETE applied")

        # --------------------------------------------------
        # APPLY UPDATE (PK ONLY)
        # --------------------------------------------------
        if df_upd.num_rows > 0:
            upd_keys = set(df_upd[self.pk_col].to_pylist())

            keep_idx = [
                i
                for i, pk in enumerate(self.df[self.pk_col].to_pylist())
                if pk not in upd_keys
            ]

            self.df = (
                self.df.take(keep_idx)
                if keep_idx
                else self.df.slice(0, 0)
            )

            self.df = pa.concat_tables([self.df, df_upd])
            logger.info(f"[APPLY] UPDATE applied")

        # --------------------------------------------------
        # APPLY INSERT
        # --------------------------------------------------
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"[APPLY] INSERT applied")

        # --------------------------------------------------
        # ARCHIVE + WRITE
        # --------------------------------------------------
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, add_ts=True))
        self.write_arrow(load_key, self.df)

        logger.info(f"[DONE] Final LOAD rows={self.df.num_rows}")


# --------------------------------------------------
# Lambda handler
# --------------------------------------------------
def lambda_handler(event, context):
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "/processed/" in key or "LOAD" in key:
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success"}),
    }

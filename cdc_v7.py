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
    # S3 helpers
    # --------------------------------------------------
    def read_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        table = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|"),
        )
        logger.info(f"[READ] {key} rows={table.num_rows}")
        return table

    def write_arrow(self, key, table):
        buf = BytesIO()
        csv.write_csv(table, buf, write_options=csv.WriteOptions(delimiter="|"))
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        logger.info(f"[WRITE] LOAD rows={table.num_rows}")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": src},
            Key=dst,
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"[ARCHIVE] {src} → {dst}")

    # --------------------------------------------------
    # Path helpers
    # --------------------------------------------------
    def get_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_ts=False):
        parts = key.split("/")
        name = parts[-1]

        if add_ts:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            name = name.replace(".csv", f"_{ts}.csv")

        for i, p in enumerate(parts):
            if p.upper().startswith("DSET"):
                return "/".join(parts[: i + 1] + ["processed"] + parts[i + 1 : -1] + [name])

        raise ValueError("DSET folder not found")

    # --------------------------------------------------
    # CDC discovery
    # --------------------------------------------------
    def list_cdc_files(self, prefix):
        files = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)

        files.sort()
        logger.info(f"[DISCOVER] CDC files={files}")
        return files

    # --------------------------------------------------
    # Schema alignment (safe)
    # --------------------------------------------------
    def align_schema(self, cdc_tbl, load_schema, op_col):
        arrays = [cdc_tbl[op_col]]

        for field in load_schema:
            if field.name in cdc_tbl.column_names:
                col = cdc_tbl[field.name]
                if col.type != field.type:
                    col = pc.cast(col, field.type, safe=False)
            else:
                col = pa.nulls(cdc_tbl.num_rows, type=field.type)

            arrays.append(col)

        names = [op_col] + [f.name for f in load_schema]
        return pa.Table.from_arrays(arrays, names=names)

    # --------------------------------------------------
    # CDC collapse (INSERT + UPDATE → last wins)
    # --------------------------------------------------
    def collapse_cdc(self, cdc_tbl, op_col):
        staged = {}
        deletes = []

        for r in cdc_tbl.to_pylist():
            op = str(r[op_col]).upper()
            pk = r[self.pk_col]

            if op in ("I", "U"):
                staged[pk] = r
            elif op == "D":
                deletes.append(r)

        rows = list(staged.values()) + deletes
        logger.info(f"[CDC-COLLAPSE] rows={len(rows)}")

        return pa.Table.from_pylist(rows, schema=cdc_tbl.schema)

    # --------------------------------------------------
    # MAIN PROCESS
    # --------------------------------------------------
    def process(self, trigger_key):
        prefix = self.get_prefix(trigger_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.read_arrow(load_key)
        self.pk_col = self.df.column_names[0]

        initial_rows = self.df.num_rows
        logger.info(f"[STATS] Initial LOAD rows={initial_rows}")

        # CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[SKIP] No CDC files")
            return

        first = self.read_arrow(cdc_files[0])
        op_col = first.column_names[0]

        aligned = [
            self.align_schema(self.read_arrow(f), self.df.schema, op_col)
            for f in cdc_files
        ]

        cdc_all = pa.concat_tables(aligned)
        collapsed = self.collapse_cdc(cdc_all, op_col)

        ops = pc.utf8_upper(pc.cast(collapsed[op_col], pa.string()))

        df_ins = collapsed.filter(pc.equal(ops, "I")).remove_column(0)
        df_upd = collapsed.filter(pc.equal(ops, "U")).remove_column(0)
        df_del = collapsed.filter(pc.equal(ops, "D")).remove_column(0)

        logger.info(
            f"[STATS] CDC → INSERT={df_ins.num_rows}, "
            f"UPDATE={df_upd.num_rows}, DELETE={df_del.num_rows}"
        )

        # ---------------- DELETE (FULL ROW MATCH) ----------------
        if df_del.num_rows > 0:
            del_rows = {tuple(r.values()) for r in df_del.to_pylist()}
            kept = [
                r for r in self.df.to_pylist()
                if tuple(r.values()) not in del_rows
            ]
            self.df = pa.Table.from_pylist(kept, schema=self.df.schema)
            logger.info(f"[APPLY] DELETE removed={initial_rows - self.df.num_rows}")

        # ---------------- UPDATE (SAFE REPLACE / UPSERT) ----------------
        if df_upd.num_rows > 0:
            upd_map = {r[self.pk_col]: r for r in df_upd.to_pylist()}

            new_rows = []
            replaced = 0

            for r in self.df.to_pylist():
                pk = r[self.pk_col]
                if pk in upd_map:
                    new_rows.append(upd_map.pop(pk))
                    replaced += 1
                else:
                    new_rows.append(r)

            if upd_map:
                new_rows.extend(upd_map.values())

            self.df = pa.Table.from_pylist(new_rows, schema=self.df.schema)
            logger.info(f"[APPLY] UPDATE replaced={replaced}")

        # ---------------- INSERT ----------------
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"[APPLY] INSERT added={df_ins.num_rows}")

        # ---------------- FINAL ----------------
        final_rows = self.df.num_rows
        logger.info(
            f"[STATS] Final LOAD rows={final_rows}, "
            f"net_change={final_rows - initial_rows:+d}"
        )

        # Archive
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, add_ts=True))
        self.write_arrow(load_key, self.df)

        logger.info("[DONE] CDC processing completed")


# --------------------------------------------------
# Lambda handler
# --------------------------------------------------
def lambda_handler(event, context):
    logger.info(json.dumps(event))

    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "LOAD" in key or "/processed/" in key:
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": "success"}

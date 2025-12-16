import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
from io import BytesIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None
        self.pk_col: str | None = None

    # ------------------------
    # S3 helpers
    # ------------------------
    def read_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"[READ] {key} rows={tbl.num_rows}")
        return tbl

    def write_arrow(self, key, table):
        buf = BytesIO()
        csv.write_csv(table, buf, write_options=csv.WriteOptions(delimiter="|"))
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        logger.info(f"[WRITE] LOAD rows={table.num_rows}")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": src}, Key=dst)
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"[ARCHIVE] {src} → {dst}")

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
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [name])
        raise ValueError("DSET folder not found")

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

    # ------------------------
    # Schema alignment
    # ------------------------
    def align_schema(self, cdc_tbl, target_schema, op_col):
        arrays = [cdc_tbl[op_col]]
        for f in target_schema:
            if f.name in cdc_tbl.column_names:
                col = cdc_tbl[f.name]
                if col.type != f.type:
                    col = pc.cast(col, f.type, safe=False)
            else:
                col = pa.nulls(cdc_tbl.num_rows, type=f.type)
            arrays.append(col)
        return pa.Table.from_arrays(arrays, names=[op_col]+[f.name for f in target_schema])

    # ------------------------
    # APPLY DELETE (full-row)
    # ------------------------
    def apply_delete(self, df_del):
        if df_del.num_rows == 0:
            return
        cols = self.df.column_names
        del_rows = {tuple(r.values()) for r in df_del.to_pylist()}
        kept_rows = [r for r in self.df.to_pylist() if tuple(r.values()) not in del_rows]
        removed = self.df.num_rows - len(kept_rows)
        self.df = pa.Table.from_pylist(kept_rows, schema=self.df.schema)
        logger.info(f"[DELETE] removed={removed}")

    # ------------------------
    # APPLY UPDATE (PK-based safe replace)
    # ------------------------
    def apply_update(self, df_upd):
        if df_upd.num_rows == 0:
            return
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
        logger.info(f"[UPDATE] replaced={replaced}, upserted={len(upd_map)}")

    # ------------------------
    # APPLY INSERT (append)
    # ------------------------
    def apply_insert(self, df_ins):
        if df_ins.num_rows == 0:
            return
        self.df = pa.concat_tables([self.df, df_ins])
        logger.info(f"[INSERT] added={df_ins.num_rows}")

    # ------------------------
    # MAIN PROCESS
    # ------------------------
    def process(self, trigger_key):
        prefix = self.get_prefix(trigger_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.read_arrow(load_key)
        self.pk_col = self.df.column_names[0]
        initial_rows = self.df.num_rows
        logger.info(f"[LOAD] rows={initial_rows}")

        # CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[SKIP] no CDC files")
            return

        op_col = self.read_arrow(cdc_files[0]).column_names[0]

        # Process each CDC file sequentially to save memory
        total_ins = total_upd = total_del = 0
        for f in cdc_files:
            cdc_tbl = self.read_arrow(f)
            aligned = self.align_schema(cdc_tbl, self.df.schema, op_col)

            # Separate ops
            ops = pc.utf8_upper(pc.cast(aligned[op_col], pa.string()))
            df_ins = aligned.filter(pc.equal(ops, "I")).remove_column(0)
            df_upd = aligned.filter(pc.equal(ops, "U")).remove_column(0)
            df_del = aligned.filter(pc.equal(ops, "D")).remove_column(0)

            total_ins += df_ins.num_rows
            total_upd += df_upd.num_rows
            total_del += df_del.num_rows

            # Apply
            self.apply_delete(df_del)
            self.apply_update(df_upd)
            self.apply_insert(df_ins)

            # Archive processed CDC
            self.move_file(f, self.get_processed_path(f))

        # Archive old LOAD
        self.move_file(load_key, self.get_processed_path(load_key, add_ts=True))

        # Write updated LOAD
        self.write_arrow(load_key, self.df)

        final_rows = self.df.num_rows
        logger.info(f"[STATS] INSERT={total_ins}, UPDATE={total_upd}, DELETE={total_del}, FINAL_LOAD={final_rows}, net_change={final_rows-initial_rows:+d}")


# ----------------------------
# Lambda handler
# ----------------------------
def lambda_handler(event, context):
    logger.info(json.dumps(event))

    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]
        if "LOAD" in key or "/processed/" in key:
            continue
        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

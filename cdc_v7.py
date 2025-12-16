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
            f"[INIT] CDCProcessorArrow initialized | bucket={bucket}, table={table}"
        )

    # -------------------------------------------------
    # Helper functions
    # -------------------------------------------------
    def get_load_prefix(self, key):
        prefix = "/".join(key.split("/")[:-1]) + "/"
        logger.debug(f"[PATH] Derived LOAD prefix: {prefix}")
        return prefix

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                path = "/".join(
                    parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [filename]
                )
                logger.info(f"[ARCHIVE] Processed path resolved: {path}")
                return path
        raise ValueError("Invalid CDC directory structure")

    def move_file(self, src, dst):
        logger.info(f"[ARCHIVE] Moving file | {src} → {dst}")
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)

    def list_cdc_files(self, prefix):
        logger.info(f"[DISCOVERY] Listing CDC files under prefix: {prefix}")
        paginator = s3.get_paginator("list_objects_v2")
        out = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    out.append(key)

        logger.info(f"[DISCOVERY] CDC files found: {len(out)} → {out}")
        return sorted(out)

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        logger.info(f"[IO] Loading CSV from S3: {key}")
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(
            f"[IO] Loaded table | rows={tbl.num_rows}, cols={tbl.num_columns}"
        )
        return tbl

    def write_arrow_table(self, key, table):
        logger.info(
            f"[IO] Writing updated LOAD | rows={table.num_rows}, key={key}"
        )
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema alignment
    # -------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col):
        logger.info(
            f"[SCHEMA] Aligning CDC schema to LOAD schema | rows={tbl.num_rows}"
        )
        cols = {}

        for field in target_schema:
            if field.name in tbl.column_names:
                col = tbl[field.name]
                if col.type != field.type:
                    logger.warning(
                        f"[SCHEMA] Type mismatch column='{field.name}' "
                        f"{col.type} → {field.type}, casting"
                    )
                    try:
                        col = pc.cast(col, field.type)
                    except Exception as e:
                        logger.error(
                            f"[SCHEMA] Cast failed for column='{field.name}': {e}"
                        )
                        col = pa.array([None] * tbl.num_rows, type=field.type)
                cols[field.name] = col
            else:
                logger.warning(
                    f"[SCHEMA] Missing column in CDC: '{field.name}', filling NULLs"
                )
                cols[field.name] = pa.array([None] * tbl.num_rows, type=field.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + [f.name for f in target_schema]
        )

    def safe_upper(self, arr):
        return pc.utf8_upper(pc.cast(arr, pa.string()))

    # -------------------------------------------------
    # ⭐ CDC COLLAPSE LOGIC WITH LOGGING
    # -------------------------------------------------
    def collapse_cdc_rows(self, cdc_tbl: pa.Table, op_col: str):
        logger.info(
            f"[CDC-COLLAPSE] Starting collapse | input_rows={cdc_tbl.num_rows}"
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

            logger.debug(
                f"[CDC-COLLAPSE] Row#{idx} op={op} key={pkey}"
            )

            if op == "I":
                logger.info(
                    f"[CDC-COLLAPSE] INSERT staged | key={pkey}"
                )
                state[pkey] = r

            elif op == "U":
                if pkey in state:
                    logger.info(
                        f"[CDC-COLLAPSE] UPDATE overwrote previous INSERT | key={pkey}"
                    )
                else:
                    logger.info(
                        f"[CDC-COLLAPSE] UPDATE recorded | key={pkey}"
                    )
                state[pkey] = r

            elif op == "D":
                if pkey in state:
                    logger.info(
                        f"[CDC-COLLAPSE] DELETE removed pending INSERT/UPDATE | key={pkey}"
                    )
                    del state[pkey]
                else:
                    logger.info(
                        f"[CDC-COLLAPSE] DELETE retained (no prior state) | key={pkey}"
                    )
                    final_rows.append(r)

        final_rows.extend(state.values())

        logger.info(
            f"[CDC-COLLAPSE] Completed | output_rows={len(final_rows)} "
            f"(from input {cdc_tbl.num_rows})"
        )

        return pa.Table.from_pylist(final_rows, schema=cdc_tbl.schema)

    # -------------------------------------------------
    # Main Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        logger.info(f"[PROCESS] CDC processing started | file={cdc_key}")

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow_table(load_key)
        self.pk_col = self.df.column_names[0]

        logger.info(
            f"[PROCESS] LOAD loaded | rows={self.df.num_rows}, pk_col={self.pk_col}"
        )

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[PROCESS] No CDC files found, exiting")
            return

        first = self.load_arrow_table(cdc_files[0])
        op_col = first.column_names[0]

        inserts, updates, deletes = [], [], []

        for f in cdc_files:
            logger.info(f"[PROCESS] CDC file → {f}")

            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, self.df.schema, op_col)
            collapsed = self.collapse_cdc_rows(aligned, op_col)

            op_arr = self.safe_upper(collapsed[op_col])

            ins = collapsed.filter(pc.equal(op_arr, "I")).remove_column(0)
            upd = collapsed.filter(pc.equal(op_arr, "U")).remove_column(0)
            dele = collapsed.filter(pc.equal(op_arr, "D")).remove_column(0)

            logger.info(
                f"[PROCESS] CDC ops after collapse | "
                f"I={ins.num_rows}, U={upd.num_rows}, D={dele.num_rows}"
            )

            if ins.num_rows:
                inserts.append(ins)
            if upd.num_rows:
                updates.append(upd)
            if dele.num_rows:
                deletes.append(dele)

        # ---------------------------
        # APPLY TO LOAD
        # ---------------------------
        if deletes:
            df_del = pa.concat_tables(deletes)
            logger.info(f"[APPLY] DELETE rows={df_del.num_rows}")

        if updates:
            df_upd = pa.concat_tables(updates)
            logger.info(f"[APPLY] UPDATE rows={df_upd.num_rows}")

        if inserts:
            df_ins = pa.concat_tables(inserts)
            logger.info(f"[APPLY] INSERT rows={df_ins.num_rows}")

        # Archive CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        self.write_arrow_table(load_key, self.df)

        logger.info("[PROCESS] CDC processing completed successfully")


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info("[LAMBDA] Triggered")
    logger.info(json.dumps(event))

    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "LOAD" in key or "/processed/" in key:
            logger.info(f"[LAMBDA] Skipping file: {key}")
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

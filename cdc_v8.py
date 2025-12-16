import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging
from io import BytesIO
import hashlib

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
        filename = parts[-1]
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
    # Row hashing for efficient full-row matching
    # --------------------------------------------------
    def hash_row(self, row_dict):
        """Create hash of all column values for efficient comparison"""
        # Sort keys for consistent hashing
        sorted_items = sorted(row_dict.items())
        # Convert to string representation, handling None
        row_str = "|".join(
            f"{k}:{v if v is not None else 'NULL'}" 
            for k, v in sorted_items
        )
        return hashlib.md5(row_str.encode()).hexdigest()

    # --------------------------------------------------
    # CDC COLLAPSE (PK-based for I/U, full-row for D)
    # --------------------------------------------------
    def collapse_all_cdc(self, cdc_tbl, op_col):
        logger.info(f"[CDC-COLLAPSE] Start rows={cdc_tbl.num_rows}")

        rows = cdc_tbl.to_pylist()

        staged = {}           # pk_value -> row (INSERT / UPDATE)
        delete_hashes = set() # hashes of full rows to DELETE

        for r in rows:
            op = str(r[op_col]).upper()
            pk_val = r[self.pk_col]

            if op == "I":
                staged[pk_val] = r

            elif op == "U":
                staged[pk_val] = r

            elif op == "D":
                if pk_val in staged:
                    del staged[pk_val]
                else:
                    # Store hash of full row for deletion
                    row_copy = {k: v for k, v in r.items() if k != op_col}
                    delete_hashes.add(self.hash_row(row_copy))

        logger.info(
            f"[CDC-COLLAPSE] staged={len(staged)}, delete_hashes={len(delete_hashes)}"
        )

        # Build final collapsed table
        final_rows = []
        for pk_val, row in staged.items():
            row[op_col] = 'I' if row[op_col].upper() == 'I' else 'U'
            final_rows.append(row)
        
        # Add one representative delete row (we'll use hashes for actual matching)
        if delete_hashes:
            final_rows.append({
                op_col: 'D',
                **{col: None for col in cdc_tbl.column_names if col != op_col}
            })

        return pa.Table.from_pylist(final_rows, schema=cdc_tbl.schema), delete_hashes

    # --------------------------------------------------
    # MAIN PROCESS
    # --------------------------------------------------
    def process(self, trigger_key):
        prefix = self.get_prefix(trigger_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        logger.info(f"[LOAD] Reading {load_key}")
        self.df = self.read_arrow(load_key)
        self.pk_col = self.df.column_names[0]
        logger.info(f"[LOAD] PK column: {self.pk_col}, rows={self.df.num_rows}")

        # CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[SKIP] No CDC files found")
            return

        # Read & align CDC
        first = self.read_arrow(cdc_files[0])
        op_col = first.column_names[0]

        logger.info(f"[CDC] Aligning {len(cdc_files)} files")
        aligned = []
        for f in cdc_files:
            raw = self.read_arrow(f)
            aligned.append(self.align_schema(raw, self.df.schema, op_col))

        all_cdc = pa.concat_tables(aligned)
        logger.info(f"[CDC] Total CDC rows before collapse: {all_cdc.num_rows}")

        # Collapse CDC
        collapsed, delete_hashes = self.collapse_all_cdc(all_cdc, op_col)

        op_arr = pc.utf8_upper(pc.cast(collapsed[op_col], pa.string()))

        df_ins = collapsed.filter(pc.equal(op_arr, "I")).remove_column(0)
        df_upd = collapsed.filter(pc.equal(op_arr, "U")).remove_column(0)
        has_deletes = len(delete_hashes) > 0

        logger.info(
            f"[CDC] After collapse | I={df_ins.num_rows}, "
            f"U={df_upd.num_rows}, D={len(delete_hashes)}"
        )

        # --------------------------------------------------
        # ⭐ APPLY DELETE (FULL ROW MATCH - OPTIMIZED WITH HASHING)
        # --------------------------------------------------
        if has_deletes:
            logger.info(f"[APPLY] DELETE processing {len(delete_hashes)} unique row hashes")
            
            load_rows = self.df.to_pylist()
            keep_rows = []
            deleted_count = 0
            
            for i, row in enumerate(load_rows):
                if i % 10000 == 0 and i > 0:
                    logger.info(f"[APPLY] DELETE progress: {i}/{len(load_rows)}")
                
                row_hash = self.hash_row(row)
                
                if row_hash not in delete_hashes:
                    keep_rows.append(row)
                else:
                    deleted_count += 1

            self.df = (
                pa.Table.from_pylist(keep_rows, schema=self.df.schema)
                if keep_rows
                else self.df.slice(0, 0)
            )

            logger.info(f"[APPLY] DELETE applied, deleted={deleted_count}, remaining={self.df.num_rows}")

        # --------------------------------------------------
        # ⭐ APPLY UPDATE (PARTIAL MATCH ON PK)
        # --------------------------------------------------
        if df_upd.num_rows > 0:
            logger.info(f"[APPLY] UPDATE processing {df_upd.num_rows} updates")
            
            # Create lookup map: pk_value -> updated_row
            upd_map = {
                row[self.pk_col]: row 
                for row in df_upd.to_pylist()
            }

            # Update existing rows in-place by PK match
            updated_rows = []
            load_rows = self.df.to_pylist()
            update_count = 0
            
            for i, row in enumerate(load_rows):
                if i % 10000 == 0 and i > 0:
                    logger.info(f"[APPLY] UPDATE progress: {i}/{len(load_rows)}")
                
                pk_val = row[self.pk_col]
                
                if pk_val in upd_map:
                    # Merge: keep existing values, overwrite with updates
                    updated_row = row.copy()
                    updated_row.update(upd_map[pk_val])
                    updated_rows.append(updated_row)
                    update_count += 1
                else:
                    # Keep unchanged
                    updated_rows.append(row)

            self.df = pa.Table.from_pylist(updated_rows, schema=self.df.schema)
            logger.info(f"[APPLY] UPDATE applied, updated={update_count} rows")

        # --------------------------------------------------
        # APPLY INSERT
        # --------------------------------------------------
        if df_ins.num_rows > 0:
            logger.info(f"[APPLY] INSERT adding {df_ins.num_rows} rows")
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"[APPLY] INSERT applied")

        # --------------------------------------------------
        # ARCHIVE + WRITE
        # --------------------------------------------------
        logger.info(f"[ARCHIVE] Moving {len(cdc_files)} CDC files to processed")
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        logger.info(f"[ARCHIVE] Moving LOAD file to processed")
        self.move_file(load_key, self.get_processed_path(load_key, add_ts=True))
        
        logger.info(f"[WRITE] Writing new LOAD file")
        self.write_arrow(load_key, self.df)

        logger.info(f"[DONE] Final LOAD rows={self.df.num_rows}")


# --------------------------------------------------
# Lambda handler
# --------------------------------------------------
def lambda_handler(event, context):
    logger.info(f"[LAMBDA] Starting with {len(event.get('Records', []))} S3 events")
    
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        logger.info(f"[LAMBDA] Processing bucket={bucket}, key={key}")

        if "/processed/" in key or "LOAD" in key:
            logger.info(f"[LAMBDA] Skipping processed/LOAD file")
            continue

        table = key.split("/")[-2]
        
        try:
            CDCProcessorArrow(bucket, table).process(key)
        except Exception as e:
            logger.error(f"[LAMBDA] Error processing {key}: {str(e)}", exc_info=True)
            raise

    logger.info(f"[LAMBDA] Completed successfully")
    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success"}),
    }

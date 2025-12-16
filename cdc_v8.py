import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging

# --------------------------------------------------
# Logging
# --------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------
# AWS
# --------------------------------------------------
s3 = boto3.client("s3")

# Batch size for processing
BATCH_SIZE = 50000


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
        logger.info(f"[WRITE] {key} rows={table.num_rows}")

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
    # CDC COLLAPSE - Keep only latest operation per PK
    # --------------------------------------------------
    def collapse_all_cdc(self, cdc_tbl, op_col):
        logger.info(f"[CDC-COLLAPSE] Start rows={cdc_tbl.num_rows}")

        # Convert to pandas for groupby (faster than Python loop)
        df = cdc_tbl.to_pandas()
        
        # Keep last operation per PK
        df_collapsed = df.groupby(self.pk_col, as_index=False).last()
        
        result = pa.Table.from_pandas(df_collapsed, schema=cdc_tbl.schema)
        
        logger.info(f"[CDC-COLLAPSE] Collapsed to {result.num_rows} rows")
        return result

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
        logger.info(f"[LOAD] PK={self.pk_col}, rows={self.df.num_rows}")

        # CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("[SKIP] No CDC files found")
            return

        # Read & align CDC
        first = self.read_arrow(cdc_files[0])
        op_col = first.column_names[0]

        logger.info(f"[CDC] Processing {len(cdc_files)} files")
        aligned = []
        for f in cdc_files:
            raw = self.read_arrow(f)
            aligned.append(self.align_schema(raw, self.df.schema, op_col))

        all_cdc = pa.concat_tables(aligned)
        logger.info(f"[CDC] Total rows before collapse: {all_cdc.num_rows}")

        # Collapse CDC
        collapsed = self.collapse_all_cdc(all_cdc, op_col)

        # Split by operation
        op_arr = pc.utf8_upper(pc.cast(collapsed[op_col], pa.string()))

        df_ins = collapsed.filter(pc.equal(op_arr, "I")).remove_column(0)
        df_upd = collapsed.filter(pc.equal(op_arr, "U")).remove_column(0)
        df_del = collapsed.filter(pc.equal(op_arr, "D")).remove_column(0)

        logger.info(
            f"[CDC] Split | I={df_ins.num_rows}, "
            f"U={df_upd.num_rows}, D={df_del.num_rows}"
        )

        # --------------------------------------------------
        # APPLY OPERATIONS USING PANDAS (MUCH FASTER)
        # --------------------------------------------------
        
        # Convert to pandas for efficient operations
        load_df = self.df.to_pandas()
        logger.info(f"[PROCESS] Converted LOAD to pandas: {len(load_df)} rows")

        # --------------------------------------------------
        # DELETE (full row match)
        # --------------------------------------------------
        if df_del.num_rows > 0:
            logger.info(f"[DELETE] Processing {df_del.num_rows} deletes")
            del_df = df_del.to_pandas()
            
            # Create composite key from all columns for matching
            load_df['_del_key'] = load_df.astype(str).apply(lambda x: '|'.join(x), axis=1)
            del_df['_del_key'] = del_df.astype(str).apply(lambda x: '|'.join(x), axis=1)
            
            # Filter out rows that match delete keys
            before = len(load_df)
            load_df = load_df[~load_df['_del_key'].isin(del_df['_del_key'])]
            load_df = load_df.drop(columns=['_del_key'])
            
            logger.info(f"[DELETE] Removed {before - len(load_df)} rows, remaining={len(load_df)}")

        # --------------------------------------------------
        # UPDATE (PK match, partial update)
        # --------------------------------------------------
        if df_upd.num_rows > 0:
            logger.info(f"[UPDATE] Processing {df_upd.num_rows} updates")
            upd_df = df_upd.to_pandas()
            
            # Remove rows with matching PKs from LOAD
            before = len(load_df)
            load_df = load_df[~load_df[self.pk_col].isin(upd_df[self.pk_col])]
            logger.info(f"[UPDATE] Removed {before - len(load_df)} old rows")
            
            # Append updated rows
            load_df = pa.concat_tables([
                pa.Table.from_pandas(load_df, schema=self.df.schema),
                df_upd
            ]).to_pandas()
            
            logger.info(f"[UPDATE] Applied updates, total={len(load_df)}")

        # --------------------------------------------------
        # INSERT
        # --------------------------------------------------
        if df_ins.num_rows > 0:
            logger.info(f"[INSERT] Adding {df_ins.num_rows} rows")
            ins_df = df_ins.to_pandas()
            
            load_df = pa.concat_tables([
                pa.Table.from_pandas(load_df, schema=self.df.schema),
                df_ins
            ]).to_pandas()
            
            logger.info(f"[INSERT] Applied, total={len(load_df)}")

        # Convert back to Arrow
        self.df = pa.Table.from_pandas(load_df, schema=self.df.schema)

        # --------------------------------------------------
        # ARCHIVE + WRITE
        # --------------------------------------------------
        logger.info(f"[ARCHIVE] Moving {len(cdc_files)} CDC files")
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        logger.info(f"[ARCHIVE] Moving LOAD file")
        self.move_file(load_key, self.get_processed_path(load_key, add_ts=True))
        
        logger.info(f"[WRITE] Writing final LOAD")
        self.write_arrow(load_key, self.df)

        logger.info(f"[DONE] Final rows={self.df.num_rows}")


# --------------------------------------------------
# Lambda handler
# --------------------------------------------------
def lambda_handler(event, context):
    logger.info(f"[LAMBDA] Starting")
    
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        logger.info(f"[LAMBDA] Event: bucket={bucket}, key={key}")

        if "/processed/" in key or "LOAD" in key:
            logger.info(f"[LAMBDA] Skipping (processed/LOAD)")
            continue

        table = key.split("/")[-2]
        
        try:
            CDCProcessorArrow(bucket, table).process(key)
        except Exception as e:
            logger.error(f"[LAMBDA] ERROR: {str(e)}", exc_info=True)
            raise

    logger.info(f"[LAMBDA] SUCCESS")
    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success"}),
    }

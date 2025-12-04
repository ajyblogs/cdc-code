import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from datetime import datetime
import logging
from typing import List, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


class CDCProcessorArrow:
    """
    Processes CDC files for a single LOAD file (Option C).
    Each LOAD is processed independently — CDC files are taken only from the LOAD's prefix.
    """

    def __init__(self, bucket: str, load_key: str):
        self.bucket = bucket
        self.load_key = load_key  # full S3 key to LOAD CSV
        self.df: Optional[pa.Table] = None
        self.pk_col: Optional[str] = None

        # Derive load prefix (folder to search CDC files in)
        self.load_prefix = self._detect_load_prefix(load_key)
        logger.info(f"Initialized CDCProcessorArrow for bucket={bucket}, load_key={load_key}, load_prefix={self.load_prefix}")

    # --------------------------
    # Prefix and path helpers
    # --------------------------
    def _detect_load_prefix(self, load_key: str) -> str:
        """
        Determine the directory/prefix that contains this LOAD file.
        Supports:
          - .../LOAD00000001.csv  (prefix = parent dir)
          - .../LOAD00000001/LOAD00000001.csv (prefix = LOAD subfolder)
        Always returns prefix ending with '/'
        """
        if load_key.endswith(".csv"):
            # If the CSV is in a LOAD subfolder, keep that subfolder as prefix.
            parts = load_key.split("/")
            if len(parts) >= 2 and parts[-2].startswith("LOAD"):
                # path like .../LOAD00000001/LOAD00000001.csv
                return "/".join(parts[:-1]) + "/"
            else:
                # path like .../something/LOAD00000001.csv -> prefix is parent folder
                return "/".join(parts[:-1]) + "/" if len(parts) > 1 else ""
        else:
            return "/".join(load_key.split("/")[:-1]) + "/"

    def get_processed_path(self, key: str, add_timestamp: bool = False) -> str:
        """
        Move file into processed/ subfolder under the same load_prefix.
        If add_timestamp True, append UTC timestamp to filename.
        """
        filename = key.split("/")[-1]
        if add_timestamp:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            filename = filename.replace(".csv", f"_{ts}.csv")
        return f"{self.load_prefix}processed/{filename}"

    def move_file(self, src: str, dst: str):
        s3.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file → {src} → {dst}")

    # --------------------------
    # S3 listing (CDC files for this LOAD)
    # --------------------------
    def list_cdc_files(self) -> List[str]:
        paginator = s3.get_paginator("list_objects_v2")
        out: List[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.load_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Skip the LOAD file itself
                if key == self.load_key:
                    continue
                # Skip processed directory
                if "/processed/" in key:
                    continue
                # Only CSVs
                if key.endswith(".csv"):
                    out.append(key)
        out_sorted = sorted(out)
        logger.info(f"CDC files discovered for LOAD '{self.load_key}': {out_sorted}")
        return out_sorted

    # --------------------------
    # Load / write Arrow
    # --------------------------
    def load_arrow_table(self, key: str) -> pa.Table:
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        # Use pipe delimiter as in your original code
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded table: {key} → rows={tbl.num_rows}, cols={tbl.num_columns}")
        return tbl

    def write_arrow_table(self, key: str, table: pa.Table) -> str:
        # Convert to CSV with pipe delimiter; this is simplest and compatible with your original approach.
        csv_bytes = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_bytes)
        logger.info(f"Wrote updated LOAD to s3://{self.bucket}/{key} (rows={table.num_rows})")
        return f"s3://{self.bucket}/{key}"

    # --------------------------
    # Schema inference & alignment utilities
    # --------------------------
    def infer_unified_schema(self, load_table: pa.Table, cdc_tables: List[pa.Table], op_col_name: str) -> pa.Schema:
        """
        Build unified schema by using LOAD schema and filling null-typed fields by inferring
        from CDC sample files. If still unknown, default to string.
        """
        unified_fields = []
        for field in load_table.schema:
            col_name = field.name
            col_type = field.type
            if pa.types.is_null(col_type):
                logger.info(f"Column '{col_name}' in LOAD is null type — attempting to infer from CDC files")
                inferred_type = None
                for cdc_tbl in cdc_tables:
                    if col_name in cdc_tbl.column_names:
                        try:
                            cdc_col_type = cdc_tbl.schema.field(col_name).type
                        except Exception:
                            cdc_col_type = None
                        if cdc_col_type and (not pa.types.is_null(cdc_col_type)):
                            inferred_type = cdc_col_type
                            logger.info(f"Inferred type for '{col_name}' from CDC: {inferred_type}")
                            break
                if inferred_type:
                    unified_fields.append(pa.field(col_name, inferred_type))
                else:
                    logger.warning(f"Could not infer type for '{col_name}', defaulting to string")
                    unified_fields.append(pa.field(col_name, pa.string()))
            else:
                unified_fields.append(field)
        return pa.schema(unified_fields)

    def upgrade_load_schema(self, load_table: pa.Table, unified_schema: pa.Schema) -> pa.Table:
        """
        Convert LOAD table columns with null type to arrays of the unified type (all null values).
        """
        cols = []
        for field in unified_schema:
            col_name = field.name
            # If LOAD lacks the column (rare), fill nulls
            if col_name not in load_table.column_names:
                cols.append(pa.array([None] * load_table.num_rows, type=field.type))
                continue
            load_col = load_table[col_name]
            if pa.types.is_null(load_col.type) and not pa.types.is_null(field.type):
                logger.info(f"Upgrading LOAD column '{col_name}' from NULL to {field.type}")
                cols.append(pa.array([None] * load_table.num_rows, type=field.type))
            else:
                cols.append(load_col)
        return pa.Table.from_arrays(cols, schema=unified_schema)

    def align_schema(self, tbl: pa.Table, target_schema: pa.Schema, op_col_name: str) -> pa.Table:
        """
        Ensure CDC table (excluding the op column) has same schema as LOAD.
        - Missing columns: added as nulls
        - Type mismatches: attempt cast to target type. If cast fails, cast to string.
        Returns table where the op column is first, then the columns matching target_schema.
        """
        cols = []
        for field in target_schema:
            name = field.name
            if name in tbl.column_names:
                col = tbl[name]
                # If types equal, keep. Otherwise try casting.
                if col.type != field.type:
                    logger.info(f"Type mismatch for '{name}': {col.type} → {field.type}. Attempting cast.")
                    try:
                        col = pc.cast(col, field.type)
                    except Exception as e:
                        logger.warning(f"Failed to cast '{name}' to {field.type}: {e}. Trying cast to string.")
                        try:
                            col = pc.cast(col, pa.string())
                        except Exception as e2:
                            logger.error(f"Failed to cast '{name}' to string: {e2}. Filling nulls of target type.")
                            col = pa.array([None] * tbl.num_rows, type=field.type)
                cols.append(col)
            else:
                logger.info(f"Missing column '{name}' in CDC file — filling nulls of type {field.type}")
                cols.append(pa.array([None] * tbl.num_rows, type=field.type))

        # re-add op column at beginning
        if op_col_name not in tbl.column_names:
            raise ValueError(f"Operation column '{op_col_name}' not found in CDC table")
        return pa.Table.from_arrays([tbl[op_col_name]] + cols, names=[op_col_name] + [f.name for f in target_schema])

    # --------------------------
    # Utility: safe uppercase for op column
    # --------------------------
    def safe_upper(self, arr: pa.Array) -> pa.Array:
        try:
            if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
                return pc.utf8_upper(arr)
            return pc.utf8_upper(pc.cast(arr, pa.string()))
        except Exception as e:
            logger.warning(f"safe_upper failed: {e} — returning original array")
            return arr

    # --------------------------
    # Main processing for this LOAD
    # --------------------------
    def process(self):
        start = datetime.utcnow()
        logger.info(f"Processing LOAD: {self.load_key}")

        # Load base LOAD table
        self.df = self.load_arrow_table(self.load_key)
        initial_rows = self.df.num_rows
        if not self.df.column_names:
            raise ValueError("LOAD file has no columns")
        self.pk_col = self.df.column_names[0]
        logger.info(f"Primary key inferred as: {self.pk_col}")

        # Discover CDC files under this load prefix
        cdc_files = self.list_cdc_files()
        if not cdc_files:
            logger.info("No CDC files found for this LOAD; nothing to do.")
            return

        # Peek at first CDC to get op column name
        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col_name = first_cdc.column_names[0]
        logger.info(f"Operation column detected: {op_col_name}")

        # Sample a few CDC files for schema inference (limit 5)
        sample_cdc_tables = [first_cdc]
        for f in cdc_files[1:min(5, len(cdc_files))]:
            sample_cdc_tables.append(self.load_arrow_table(f))

        # Build unified schema using LOAD and CDC samples
        unified_schema = self.infer_unified_schema(self.df, sample_cdc_tables, op_col_name)
        logger.info(f"Unified schema built: {unified_schema}")

        # Upgrade LOAD to unified schema (replace null-typed columns)
        self.df = self.upgrade_load_schema(self.df, unified_schema)
        logger.info(f"LOAD upgraded to unified schema — rows={self.df.num_rows}, cols={self.df.num_columns}")

        # Process CDC files in batches to avoid memory spikes
        batch_size = 10
        all_inserts = []
        all_updates = []
        all_deletes = []

        total_batches = (len(cdc_files) + batch_size - 1) // batch_size
        for batch_idx in range(0, len(cdc_files), batch_size):
            batch = cdc_files[batch_idx: batch_idx + batch_size]
            logger.info(f"Processing batch {batch_idx // batch_size + 1}/{total_batches} ({len(batch)} files)")

            batch_tables = []
            for f in batch:
                raw_tbl = self.load_arrow_table(f)
                aligned = self.align_schema(raw_tbl, unified_schema, op_col_name)
                batch_tables.append(aligned)

            batch_combined = batch_tables[0] if len(batch_tables) == 1 else pa.concat_tables(batch_tables)
            logger.info(f"Batch combined rows: {batch_combined.num_rows}")

            # Normalize op column to uppercase
            op_arr = self.safe_upper(batch_combined[op_col_name])

            # Filter by operations. .filter expects boolean array; pc.equal returns boolean array
            try:
                ins_mask = pc.equal(op_arr, pa.scalar("I"))
                upd_mask = pc.equal(op_arr, pa.scalar("U"))
                del_mask = pc.equal(op_arr, pa.scalar("D"))
            except Exception as e:
                # If direct scalar compare fails, cast to string first
                s_op_arr = pc.cast(op_arr, pa.string())
                ins_mask = pc.equal(s_op_arr, pa.scalar("I"))
                upd_mask = pc.equal(s_op_arr, pa.scalar("U"))
                del_mask = pc.equal(s_op_arr, pa.scalar("D"))

            batch_ins = batch_combined.filter(ins_mask).remove_column(0) if batch_combined.filter(ins_mask).num_rows > 0 else None
            batch_upd = batch_combined.filter(upd_mask).remove_column(0) if batch_combined.filter(upd_mask).num_rows > 0 else None
            batch_del = batch_combined.filter(del_mask).remove_column(0) if batch_combined.filter(del_mask).num_rows > 0 else None

            if batch_ins and batch_ins.num_rows > 0:
                all_inserts.append(batch_ins)
            if batch_upd and batch_upd.num_rows > 0:
                all_updates.append(batch_upd)
            if batch_del and batch_del.num_rows > 0:
                all_deletes.append(batch_del)

        # Combine operations (create empty tables with unified schema when none)
        def _concat_or_empty(lst: List[pa.Table]) -> pa.Table:
            if not lst:
                # empty table with unified_schema; note unified_schema does NOT include op column
                # build empty columns for each field
                cols = [pa.array([], type=f.type) for f in unified_schema]
                return pa.Table.from_arrays(cols, names=[f.name for f in unified_schema])
            return pa.concat_tables(lst)

        df_ins = _concat_or_empty(all_inserts)
        df_upd = _concat_or_empty(all_updates)
        df_del = _concat_or_empty(all_deletes)

        logger.info(f"CDC Ops summary for LOAD {self.load_key} → INSERT={df_ins.num_rows}, UPDATE={df_upd.num_rows}, DELETE={df_del.num_rows}")

        # --------------------------
        # DELETE
        # --------------------------
        if df_del.num_rows > 0:
            try:
                del_pk_list = df_del[self.pk_col].to_pylist()
            except Exception:
                # If pk column name mismatch or missing, try index 0 of unified schema
                del_pk_list = df_del[df_del.column_names[0]].to_pylist()
            del_pk_set = set(del_pk_list)
            logger.info(f"DELETE: processing {len(del_pk_set)} unique keys")

            current_pks = self.df[self.pk_col].to_pylist()
            keep_indices = [i for i, pk in enumerate(current_pks) if pk not in del_pk_set]
            before = self.df.num_rows
            if keep_indices:
                self.df = self.df.take(keep_indices)
            else:
                # All rows removed: build empty table with same schema
                cols = [pa.array([], type=f.type) for f in self.df.schema]
                self.df = pa.Table.from_arrays(cols, names=[f.name for f in self.df.schema])
            logger.info(f"DELETE removed rows: {before - self.df.num_rows}")

        # --------------------------
        # UPDATE
        # --------------------------
        if df_upd.num_rows > 0:
            try:
                upd_pk_list = df_upd[self.pk_col].to_pylist()
            except Exception:
                upd_pk_list = df_upd[df_upd.column_names[0]].to_pylist()
            upd_pk_set = set(upd_pk_list)
            logger.info(f"UPDATE: processing {len(upd_pk_set)} unique keys")

            current_pks = self.df[self.pk_col].to_pylist()
            keep_indices = [i for i, pk in enumerate(current_pks) if pk not in upd_pk_set]
            before = self.df.num_rows
            if keep_indices:
                self.df = self.df.take(keep_indices)
            else:
                cols = [pa.array([], type=f.type) for f in self.df.schema]
                self.df = pa.Table.from_arrays(cols, names=[f.name for f in self.df.schema])
            logger.info(f"UPDATE removed old rows: {before - self.df.num_rows}")

            # Append updated rows
            # Ensure df_upd schema matches self.df schema (df_upd built from unified_schema)
            try:
                self.df = pa.concat_tables([self.df, df_upd])
            except Exception as e:
                logger.warning(f"Concat during UPDATE failed: {e}. Attempting to align schemas then concat.")
                # Attempt to align df_upd to self.df.schema by casting columns
                aligned_updates_cols = []
                for f in self.df.schema:
                    if f.name in df_upd.column_names:
                        try:
                            aligned_updates_cols.append(pc.cast(df_upd[f.name], f.type))
                        except Exception:
                            aligned_updates_cols.append(pc.cast(df_upd[f.name], pa.string()))
                    else:
                        aligned_updates_cols.append(pa.array([None] * df_upd.num_rows, type=f.type))
                aligned_updates = pa.Table.from_arrays(aligned_updates_cols, names=[f.name for f in self.df.schema])
                self.df = pa.concat_tables([self.df, aligned_updates])
            logger.info(f"UPDATE inserted new rows: {df_upd.num_rows}")

        # --------------------------
        # INSERT
        # --------------------------
        if df_ins.num_rows > 0:
            try:
                self.df = pa.concat_tables([self.df, df_ins])
                logger.info(f"INSERT rows added: {df_ins.num_rows}")
            except Exception as e:
                logger.warning(f"Insert concat failed: {e}. Attempting to align insert schema then concat.")
                aligned_inserts_cols = []
                for f in self.df.schema:
                    if f.name in df_ins.column_names:
                        try:
                            aligned_inserts_cols.append(pc.cast(df_ins[f.name], f.type))
                        except Exception:
                            aligned_inserts_cols.append(pc.cast(df_ins[f.name], pa.string()))
                    else:
                        aligned_inserts_cols.append(pa.array([None] * df_ins.num_rows, type=f.type))
                aligned_inserts = pa.Table.from_arrays(aligned_inserts_cols, names=[f.name for f in self.df.schema])
                self.df = pa.concat_tables([self.df, aligned_inserts])
                logger.info(f"INSERT rows added (after alignment): {df_ins.num_rows}")

        # --------------------------
        # Archive CDC and LOAD, write updated LOAD
        # --------------------------
        for f in cdc_files:
            try:
                self.move_file(f, self.get_processed_path(f))
            except Exception as e:
                logger.error(f"Failed to archive CDC file {f}: {e}")

        # Archive old LOAD with timestamp and write new LOAD in same path
        try:
            new_load_archive = self.get_processed_path(self.load_key, add_timestamp=True)
            self.move_file(self.load_key, new_load_archive)
        except Exception as e:
            logger.error(f"Failed to archive old LOAD {self.load_key}: {e}")

        out_path = None
        try:
            out_path = self.write_arrow_table(self.load_key, self.df)
        except Exception as e:
            logger.error(f"Failed to write updated LOAD to {self.load_key}: {e}")
            raise

        final_rows = self.df.num_rows
        result = {
            "status": "success",
            "load_key": self.load_key,
            "initial_rows": initial_rows,
            "final_rows": final_rows,
            "row_change": final_rows - initial_rows,
            "inserts": df_ins.num_rows,
            "updates": df_upd.num_rows,
            "deletes": df_del.num_rows,
            "output": out_path,
            "time_sec": (datetime.utcnow() - start).total_seconds(),
        }
        logger.info(json.dumps(result))


# --------------------------
# Lambda handler
# --------------------------
def lambda_handler(event, context):
    logger.info("Lambda triggered with event:")
    logger.info(json.dumps(event))

    try:
        for record in event.get("Records", []):
            # S3 event structure
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            logger.info(f"Event for s3://{bucket}/{key}")

            # Ignore events for processed folder or non-csv files
            if "/processed/" in key:
                logger.info(f"Skipping processed file event: {key}")
                continue
            if not key.endswith(".csv"):
                logger.info(f"Skipping non-csv file event: {key}")
                continue

            # If the event is for a LOAD upload itself, we skip processing here.
            # We expect CDC files to trigger processing for loads in the same prefix.
            if "LOAD" in key and key.endswith(".csv"):
                logger.info(f"Skipping LOAD upload event (no action): {key}")
                continue

            # For this CDC file, find LOAD files in the same prefix (folder)
            prefix = "/".join(key.split("/")[:-1]) + "/" if "/" in key else ""
            logger.info(f"Looking for LOAD files in prefix: {prefix}")

            list_resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            contents = list_resp.get("Contents", []) or []
            load_files = [o["Key"] for o in contents if "LOAD" in o["Key"] and o["Key"].endswith(".csv")]

            if not load_files:
                # Try one level up (in some layouts CDC may be sibling to LOAD folder)
                alt_prefix = "/".join(prefix.rstrip("/").split("/")[:-1]) + "/" if prefix else ""
                if alt_prefix and alt_prefix != prefix:
                    logger.info(f"No LOADs in {prefix}, trying parent folder {alt_prefix}")
                    list_resp2 = s3.list_objects_v2(Bucket=bucket, Prefix=alt_prefix)
                    contents2 = list_resp2.get("Contents", []) or []
                    load_files = [o["Key"] for o in contents2 if "LOAD" in o["Key"] and o["Key"].endswith(".csv")]

            if not load_files:
                logger.error(f"No LOAD files found for CDC file {key} in prefix {prefix}")
                continue

            for load_key in load_files:
                try:
                    proc = CDCProcessorArrow(bucket, load_key)
                    proc.process()
                except Exception as e:
                    logger.error(f"Error while processing LOAD {load_key}: {e}", exc_info=True)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(f"Unhandled error in lambda handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

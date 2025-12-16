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
    def __init__(self, bucket, table, update_key_columns=None):
        """
        bucket: S3 bucket name
        table: Table name
        update_key_columns: List of column names used for partial matching in UPDATEs
                           If None, will use first column as key
        """
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None
        self.update_key_columns = update_key_columns

        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")
        if update_key_columns:
            logger.info(f"Update key columns: {update_key_columns}")

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
        s3.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file → {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "LOAD" not in key and "/processed/" not in key:
                    out.append(key)
        logger.info(f"CDC files discovered: {out}")
        return sorted(out)

    # -------------------------------------------------
    # Load / Write Arrow tables
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded table: {key} → rows={tbl.num_rows}, cols={tbl.num_columns}")
        return tbl

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Updated LOAD written to s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Infer proper schema from CDC files
    # -------------------------------------------------
    def infer_unified_schema(self, load_table, cdc_tables, op_col_name):
        """
        Build unified schema by inferring types from CDC files when LOAD has null columns.
        """
        unified_fields = []
        
        for field in load_table.schema:
            col_name = field.name
            col_type = field.type
            
            # If LOAD column is null type, try to infer from CDC files
            if pa.types.is_null(col_type):
                logger.info(f"Column '{col_name}' in LOAD is null type, inferring from CDC files...")
                inferred_type = None
                
                for cdc_tbl in cdc_tables:
                    if col_name in cdc_tbl.column_names:
                        cdc_col_type = cdc_tbl.schema.field(col_name).type
                        if not pa.types.is_null(cdc_col_type):
                            inferred_type = cdc_col_type
                            logger.info(f"Inferred type for '{col_name}': {inferred_type}")
                            break
                
                # Use inferred type or default to string
                if inferred_type:
                    unified_fields.append(pa.field(col_name, inferred_type))
                else:
                    logger.warning(f"Could not infer type for '{col_name}', defaulting to string")
                    unified_fields.append(pa.field(col_name, pa.string()))
            else:
                unified_fields.append(field)
        
        return pa.schema(unified_fields)

    # -------------------------------------------------
    # Convert LOAD table to unified schema
    # -------------------------------------------------
    def upgrade_load_schema(self, load_table, unified_schema):
        """
        Convert LOAD table columns from null type to proper types.
        """
        cols = []
        for field in unified_schema:
            col_name = field.name
            load_col = load_table[col_name]
            
            if pa.types.is_null(load_col.type) and not pa.types.is_null(field.type):
                logger.info(f"Converting LOAD column '{col_name}' from null to {field.type}")
                # Create array of nulls with correct type
                cols.append(pa.array([None] * load_table.num_rows, type=field.type))
            else:
                cols.append(load_col)
        
        return pa.Table.from_arrays(cols, schema=unified_schema)

    # -------------------------------------------------
    # Schema Alignment (excluding op column)
    # -------------------------------------------------
    def align_schema(self, tbl, target_schema, op_col_name):
        """
        Ensures CDC table (minus op column) has same schema as LOAD.
        - Missing columns → added as null column
        - Type mismatches → cast to LOAD type (fallback: cast to string)
        """
        cols = {}
        for field in target_schema:
            name = field.name
            if name in tbl.column_names:
                col = tbl[name]
                if col.type != field.type:
                    logger.info(f"Type mismatch in column '{name}' → {col.type} vs {field.type}, casting...")
                    try:
                        col = pc.cast(col, field.type)
                    except Exception as e:
                        logger.warning(f"Failed to cast column '{name}' to {field.type}: {e}, forcing STRING type")
                        try:
                            col = pc.cast(col, pa.string())
                        except Exception as e2:
                            logger.error(f"Failed to cast to string: {e2}, using nulls")
                            col = pa.array([None] * tbl.num_rows, type=field.type)
                cols[name] = col
            else:
                logger.info(f"Missing column '{name}' in CDC → filling nulls")
                cols[name] = pa.array([None] * tbl.num_rows, type=field.type)
        
        # Re-add operation column at the beginning
        if op_col_name in tbl.column_names:
            return pa.Table.from_arrays(
                [tbl[op_col_name]] + list(cols.values()), 
                names=[op_col_name] + [f.name for f in target_schema]
            )
        else:
            raise ValueError(f"Operation column '{op_col_name}' not found in CDC table")

    # -------------------------------------------------
    # SAFE UTF8_UPPER for op column
    # -------------------------------------------------
    def safe_upper(self, arr: pa.Array):
        """Safely convert array to uppercase strings"""
        try:
            if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
                return pc.utf8_upper(arr)
            # Convert to string first
            str_arr = pc.cast(arr, pa.string())
            return pc.utf8_upper(str_arr)
        except Exception as e:
            logger.warning(f"Failed to uppercase array: {e}, returning as-is")
            return arr

    # -------------------------------------------------
    # Create composite key for matching
    # -------------------------------------------------
    def create_composite_key(self, df, key_columns):
        """Create a composite key by concatenating specified columns"""
        if not key_columns:
            return None
        
        # Convert all key columns to string and concatenate with separator
        key_parts = [df[col].astype(str) for col in key_columns]
        return key_parts[0].str.cat(key_parts[1:], sep='|~|') if len(key_parts) > 1 else key_parts[0]

    def create_full_row_key(self, df):
        """Create a key from all columns for exact matching"""
        # Concatenate all columns
        all_cols = df.columns.tolist()
        key_parts = [df[col].astype(str).fillna('__NULL__') for col in all_cols]
        return key_parts[0].str.cat(key_parts[1:], sep='|~|') if len(key_parts) > 1 else key_parts[0]

    # -------------------------------------------------
    # NEW: CDC Compaction with partial/full matching
    # -------------------------------------------------
    def compact_cdc_operations(self, cdc_combined, op_col_name, data_columns):
        """
        Compact CDC operations based on matching rules:
        - INSERT: No matching, just keep all inserts
        - UPDATE: Match on update_key_columns (partial match)
        - DELETE: Match on all columns (full match)
        
        Compaction rules:
        - I → U (same key): Keep only U
        - I → D (same full row): Skip both (net zero)
        - U → U (same key): Keep only last U
        - U → D (same full row): Keep only D
        """
        logger.info(f"Starting CDC compaction on {cdc_combined.num_rows} operations")
        
        # Convert to pandas for easier manipulation
        df = cdc_combined.to_pandas()
        df['_seq'] = range(len(df))
        df['_op_upper'] = df[op_col_name].str.upper()
        
        # Split by operation type first
        inserts = df[df['_op_upper'] == 'I'].copy()
        updates = df[df['_op_upper'] == 'U'].copy()
        deletes = df[df['_op_upper'] == 'D'].copy()
        
        logger.info(f"Raw ops → I={len(inserts)}, U={len(updates)}, D={len(deletes)}")
        
        # Create matching keys
        if self.update_key_columns:
            # Partial key for updates
            if len(inserts) > 0:
                inserts['_update_key'] = self.create_composite_key(inserts, self.update_key_columns)
            if len(updates) > 0:
                updates['_update_key'] = self.create_composite_key(updates, self.update_key_columns)
        
        # Full row key for deletes
        if len(inserts) > 0:
            inserts['_full_key'] = self.create_full_row_key(inserts[data_columns])
        if len(updates) > 0:
            updates['_full_key'] = self.create_full_row_key(updates[data_columns])
        if len(deletes) > 0:
            deletes['_full_key'] = self.create_full_row_key(deletes[data_columns])
        
        # Track what to remove
        rows_to_remove = set()
        final_inserts = []
        final_updates = []
        final_deletes = []
        
        # Process INSERTS
        for idx, insert_row in inserts.iterrows():
            removed = False
            
            # Check if this insert is later updated (I → U)
            if self.update_key_columns and '_update_key' in insert_row:
                matching_updates = updates[
                    (updates['_update_key'] == insert_row['_update_key']) & 
                    (updates['_seq'] > insert_row['_seq'])
                ]
                if len(matching_updates) > 0:
                    # Keep only the LAST update
                    last_update = matching_updates.loc[matching_updates['_seq'].idxmax()]
                    final_updates.append(last_update)
                    rows_to_remove.add(insert_row['_seq'])
                    # Mark all these updates as processed
                    rows_to_remove.update(matching_updates['_seq'].tolist())
                    removed = True
                    logger.debug(f"I→U compaction: insert seq={insert_row['_seq']} replaced by update seq={last_update['_seq']}")
            
            # Check if this insert is later deleted (I → D)
            if not removed and '_full_key' in insert_row:
                matching_deletes = deletes[
                    (deletes['_full_key'] == insert_row['_full_key']) & 
                    (deletes['_seq'] > insert_row['_seq'])
                ]
                if len(matching_deletes) > 0:
                    # Net zero change - remove both insert and delete
                    rows_to_remove.add(insert_row['_seq'])
                    rows_to_remove.update(matching_deletes['_seq'].tolist())
                    removed = True
                    logger.debug(f"I→D compaction: insert seq={insert_row['_seq']} cancelled by delete")
            
            # If not removed, keep the insert
            if not removed:
                final_inserts.append(insert_row)
        
        # Process UPDATES (that weren't already handled by I→U)
        for idx, update_row in updates.iterrows():
            if update_row['_seq'] in rows_to_remove:
                continue
            
            removed = False
            
            # Check if this update is later updated (U → U)
            if self.update_key_columns and '_update_key' in update_row:
                matching_updates = updates[
                    (updates['_update_key'] == update_row['_update_key']) & 
                    (updates['_seq'] > update_row['_seq']) &
                    (~updates['_seq'].isin(rows_to_remove))
                ]
                if len(matching_updates) > 0:
                    # This update is superseded by a later one
                    rows_to_remove.add(update_row['_seq'])
                    removed = True
                    continue
            
            # Check if this update is later deleted (U → D)
            if not removed and '_full_key' in update_row:
                matching_deletes = deletes[
                    (deletes['_full_key'] == update_row['_full_key']) & 
                    (deletes['_seq'] > update_row['_seq'])
                ]
                if len(matching_deletes) > 0:
                    # Keep only the delete
                    last_delete = matching_deletes.loc[matching_deletes['_seq'].idxmax()]
                    final_deletes.append(last_delete)
                    rows_to_remove.add(update_row['_seq'])
                    rows_to_remove.update(matching_deletes['_seq'].tolist())
                    removed = True
                    logger.debug(f"U→D compaction: update seq={update_row['_seq']} replaced by delete seq={last_delete['_seq']}")
            
            # If not removed, keep the update
            if not removed:
                final_updates.append(update_row)
        
        # Process DELETES (that weren't already handled)
        for idx, delete_row in deletes.iterrows():
            if delete_row['_seq'] not in rows_to_remove:
                final_deletes.append(delete_row)
        
        # Convert back to DataFrames
        df_ins = inserts.loc[[r['_seq'] for r in final_inserts]] if final_inserts else inserts.iloc[0:0]
        df_upd = updates.loc[[r['_seq'] for r in final_updates]] if final_updates else updates.iloc[0:0]
        df_del = deletes.loc[[r['_seq'] for r in final_deletes]] if final_deletes else deletes.iloc[0:0]
        
        # Clean up helper columns
        for df_temp in [df_ins, df_upd, df_del]:
            cols_to_drop = [c for c in ['_seq', '_op_upper', '_update_key', '_full_key'] if c in df_temp.columns]
            df_temp.drop(columns=cols_to_drop, inplace=True)
            df_temp.drop(columns=[op_col_name], inplace=True, errors='ignore')
        
        logger.info(f"Compaction: {len(df)} → {len(df_ins) + len(df_upd) + len(df_del)} operations")
        logger.info(f"Compacted CDC → INSERT={len(df_ins)}, UPDATE={len(df_upd)}, DELETE={len(df_del)}")
        
        # Convert back to PyArrow tables
        schema_without_op = pa.schema([f for f in cdc_combined.schema if f.name != op_col_name])
        
        tbl_ins = pa.Table.from_pandas(df_ins, schema=schema_without_op, preserve_index=False) if not df_ins.empty else pa.table({}, schema=schema_without_op)
        tbl_upd = pa.Table.from_pandas(df_upd, schema=schema_without_op, preserve_index=False) if not df_upd.empty else pa.table({}, schema=schema_without_op)
        tbl_del = pa.Table.from_pandas(df_del, schema=schema_without_op, preserve_index=False) if not df_del.empty else pa.table({}, schema=schema_without_op)
        
        return tbl_ins, tbl_upd, tbl_del

    # -------------------------------------------------
    # Apply UPDATE using partial key matching
    # -------------------------------------------------
    def apply_updates(self, load_df, update_df):
        """Apply updates by matching on update_key_columns (partial match)"""
        if update_df.num_rows == 0:
            return load_df
        
        if not self.update_key_columns:
            logger.warning("No update key columns specified, using first column")
            self.update_key_columns = [load_df.column_names[0]]
        
        logger.info(f"Applying {update_df.num_rows} updates using key columns: {self.update_key_columns}")
        
        # Convert to pandas for easier manipulation
        df_load = load_df.to_pandas()
        df_upd = update_df.to_pandas()
        
        # Create composite keys
        load_key = self.create_composite_key(df_load, self.update_key_columns)
        upd_key = self.create_composite_key(df_upd, self.update_key_columns)
        
        df_load['_match_key'] = load_key
        df_upd['_match_key'] = upd_key
        
        # Remove rows that will be updated
        upd_keys = set(df_upd['_match_key'].unique())
        df_remaining = df_load[~df_load['_match_key'].isin(upd_keys)].drop(columns=['_match_key'])
        
        # Add updated rows
        df_upd_clean = df_upd.drop(columns=['_match_key'])
        df_result = pd.concat([df_remaining, df_upd_clean], ignore_index=True)
        
        logger.info(f"Updates applied: {len(df_load)} → {len(df_result)} rows")
        
        # Convert back to PyArrow
        return pa.Table.from_pandas(df_result, schema=load_df.schema, preserve_index=False)

    # -------------------------------------------------
    # Apply DELETE using full row matching
    # -------------------------------------------------
    def apply_deletes(self, load_df, delete_df):
        """Apply deletes by matching on ALL columns (exact match)"""
        if delete_df.num_rows == 0:
            return load_df
        
        logger.info(f"Applying {delete_df.num_rows} deletes using full row match")
        
        # Convert to pandas
        df_load = load_df.to_pandas()
        df_del = delete_df.to_pandas()
        
        # Create full row keys
        all_cols = df_load.columns.tolist()
        load_key = self.create_full_row_key(df_load)
        del_key = self.create_full_row_key(df_del)
        
        df_load['_full_key'] = load_key
        df_del['_full_key'] = del_key
        
        # Remove matching rows
        del_keys = set(df_del['_full_key'].unique())
        df_result = df_load[~df_load['_full_key'].isin(del_keys)].drop(columns=['_full_key'])
        
        logger.info(f"Deletes applied: {len(df_load)} → {len(df_result)} rows")
        
        # Convert back to PyArrow
        return pa.Table.from_pandas(df_result, schema=load_df.schema, preserve_index=False)

    # -------------------------------------------------
    # Main CDC Processor
    # -------------------------------------------------
    def process(self, cdc_key):
        logger.info(f"Processing CDC file: {cdc_key}")
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_file = f"{prefix}LOAD00000001.csv"
        logger.info(f"Loading base LOAD file: {load_file}")

        # LOAD base dataset
        self.df = self.load_arrow_table(load_file)
        initial_rows = self.df.num_rows
        
        # Set update key columns to first column if not specified
        if not self.update_key_columns:
            self.update_key_columns = [self.df.column_names[0]]
        
        logger.info(f"Update key columns: {self.update_key_columns}")

        # Discover CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found.")
            return

        logger.info(f"Processing {len(cdc_files)} CDC files...")

        # First, peek at first CDC file to get operation column name and infer schema
        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col_name = first_cdc.column_names[0]
        logger.info(f"Operation column detected: {op_col_name}")

        # Sample first few CDC files for schema inference (max 5 to save time)
        sample_cdc_tables = [first_cdc]
        for f in cdc_files[1:min(5, len(cdc_files))]:
            sample_cdc_tables.append(self.load_arrow_table(f))
        
        # Infer unified schema from LOAD + sample CDC files
        unified_schema = self.infer_unified_schema(self.df, sample_cdc_tables, op_col_name)
        logger.info(f"Unified schema: {unified_schema}")

        # Upgrade LOAD table to unified schema
        self.df = self.upgrade_load_schema(self.df, unified_schema)
        logger.info("LOAD table upgraded to unified schema")

        # Load and align all CDC files
        logger.info(f"Loading and aligning {len(cdc_files)} CDC files...")
        all_cdc_tables = []
        
        for f in cdc_files:
            raw_tbl = self.load_arrow_table(f)
            aligned = self.align_schema(raw_tbl, unified_schema, op_col_name)
            all_cdc_tables.append(aligned)
        
        # Combine all CDC operations into single table
        cdc_combined = pa.concat_tables(all_cdc_tables) if len(all_cdc_tables) > 1 else all_cdc_tables[0]
        logger.info(f"Combined CDC operations: {cdc_combined.num_rows} rows")
        
        # Normalize operation column to uppercase
        op_arr_upper = self.safe_upper(cdc_combined[op_col_name])
        cdc_combined = cdc_combined.set_column(0, op_col_name, op_arr_upper)
        
        # Get data columns (all except operation column)
        data_columns = [col for col in cdc_combined.column_names if col != op_col_name]
        
        # **Compact CDC operations with partial/full matching**
        df_ins, df_upd, df_del = self.compact_cdc_operations(cdc_combined, op_col_name, data_columns)

        # -------------------------------------------------
        # Apply operations in order: DELETE → UPDATE → INSERT
        # -------------------------------------------------
        
        # DELETE (full row match)
        self.df = self.apply_deletes(self.df, df_del)
        
        # UPDATE (partial key match)
        self.df = self.apply_updates(self.df, df_upd)
        
        # INSERT (just append)
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])
            logger.info(f"INSERT rows added: {df_ins.num_rows}")

        # Archive CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Archive old LOAD
        new_load_archive = self.get_processed_path(load_file, add_timestamp=True)
        self.move_file(load_file, new_load_archive)

        # Write updated LOAD
        out = self.write_arrow_table(load_file, self.df)
        final_rows = self.df.num_rows

        result = {
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": final_rows,
            "row_change": final_rows - initial_rows,
            "inserts": df_ins.num_rows,
            "updates": df_upd.num_rows,
            "deletes": df_del.num_rows,
            "output": out,
            "time_sec": (datetime.utcnow() - start).total_seconds()
        }
        logger.info(json.dumps(result))


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info("Lambda triggered:")
    logger.info(json.dumps(event))

    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "/processed/" in key or not key.endswith(".csv") or "LOAD" in key:
                logger.info(f"Skipping file: {key}")
                continue

            table_name = key.split("/")[-2]
            
            # Define which columns to use for UPDATE matching
            # Adjust this based on your table structure
            update_key_columns = None  # Will default to first column
            # Example: update_key_columns = ['id', 'customer_code']
            
            proc = CDCProcessorArrow(bucket, table_name, update_key_columns)
            proc.process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(f"Error in CDC Lambda: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({
            "status": "error",
            "message": str(e)
        })}

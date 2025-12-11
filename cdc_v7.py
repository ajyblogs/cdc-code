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

        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")

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
    # FIX: Schema Alignment (excluding op column)
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
        self.pk_col = self.df.column_names[0]
        logger.info(f"Primary key column: {self.pk_col}")

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

        # Process CDC files in batches to avoid memory issues
        batch_size = 10
        all_inserts = []
        all_updates = []
        all_deletes = []
        
        for i in range(0, len(cdc_files), batch_size):
            batch = cdc_files[i:i + batch_size]
            logger.info(f"Processing CDC batch {i//batch_size + 1}/{(len(cdc_files)-1)//batch_size + 1} ({len(batch)} files)")
            
            # Load and align batch
            batch_tables = []
            for f in batch:
                raw_tbl = self.load_arrow_table(f)
                aligned = self.align_schema(raw_tbl, unified_schema, op_col_name)
                batch_tables.append(aligned)
            
            # Merge batch
            if len(batch_tables) == 1:
                batch_combined = batch_tables[0]
            else:
                batch_combined = pa.concat_tables(batch_tables)
            
            logger.info(f"Batch combined: {batch_combined.num_rows} rows")
            
            # Parse operations in batch
            op_arr = self.safe_upper(batch_combined[op_col_name])
            
            batch_ins = batch_combined.filter(pc.equal(op_arr, pa.scalar("I"))).remove_column(0)
            batch_upd = batch_combined.filter(pc.equal(op_arr, pa.scalar("U"))).remove_column(0)
            batch_del = batch_combined.filter(pc.equal(op_arr, pa.scalar("D"))).remove_column(0)
            
            if batch_ins.num_rows > 0:
                all_inserts.append(batch_ins)
            if batch_upd.num_rows > 0:
                all_updates.append(batch_upd)
            if batch_del.num_rows > 0:
                all_deletes.append(batch_del)
        
        # Combine all operations
        df_ins = pa.concat_tables(all_inserts) if all_inserts else pa.table({}, schema=unified_schema)
        df_upd = pa.concat_tables(all_updates) if all_updates else pa.table({}, schema=unified_schema)
        df_del = pa.concat_tables(all_deletes) if all_deletes else pa.table({}, schema=unified_schema)
        
        logger.info(f"CDC Ops → INSERT={df_ins.num_rows}, UPDATE={df_upd.num_rows}, DELETE={df_del.num_rows}")

        # -------------------------------------------------
        # IMPROVED FIX: Build final state map for each PK
        # Handles INSERT+UPDATE, INSERT+DELETE, UPDATE+DELETE correctly
        # -------------------------------------------------
        
        # Track operation counts for reporting
        inserts_applied = 0
        updates_applied = 0
        deletes_applied = 0
        
        # Step 1: Collect all PKs that have any CDC operations
        all_cdc_pks = set()
        
        if df_ins.num_rows > 0:
            all_cdc_pks.update(df_ins[self.pk_col].to_pylist())
            
        if df_upd.num_rows > 0:
            all_cdc_pks.update(df_upd[self.pk_col].to_pylist())
            
        if df_del.num_rows > 0:
            all_cdc_pks.update(df_del[self.pk_col].to_pylist())
        
        logger.info(f"Total unique PKs affected by CDC: {len(all_cdc_pks)}")
        
        # Step 2: Build final state map for each PK
        # Last operation wins (in order: INSERT → UPDATE → DELETE)
        final_state = {}  # pk -> (operation, row_dict) or (operation, None)
        
        # Process INSERTs first
        if df_ins.num_rows > 0:
            ins_dict = df_ins.to_pydict()
            for idx in range(df_ins.num_rows):
                pk = ins_dict[self.pk_col][idx]
                row = {col: ins_dict[col][idx] for col in df_ins.column_names}
                final_state[pk] = ('INSERT', row)
                inserts_applied += 1
        
        # Process UPDATEs (overwrites INSERTs if same PK)
        if df_upd.num_rows > 0:
            upd_dict = df_upd.to_pydict()
            for idx in range(df_upd.num_rows):
                pk = upd_dict[self.pk_col][idx]
                row = {col: upd_dict[col][idx] for col in df_upd.column_names}
                if pk in final_state:
                    logger.info(f"PK {pk}: UPDATE overriding previous {final_state[pk][0]}")
                final_state[pk] = ('UPDATE', row)
                updates_applied += 1
        
        # Process DELETEs (marks for deletion, overwrites any previous operation)
        if df_del.num_rows > 0:
            del_pks = df_del[self.pk_col].to_pylist()
            for pk in del_pks:
                if pk in final_state:
                    logger.info(f"PK {pk}: DELETE overriding previous {final_state[pk][0]}")
                final_state[pk] = ('DELETE', None)
                deletes_applied += 1
        
        logger.info(f"Final state map: {len(final_state)} PKs with operations")
        logger.info(f"Operations applied: INSERT={inserts_applied}, UPDATE={updates_applied}, DELETE={deletes_applied}")
        
        # Step 3: Apply final state
        # Remove all rows that have CDC operations
        current_pks = self.df[self.pk_col].to_pylist()
        keep_mask = [pk not in all_cdc_pks for pk in current_pks]
        keep_indices = [i for i, keep in enumerate(keep_mask) if keep]
        
        if keep_indices:
            self.df = self.df.take(keep_indices)
        else:
            # All rows affected, start with empty table
            self.df = pa.table({col: pa.array([], type=self.df.schema.field(col).type) 
                               for col in self.df.column_names})
        
        logger.info(f"Removed {len(current_pks) - len(keep_indices)} rows affected by CDC operations")
        
        # Step 4: Add back rows based on final state (INSERT or UPDATE)
        rows_to_add = []
        for pk, (operation, row_data) in final_state.items():
            if operation in ('INSERT', 'UPDATE'):
                rows_to_add.append(row_data)
        
        if rows_to_add:
            # Convert list of dicts to Arrow table
            new_rows_dict = {col: [] for col in self.df.column_names}
            for row in rows_to_add:
                for col in self.df.column_names:
                    new_rows_dict[col].append(row[col])
            
            new_rows_table = pa.table(new_rows_dict, schema=self.df.schema)
            self.df = pa.concat_tables([self.df, new_rows_table])
            logger.info(f"Added {len(rows_to_add)} rows from final state (INSERTs + UPDATEs)")
        
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
            proc = CDCProcessorArrow(bucket, table_name)
            proc.process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error(f"Error in CDC Lambda: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({
            "status": "error",
            "message": str(e)
        })}

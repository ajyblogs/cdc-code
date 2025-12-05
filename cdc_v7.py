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
        for i,p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [filename])
        raise ValueError("Invalid CDC structure")

    def move_file(self, src, dst):
        s3.copy_object(Bucket=self.bucket, Key=dst,
                       CopySource={"Bucket": self.bucket, "Key": src})
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
        return sorted(out)

    # -------------------------------------------------
    # Load / Write functions
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded table: {key} → rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        # Efficient writer
        csv_buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_buf)
        logger.info(f"Written updated LOAD → {key}")

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
    # MAIN PROCESSOR
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        prefix = self.get_load_prefix(cdc_key)
        load_file = f"{prefix}LOAD00000001.csv"

        # Load base load
        self.df = self.load_arrow_table(load_file)
        initial_rows = self.df.num_rows
        self.pk_col = self.df.column_names[0]

        # Find CDC files
        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            logger.info("No CDC files found.")
            return

        # Fetch first file for OP column name
        first_cdc = self.load_arrow_table(cdc_files[0])
        op_col_name = first_cdc.column_names[0]

        # Align all CDC files
        aligned_tables = []
        for f in cdc_files:
            raw_tbl = self.load_arrow_table(f)
            aligned_tbl = self.align_schema(raw_tbl, self.df.schema, op_col_name)
            aligned_tables.append(aligned_tbl)

        # Combine all CDC
        cdc_all = pa.concat_tables(aligned_tables)
        op_arr = self.safe_upper(cdc_all[op_col_name])

        # Split CDC
        df_ins = cdc_all.filter(pc.equal(op_arr, pa.scalar("I"))).remove_column(0)
        df_upd = cdc_all.filter(pc.equal(op_arr, pa.scalar("U"))).remove_column(0)
        df_del = cdc_all.filter(pc.equal(op_arr, pa.scalar("D"))).remove_column(0)

        # DELETE
        if df_del.num_rows > 0:
            del_keys = df_del[self.pk_col].combine_chunks()
            del_mask = pc.is_in(self.df[self.pk_col], value_set=del_keys)
            self.df = self.df.filter(pc.invert(del_mask))

        # UPDATE
        if df_upd.num_rows > 0:
            upd_keys = df_upd[self.pk_col].combine_chunks()
            upd_mask = pc.is_in(self.df[self.pk_col], value_set=upd_keys)
            self.df = self.df.filter(pc.invert(upd_mask))
            self.df = pa.concat_tables([self.df, df_upd])

        # INSERT
        if df_ins.num_rows > 0:
            self.df = pa.concat_tables([self.df, df_ins])

        final_rows = self.df.num_rows

        # Move all CDC files
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # -------------------------------------------------
        # SAFE LOAD ROTATION (TEMP → ARCHIVE → PROMOTE)
        # -------------------------------------------------
        tmp_load = load_file + ".tmp"
        self.write_arrow_table(tmp_load, self.df)

        # Archive only if it exists
        try:
            s3.head_object(Bucket=self.bucket, Key=load_file)
            archive_key = self.get_processed_path(load_file, add_timestamp=True)
            self.move_file(load_file, archive_key)
        except:
            logger.warning("No existing LOAD to archive.")

        # Promote temp → final LOAD
        self.move_file(tmp_load, load_file)

        # -------------------------------------------------
        # SUMMARY RESULT
        # -------------------------------------------------
        result = {
            "status": "success",
            "table": self.table,
            "bucket": self.bucket,
            "initial_rows": initial_rows,
            "final_rows": final_rows,
            "row_change": final_rows - initial_rows,
            "inserts": df_ins.num_rows,
            "updates": df_upd.num_rows,
            "deletes": df_del.num_rows,
            "cdc_files_processed": len(cdc_files),
            "load_file": load_file,
            "time_sec": (datetime.utcnow() - start).total_seconds()
        }
        logger.info(f"CDC Processing Summary: {json.dumps(result)}")
        
        return result


# -------------------------------------------------
# Lambda handler
# -------------------------------------------------
def lambda_handler(event, context):
    results = []
    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "/processed/" in key or "LOAD" in key or not key.endswith(".csv"):
                continue

            table_name = key.split("/")[-2]
            result = CDCProcessorArrow(bucket, table_name).process(key)
            if result:
                results.append(result)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "success",
                "processed_tables": len(results),
                "results": results
            })
        }

    except Exception as e:
        logger.error(str(e), exc_info=True)
        return {"statusCode": 500, "body": str(e)}

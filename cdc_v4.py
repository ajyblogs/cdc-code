import json
import boto3
import pandas as pd
from datetime import datetime
from io import BytesIO, StringIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')


class CDCProcessor:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df = None
        self.pk_col = None
        self.op_col = None

    # ------------------------
    # Read CSV in chunks
    # ------------------------
    def read_csv_chunks(self, key, chunksize=5000):
        try:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
            stream = obj['Body']
            for chunk in pd.read_csv(stream, chunksize=chunksize, sep='|'):
                yield chunk
        except Exception as e:
            logger.error(f"Error reading CSV {key}: {e}")
            raise

    # ------------------------
    # Write CSV to S3
    # ------------------------
    def write_csv(self, key):
        buffer = BytesIO()
        self.df.to_csv(buffer, index=False, sep='|')
        buffer.seek(0)
        s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{key}"

    # ------------------------
    # Move file in S3
    # ------------------------
    def move_file(self, source_key, target_key):
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': source_key},
                       Bucket=self.bucket, Key=target_key)
        s3.delete_object(Bucket=self.bucket, Key=source_key)
        logger.info(f"Moved {source_key} -> {target_key}")

    # ------------------------
    # Generate processed path
    # ------------------------
    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split('/')
        for i, part in enumerate(parts):
            if part.startswith('DSET'):
                filename = parts[-1]
                if add_timestamp:
                    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
                    filename = filename.replace('.csv', f'_{timestamp}.csv')
                return '/'.join(parts[:i+1] + ['processed'] + parts[i+1:-1] + [filename])
        raise ValueError(f"DSET not found in path: {key}")

    # ------------------------
    # List CDC files
    # ------------------------
    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') and 'LOAD' not in key and '/processed/' not in key:
                    files.append(key)
        return sorted(files)

    # ------------------------
    # Extract table name from path
    # ------------------------
    def extract_table_name(self, key):
        parts = key.split('/')
        for part in parts:
            if part.startswith('DSET'):
                return part
        return 'unknown'

    # ------------------------
    # Get LOAD prefix
    # ------------------------
    def get_load_prefix(self, cdc_key):
        parts = cdc_key.split('/')
        return '/'.join(parts[:-1]) + '/'

    # ------------------------
    # Match primary key
    # ------------------------
    def match_by_primary_key(self, row):
        df_col = self.df[self.pk_col].astype(str).str.strip().str.lower()
        df_col = df_col.replace('nan', '')
        cdc_value = str(row[self.pk_col]).strip().lower()
        if cdc_value == 'nan':
            cdc_value = ''
        return df_col == cdc_value

    # ------------------------
    # Match all columns for DELETE
    # ------------------------
    def match_all_columns(self, row, exclude_op_col=True):
        cols_to_match = [col for col in row.index if col in self.df.columns and (not exclude_op_col or col != self.op_col)]
        if not cols_to_match:
            return pd.Series([False] * len(self.df), index=self.df.index)
        mask = pd.Series([True] * len(self.df), index=self.df.index)
        for col in cols_to_match:
            df_col = self.df[col].astype(str).str.strip().str.lower()
            df_col = df_col.replace('nan', '')
            cdc_value = str(row[col]).strip().lower()
            if cdc_value == 'nan':
                cdc_value = ''
            mask &= (df_col == cdc_value)
        return mask

    # ------------------------
    # Apply single CDC row
    # ------------------------
    def apply_cdc_operation(self, row):
        op = str(row[self.op_col]).strip().upper()
        if op in ['I', 'INSERT']:
            new_row = row.drop(self.op_col).to_frame().T
            self.df = pd.concat([self.df, new_row], ignore_index=True)
            return 'I'
        elif op in ['U', 'UPDATE']:
            mask = self.match_by_primary_key(row)
            if mask.any():
                for col in row.index:
                    if col != self.op_col and col in self.df.columns:
                        self.df.loc[mask, col] = row[col]
                logger.info(f"UPDATE: Matched {mask.sum()} row(s) by PK")
                return 'U'
            else:
                new_row = row.drop(self.op_col).to_frame().T
                self.df = pd.concat([self.df, new_row], ignore_index=True)
                logger.warning("UPDATE: No matching PK found, inserted as new row")
                return 'U'
        elif op in ['D', 'DELETE']:
            mask = self.match_all_columns(row)
            if mask.any():
                deleted_count = mask.sum()
                self.df = self.df.loc[~mask].reset_index(drop=True)
                logger.info(f"DELETE: Removed {deleted_count} row(s)")
                return 'D'
            else:
                logger.warning("DELETE: No matching row found - skipped")
                return 'X'
        else:
            logger.warning(f"Unknown operation: {op}")
            return 'X'

    # ------------------------
    # Main CDC processing
    # ------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        load_prefix = self.get_load_prefix(cdc_key)
        load_key = f"{load_prefix}LOAD00000001.csv"

        # Load base table in chunks
        logger.info(f"Loading base file {load_key}")
        chunk_list = []
        for chunk in self.read_csv_chunks(load_key, chunksize=300):
            chunk_list.append(chunk)
        self.df = pd.concat(chunk_list, ignore_index=True)
        initial_rows = len(self.df)

        # List CDC files
        cdc_files = self.list_cdc_files(load_prefix)
        if not cdc_files:
            logger.info("No CDC files found")
            return {'status': 'success', 'message': 'No CDC files to process', 'table_name': self.table}

        # Setup PK and OP column
        first_cdc = next(self.read_csv_chunks(cdc_files[0], chunksize=300))
        self.pk_col = self.df.columns[0]
        self.op_col = first_cdc.columns[0]

        # Apply CDC
        ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}
        for i, cdc_file in enumerate(cdc_files):
            logger.info(f"Processing CDC file {i+1}/{len(cdc_files)}: {cdc_file}")
            for chunk in self.read_csv_chunks(cdc_file, chunksize=300):
                for _, row in chunk.iterrows():
                    op = self.apply_cdc_operation(row)
                    ops[op] += 1
            processed_path = self.get_processed_path(cdc_file)
            self.move_file(cdc_file, processed_path)

        # Move LOAD file to processed
        load_processed = self.get_processed_path(load_key, add_timestamp=True)
        self.move_file(load_key, load_processed)

        # Write updated LOAD
        output = self.write_csv(load_key)

        return {
            'status': 'success',
            'table_name': self.table,
            'initial_rows': initial_rows,
            'final_rows': len(self.df),
            'row_change': len(self.df) - initial_rows,
            'cdc_files_processed': len(cdc_files),
            'operations': {'inserts': ops['I'], 'updates': ops['U'], 'deletes': ops['D'], 'skipped': ops['X']},
            'output_location': output,
            'processing_time_seconds': (datetime.utcnow() - start).total_seconds()
        }


# ------------------------
# Lambda Handler
# ------------------------
def lambda_handler(event, context):
    try:
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            logger.info(f"S3 Event → Bucket: {bucket}, Key: {key}")

            if '/processed/' in key or not key.endswith('.csv') or 'LOAD' in key:
                logger.info(f"Skipping file: {key}")
                continue

            processor = CDCProcessor(bucket, key.split('/')[-2])
            processor.table = processor.extract_table_name(key)
            result = processor.process(key)
            return {'statusCode': 200, 'body': json.dumps(result)}

        return {'statusCode': 200, 'body': json.dumps({'status': 'skipped', 'message': 'No valid CDC files'})}

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {'statusCode': 500, 'body': json.dumps({'status': 'error', 'message': str(e)})}

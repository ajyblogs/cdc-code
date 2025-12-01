import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')


class CDCProcessor:
    def __init__(self, bucket, table, chunk_size=300):
        self.bucket = bucket
        self.table = table
        self.df = None
        self.pk_col = None
        self.op_col = None
        self.chunk_size = chunk_size

    def read_csv(self, key):
        """Read CSV from S3 with encoding fallback."""
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        content = obj['Body'].read()

        for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                return pd.read_csv(StringIO(content.decode(encoding)), sep='|')
            except UnicodeDecodeError:
                continue

        return pd.read_csv(StringIO(content.decode('latin-1', errors='replace')), sep='|')

    def read_cdc_chunks(self, key):
        """Stream CDC file in chunks."""
        logger.info(f"Reading CDC in chunks (size={self.chunk_size}) → {key}")
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(obj['Body'], sep='|', chunksize=self.chunk_size)

    def write_csv(self, key):
        """Write DataFrame to S3."""
        buffer = StringIO()
        self.df.to_csv(buffer, index=False, sep='|')
        s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{key}"

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

    def move_file(self, source_key, target_key):
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': source_key},
                      Bucket=self.bucket, Key=target_key)
        s3.delete_object(Bucket=self.bucket, Key=source_key)
        logger.info(f"Moved file → {target_key}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') and 'LOAD' not in key and '/processed/' not in key:
                    files.append(key)
        return sorted(files)

    def extract_table_name(self, key):
        parts = key.split('/')
        for part in parts:
            if part.startswith('DSET'):
                return part
        return 'unknown'

    def get_load_prefix(self, cdc_key):
        return '/'.join(cdc_key.split('/')[:-1]) + '/'

    def match_by_primary_key(self, row):
        df_col = self.df[self.pk_col].astype(str).str.strip().str.lower().replace('nan', '')
        cdc_value = str(row[self.pk_col]).strip().lower()
        if cdc_value == 'nan':
            cdc_value = ''
        return df_col == cdc_value

    def match_all_columns(self, row, exclude_op_col=True):
        cols_to_match = [
            col for col in row.index
            if col in self.df.columns and (not exclude_op_col or col != self.op_col)
        ]

        if not cols_to_match:
            return pd.Series([False] * len(self.df), index=self.df.index)

        mask = pd.Series([True] * len(self.df), index=self.df.index)

        for col in cols_to_match:
            df_col = self.df[col].astype(str).str.strip().str.lower().replace('nan', '')
            r_val = str(row[col]).strip().lower()
            if r_val == 'nan':
                r_val = ''
            mask &= (df_col == r_val)

        return mask

    def apply_cdc_operation(self, row):
        op = str(row[self.op_col]).strip().upper()

        if op in ['I', 'INSERT']:
            self.df = pd.concat([self.df, row.drop(self.op_col).to_frame().T], ignore_index=True)
            return 'I'

        elif op in ['U', 'UPDATE']:
            mask = self.match_by_primary_key(row)
            if mask.any():
                for col in row.index:
                    if col != self.op_col and col in self.df.columns:
                        self.df.loc[mask, col] = row[col]
                return 'U'
            else:
                self.df = pd.concat([self.df, row.drop(self.op_col).to_frame().T], ignore_index=True)
                return 'U'

        elif op in ['D', 'DELETE']:
            mask = self.match_all_columns(row)
            if mask.any():
                self.df = self.df.loc[~mask].reset_index(drop=True)
                return 'D'
            else:
                return 'X'

        return 'X'

    def process(self, cdc_key):
        start = datetime.utcnow()

        load_prefix = self.get_load_prefix(cdc_key)
        load_key = f"{load_prefix}LOAD00000001.csv"

        logger.info(f"Loading base table → {load_key}")
        self.df = self.read_csv(load_key)
        initial_rows = len(self.df)

        cdc_files = self.list_cdc_files(load_prefix)
        if not cdc_files:
            logger.info("No CDC files found.")
            return {
                'status': 'success',
                'message': 'No CDC files to process',
                'table_name': self.table
            }

        logger.info(f"Found {len(cdc_files)} CDC files to process")

        first_cdc = self.read_csv(cdc_files[0])
        self.pk_col = self.df.columns[0]
        self.op_col = first_cdc.columns[0]

        ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}

        for cdc_file in cdc_files:
            logger.info(f"Processing CDC file → {cdc_file}")

            for chunk in pd.read_csv(
                s3.get_object(Bucket=self.bucket, Key=cdc_file)['Body'],
                sep='|',
                chunksize=self.chunk_size
            ):
                for _, row in chunk.iterrows():
                    result = self.apply_cdc_operation(row)
                    ops[result] += 1

            processed_path = self.get_processed_path(cdc_file)
            self.move_file(cdc_file, processed_path)

        load_processed = self.get_processed_path(load_key, add_timestamp=True)
        self.move_file(load_key, load_processed)

        output = self.write_csv(load_key)

        # 🔥 YOUR ORIGINAL RETURN BLOCK EXACTLY PRESERVED
        return {
            'status': 'success',
            'table_name': self.table,
            'initial_rows': initial_rows,
            'final_rows': len(self.df),
            'row_change': len(self.df) - initial_rows,
            'cdc_files_processed': len(cdc_files),
            'operations': {
                'inserts': ops['I'],
                'updates': ops['U'],
                'deletes': ops['D'],
                'skipped': ops['X']
            },
            'output_location': output,
            'processing_time_seconds': (datetime.utcnow() - start).total_seconds()
        }


def lambda_handler(event, context):
    try:
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            logger.info(f"S3 Event → Bucket: {bucket}, Key: {key}")

            if '/processed/' in key or not key.endswith('.csv') or 'LOAD' in key:
                continue

            processor = CDCProcessor(bucket, "unknown")
            processor.table = processor.extract_table_name(key)

            result = processor.process(key)
            return {'statusCode': 200, 'body': json.dumps(result)}

        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'skipped', 'message': 'No valid CDC files'})
        }

    except Exception as e:
        logger.error(str(e), exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'message': str(e)})
        }

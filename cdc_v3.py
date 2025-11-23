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
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df = None
        self.op_col = None

    def read_csv(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        content = obj['Body'].read()

        for enc in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                return pd.read_csv(StringIO(content.decode(enc)), sep='|')
            except UnicodeDecodeError:
                continue

        return pd.read_csv(StringIO(content.decode('latin-1', errors='replace')), sep='|')

    def write_csv(self, key):
        buffer = StringIO()
        self.df.to_csv(buffer, index=False, sep='|')
        s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{key}"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split('/')
        for i, p in enumerate(parts):
            if p.startswith('DSET'):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
                    filename = filename.replace('.csv', f'_{ts}.csv')
                return '/'.join(parts[:i+1] + ['processed'] + parts[i+1:-1] + [filename])
        raise ValueError("DSET folder not found.")

    def move_file(self, source_key, target_key):
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': source_key},
                       Bucket=self.bucket, Key=target_key)
        s3.delete_object(Bucket=self.bucket, Key=source_key)
        logger.info(f"Moved: {source_key} -> {target_key}")

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
        for p in key.split('/'):
            if p.startswith('DSET'):
                return p
        return 'unknown'

    def get_load_prefix(self, key):
        return '/'.join(key.split('/')[:-1]) + '/'

    def apply_cdc_operation(self, row):
        op = str(row[self.op_col]).strip().upper()

        match_cols = [col for col in row.index if col != self.op_col and col in self.df.columns]

        mask = pd.Series(True, index=self.df.index)
        for col in match_cols:
            df_val = self.df[col].astype(str).str.strip().str.lower().fillna("")
            cdc_val = str(row[col]).strip().lower()
            mask &= (df_val == cdc_val)

        if op in ['I', 'INSERT']:
            self.df = pd.concat([self.df, row.drop(self.op_col).to_frame().T], ignore_index=True)
            return 'I'

        elif op in ['U', 'UPDATE']:
            if mask.any():
                idx = mask.idxmax()
                for col in match_cols:
                    self.df.loc[idx, col] = row[col]
                return 'U'
            else:
                self.df = pd.concat([self.df, row.drop(self.op_col).to_frame().T], ignore_index=True)
                return 'I'

        elif op in ['D', 'DELETE']:
            if mask.any():
                self.df = self.df.loc[~mask].reset_index(drop=True)
                return 'D'
            logger.warning(f"DELETE skipped — no match found.")
            return 'X'

        return 'X'

    def process(self, cdc_key):
        start = datetime.utcnow()

        load_prefix = self.get_load_prefix(cdc_key)
        load_key = f"{load_prefix}LOAD00000001.csv"

        self.df = self.read_csv(load_key)
        initial_rows = len(self.df)

        cdc_files = self.list_cdc_files(load_prefix)

        if not cdc_files:
            return {'status': 'success', 'message': 'No CDC files found', 'table': self.table}

        first_file = self.read_csv(cdc_files[0])
        self.op_col = first_file.columns[0]

        ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}

        for cdc_file in cdc_files:
            cdc_df = self.read_csv(cdc_file)
            for _, row in cdc_df.iterrows():
                op = self.apply_cdc_operation(row)
                ops[op] += 1

            self.move_file(cdc_file, self.get_processed_path(cdc_file))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        output = self.write_csv(load_key)

        return {
            'status': 'success',
            'table': self.table,
            'initial_rows': initial_rows,
            'final_rows': len(self.df),
            'operations': ops,
            'output': output,
            'processing_seconds': (datetime.utcnow() - start).total_seconds()
        }


def lambda_handler(event, context):
    try:
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            if '/processed/' in key or not key.endswith('.csv') or 'LOAD' in key:
                continue

            processor = CDCProcessor(bucket, processor.extract_table_name(key))
            result = processor.process(key)
            return {'statusCode': 200, 'body': json.dumps(result)}

        return {'statusCode': 200, 'body': json.dumps({'status': 'no-op'})}

    except Exception as e:
        logger.error(str(e), exc_info=True)
        return {'statusCode': 500, 'body': str(e)}

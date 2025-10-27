import json
import boto3
import pandas as pd
from datetime import datetime, timedelta
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
        self.pk_col = None
        self.op_col = None
        
    def read_csv(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        content = obj['Body'].read()
        
        # Try different encodings
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
            try:
                return pd.read_csv(StringIO(content.decode(encoding)), sep='|')
            except UnicodeDecodeError:
                continue
        
        # If all fail, use latin-1 with errors='replace'
        return pd.read_csv(StringIO(content.decode('latin-1', errors='replace')), sep='|')
    
    def write_csv(self, key):
        buffer = StringIO()
        self.df.to_csv(buffer, index=False, sep='|')
        s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{key}"
    
    def list_s3_files(self, prefix, pattern=None):
        """Common function to list S3 CSV files with optional pattern filtering"""
        files = []
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') and (not pattern or pattern in key):
                    files.append(key)
        
        return sorted(files)
    
    def get_load_file(self, prefix):
        """Get LOAD00000001.csv file"""
        return f"{prefix}LOAD00000001.csv"
    
    def list_cdc_files(self, prefix):
        """List all CDC files (non-LOAD files), excluding processed folder"""
        return [f for f in self.list_s3_files(prefix) if 'LOAD' not in f and '/processed/' not in f]
    
    def move_to_processed(self, key):
        processed_key = f"{key.rsplit('/', 1)[0]}/processed/{key.split('/')[-1]}"
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': key}, Bucket=self.bucket, Key=processed_key)
        s3.delete_object(Bucket=self.bucket, Key=key)
        return processed_key
    
    def move_load_to_processed(self, key):
        """Move LOAD file to processed folder with timestamp"""
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        filename = key.split('/')[-1]
        # Replace LOAD00000001.csv with LOAD00000001_20241026120000.csv
        new_filename = filename.replace('.csv', f'_{timestamp}.csv')
        processed_key = f"{key.rsplit('/', 1)[0]}/processed/{new_filename}"
        
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': key}, Bucket=self.bucket, Key=processed_key)
        s3.delete_object(Bucket=self.bucket, Key=key)
        logger.info(f"Moved LOAD file to: {processed_key}")
        return processed_key
    
    def apply_operation(self, row):
        op = str(row[self.op_col]).strip().upper()
        pk_val = row[self.pk_col]
        mask = self.df[self.pk_col] == pk_val
        
        if op in ['I', 'INSERT']:
            if not mask.any():
                self.df = pd.concat([self.df, row.drop(self.op_col).to_frame().T], ignore_index=True)
            return 'I'
        elif op in ['U', 'UPDATE']:
            if mask.any():
                for col in row.index:
                    if col != self.op_col and col in self.df.columns:
                        self.df.loc[mask, col] = row[col]
            return 'U'
        elif op in ['D', 'DELETE']:
            if mask.any():
                self.df = self.df[~mask].reset_index(drop=True)
            return 'D'
        return 'X'
    
    def process_cdc_file(self, cdc_df):
        ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}
        for _, row in cdc_df.iterrows():
            op = self.apply_operation(row)
            ops[op] += 1
        return ops
    
    def run(self, load_prefix, cdc_prefix):
        start = datetime.utcnow()
        
        # Load historic file
        load_key = self.get_load_file(load_prefix)
        self.df = self.read_csv(load_key)
        initial_rows = len(self.df)
        
        # Get all CDC files
        cdc_files = self.list_cdc_files(cdc_prefix)
        if not cdc_files:
            return {'status': 'success', 'message': 'No CDC files', 'table_name': self.table}
        
        # Initialize columns from first CDC
        first_cdc = self.read_csv(cdc_files[0])
        self.pk_col = self.df.columns[0]
        self.op_col = first_cdc.columns[0]
        
        # Process all CDC files
        total_ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}
        for cdc_file in cdc_files:
            cdc_df = self.read_csv(cdc_file) if cdc_file != cdc_files[0] else first_cdc
            ops = self.process_cdc_file(cdc_df)
            for k, v in ops.items():
                total_ops[k] += v
            self.move_to_processed(cdc_file)
        
        # Move original LOAD file to processed folder
        self.move_load_to_processed(load_key)
        
        # Save updated LOAD file with same name and location
        output_loc = self.write_csv(load_key)
        
        end = datetime.utcnow()
        
        return {
            'status': 'success',
            'table_name': self.table,
            'initial_rows': initial_rows,
            'final_rows': len(self.df),
            'row_change': len(self.df) - initial_rows,
            'cdc_files_processed': len(cdc_files),
            'operations': total_ops,
            'output_location': output_loc,
            'processing_time_seconds': (end - start).total_seconds()
        }

def lambda_handler(event, context):
    try:
        logger.info(f"Processing table: {event['table_name']}")
        processor = CDCProcessor(event['bucket_name'], event['table_name'])
        result = processor.run(
            event['load_file_prefix'],
            event['cdc_prefix']
        )
        return {'statusCode': 200, 'body': json.dumps(result, default=str)}
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'table_name': event.get('table_name', 'unknown')
            })
        }

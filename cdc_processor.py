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
        """Move CDC file to processed folder at DSET level"""
        # Extract path parts: land/cin/dflt/g5c/DSET00030052/DRPG5C_DBO/DA_AW_PROJ_CATG/file.csv
        # Target: land/cin/dflt/g5c/DSET00030052/processed/DRPG5C_DBO/DA_AW_PROJ_CATG/file.csv
        parts = key.split('/')
        
        # Find DSET position and insert 'processed' after it
        for i, part in enumerate(parts):
            if part.startswith('DSET'):
                # Reconstruct path with 'processed' after DSET
                processed_key = '/'.join(parts[:i+1] + ['processed'] + parts[i+1:])
                break
        else:
            # Fallback if DSET not found
            processed_key = f"{key.rsplit('/', 1)[0]}/processed/{key.split('/')[-1]}"
        
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': key}, Bucket=self.bucket, Key=processed_key)
        s3.delete_object(Bucket=self.bucket, Key=key)
        return processed_key
    
    def move_load_to_processed(self, key):
        """Move LOAD file to processed folder at DSET level with timestamp"""
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        filename = key.split('/')[-1]
        new_filename = filename.replace('.csv', f'_{timestamp}.csv')
        
        # Extract path parts and insert 'processed' after DSET
        parts = key.split('/')
        for i, part in enumerate(parts):
            if part.startswith('DSET'):
                # Reconstruct path with 'processed' after DSET and new filename
                processed_key = '/'.join(parts[:i+1] + ['processed'] + parts[i+1:-1] + [new_filename])
                break
        else:
            # Fallback if DSET not found
            processed_key = f"{key.rsplit('/', 1)[0]}/processed/{new_filename}"
        
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': key}, Bucket=self.bucket, Key=processed_key)
        s3.delete_object(Bucket=self.bucket, Key=key)
        logger.info(f"Moved LOAD file to: {processed_key}")
        return processed_key
    
    def apply_operation(self, row):
        op = str(row[self.op_col]).strip().upper()
        
        # Create mask using all data columns (excluding operation column)
        data_cols = [col for col in row.index if col != self.op_col and col in self.df.columns]
        mask = pd.Series([True] * len(self.df))
        for col in data_cols:
            mask &= (self.df[col] == row[col])
        
        if op in ['I', 'INSERT']:
            if not mask.any():
                self.df = pd.concat([self.df, row.drop(self.op_col).to_frame().T], ignore_index=True)
            return 'I'
        elif op in ['U', 'UPDATE']:
            if mask.any():
                for col in data_cols:
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
        self.op_col = first_cdc.columns[0]
        logger.info(f"Operation column: {self.op_col}")
        logger.info(f"Using all columns for row matching (no primary key)")
        
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

def parse_s3_event(event):
    """
    Parse S3 event to extract bucket name, table name, and key information.
    
    Expected S3 path structure:
    bucket-name/prefix/table_name/file.csv
    
    Returns: bucket_name, table_name, file_key
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Handle S3 event structure
    if 'Records' in event:
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        
        # Extract table_name from the path
        # Expected path: prefix/table_name/file.csv or prefix/schema/table_name/file.csv
        path_parts = file_key.split('/')
        
        # The table name is typically the folder containing the file
        # Assuming structure: .../table_name/file.csv or .../schema/table_name/file.csv
        if len(path_parts) >= 2:
            table_name = path_parts[-2]  # Folder name before the file
        else:
            table_name = 'unknown'
        
        logger.info(f"Parsed - Bucket: {bucket_name}, Table: {table_name}, Key: {file_key}")
        return bucket_name, table_name, file_key
    
    # Fallback for direct invocation with parameters
    elif 'bucket_name' in event and 'table_name' in event:
        return event['bucket_name'], event['table_name'], event.get('file_key', '')
    
    else:
        raise ValueError("Invalid event structure. Expected S3 event or direct parameters.")

def lambda_handler(event, context):
    try:
        # Parse S3 event to get bucket and table information
        bucket_name, table_name, file_key = parse_s3_event(event)
        
        # Extract prefix from file_key (everything before the filename)
        # Example: land/cin/dflt/g5c/DSET00030052/DRPG5C_DBO/DA_AW_PROJ_CATG/file.csv
        # Prefix: land/cin/dflt/g5c/DSET00030052/DRPG5C_DBO/DA_AW_PROJ_CATG/
        prefix = '/'.join(file_key.split('/')[:-1]) + '/'
        
        # Hard-coded prefix paths based on the prefix structure
        load_file_prefix = prefix
        cdc_prefix = prefix
        
        logger.info(f"Processing table: {table_name}")
        logger.info(f"Load file prefix: {load_file_prefix}")
        logger.info(f"CDC prefix: {cdc_prefix}")
        
        # Process CDC files
        processor = CDCProcessor(bucket_name, table_name)
        result = processor.run(load_file_prefix, cdc_prefix)
        
        return {
            'statusCode': 200,
            'body': json.dumps(result, default=str)
        }
        
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

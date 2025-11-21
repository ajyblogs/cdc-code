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
        self.pk_col = None
        self.op_col = None

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

    def write_csv(self, key):
        """Write DataFrame to S3."""
        buffer = StringIO()
        self.df.to_csv(buffer, index=False, sep='|')
        s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{key}"

    def get_processed_path(self, key, add_timestamp=False):
        """Create processed path at DSET level."""
        parts = key.split('/')

        # Find DSET folder
        for i, part in enumerate(parts):
            if part.startswith('DSET'):
                filename = parts[-1]
                if add_timestamp:
                    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
                    filename = filename.replace('.csv', f'_{timestamp}.csv')

                return '/'.join(parts[:i+1] + ['processed'] + parts[i+1:-1] + [filename])

        raise ValueError(f"DSET not found in path: {key}")

    def move_file(self, source_key, target_key):
        """Move file in S3."""
        s3.copy_object(CopySource={'Bucket': self.bucket, 'Key': source_key},
                      Bucket=self.bucket, Key=target_key)
        s3.delete_object(Bucket=self.bucket, Key=source_key)
        logger.info(f"Moved: {source_key} -> {target_key}")

    def list_cdc_files(self, prefix):
        """List CDC files (exclude LOAD and processed)."""
        paginator = s3.get_paginator('list_objects_v2')
        files = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') and 'LOAD' not in key and '/processed/' not in key:
                    files.append(key)

        return sorted(files)

    def extract_table_name(self, key):
        """Extract table name from S3 key path."""
        parts = key.split('/')
        for part in parts:
            if part.startswith('DSET'):
                return part
        return 'unknown'

    def get_load_prefix(self, cdc_key):
        """Derive LOAD file prefix from CDC file path."""
        parts = cdc_key.split('/')
        # Remove filename and return path up to table folder
        return '/'.join(parts[:-1]) + '/'

    def apply_cdc_operation(self, row):
        """Apply single CDC operation."""
        op = str(row[self.op_col]).strip().upper()
        pk_val = row[self.pk_col]
        mask = self.df[self.pk_col] == pk_val

        if op in ['I', 'INSERT']:
            new_row = row.drop(self.op_col).to_frame().T
            self.df = pd.concat([self.df, new_row], ignore_index=True)
            return 'I'

        # ----- UPDATE -----
        elif op in ['U', 'UPDATE']:
            if mask.any():
                for col in row.index:
                    if col != self.op_col and col in self.df.columns:
                        self.df.loc[mask, col] = row[col]
            else:
                # If record missing → auto insert (optional)
                new_row = row.drop(self.op_col).to_frame().T
                self.df = pd.concat([self.df, new_row], ignore_index=True)
            return 'U'

        # ----- DELETE -----
        elif op in ['D', 'DELETE']:
            # Pick only columns present in both DF and CDC row (excluding op col)
            data_cols = [col for col in row.index if col != self.op_col and col in self.df.columns]
            # Create a boolean mask initialized to True
            mask = pd.Series([True] * len(self.df), index=self.df.index)
            for col in data_cols:
                # Normalize both sides to string for comparison
                df_col = self.df[col].astype(str).str.strip().str.lower().fillna("")
                cdc_value = str(row[col]).strip().lower()
                mask &= (df_col == cdc_value)
        
            if mask.any():
                self.df = self.df.loc[~mask].reset_index(drop=True)
                return 'D'
            else:
                logger.warning("DELETE skipped — no exact string match found for row.")
                return 'X'
        # elif op in ['D', 'DELETE']:
        #     data_cols = [col for col in row.index if col != self.op_col and col in self.df.columns]
        #     mask = pd.Series([True] * len(self.df))
        #     for col in data_cols:
        #         mask &= (self.df[col] == row[col])
        #     if mask.any():
        #         self.df = self.df[~mask].reset_index(drop=True)
        #     return 'D'

        # return 'X'

    def process(self, cdc_key):
        """Main processing logic triggered by S3 event."""
        start = datetime.utcnow()

        # Derive paths from CDC file
        load_prefix = self.get_load_prefix(cdc_key)
        load_key = f"{load_prefix}LOAD00000001.csv"

        # Load base file
        logger.info(f"Loading: {load_key}")
        self.df = self.read_csv(load_key)
        initial_rows = len(self.df)

        # Get all CDC files in the same folder
        cdc_files = self.list_cdc_files(load_prefix)

        if not cdc_files:
            logger.info("No CDC files found")
            return {
                'status': 'success',
                'message': 'No CDC files to process',
                'table_name': self.table
            }

        logger.info(f"Found {len(cdc_files)} CDC files")

        # Setup columns
        first_cdc = self.read_csv(cdc_files[0])
        self.pk_col = self.df.columns[0]
        self.op_col = first_cdc.columns[0]

        # Process all CDC files
        ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}

        for i, cdc_file in enumerate(cdc_files):
            logger.info(f"Processing CDC file {i+1}/{len(cdc_files)}: {cdc_file}")
            cdc_df = first_cdc if i == 0 else self.read_csv(cdc_file)

            for _, row in cdc_df.iterrows():
                op = self.apply_cdc_operation(row)
                ops[op] += 1

            # Move each CDC file to processed after processing
            processed_path = self.get_processed_path(cdc_file)
            self.move_file(cdc_file, processed_path)

        # Move LOAD to processed with timestamp
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
            'operations': {'inserts': ops['I'], 'updates': ops['U'], 'deletes': ops['D']},
            'output_location': output,
            'processing_time_seconds': (datetime.utcnow() - start).total_seconds()
        }


def lambda_handler(event, context):
    """Lambda entry point for S3 events."""
    try:
        # Parse S3 event
        for record in event.get('Records', []):
            # Extract S3 object details
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            logger.info(f"S3 Event received - Bucket: {bucket}, Key: {key}")
            
            # Skip if file is in processed folder
            if '/processed/' in key:
                logger.info(f"Skipping processed file: {key}")
                continue
            
            # Skip if not a CSV file
            if not key.endswith('.csv'):
                logger.info(f"Skipping non-CSV file: {key}")
                continue
            
            # Skip if it's a LOAD file
            if 'LOAD' in key:
                logger.info(f"Skipping LOAD file: {key}")
                continue
            
            # Process CDC file
            table_name = key.split('/')[-1].split('_')[0] if '_' in key else 'unknown'
            processor = CDCProcessor(bucket, table_name)
            
            # Extract table name from path
            processor.table = processor.extract_table_name(key)
            
            result = processor.process(key)
            logger.info(f"Success: {json.dumps(result)}")
            
            return {'statusCode': 200, 'body': json.dumps(result)}
        
        # No valid records to process
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'skipped',
                'message': 'No valid CDC files to process'
            })
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'table_name': 'unknown'
            })
        }

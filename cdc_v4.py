import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import logging
import gc

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
        # Batch operations to reduce memory
        self.batch_inserts = []
        self.batch_size = 1000

    def read_csv(self, key, chunksize=None):
        """Read CSV from S3 with encoding fallback and optional chunking."""
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        content = obj['Body'].read()

        for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                decoded = content.decode(encoding)
                if chunksize:
                    return pd.read_csv(StringIO(decoded), sep='|', chunksize=chunksize)
                return pd.read_csv(StringIO(decoded), sep='|', dtype=str, low_memory=False)
            except UnicodeDecodeError:
                continue

        decoded = content.decode('latin-1', errors='replace')
        if chunksize:
            return pd.read_csv(StringIO(decoded), sep='|', chunksize=chunksize)
        return pd.read_csv(StringIO(decoded), sep='|', dtype=str, low_memory=False)

    def write_csv(self, key):
        """Write DataFrame to S3 in chunks to reduce memory."""
        buffer = StringIO()
        # Write in smaller chunks if DataFrame is large
        if len(self.df) > 100000:
            # Write header
            self.df.iloc[:0].to_csv(buffer, index=False, sep='|')
            # Write data in chunks
            chunk_size = 50000
            for i in range(0, len(self.df), chunk_size):
                chunk = self.df.iloc[i:i+chunk_size]
                chunk.to_csv(buffer, index=False, sep='|', header=False, mode='a')
                del chunk
                gc.collect()
        else:
            self.df.to_csv(buffer, index=False, sep='|')
        
        s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        buffer.close()
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
        return '/'.join(parts[:-1]) + '/'

    def normalize_value(self, val):
        """Normalize a single value for comparison."""
        if pd.isna(val):
            return ''
        return str(val).strip().lower()

    def match_by_primary_key(self, row):
        """Create mask matching primary key - optimized."""
        cdc_value = self.normalize_value(row[self.pk_col])
        # Use direct comparison without creating intermediate Series
        return self.df[self.pk_col].apply(lambda x: self.normalize_value(x) == cdc_value)

    def match_all_columns(self, row, exclude_op_col=True):
        """Create mask matching all columns - optimized."""
        cols_to_match = [col for col in row.index 
                        if col in self.df.columns and (not exclude_op_col or col != self.op_col)]
        
        if not cols_to_match:
            return pd.Series([False] * len(self.df), index=self.df.index)
        
        def row_matches(df_row):
            return all(self.normalize_value(df_row[col]) == self.normalize_value(row[col]) 
                      for col in cols_to_match)
        
        return self.df.apply(row_matches, axis=1)

    def flush_inserts(self):
        """Flush batched inserts to DataFrame."""
        if self.batch_inserts:
            logger.info(f"Flushing {len(self.batch_inserts)} batched inserts")
            new_rows = pd.DataFrame(self.batch_inserts)
            self.df = pd.concat([self.df, new_rows], ignore_index=True)
            self.batch_inserts = []
            gc.collect()

    def apply_cdc_operation(self, row):
        """Apply single CDC operation - memory optimized."""
        op = str(row[self.op_col]).strip().upper()

        if op in ['I', 'INSERT']:
            # Batch inserts instead of immediate concat
            new_row = row.drop(self.op_col).to_dict()
            self.batch_inserts.append(new_row)
            
            if len(self.batch_inserts) >= self.batch_size:
                self.flush_inserts()
            
            return 'I'

        elif op in ['U', 'UPDATE']:
            mask = self.match_by_primary_key(row)
            
            if mask.any():
                # Update in-place without creating copies
                indices = self.df.index[mask]
                for col in row.index:
                    if col != self.op_col and col in self.df.columns:
                        self.df.loc[indices, col] = row[col]
                logger.info(f"UPDATE: Updated {len(indices)} row(s) by PK")
                return 'U'
            else:
                logger.warning("UPDATE: No matching PK found, inserting as new row")
                new_row = row.drop(self.op_col).to_dict()
                self.batch_inserts.append(new_row)
                if len(self.batch_inserts) >= self.batch_size:
                    self.flush_inserts()
                return 'U'

        elif op in ['D', 'DELETE']:
            mask = self.match_all_columns(row, exclude_op_col=True)
            
            if mask.any():
                deleted_count = mask.sum()
                # Use drop instead of loc to avoid copy
                indices_to_drop = self.df.index[mask]
                self.df.drop(indices_to_drop, inplace=True)
                self.df.reset_index(drop=True, inplace=True)
                logger.info(f"DELETE: Removed {deleted_count} row(s)")
                gc.collect()
                return 'D'
            else:
                logger.warning("DELETE: No matching row found - skipped")
                return 'X'

        else:
            logger.warning(f"Unknown operation: {op}")
            return 'X'

    def process(self, cdc_key):
        """Main processing logic - memory optimized."""
        start = datetime.utcnow()

        # Derive paths
        load_prefix = self.get_load_prefix(cdc_key)
        load_key = f"{load_prefix}LOAD00000001.csv"

        # Load base file
        logger.info(f"Loading: {load_key}")
        self.df = self.read_csv(load_key)
        initial_rows = len(self.df)
        
        logger.info(f"Loaded {initial_rows} rows, DataFrame memory: {self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Get CDC files
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

        # Process CDC files
        ops = {'I': 0, 'U': 0, 'D': 0, 'X': 0}

        for i, cdc_file in enumerate(cdc_files):
            logger.info(f"Processing CDC file {i+1}/{len(cdc_files)}: {cdc_file}")
            cdc_df = first_cdc if i == 0 else self.read_csv(cdc_file)

            for _, row in cdc_df.iterrows():
                op = self.apply_cdc_operation(row)
                ops[op] += 1

            # Flush any pending inserts after each file
            self.flush_inserts()
            
            # Move CDC file to processed
            processed_path = self.get_processed_path(cdc_file)
            self.move_file(cdc_file, processed_path)
            
            # Clean up
            del cdc_df
            gc.collect()

        # Flush any remaining inserts
        self.flush_inserts()

        # Move LOAD to processed
        load_processed = self.get_processed_path(load_key, add_timestamp=True)
        self.move_file(load_key, load_processed)

        # Write updated LOAD
        output = self.write_csv(load_key)

        # Clean up
        final_rows = len(self.df)
        del self.df
        gc.collect()

        return {
            'status': 'success',
            'table_name': self.table,
            'initial_rows': initial_rows,
            'final_rows': final_rows,
            'row_change': final_rows - initial_rows,
            'cdc_files_processed': len(cdc_files),
            'operations': {'inserts': ops['I'], 'updates': ops['U'], 'deletes': ops['D'], 'skipped': ops['X']},
            'output_location': output,
            'processing_time_seconds': (datetime.utcnow() - start).total_seconds()
        }


def lambda_handler(event, context):
    """Lambda entry point for S3 events."""
    try:
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            logger.info(f"S3 Event received - Bucket: {bucket}, Key: {key}")
            
            # Skip filters
            if '/processed/' in key or not key.endswith('.csv') or 'LOAD' in key:
                logger.info(f"Skipping file: {key}")
                continue
            
            # Process CDC file
            table_name = key.split('/')[-1].split('_')[0] if '_' in key else 'unknown'
            processor = CDCProcessor(bucket, table_name)
            processor.table = processor.extract_table_name(key)
            
            result = processor.process(key)
            logger.info(f"Success: {json.dumps(result)}")
            
            # Force cleanup
            del processor
            gc.collect()
            
            return {'statusCode': 200, 'body': json.dumps(result)}
        
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

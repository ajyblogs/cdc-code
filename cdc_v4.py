import json
import boto3
import pandas as pd
from datetime import datetime
from io import BytesIO
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

    # Load base table
    def load_base_file(self, key, chunksize=5000):
        logger.info(f"Loading base table: s3://{self.bucket}/{key}")
        try:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
            stream = obj["Body"]
        except Exception as e:
            logger.error(f"Failed to load base file: {e}")
            raise

        chunk_list = []
        for chunk in pd.read_csv(stream, chunksize=chunksize):
            chunk_list.append(chunk)

        self.df = pd.concat(chunk_list, ignore_index=True)
        logger.info(f"Base table loaded with {len(self.df)} rows.")

    # Apply CDC files
    def apply_cdc_files(self, cdc_files, chunksize=5000):
        logger.info(f"Processing {len(cdc_files)} CDC files...")
        ops = {"I": 0, "U": 0, "D": 0, "X": 0}

        for f in cdc_files:
            logger.info(f"Applying CDC file: {f}")
            obj = s3.get_object(Bucket=self.bucket, Key=f)
            stream = obj["Body"]

            for chunk in pd.read_csv(stream, chunksize=chunksize):
                for _, row in chunk.iterrows():
                    op = row.get("op")

                    # INSERT
                    if op == "I":
                        self.df = pd.concat([self.df, row.to_frame().T], ignore_index=True)
                        ops["I"] += 1

                    # UPDATE
                    elif op == "U":
                        pk_value = row[self.pk_col]
                        mask = self.df[self.pk_col] == pk_value
                        if mask.any():
                            self.df.loc[mask, :] = row
                            ops["U"] += 1
                        else:
                            ops["X"] += 1

                    # DELETE
                    elif op == "D":
                        pk_value = row[self.pk_col]
                        before = len(self.df)
                        self.df = self.df[self.df[self.pk_col] != pk_value]
                        if len(self.df) < before:
                            ops["D"] += 1
                        else:
                            ops["X"] += 1

                    # UNKNOWN
                    else:
                        ops["X"] += 1

        logger.info(f"CDC processing completed. Operations: {ops}")
        return ops

    # Write output to S3
    def write_output(self, output_key):
        logger.info(f"Writing output to s3://{self.bucket}/{output_key}")
        csv_buffer = BytesIO()
        self.df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        s3.put_object(Bucket=self.bucket, Key=output_key, Body=csv_buffer.getvalue())
        return f"s3://{self.bucket}/{output_key}"

    # Main CDC processing
    def run(self, base_file, cdc_files, output_key):
        start = datetime.utcnow()
        self.load_base_file(base_file)
        initial_rows = len(self.df)

        # Auto-select first column as PK
        self.pk_col = self.df.columns[0]

        ops = self.apply_cdc_files(cdc_files)
        output = self.write_output(output_key)

        # 🔥 Original return block preserved
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


# ---------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------
def lambda_handler(event, context):
    try:
        # Extract S3 event info
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            logger.info(f"S3 Event → Bucket: {bucket}, Key: {key}")

            # Skip non-CSV, LOAD or processed files
            if '/processed/' in key or not key.endswith('.csv') or 'LOAD' in key:
                logger.info(f"Skipping file: {key}")
                continue

            # Determine base file and CDC files
            base_prefix = '/'.join(key.split('/')[:-1]) + '/'
            base_file = f"{base_prefix}LOAD00000001.csv"

            # List all CDC files in folder
            paginator = s3.get_paginator('list_objects_v2')
            cdc_files = []
            for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
                for obj in page.get('Contents', []):
                    cdc_key = obj['Key']
                    if cdc_key.endswith('.csv') and 'LOAD' not in cdc_key and '/processed/' not in cdc_key:
                        cdc_files.append(cdc_key)
            cdc_files = sorted(cdc_files)

            if not cdc_files:
                logger.info("No CDC files found to process.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'skipped',
                        'message': 'No valid CDC files to process'
                    })
                }

            # Process CDC
            processor = CDCProcessor(bucket, key.split('/')[-2])  # Use folder as table name
            result = processor.run(base_file, cdc_files, base_file)

            # Return result
            return {'statusCode': 200, 'body': json.dumps(result)}

        # No valid records
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'skipped',
                'message': 'No valid CDC files in event'
            })
        }

    except Exception as e:
        logger.error(f"Error during CDC processing: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }

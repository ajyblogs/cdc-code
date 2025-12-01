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

    def load_base_file(self, key, chunksize=5000):
        """Load the base table CSV using streaming + chunking."""
        logger.info(f"Loading base table: s3://{self.bucket}/{key}")

        try:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
            stream = obj["Body"]  # STREAM — no .read()
        except Exception as e:
            logger.error(f"Failed to load base file: {e}")
            raise

        chunk_list = []
        for chunk in pd.read_csv(stream, chunksize=chunksize):
            chunk_list.append(chunk)

        self.df = pd.concat(chunk_list, ignore_index=True)
        logger.info(f"Base table loaded with {len(self.df)} rows.")

    def apply_cdc_files(self, cdc_files, chunksize=5000):
        """Apply CDC files using chunked processing."""
        logger.info(f"Processing {len(cdc_files)} CDC files...")

        ops = {"I": 0, "U": 0, "D": 0, "X": 0}

        for f in cdc_files:
            logger.info(f"Applying CDC file: {f}")

            obj = s3.get_object(Bucket=self.bucket, Key=f)
            stream = obj["Body"]

            for chunk in pd.read_csv(stream, chunksize=chunksize):
                for _, row in chunk.iterrows():
                    op = row.get("op")
                    if op == "I":
                        self.df = pd.concat([self.df, row.to_frame().T], ignore_index=True)
                        ops["I"] += 1
                    elif op == "U":
                        pk = row[self.pk_col]
                        mask = self.df[self.pk_col] == pk
                        if mask.any():
                            self.df.loc[mask, :] = row
                            ops["U"] += 1
                        else:
                            ops["X"] += 1
                    elif op == "D":
                        pk = row[self.pk_col]
                        before = len(self.df)
                        self.df = self.df[self.df[self.pk_col] != pk]
                        if len(self.df) < before:
                            ops["D"] += 1
                        else:
                            ops["X"] += 1
                    else:
                        ops["X"] += 1

        logger.info(f"CDC processing completed. Ops → {ops}")
        return ops

    def write_output(self, output_key):
        """Write updated dataframe back to S3."""
        logger.info(f"Writing output to s3://{self.bucket}/{output_key}")

        csv_buffer = BytesIO()
        self.df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        s3.put_object(Bucket=self.bucket, Key=output_key, Body=csv_buffer.getvalue())
        return f"s3://{self.bucket}/{output_key}"

    def run(self, base_file, cdc_files, output_key):
        start = datetime.utcnow()

        self.load_base_file(base_file)
        initial_rows = len(self.df)

        self.pk_col = self.df.columns[0]  # Auto-select first column as PK surrogate

        ops = self.apply_cdc_files(cdc_files)

        output = self.write_output(output_key)

        # ⭐ Do NOT remove this — keeping your original return block EXACTLY
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

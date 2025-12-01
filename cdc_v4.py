import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

s3 = boto3.client('s3')


class CDCProcessor:
    def __init__(self, bucket, table, chunk_size=50000):
        self.bucket = bucket
        self.table = table
        self.chunk_size = chunk_size
        self.pk_col = None
        self.full_df = None

    def load_existing_table(self):
        """Loads the existing table from S3"""
        key = f"tables/{self.table}.csv"
        logger.debug(f"Loading existing table from s3://{self.bucket}/{key}")

        try:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
            self.full_df = pd.read_csv(obj['Body'])
            logger.debug(f"Existing table loaded: {len(self.full_df)} rows")
        except s3.exceptions.NoSuchKey:
            logger.debug("No existing table found. Creating empty dataframe.")
            self.full_df = pd.DataFrame()

    def detect_pk(self):
        """Detects primary key by checking column ending with _id, id, key"""
        logger.debug("Detecting primary key...")

        if self.full_df.empty:
            logger.debug("Existing DF empty → cannot detect PK. Using first column later.")
            return

        for col in self.full_df.columns:
            if col.lower() in ["id", "pk", "key"] or col.lower().endswith("_id"):
                self.pk_col = col
                logger.debug(f"Primary key detected: {self.pk_col}")
                return

        logger.debug("No primary key found based on column names.")

    def load_cdc_file_in_chunks(self, key):
        """Reads CDC file in chunks"""
        logger.debug(
            f"Reading CDC file in chunks: s3://{self.bucket}/{key} (chunk_size={self.chunk_size})"
        )

        obj = s3.get_object(Bucket=self.bucket, Key=key)

        return pd.read_csv(obj["Body"], chunksize=self.chunk_size)

    def apply_cdc_logic(self, chunk):
        """Apply insert/update/delete logic to chunk"""
        logger.debug(f"Processing chunk with {len(chunk)} rows")

        op_col = "op"

        # If PK missing → pick first non-op column
        if not self.pk_col:
            self.pk_col = [c for c in chunk.columns if c != op_col][0]
            logger.debug(f"Primary key auto-selected: {self.pk_col}")

        inserts = chunk[chunk[op_col] == "I"]
        updates = chunk[chunk[op_col] == "U"]
        deletes = chunk[chunk[op_col] == "D"]

        logger.debug(f"Chunk ops → Inserts: {len(inserts)}, Updates: {len(updates)}, Deletes: {len(deletes)}")

        # INSERT
        if not inserts.empty:
            logger.debug("Applying INSERT logic...")
            self.full_df = pd.concat([self.full_df, inserts.drop(columns=[op_col])], ignore_index=True)

        # UPDATE
        if not updates.empty:
            logger.debug("Applying UPDATE logic...")
            temp_df = updates.drop(columns=[op_col])
            self.full_df = self.full_df.set_index(self.pk_col)
            temp_df = temp_df.set_index(self.pk_col)

            # Update only overlapping columns
            self.full_df.update(temp_df)
            self.full_df.reset_index(inplace=True)

        # DELETE
        if not deletes.empty:
            logger.debug("Applying DELETE logic...")

            del_keys = deletes[self.pk_col].unique().tolist()
            logger.debug(f"Deleting PKs: {del_keys}")

            before_delete = len(self.full_df)
            self.full_df = self.full_df[~self.full_df[self.pk_col].isin(del_keys)]
            logger.debug(f"Deleted {before_delete - len(self.full_df)} rows")

    def write_back(self):
        """Write updated table back to S3"""
        logger.debug("Writing final table back to S3...")

        csv_buffer = StringIO()
        self.full_df.to_csv(csv_buffer, index=False)

        key = f"tables/{self.table}.csv"
        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_buffer.getvalue())

        logger.debug(f"Write complete. Final table size: {len(self.full_df)} rows")

    def process_cdc(self, cdc_key):
        """Main driver method"""
        logger.debug(f"*** Starting CDC processing for {cdc_key} ***")

        self.load_existing_table()
        self.detect_pk()

        chunks = self.load_cdc_file_in_chunks(cdc_key)

        for chunk_index, chunk in enumerate(chunks, start=1):
            logger.debug(f"--- Processing chunk {chunk_index} ---")
            self.apply_cdc_logic(chunk)

        self.write_back()

        logger.debug("*** CDC processing complete. ***")


def lambda_handler(event, context):
    bucket = event["bucket"]
    table = event["table"]
    cdc_key = event["cdc_key"]

    logger.debug(f"Lambda triggered with event: {event}")

    processor = CDCProcessor(bucket, table)
    processor.process_cdc(cdc_key)

    return {"status": "SUCCESS", "message": "CDC applied"}

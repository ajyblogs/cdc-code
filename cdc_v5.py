import json
import boto3
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df = None
        self.pk_col = None

    # ---------------------- UTILITIES ----------------------

    def extract_table_name(self, key):
        return key.split("/")[-1].split(".")[0]

    def load_arrow_table(self, key):
        logger.info(f"Loading base table: s3://{self.bucket}/{key}")

        obj = s3.get_object(Bucket=self.bucket, Key=key)
        table = pa_parquet.read_table(obj["Body"])

        logger.info(f"Loaded base table with {table.num_rows} rows.")
        return table

    def write_arrow_table(self, table, key):
        logger.info(f"Writing final table to: s3://{self.bucket}/{key}")
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        logger.info("Write complete.")

    def scan_csv_chunks(self, key, batch_size=500):
        logger.info(f"Streaming CDC file: {key}")
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        reader = pa_csv.open_csv(obj["Body"])

        for batch in reader.read_batches(batch_size=batch_size):
            yield batch.to_table()

    # ---------------------- CDC LOGIC ----------------------

    def merge_insert(self, base, cdc):
        logger.info(f"Applying INSERT operations for {cdc.num_rows} rows")
        return pa.concat_tables([base, cdc], promote=True)

    def merge_update(self, base, cdc):
        logger.info(f"Applying UPDATE for {cdc.num_rows} rows")

        # remove rows that match PK
        pk = self.pk_col
        base_no_match = base.filter(pc.is_in(base[pk], value_set=cdc[pk]).invert())

        # add updated rows
        return pa.concat_tables([base_no_match, cdc], promote=True)

    def merge_delete(self, base, cdc):
        logger.info(f"Applying DELETE for {cdc.num_rows} rows")
        pk = self.pk_col
        return base.filter(pc.is_in(base[pk], value_set=cdc[pk]).invert())

    # ---------------------- MAIN PROCESS ----------------------

    def process(self, load_key, cdc_files):
        logger.info("Starting CDC processing using PyArrow...")

        start = datetime.utcnow()
        base = self.load_arrow_table(load_key)

        # Auto-detect PK (first column)
        self.pk_col = base.column_names[0]
        logger.info(f"Detected primary key column: {self.pk_col}")

        initial_rows = base.num_rows
        logger.info(f"Initial row count: {initial_rows}")

        ops = {"I": 0, "U": 0, "D": 0, "X": 0}
        total_cdc_rows = 0

        for idx, cdc_path in enumerate(cdc_files):
            logger.info(f"Processing CDC file {idx+1}/{len(cdc_files)}: {cdc_path}")

            for cdc_chunk in self.scan_csv_chunks(cdc_path):

                if "op" not in cdc_chunk.column_names:
                    logger.warning("CDC chunk missing 'op' column. Skipping.")
                    continue

                total_cdc_rows += cdc_chunk.num_rows

                # Split operations
                inserts = cdc_chunk.filter(pc.equal(cdc_chunk["op"], "I"))
                updates = cdc_chunk.filter(pc.equal(cdc_chunk["op"], "U"))
                deletes = cdc_chunk.filter(pc.equal(cdc_chunk["op"], "D"))

                if inserts.num_rows > 0:
                    base = self.merge_insert(base, inserts)
                    ops["I"] += inserts.num_rows

                if updates.num_rows > 0:
                    base = self.merge_update(base, updates)
                    ops["U"] += updates.num_rows

                if deletes.num_rows > 0:
                    base = self.merge_delete(base, deletes)
                    ops["D"] += deletes.num_rows

                # Progress every 500 rows
                if total_cdc_rows % 500 == 0:
                    logger.info(f"Processed {total_cdc_rows} CDC rows so far...")

        logger.info("CDC application complete.")
        logger.info(f"Final row count: {base.num_rows}")

        # Write final output
        output_key = f"output/{self.table}_final.parquet"
        self.write_arrow_table(base, output_key)

        # Final summary log
        logger.info(json.dumps({
            "status": "success",
            "table_name": self.table,
            "initial_rows": initial_rows,
            "final_rows": base.num_rows,
            "row_change": base.num_rows - initial_rows,
            "cdc_files_processed": len(cdc_files),
            "total_cdc_rows": total_cdc_rows,
            "operations": ops,
            "output_location": output_key,
            "processing_time_seconds": (datetime.utcnow() - start).total_seconds(),
        }, indent=2))

        return True

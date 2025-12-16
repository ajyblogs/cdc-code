import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
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

        self.ins_cnt = 0
        self.upd_cnt = 0
        self.del_cnt = 0

        logger.info(f"CDC Processor initialized | bucket={bucket}, table={table}")

    # -------------------------------------------------
    # S3 helpers
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, ts=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                fname = parts[-1]
                if ts:
                    fname = fname.replace(".csv", f"_{datetime.utcnow():%Y%m%d%H%M%S}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [fname])
        raise ValueError("Invalid directory structure")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Archived {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for p in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in p.get("Contents", []):
                k = o["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        files = sorted(files)
        logger.info(f"CDC files discovered: {files}")
        return files

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded {key} | rows={tbl.num_rows}")
        return tbl

    def write_arrow(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Updated LOAD written → s3://{self.bucket}/{key}")

    # -------------------------------------------------
    # Schema alignment
    # -------------------------------------------------
    def align_schema(self, cdc, base_schema, op_col):
        cols = {}
        for f in base_schema:
            if f.name in cdc.column_names:
                col = cdc[f.name]
                if col.type != f.type:
                    col = pc.cast(col, f.type, safe=False)
            else:
                col = pa.array([None] * cdc.num_rows, type=f.type)
            cols[f.name] = col

        return pa.Table.from_arrays(
            [cdc[op_col]] + list(cols.values()),
            names=[op_col] + base_schema.names
        )

    # -------------------------------------------------
    # FAST CDC APPLY (VECTORISED)
    # -------------------------------------------------
    def apply_cdc(self, base, cdc, op_col):
        data_cols = base.schema.names

        # Row signature = concat all columns
        def signature(tbl):
            return pc.binary_join_element_wise(
                [pc.cast(tbl[c], pa.string()) for c in data_cols],
                separator="|"
            )

        base_sig = signature(base)
        cdc_sig = signature(cdc)

        # DELETE
        del_mask = pc.equal(cdc[op_col], pa.scalar("D"))
        del_sig = pc.filter(cdc_sig, del_mask)

        if del_sig.num_rows:
            self.del_cnt += del_sig.num_rows
            base = base.filter(pc.invert(pc.is_in(base_sig, value_set=del_sig)))
            base_sig = signature(base)

        # UPDATE
        upd_mask = pc.equal(cdc[op_col], pa.scalar("U"))
        upd_rows = cdc.filter(upd_mask).remove_column(0)
        if upd_rows.num_rows:
            self.upd_cnt += upd_rows.num_rows
            upd_sig = signature(upd_rows)
            base = base.filter(pc.invert(pc.is_in(base_sig, value_set=upd_sig)))
            base = pa.concat_tables([base, upd_rows])
            base_sig = signature(base)

        # INSERT
        ins_mask = pc.equal(cdc[op_col], pa.scalar("I"))
        ins_rows = cdc.filter(ins_mask).remove_column(0)
        if ins_rows.num_rows:
            self.ins_cnt += ins_rows.num_rows
            base = pa.concat_tables([base, ins_rows])

        return base

    # -------------------------------------------------
    # Main
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()

        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow(load_key)
        base_schema = self.df.schema

        cdc_files = self.list_cdc_files(prefix)
        first = self.load_arrow(cdc_files[0])
        op_col = first.column_names[0]

        for f in cdc_files:
            raw = self.load_arrow(f)
            aligned = self.align_schema(raw, base_schema, op_col)
            self.df = self.apply_cdc(self.df, aligned, op_col)
            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        self.write_arrow(load_key, self.df)

        logger.info(json.dumps({
            "status": "success",
            "final_rows": self.df.num_rows,
            "inserted": self.ins_cnt,
            "updated": self.upd_cnt,
            "deleted": self.del_cnt,
            "time_sec": (datetime.utcnow() - start).total_seconds()
        }))


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    logger.info(json.dumps(event))
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if "/processed/" in key or "LOAD" in key or not key.endswith(".csv"):
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200}

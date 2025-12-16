import json
import boto3
import pyarrow as pa
import pyarrow.csv as csv
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


class CDCProcessorArrow:
    def __init__(self, bucket, table):
        self.bucket = bucket
        self.table = table
        self.df: pa.Table | None = None

        # metrics
        self.ins_cnt = 0
        self.upd_cnt = 0
        self.del_cnt = 0

        logger.info(f"CDC Processor initialized | bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_ts=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                fname = parts[-1]
                if add_ts:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    fname = fname.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i + 1] + ["processed"] + parts[i + 1:-1] + [fname])
        raise ValueError("Invalid directory structure")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved file | {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                k = o["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        logger.info(f"CDC files discovered: {files}")
        return sorted(files)

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(
            obj["Body"],
            parse_options=csv.ParseOptions(delimiter="|")
        )
        logger.info(f"Loaded {key} | rows={tbl.num_rows}")
        return tbl

    def write_arrow(self, key, tbl):
        buf = tbl.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)
        logger.info(f"Updated LOAD written | s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema handling
    # -------------------------------------------------
    def unify_schema(self, load_tbl, cdc_tbl):
        fields = []
        for f in load_tbl.schema:
            if pa.types.is_null(f.type):
                if f.name in cdc_tbl.column_names:
                    fields.append(pa.field(f.name, cdc_tbl.schema.field(f.name).type))
                else:
                    fields.append(pa.field(f.name, pa.string()))
            else:
                fields.append(f)
        return pa.schema(fields)

    def upgrade_load(self, tbl, schema):
        cols = []
        for f in schema:
            if pa.types.is_null(tbl[f.name].type):
                cols.append(pa.array([None] * tbl.num_rows, type=f.type))
            else:
                cols.append(tbl[f.name])
        return pa.Table.from_arrays(cols, schema=schema)

    def align_schema(self, tbl, schema, op_col):
        cols = {}
        for f in schema:
            if f.name in tbl.column_names:
                cols[f.name] = tbl[f.name]
            else:
                cols[f.name] = pa.array([None] * tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + schema.names
        )

    # -------------------------------------------------
    # PURE PYARROW CDC (NO PK)
    # -------------------------------------------------
    def apply_cdc(self, base: pa.Table, cdc: pa.Table, op_col: str):
        schema = base.schema
        base_rows = [tuple(col[i].as_py() for col in base.columns) for i in range(base.num_rows)]
        tombstones = set()

        cdc_cols = {n: cdc[n].to_pylist() for n in cdc.column_names}

        for i in range(cdc.num_rows):
            op = str(cdc_cols[op_col][i]).upper()
            row = tuple(cdc_cols[c][i] for c in schema.names)

            # DELETE ALL
            if op == "D" and all(v is None for v in row):
                self.del_cnt += len(base_rows) - len(tombstones)
                tombstones = set(range(len(base_rows)))
                logger.info("DELETE ALL detected")
                continue

            if op == "D":
                for idx, r in enumerate(base_rows):
                    if idx not in tombstones and r == row:
                        tombstones.add(idx)
                        self.del_cnt += 1
                continue

            if op == "U":
                updated = False
                for idx, r in enumerate(base_rows):
                    if idx not in tombstones and r == row:
                        tombstones.add(idx)
                        base_rows.append(row)
                        updated = True
                        self.upd_cnt += 1
                        break
                if not updated:
                    base_rows.append(row)
                    self.upd_cnt += 1
                continue

            if op == "I":
                base_rows.append(row)
                self.ins_cnt += 1

        final_rows = [
            r for i, r in enumerate(base_rows) if i not in tombstones
        ]

        cols = list(zip(*final_rows)) if final_rows else [[] for _ in schema]
        return pa.Table.from_arrays(
            [pa.array(c, type=schema[i].type) for i, c in enumerate(cols)],
            schema=schema
        )

    # -------------------------------------------------
    # Main
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow(load_key)
        initial_rows = self.df.num_rows

        cdc_files = self.list_cdc_files(prefix)
        first = self.load_arrow(cdc_files[0])
        op_col = first.column_names[0]

        schema = self.unify_schema(self.df, first)
        self.df = self.upgrade_load(self.df, schema)

        for f in cdc_files:
            raw = self.load_arrow(f)
            aligned = self.align_schema(raw, schema, op_col)

            logger.info(f"Applying CDC | file={f}, rows={aligned.num_rows}")
            self.df = self.apply_cdc(self.df, aligned, op_col)

            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        out = self.write_arrow(load_key, self.df)

        logger.info(json.dumps({
            "status": "success",
            "initial_rows": initial_rows,
            "final_rows": self.df.num_rows,
            "inserted": self.ins_cnt,
            "updated": self.upd_cnt,
            "deleted": self.del_cnt,
            "time_sec": (datetime.utcnow() - start).total_seconds(),
            "output": out
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
            logger.info(f"Skipping {key}")
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

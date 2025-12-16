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
        self.df: pa.Table | None = None
        self.pk_col = None

        logger.info(f"CDC Processor initialized for bucket={bucket}, table={table}")

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                fname = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    fname = fname.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[:i+1] + ["processed"] + parts[i+1:-1] + [fname])
        raise ValueError("Invalid path")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src}
        )
        s3.delete_object(Bucket=self.bucket, Key=src)
        logger.info(f"Moved {src} → {dst}")

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        logger.info(f"CDC files: {files}")
        return sorted(files)

    # -------------------------------------------------
    # Arrow IO
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        tbl = csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))
        logger.info(f"Loaded {key} rows={tbl.num_rows}")
        return tbl

    def write_arrow_table(self, key, table):
        data = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        logger.info(f"Wrote LOAD {key}")
        return f"s3://{self.bucket}/{key}"

    # -------------------------------------------------
    # Schema handling
    # -------------------------------------------------
    def infer_schema(self, load_tbl, cdc_tbls):
        fields = []
        for f in load_tbl.schema:
            if pa.types.is_null(f.type):
                inferred = None
                for t in cdc_tbls:
                    if f.name in t.column_names:
                        ttype = t.schema.field(f.name).type
                        if not pa.types.is_null(ttype):
                            inferred = ttype
                            break
                fields.append(pa.field(f.name, inferred or pa.string()))
            else:
                fields.append(f)
        return pa.schema(fields)

    def upgrade_load_schema(self, load_tbl, schema):
        cols = []
        for f in schema:
            col = load_tbl[f.name]
            if pa.types.is_null(col.type):
                cols.append(pa.array([None]*load_tbl.num_rows, type=f.type))
            else:
                cols.append(col)
        return pa.Table.from_arrays(cols, schema=schema)

    def align_schema(self, tbl, schema, op_col):
        cols = {}
        for f in schema:
            if f.name in tbl.column_names:
                col = tbl[f.name]
                if col.type != f.type:
                    try:
                        col = pc.cast(col, f.type)
                    except Exception:
                        col = pa.array([None]*tbl.num_rows, type=f.type)
                cols[f.name] = col
            else:
                cols[f.name] = pa.array([None]*tbl.num_rows, type=f.type)

        return pa.Table.from_arrays(
            [tbl[op_col]] + list(cols.values()),
            names=[op_col] + schema.names
        )

    # -------------------------------------------------
    # CDC APPLY (VECTORISED & FAST)
    # -------------------------------------------------
    def apply_cdc(self, base, cdc, op_col):
        pk = self.pk_col

        op = pc.utf8_upper(cdc[op_col])
        ins = cdc.filter(pc.equal(op, "I")).remove_column(0)
        upd = cdc.filter(pc.equal(op, "U")).remove_column(0)
        dele = cdc.filter(pc.equal(op, "D")).remove_column(0)

        ins_cnt = ins.num_rows
        upd_cnt = upd.num_rows
        del_cnt = dele.num_rows

        # ---------------- DELETE (FULL ROW MATCH) ----------------
        if dele.num_rows > 0:
            mask = pa.scalar(True)
            for col in base.column_names:
                base_col = base[col]
                del_col = dele[col]
                match = pc.is_in(base_col, value_set=del_col)
                mask = pc.and_(mask, pc.invert(match))
            base = base.filter(mask)

        # ---------------- UPDATE (PK MATCH ONLY) ----------------
        if upd.num_rows > 0:
            pk_set = upd[pk]
            keep_mask = pc.invert(pc.is_in(base[pk], value_set=pk_set))
            base = base.filter(keep_mask)
            base = pa.concat_tables([base, upd])

        # ---------------- INSERT (APPEND) ----------------
        if ins.num_rows > 0:
            base = pa.concat_tables([base, ins])

        return base, ins_cnt, upd_cnt, del_cnt

    # -------------------------------------------------
    # Main
    # -------------------------------------------------
    def process(self, cdc_key):
        start = datetime.utcnow()
        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        self.df = self.load_arrow_table(load_key)
        self.pk_col = self.df.column_names[0]

        cdc_files = self.list_cdc_files(prefix)
        if not cdc_files:
            return

        first = self.load_arrow_table(cdc_files[0])
        op_col = first.column_names[0]

        schema = self.infer_schema(self.df, [first])
        self.df = self.upgrade_load_schema(self.df, schema)

        total_i = total_u = total_d = 0

        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, schema, op_col)

            self.df, i, u, d = self.apply_cdc(self.df, aligned, op_col)
            total_i += i
            total_u += u
            total_d += d

            self.move_file(f, self.get_processed_path(f))

        self.move_file(load_key, self.get_processed_path(load_key, True))
        out = self.write_arrow_table(load_key, self.df)

        logger.info(json.dumps({
            "final_rows": self.df.num_rows,
            "inserts": total_i,
            "updates": total_u,
            "deletes": total_d,
            "output": out,
            "time_sec": (datetime.utcnow() - start).total_seconds()
        }))


# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    try:
        for r in event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]

            if "LOAD" in key or "/processed/" in key or not key.endswith(".csv"):
                continue

            table = key.split("/")[-2]
            CDCProcessorArrow(bucket, table).process(key)

        return {"statusCode": 200, "body": json.dumps({"status": "success"})}

    except Exception as e:
        logger.error("CDC failure", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

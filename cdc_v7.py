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

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------
    def get_load_prefix(self, key):
        return "/".join(key.split("/")[:-1]) + "/"

    def get_processed_path(self, key, add_timestamp=False):
        parts = key.split("/")
        for i, p in enumerate(parts):
            if p.startswith("DSET"):
                filename = parts[-1]
                if add_timestamp:
                    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    filename = filename.replace(".csv", f"_{ts}.csv")
                return "/".join(parts[: i + 1] + ["processed"] + parts[i + 1 : -1] + [filename])
        raise ValueError("Invalid CDC directory structure")

    def move_file(self, src, dst):
        s3.copy_object(
            Bucket=self.bucket,
            Key=dst,
            CopySource={"Bucket": self.bucket, "Key": src},
        )
        s3.delete_object(Bucket=self.bucket, Key=src)

    def list_cdc_files(self, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".csv") and "LOAD" not in k and "/processed/" not in k:
                    files.append(k)
        return sorted(files)

    # -------------------------------------------------
    # Arrow I/O
    # -------------------------------------------------
    def load_arrow_table(self, key):
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        return csv.read_csv(obj["Body"], parse_options=csv.ParseOptions(delimiter="|"))

    def write_arrow_table(self, key, table):
        buf = table.to_pandas().to_csv(index=False, sep="|").encode()
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf)

    # -------------------------------------------------
    # Schema alignment
    # -------------------------------------------------
    def align_schema(self, cdc_tbl, load_schema, op_col):
        cols = {}
        for field in load_schema:
            name = field.name
            if name in cdc_tbl.column_names:
                col = cdc_tbl[name]
                if col.type != field.type:
                    try:
                        col = pc.cast(col, field.type)
                    except Exception:
                        col = pa.array([None] * cdc_tbl.num_rows, type=field.type)
                cols[name] = col
            else:
                cols[name] = pa.array([None] * cdc_tbl.num_rows, type=field.type)

        return pa.Table.from_arrays(
            [cdc_tbl[op_col]] + list(cols.values()),
            names=[op_col] + [f.name for f in load_schema],
        )

    # -------------------------------------------------
    # CDC collapse
    # -------------------------------------------------
    def collapse_cdc_by_pk(self, tbl, pk_col, op_col):
        rows = tbl.to_pylist()
        state = {}

        for row in rows:
            pk = row[pk_col]
            op = str(row[op_col]).upper()

            if pk not in state:
                state[pk] = {"ops": [], "row": row}

            state[pk]["ops"].append(op)
            state[pk]["row"] = row  # last row wins

        final_rows = []

        for pk, info in state.items():
            ops = info["ops"]
            last = info["row"]

            # INSERT → DELETE = NO-OP
            if "I" in ops and "D" in ops and ops.index("I") < ops.index("D"):
                continue

            # INSERT → UPDATE = INSERT
            if ops[0] == "I" and last[op_col] == "U":
                last = dict(last)
                last[op_col] = "I"

            final_rows.append(last)

        if not final_rows:
            return pa.table({c: [] for c in tbl.column_names}, schema=tbl.schema)

        return pa.Table.from_pylist(final_rows, schema=tbl.schema)

    def safe_upper(self, arr):
        return pc.utf8_upper(pc.cast(arr, pa.string()))

    # -------------------------------------------------
    # Row hash builder for DELETE
    # -------------------------------------------------
    def build_row_hash(self, tbl, cols):
        arrays = [pc.cast(tbl[c], pa.string()) for c in cols]

        combined = arrays[0]
        for a in arrays[1:]:
            combined = pc.binary_join_element_wise(combined, a, "")

        return pc.hash(combined)

    # -------------------------------------------------
    # Main process
    # -------------------------------------------------
    def process(self, cdc_key):
        prefix = self.get_load_prefix(cdc_key)
        load_key = f"{prefix}LOAD00000001.csv"

        # LOAD
        self.df = self.load_arrow_table(load_key)
        load_schema = self.df.schema
        self.pk_col = load_schema.names[0]
        initial_rows = self.df.num_rows

        # CDC
        cdc_files = self.list_cdc_files(prefix)
        first = self.load_arrow_table(cdc_files[0])
        op_col = first.column_names[0]

        ins, upd, dele = [], [], []

        for f in cdc_files:
            raw = self.load_arrow_table(f)
            aligned = self.align_schema(raw, load_schema, op_col)

            # collapse CDC before touching LOAD
            collapsed = self.collapse_cdc_by_pk(aligned, self.pk_col, op_col)

            op = self.safe_upper(collapsed[op_col])

            ins.append(collapsed.filter(pc.equal(op, "I")).remove_column(0))
            upd.append(collapsed.filter(pc.equal(op, "U")).remove_column(0))
            dele.append(collapsed.filter(pc.equal(op, "D")).remove_column(0))

        df_ins = pa.concat_tables(ins) if ins else pa.table({}, schema=load_schema)
        df_upd = pa.concat_tables(upd) if upd else pa.table({}, schema=load_schema)
        df_del = pa.concat_tables(dele) if dele else pa.table({}, schema=load_schema)

        # ---------------- DELETE (FULL ROW MATCH) ----------------
        if df_del.num_rows:
            cols = load_schema.names
            load_hash = self.build_row_hash(self.df, cols)
            del_hash = self.build_row_hash(df_del, cols)
            mask = pc.invert(pc.is_in(load_hash, del_hash))
            self.df = self.df.filter(mask)

        # ---------------- UPDATE (in-place, preserve LOAD row count) ----------------
        if df_upd.num_rows:
            # Map PK -> CDC row
            upd_dict = {r[self.pk_col]: r for r in df_upd.to_pylist()}

            keep_rows = []
            updated_rows = []

            for r in self.df.to_pylist():
                pk = r[self.pk_col]
                if pk in upd_dict:
                    # Replace this row with CDC row values
                    updated_rows.append(dict(upd_dict[pk]))
                else:
                    keep_rows.append(r)

            self.df = pa.Table.from_pylist(keep_rows + updated_rows, schema=self.df.schema)

        # ---------------- INSERT ----------------
        if df_ins.num_rows:
            self.df = pa.concat_tables([self.df, df_ins])

        # Archive CDC
        for f in cdc_files:
            self.move_file(f, self.get_processed_path(f))

        # Archive old LOAD
        self.move_file(load_key, self.get_processed_path(load_key, True))

        # Write new LOAD
        self.write_arrow_table(load_key, self.df)

        logger.info(
            json.dumps(
                {
                    "initial_rows": initial_rows,
                    "final_rows": self.df.num_rows,
                    "insert": df_ins.num_rows,
                    "update": df_upd.num_rows,
                    "delete": df_del.num_rows,
                }
            )
        )


# -------------------------------------------------
# Lambda handler
# -------------------------------------------------
def lambda_handler(event, context):
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        if not key.endswith(".csv") or "LOAD" in key or "/processed/" in key:
            continue

        table = key.split("/")[-2]
        CDCProcessorArrow(bucket, table).process(key)

    return {"statusCode": 200, "body": json.dumps({"status": "success"})}

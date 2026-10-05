"""
Load the MMG historical dataset from S3 into BigQuery.

"""
import argparse
import os
import pathlib
import sys
import tempfile

import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import bigquery

import s3_io

DATASET = "mmg_raw"
LOCATION = "EU"
PREFIX = "raw/steam_history/"

MONTHLY_ON_CREATED = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.MONTH,
    field="timestamp_created",
)

TABLES = {
    "reviews":                {"partition": MONTHLY_ON_CREATED, "cluster": ["appid"]},
    "applications":           {"partition": None, "cluster": ["appid"]},
    "application_genres":     {"partition": None, "cluster": ["appid"]},
    "application_categories": {"partition": None, "cluster": ["appid"]},
    "application_developers": {"partition": None, "cluster": ["appid"]},
    "application_publishers": {"partition": None, "cluster": ["appid"]},
    "application_platforms":  {"partition": None, "cluster": ["appid"]},
    "genres":                 {"partition": None, "cluster": None},
    "categories":             {"partition": None, "cluster": None},
    "developers":             {"partition": None, "cluster": None},
    "publishers":             {"partition": None, "cluster": None},
    "platforms":              {"partition": None, "cluster": None},
}

BASELINES = {
    "reviews_unpartitioned": ("reviews", {"partition": None, "cluster": None}),
}


def find_keys(s3, bkt, dt):
    keys, dts = {}, set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bkt, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            # raw/steam_history/<table>/dt=YYYY-MM-DD/data.parquet
            parts = obj["Key"][len(PREFIX):].split("/")
            if len(parts) != 3 or not parts[1].startswith("dt=") or not parts[2].endswith(".parquet"):
                continue
            table, obj_dt = parts[0], parts[1][3:]
            dts.add(obj_dt)
            if dt is None or obj_dt == dt:
                if table in keys and dt is None:
                    raise RuntimeError(f"{table}: several ingest dates in S3 {sorted(dts)}, pass --dt")
                keys[table] = obj["Key"]
    if dt is None and len(dts) > 1:
        raise RuntimeError(f"several ingest dates in S3 {sorted(dts)}, pass --dt")
    return keys


def load_one(client, local_path, table_id, layout):
    client.delete_table(table_id, not_found_ok=True)

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=layout["partition"],
        clustering_fields=layout["cluster"],
    )
    with open(local_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=config)
    job.result()                      # waits; raises if the load failed
    return client.get_table(table_id)


def describe_layout(table):
    parts = []
    if table.time_partitioning:
        parts.append(f"partitioned {table.time_partitioning.type_} on {table.time_partitioning.field}")
    if table.clustering_fields:
        parts.append(f"clustered on {', '.join(table.clustering_fields)}")
    return "; ".join(parts) or "plain"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", default=list(TABLES) + list(BASELINES),
                    help="subset of target tables (default: all, incl. reviews_unpartitioned)")
    ap.add_argument("--dt", help="ingest date under raw/steam_history (default: the only one in S3)")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would be loaded and how, change nothing")
    args = ap.parse_args()

    targets = {t: (t, TABLES[t]) for t in TABLES}
    targets.update(BASELINES)
    unknown = set(args.tables) - set(targets)
    if unknown:
        sys.exit(f"unknown tables: {', '.join(sorted(unknown))}")

    load_dotenv()
    project = os.environ["GCP_PROJECT"]
    s3 = s3_io.client()
    bkt = s3_io.bucket()

    keys = find_keys(s3, bkt, args.dt)
    missing = {targets[t][0] for t in args.tables} - set(keys)
    if missing:
        sys.exit(f"not in S3 under {PREFIX}: {', '.join(sorted(missing))}")

    if args.dry_run:
        for t in args.tables:
            src, layout = targets[t]
            p = layout["partition"]
            how = (f"MONTH on {p.field}" if p else "no partition") + \
                  (f", cluster {layout['cluster']}" if layout["cluster"] else "")
            print(f"  would load {DATASET}.{t:<24} <- {keys[src]}  ({how})")
        return

    client = bigquery.Client(project=project, location=LOCATION)
    client.create_dataset(bigquery.Dataset(f"{project}.{DATASET}"), exists_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        downloaded = {}
        for t in args.tables:
            src, layout = targets[t]

            if src not in downloaded:
                local = pathlib.Path(tmp) / f"{src}.parquet"
                s3.download_file(bkt, keys[src], str(local))
                downloaded[src] = (local, pq.ParquetFile(local).metadata.num_rows)
            local, n_parquet = downloaded[src]

            table_id = f"{project}.{DATASET}.{t}"
            table = load_one(client, local, table_id, layout)

            if table.num_rows != n_parquet:
                raise ValueError(f"{t}: parquet has {n_parquet:,} rows, BigQuery has {table.num_rows:,}")
            print(f"  {t:<24} {table.num_rows:>10,} rows  "
                  f"{table.num_bytes / 1e6:8.1f} MB  {describe_layout(table)}")

    print(f"done -> {project}.{DATASET} ({LOCATION})")


if __name__ == "__main__":
    main()
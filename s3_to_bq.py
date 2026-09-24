"""
Load the raw Steam files from S3 into BigQuery incrementally.

--full-refresh truncates and rebuilds both tables from everything in S3 (for
schema changes). It refuses if BigQuery has a file that S3 doesn't.

"""
import argparse
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

import s3_io

DATASET = "steam_raw"
PREFIX = "raw/steam_api/"

PRICE_SCHEMA = [
    bigquery.SchemaField("appid", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("response_json", "STRING"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

CHART_SCHEMA = [
    bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("response_json", "STRING"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def list_s3(s3, bkt):
    # {file name: S3 key} for every raw file under PREFIX.
    files = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bkt, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name in files:
                raise RuntimeError(f"{name} exists under two keys: {files[name]}, {obj['Key']}")
            files[name] = obj["Key"]
    return files


def loaded_files(client, table_id):
    try:
        return {r.source_file for r in client.query(
            f"select distinct source_file from `{table_id}`").result()}
    except NotFound:
        return set()


def read_text(s3, bkt, key):
    return s3.get_object(Bucket=bkt, Key=key)["Body"].read().decode("utf-8")


def price_rows(text, fname, loaded_at):
    rows = []
    for line in text.split("\n"):
        if not line.strip():
            continue
        row = json.loads(line)
        rows.append({
            "appid": row["appid"],
            "collected_at": row["collected_at"],
            "response_json": row["response_json"],
            "source_file": fname,
            "loaded_at": loaded_at,
        })
    return rows


def chart_rows(text, fname, loaded_at):
    blob = json.loads(text)
    return [{
        "collected_at": blob["collected_at"],
        "response_json": json.dumps(blob["response"], ensure_ascii=False),
        "source_file": fname,
        "loaded_at": loaded_at,
    }]


def row_count(client, table_id):
    try:
        return client.get_table(table_id).num_rows
    except NotFound:
        return 0


def sync(client, s3, bkt, s3_files, table_id, prefix, schema, parse, full_refresh, loaded_at):
    in_s3 = {n for n in s3_files if n.startswith(prefix)}
    in_bq = loaded_files(client, table_id)

    orphans = in_bq - in_s3
    if orphans:
        # Flag if sth  is in BigQuery that S3 does not have
        msg = f"{table_id}: {len(orphans)} file(s) in BigQuery missing from S3, e.g. {sorted(orphans)[:3]}"
        if full_refresh:
            raise RuntimeError(msg + " - refusing to truncate")
        print("  WARNING " + msg)

    todo = sorted(in_s3) if full_refresh else sorted(in_s3 - in_bq)
    if not todo:
        print(f"{table_id}: nothing new ({len(in_bq)} files already loaded)")
        return

    rows = []
    for name in todo:
        rows += parse(read_text(s3, bkt, s3_files[name]), name, loaded_at)

    before = 0 if full_refresh else row_count(client, table_id)
    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE" if full_refresh else "WRITE_APPEND",
        ),
    )
    job.result()

    after = row_count(client, table_id)
    if after != before + len(rows):
        raise ValueError(f"{table_id}: expected {before} + {len(rows)} rows, table has {after}")
    print(f"{table_id}: {len(todo)} file(s), {len(rows)} rows "
          f"{'rebuilt' if full_refresh else 'appended'}, {after} in table")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-refresh", action="store_true",
                    help="truncate and rebuild both tables from everything in S3")
    args = ap.parse_args()

    load_dotenv()
    project = os.environ["GCP_PROJECT"]
    client = bigquery.Client(project=project)
    s3 = s3_io.client()
    bkt = s3_io.bucket()
    loaded_at = datetime.now(timezone.utc).isoformat()

    s3_files = list_s3(s3, bkt)
    print(f"{len(s3_files)} files in s3://{bkt}/{PREFIX}")

    sync(client, s3, bkt, s3_files, f"{project}.{DATASET}.raw_prices",
         "prices_", PRICE_SCHEMA, price_rows, args.full_refresh, loaded_at)
    sync(client, s3, bkt, s3_files, f"{project}.{DATASET}.raw_charts",
         "charts_", CHART_SCHEMA, chart_rows, args.full_refresh, loaded_at)


if __name__ == "__main__":
    main()
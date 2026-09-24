import json, glob, os
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

DATASET = "steam_raw"
RAW_DIR = "data/raw/steam_prices"

price_schema = [
    bigquery.SchemaField("appid", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("response_json", "STRING"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

chart_schema = [
    bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("response_json", "STRING"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def guard_history(client, table_id, rows):
    # no truncate if a file is missing localy
    try:
        existing = {r.source_file for r in client.query(
            f"select distinct source_file from `{table_id}`").result()}
    except NotFound:
        return
    missing = existing - {r["source_file"] for r in rows}
    if missing:
        raise RuntimeError(
            f"{table_id}: {len(missing)} file(s) in BigQuery are missing locally, "
            f"refusing to truncate. e.g. {sorted(missing)[:3]}")


def load(rows, schema, table, client, project):
    table_id = f"{project}.{DATASET}.{table}"

    if not rows:
        print(f"{table}: nothing to load")
        return

    guard_history(client, table_id, rows)

    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()

    n = client.get_table(table_id).num_rows
    if n != len(rows):
        raise ValueError(f"{table}: sent {len(rows)}, table has {n}")
    print(f"{table}: {len(rows)} rows sent, {n} in table")


def main():
    load_dotenv()
    project = os.environ["GCP_PROJECT"]
    client = bigquery.Client(project=project)
    loaded_at = datetime.now(timezone.utc).isoformat()

    price_rows = []
    chart_rows = []

    for path in sorted(glob.glob(f"{RAW_DIR}/prices_*.jsonl")):
        fname = os.path.basename(path)
        with open(path, encoding="utf-8") as f:
            for line in f:
                row = json.loads(line)
                price_rows.append({
                    "appid": row["appid"],
                    "collected_at": row["collected_at"],
                    "response_json": row["response_json"],
                    "source_file": fname,
                    "loaded_at": loaded_at,
                })

    for path in sorted(glob.glob(f"{RAW_DIR}/charts_*.json")):
        fname = os.path.basename(path)
        with open(path, encoding="utf-8") as f:
            blob = json.load(f)
        chart_rows.append({
            "collected_at": blob["collected_at"],
            "response_json": json.dumps(blob["response"], ensure_ascii=False),
            "source_file": fname,
            "loaded_at": loaded_at,
        })

    load(price_rows, price_schema, "raw_prices", client, project)
    load(chart_rows, chart_schema, "raw_charts", client, project)


if __name__ == "__main__":
    main()
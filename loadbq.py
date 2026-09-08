import json, glob, os
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.environ["GCP_PROJECT"]
DATASET = "steam_raw"
RAW_DIR = "data/raw/steam_prices"

client = bigquery.Client(project=PROJECT)
loaded_at = datetime.now(timezone.utc).isoformat()

price_rows = []
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

price_schema = [
    bigquery.SchemaField("appid", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("response_json", "STRING"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

chart_rows = []
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

chart_schema = [
    bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("response_json", "STRING"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

def load(rows, schema, table):
    if not rows:
        print(f"{table}: nothing to load")
        return
    job = client.load_table_from_json(
        rows,
        f"{PROJECT}.{DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()
    n = client.get_table(f"{PROJECT}.{DATASET}.{table}").num_rows
    print(f"{table}: {len(rows)} rows sent, {n} in table")

load(price_rows, price_schema, "raw_prices")
load(chart_rows, chart_schema, "raw_charts")
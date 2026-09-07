import requests, csv, os
from datetime import date
import duckdb

MMG_APPLICATIONS = "data/mmg/applications.csv"
MMG_REVIEWS      = "data/mmg/reviews.csv"
OUT              = "config/app_ids.csv"
WATCHLIST        = []

os.makedirs("config", exist_ok=True)
today = date.today().isoformat()
rows = {}  

def add(appid, source):
    appid = int(appid)
    if appid not in rows:
        rows[appid] = source

S = requests.Session()
S.headers["User-Agent"] = "steam-pipeline/0.1"

for appid in WATCHLIST:
    add(appid, "watchlist")


charts = S.get("https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/",
               timeout=60).json()
for r in charts["response"]["ranks"]:
    add(r["appid"], "charts_top100")
print(f"charts:            {len(rows)}")


feat = S.get("https://store.steampowered.com/api/featuredcategories",
             params={"cc": "us", "l": "en"}, timeout=60).json()
for key in ["specials", "top_sellers", "new_releases", "coming_soon"]:
    for item in feat.get(key, {}).get("items", []):
        add(item["id"], f"featured_{key}")
print(f"+ featured:        {len(rows)}")

con = duckdb.connect()
top_paid = con.execute(f"""
    WITH apps AS (
        SELECT * FROM read_csv_auto('{MMG_APPLICATIONS}',
                                    sample_size=-1, all_varchar=true)
    ),
    revs AS (
        SELECT CAST(appid AS BIGINT) AS appid, count(*) AS c
        FROM read_csv_auto('{MMG_REVIEWS}', sample_size=-1)
        GROUP BY 1
    )
    SELECT CAST(a.appid AS BIGINT) AS appid
    FROM apps as a
    JOIN revs as r ON CAST(a.appid AS BIGINT) = r.appid
    WHERE a.mat_currency = 'USD'
      AND TRY_CAST(a.mat_initial_price AS BIGINT) > 0
    ORDER BY r.c DESC, appid
    LIMIT 300
""").fetchall()
for (appid,) in top_paid:
    add(appid, "mmg_top_paid")
print(f"+ mmg_top_paid:    {len(rows)}")

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["appid", "source", "added_at"])
    for appid in sorted(rows):
        w.writerow([appid, rows[appid], today])

print(f"\n{len(rows)} (~{len(rows) * 1.5 / 60:.0f} min per run)")
for src in sorted(set(rows.values())):
    print(f"  {src:24} {sum(1 for v in rows.values() if v == src)}")
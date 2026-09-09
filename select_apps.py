import requests, csv, os
from datetime import date
import duckdb
from collections import Counter

MMG_APPLICATIONS = "data/mmg/applications.csv"
MMG_REVIEWS      = "data/mmg/reviews.csv"
OUT              = "config/app_ids.csv"
WATCHLIST        = [2595260,
1903340,
1030300,
1245620,
1488490,
1666480,
383870,
534380,
3008130,
1798230,
300570,
2909400,
526870]

os.makedirs("config", exist_ok=True)
today = date.today().isoformat()
rows = {}  
existing = {}

if os.path.exists(OUT):
    with open(OUT, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            existing[int(r["appid"])] = (r["source"], r["added_at"])
    print(f"existing: {len(existing)}")

def add(appid, source):
    appid = int(appid)
    if appid in existing:
        return                 
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
    LIMIT 600
""").fetchall()

for (appid,) in top_paid:
    add(appid, "mmg_top_paid")
print(f"+ mmg_top_paid:    {len(rows)}")

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["appid", "source", "added_at"])
    for appid in sorted(set(existing) | set(rows)):
        if appid in existing:
            src, added = existing[appid]
        else:
            src, added = rows[appid], today
        w.writerow([appid, src, added])

total = len(existing) + len(rows)
print(f"\n{total} apps ({len(rows)} new) -> {OUT}  (~{total * 1.5 / 60:.0f} min per run)")

counts = Counter([src for src, _ in existing.values()] + list(rows.values()))
for src, n in sorted(counts.items()):
    print(f"  {src:24} {n}")
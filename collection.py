import requests, json, time, os, csv
from datetime import datetime, timezone

APP_LIST = "config/app_ids.csv"
OUT      = "data/raw/steam_prices"

os.makedirs(OUT, exist_ok=True)
run_ts = datetime.now(timezone.utc)

with open(APP_LIST, encoding="utf-8") as f:
    app_ids = [int(r["appid"]) for r in csv.DictReader(f)]
print(f"{len(app_ids)} apps, ~{len(app_ids) * 1.5 / 60:.0f} min")

S = requests.Session()
S.headers["User-Agent"] = "steam-pipeline/0.1"

charts = S.get("https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/",
               timeout=60).json()
with open(f"{OUT}/charts_{run_ts:%Y%m%dT%H%M%S}.json", "w", encoding="utf-8") as f:
    json.dump({"collected_at": run_ts.isoformat(), "response": charts}, f)

path = f"{OUT}/prices_{run_ts:%Y%m%dT%H%M%S}.jsonl"
ok = failed = 0

with open(path, "w", encoding="utf-8") as f:
    for n, appid in enumerate(app_ids, 1):
        body = None
        for attempt in range(3):
            try:
                r = S.get("https://store.steampowered.com/api/appdetails",
                          params={"appids": appid, "cc": "us", "l": "en"}, timeout=30)
                if r.status_code == 429:
                    wait = 60 * (attempt + 1)
                    print(f"  rate limited, sleeping {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                body = r.json().get(str(appid), {})
                break
            except Exception as e:
                if attempt == 2:
                    body = {"success": False, "_error": str(e)}
                else:
                    time.sleep(5)

        if body is None:
            body = {"success": False, "_error": "rate_limited"}

        ok, failed = (ok + 1, failed) if body.get("success") else (ok, failed + 1)

        f.write(json.dumps({
            "appid": appid,
            "collected_at": run_ts.isoformat(),
            "response_json": json.dumps(body, ensure_ascii=False),
        }, ensure_ascii=False) + "\n")
        f.flush()

        if n % 50 == 0:
            print(f"  {n}/{len(app_ids)}  ok={ok} failed={failed}")
        time.sleep(1.5)

print(f"\nwrote {ok + failed} rows ({ok} ok, {failed} failed) to {path}")
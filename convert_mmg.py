import argparse
import datetime as dt
import pathlib
import sys

import duckdb

SRC = pathlib.Path("data/mmg")
DST = pathlib.Path("data/parquet")
INGEST_DATE = dt.date.today().isoformat()
TABLES = {
    "applications":           {"bigint_cols": ["appid"], "epoch_cols": []},
    "reviews":                {"bigint_cols": ["appid"], "epoch_cols": ["timestamp_created"]},
    "application_genres":     {"bigint_cols": ["appid"], "epoch_cols": []},
    "application_categories": {"bigint_cols": ["appid"], "epoch_cols": []},
    "application_developers": {"bigint_cols": ["appid"], "epoch_cols": []},
    "application_publishers": {"bigint_cols": ["appid"], "epoch_cols": []},
    "application_platforms":  {"bigint_cols": ["appid"], "epoch_cols": []},
    "genres":                 {"bigint_cols": [], "epoch_cols": []},
    "categories":             {"bigint_cols": [], "epoch_cols": []},
    "developers":             {"bigint_cols": [], "epoch_cols": []},
    "publishers":             {"bigint_cols": [], "epoch_cols": []},
    "platforms":              {"bigint_cols": [], "epoch_cols": []},
}


def build_select(con, csv_path, spec):
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{csv_path}', all_varchar=true)"
    ).fetchall()]

    projections = []
    for c in cols:
        if c in spec["bigint_cols"]:
            projections.append(f'cast("{c}" as BIGINT) as "{c}"')
        elif c in spec["epoch_cols"]:
            projections.append(f'to_timestamp(cast("{c}" as BIGINT)) as "{c}"')
        else:
            projections.append(f'"{c}"')

    for c in spec["bigint_cols"] + spec["epoch_cols"]:
        if c not in cols:
            raise ValueError(f"{csv_path.name}: expected column '{c}' not found")

    return ",\n       ".join(projections), cols


def convert(con, table, spec):
    csv_path = SRC / f"{table}.csv"
    if not csv_path.exists():
        print(f"  skip {table}: no such file")
        return None

    out_dir = DST / table / f"dt={INGEST_DATE}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    projection, cols = build_select(con, csv_path, spec)

    con.execute(f"""
        COPY (
            SELECT {projection}
            FROM read_csv('{csv_path.as_posix()}', all_varchar=true)
        )
        TO '{out_path.as_posix()}'
        (FORMAT parquet, COMPRESSION zstd)
    """)

    n_csv = con.execute(
        f"SELECT count(*) FROM read_csv('{csv_path.as_posix()}', all_varchar=true)"
    ).fetchone()[0]
    n_pq = con.execute(
        f"SELECT count(*) FROM '{out_path.as_posix()}'"
    ).fetchone()[0]

    if n_csv != n_pq:
        raise ValueError(f"{table}: row count mismatch, csv={n_csv} parquet={n_pq}")

    csv_mb = csv_path.stat().st_size / 1024 / 1024
    pq_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  {table}: {n_pq:,} rows, {len(cols)} cols, "
          f"{csv_mb:.1f} MB -> {pq_mb:.1f} MB ({pq_mb / csv_mb:.0%})")

    return n_pq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", default=list(TABLES),
                    help="subset of tables to convert (default: all)")
    args = ap.parse_args()

    unknown = set(args.tables) - set(TABLES)
    if unknown:
        sys.exit(f"unknown tables: {', '.join(sorted(unknown))}")

    con = duckdb.connect()
    print(f"ingest date: {INGEST_DATE}")

    for table in args.tables:
        convert(con, table, TABLES[table])

    print("done")


if __name__ == "__main__":
    main()
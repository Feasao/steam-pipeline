"""
Uploader which syncs the collected daily and saved api called titles to S3
"""
import argparse
import pathlib
import sys
from dotenv import load_dotenv
import s3_io

SRC = pathlib.Path("data/raw/steam_prices")
PREFIX = "raw/steam_api"
PATTERNS = ("prices_*.jsonl", "charts_*.json")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print but upload nothing")
    ap.add_argument("--force", action="store_true", help="overwrite objects whose size differs")
    args = ap.parse_args()

    load_dotenv()
    bkt = s3_io.bucket()
    s3 = s3_io.client()

    files = sorted(p for pat in PATTERNS for p in SRC.glob(pat))
    if not files:
        sys.exit(f"no raw files under {SRC}")

    uploaded = skipped = 0
    conflicts = []

    for path in files:
        key = f"{PREFIX}/dt={s3_io.dt_from_filename(path.name)}/{path.name}"
        local = path.stat().st_size
        remote = s3_io.remote_size(s3, bkt, key)

        if remote == local:
            skipped += 1
            continue
        if remote is not None and not args.force:
            conflicts.append(f"{key}: local {local} B, s3 {remote} B")
            continue

        if args.dry_run:
            print(f"  would upload {local / 1024:8.1f} KB  {key}")
            uploaded += 1
            continue

        s3.upload_file(str(path), bkt, key)
        after = s3_io.remote_size(s3, bkt, key)
        if after != local:
            raise RuntimeError(f"{key}: uploaded {local} B, s3 reports {after} B")
        uploaded += 1
        print(f"  uploaded {local / 1024:8.1f} KB  {key}")

    verb = "would upload" if args.dry_run else "uploaded"
    print(f"{len(files)} local files: {verb} {uploaded}, already in s3 {skipped}, "
          f"conflicts {len(conflicts)}  ->  s3://{bkt}/{PREFIX}/")

    if conflicts:
        print("size mismatch, not overwritten (rerun with --force if the local copy is right):")
        for c in conflicts:
            print("  " + c)
        sys.exit(1)


if __name__ == "__main__":
    main()
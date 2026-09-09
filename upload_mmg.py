import argparse
import os
import pathlib
import sys
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

SRC = pathlib.Path("data/parquet")
PREFIX = "raw/steam_history"

BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("AWS_REGION")

if not BUCKET:
    sys.exit("S3_BUCKET not set in .env")


def exists(s3, key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="re-upload even if the object already exists")
    args = ap.parse_args()

    if not SRC.exists():
        sys.exit(f"{SRC} not found - run convert_mmg.py first")

    s3 = boto3.client("s3", region_name=REGION)

    files = sorted(SRC.rglob("*.parquet"))
    if not files:
        sys.exit(f"no parquet files under {SRC}")

    total = 0
    for path in files:
        rel = path.relative_to(SRC).as_posix()
        key = f"{PREFIX}/{rel}"
        mb = path.stat().st_size / 1024 / 1024

        if not args.force and exists(s3, key):
            print(f"  exists, skipping: {key}")
            continue

        s3.upload_file(str(path), BUCKET, key)
        total += mb
        print(f"  uploaded {mb:7.1f} MB  {key}")

    print(f"done - {total:.1f} MB to s3://{BUCKET}/{PREFIX}/")


if __name__ == "__main__":
    main()
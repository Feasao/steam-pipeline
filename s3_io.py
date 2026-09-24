import os
import re
import boto3
from botocore.exceptions import ClientError

_TS = re.compile(r"_(\d{4})(\d{2})(\d{2})T\d{6}\.")


def bucket():
    name = os.environ.get("S3_BUCKET")
    if not name:
        raise RuntimeError("S3_BUCKET not set in .env")
    return name


def client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION"))


def dt_from_filename(name):
    m = _TS.search(name)
    if not m:
        raise ValueError(f"no run timestamp in file name: {name}")
    return f"{m[1]}-{m[2]}-{m[3]}"


def remote_size(s3, bkt, key):
    try:
        return s3.head_object(Bucket=bkt, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
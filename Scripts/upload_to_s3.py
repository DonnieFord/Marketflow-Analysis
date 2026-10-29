import argparse
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

import boto3
from botocore.exceptions import ClientError


PROJECT = Path(__file__).resolve().parents[1]
DATA = PROJECT / "dataframes"
BUCKET = "marketflow-analysis-donnie-2026"
PREFIX = "parquet/"


def completed_files():
    metadata = DATA / "_spark_metadata"
    logs = []

    for path in metadata.iterdir():
        match = re.fullmatch(r"(\d+)(\.compact)?", path.name)
        if match:
            logs.append((int(match[1]), bool(match[2]), path))

    if not logs:
        raise RuntimeError("No Spark output metadata found.")

    # A compact log includes the earlier batches.
    compact_batches = [batch for batch, compact, _ in logs if compact]
    latest_compact = max(compact_batches, default=-1)

    selected = [
        item for item in logs
        if item[0] > latest_compact
        or (item[0] == latest_compact and item[1])
    ]

    committed = {}
    for _, _, log in sorted(selected):
        with log.open(encoding="utf-8") as handle:
            if handle.readline().strip() != "v1":
                raise RuntimeError(f"Unsupported metadata version: {log}")

            for line in handle:
                if not line.strip():
                    continue
                entry = json.loads(line)
                if entry.get("action") != "add":
                    raise RuntimeError("Unexpected Spark metadata action.")

                uri = urlparse(entry["path"])
                if uri.scheme != "file":
                    raise RuntimeError("Expected a local file URI.")

                name = unquote(uri.path)
                if re.match(r"^/[A-Za-z]:/", name):
                    name = name[1:]

                path = Path(name).resolve()
                relative = path.relative_to(DATA.resolve())

                if path.suffix != ".parquet":
                    continue
                if path.stat().st_size != entry["size"]:
                    raise RuntimeError(f"File size changed: {path}")

                committed[relative.as_posix()] = path

    return committed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    files = completed_files()
    total = sum(path.stat().st_size for path in files.values())
    print(f"Completed files: {len(files)}")
    print(f"Total size: {total / 1024 / 1024:.2f} MB")
    print(f"Destination: s3://{BUCKET}/{PREFIX}")

    if not args.upload:
        print("Preview only. No files uploaded.")
        return

    session = boto3.Session(
        profile_name="marketflow",
        region_name="us-east-2",
    )
    s3 = session.client("s3")
    uploaded = skipped = 0

    for relative, path in sorted(files.items()):
        key = PREFIX + relative
        try:
            existing = s3.head_object(Bucket=BUCKET, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                raise
        else:
            if existing["ContentLength"] != path.stat().st_size:
                raise RuntimeError(f"Existing S3 object has a different size: {key}")
            skipped += 1
            continue

        s3.upload_file(str(path), BUCKET, key)
        uploaded += 1

    print(f"Uploaded: {uploaded} | Already present: {skipped}")


if __name__ == "__main__":
    main()
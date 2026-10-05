"""Publish a committed local batch with checksum checks and a manifest written last."""

import argparse
import hashlib
import json
import os
from pathlib import Path


def put_immutable(client, bucket, key, payload):
    from botocore.exceptions import ClientError

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            IfNoneMatch="*",
            ServerSideEncryption="AES256",
            Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
        )
        return "created"
    except ClientError as exc:
        if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
            raise
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        try:
            existing = body.read()
        finally:
            body.close()
        if existing != payload:
            raise ValueError("S3 content collision")
        return "existing"


def publish(root, batch_hash, bucket, client=None):
    import sqlite3

    if len(batch_hash) != 64 or any(c not in "0123456789abcdef" for c in batch_hash):
        raise ValueError("Invalid hash")
    if not bucket:
        raise ValueError("Set AWS_S3_BUCKET")
    root = Path(root)
    with sqlite3.connect(root / "journal.sqlite") as db:
        row = db.execute(
            "SELECT report_json FROM batches WHERE batch_hash=?", (batch_hash,)
        ).fetchone()
    if not row:
        raise ValueError("Batch has not committed locally")
    report = json.loads(row[0])
    manifest_key = f"objects/manifests/{batch_hash}.json"
    objects = []
    for obj in report["objects"]:
        payload = (root / obj["key"]).read_bytes()
        if hashlib.sha256(payload).hexdigest() != obj["sha256"]:
            raise ValueError("Local artifact checksum mismatch")
        objects.append((obj["key"], payload))
    manifest = (root / manifest_key).read_bytes()
    if json.loads(manifest) != report:
        raise ValueError("Manifest mismatch")
    if client is None:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            config=Config(retries={"mode": "standard", "max_attempts": 4}),
        )
    results = [{"key": k, "status": put_immutable(client, bucket, k, p)} for k, p in objects]
    results.append(
        {"key": manifest_key, "status": put_immutable(client, bucket, manifest_key, manifest)}
    )
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=os.getenv("PIPELINE_ROOT", "artifacts"))
    p.add_argument("--batch-hash", required=True)
    a = p.parse_args()
    print(json.dumps(publish(a.root, a.batch_hash, os.getenv("AWS_S3_BUCKET", "")), indent=2))


if __name__ == "__main__":
    main()

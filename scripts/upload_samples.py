"""Upload the sample documents to the enrichment bucket.

Uploads every file in ``sample_documents/`` to the ``documents/`` prefix
of the given bucket, which triggers the enrichment Lambda. With
``--wait``, polls ``list_object_annotations`` until AI annotations
appear on every uploaded object (or a 120-second timeout elapses).

Usage:
    python3 scripts/upload_samples.py --bucket-name my-bucket [--wait]
"""

import argparse
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

#: Prefix under which documents are uploaded (matches the S3 event filter).
DOCUMENTS_PREFIX = "documents/"

#: Directory containing the sample documents, relative to the project root.
SAMPLE_DOCUMENTS_DIR = Path(__file__).resolve().parent.parent / "sample_documents"

#: Total time to wait for annotations before giving up (seconds).
WAIT_TIMEOUT_SECONDS = 120

#: Delay between annotation polls (seconds).
POLL_INTERVAL_SECONDS = 5


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Upload sample documents to the enrichment bucket and "
            "optionally wait for AI annotations to appear."
        )
    )
    parser.add_argument(
        "--bucket-name",
        required=True,
        help="Name of the enrichment data bucket (StorageStack output)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help=(
            "Poll for annotations on the uploaded objects "
            f"(up to {WAIT_TIMEOUT_SECONDS} seconds)"
        ),
    )
    return parser.parse_args()


def upload_samples(s3_client: "boto3.client", bucket_name: str) -> list[str]:
    """Upload every sample document and return the uploaded S3 keys.

    Args:
        s3_client: A boto3 S3 client.
        bucket_name: Destination bucket name.

    Returns:
        The list of S3 keys that were uploaded.
    """
    sample_files = sorted(
        path for path in SAMPLE_DOCUMENTS_DIR.iterdir() if path.is_file()
    )
    if not sample_files:
        print(f"No sample documents found in {SAMPLE_DOCUMENTS_DIR}")
        return []

    uploaded_keys: list[str] = []
    for sample_file in sample_files:
        key = f"{DOCUMENTS_PREFIX}{sample_file.name}"
        s3_client.upload_file(str(sample_file), bucket_name, key)
        uploaded_keys.append(key)
        print(f"Uploaded s3://{bucket_name}/{key}")
    return uploaded_keys


def has_annotations(
    s3_client: "boto3.client", bucket_name: str, key: str
) -> bool:
    """Return True if the object has at least one annotation.

    Args:
        s3_client: A boto3 S3 client.
        bucket_name: Bucket containing the object.
        key: Object key to check.

    Returns:
        True when ``list_object_annotations`` reports annotations.
    """
    try:
        response = s3_client.list_object_annotations(
            Bucket=bucket_name, Key=key
        )
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "NoSuchAnnotation", "404"):
            return False
        raise
    return bool(response.get("Annotations"))


def wait_for_annotations(
    s3_client: "boto3.client", bucket_name: str, keys: list[str]
) -> None:
    """Poll until annotations appear on every key or the timeout elapses.

    Args:
        s3_client: A boto3 S3 client.
        bucket_name: Bucket containing the uploaded objects.
        keys: S3 keys to poll for annotations.
    """
    pending = set(keys)
    deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS
    print(
        f"\nWaiting up to {WAIT_TIMEOUT_SECONDS} seconds for annotations "
        f"on {len(pending)} object(s)..."
    )

    while pending and time.monotonic() < deadline:
        for key in sorted(pending):
            if has_annotations(s3_client, bucket_name, key):
                pending.discard(key)
                print(f"  Annotations found for {key}")
        if pending:
            time.sleep(POLL_INTERVAL_SECONDS)

    if pending:
        print(
            f"\nTimed out after {WAIT_TIMEOUT_SECONDS} seconds. "
            "Annotations not yet present for:"
        )
        for key in sorted(pending):
            print(f"  {key}")
        print(
            "The Lambda may still be processing; re-check with "
            "scripts/verify_annotations.py in a moment."
        )
    else:
        print("\nAll uploaded objects have annotations.")


def main() -> int:
    """Entry point. Returns a process exit code."""
    args = parse_args()
    s3_client = boto3.client("s3")

    try:
        uploaded_keys = upload_samples(s3_client, args.bucket_name)
    except (ClientError, BotoCoreError) as error:
        print(f"Upload failed: {error}", file=sys.stderr)
        return 1

    if not uploaded_keys:
        return 1

    if args.wait:
        try:
            wait_for_annotations(s3_client, args.bucket_name, uploaded_keys)
        except (ClientError, BotoCoreError) as error:
            print(f"Annotation polling failed: {error}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

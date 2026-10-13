"""Verify the annotations attached to a single S3 object.

Calls ``list_object_annotations`` for the object, then
``get_object_annotation`` for each annotation name, printing every
annotation's content. Useful for immediate verification right after the
enrichment Lambda runs, before the annotation tables populate.

Usage:
    python3 scripts/verify_annotations.py \
        --bucket-name my-bucket --key documents/contract_sample.txt
"""

import argparse
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="List and print all annotations attached to an S3 object."
    )
    parser.add_argument(
        "--bucket-name",
        required=True,
        help="Name of the enrichment data bucket",
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Object key to inspect (e.g. documents/contract_sample.txt)",
    )
    return parser.parse_args()


def list_annotation_names(
    s3_client: "boto3.client", bucket_name: str, key: str
) -> list[str]:
    """Return the annotation names attached to the object.

    Args:
        s3_client: A boto3 S3 client.
        bucket_name: Bucket containing the object.
        key: Object key to inspect.

    Returns:
        A sorted list of annotation names (may be empty).
    """
    response = s3_client.list_object_annotations(Bucket=bucket_name, Key=key)
    return sorted(
        annotation["AnnotationName"]
        for annotation in response.get("Annotations", [])
    )


def print_annotation(
    s3_client: "boto3.client", bucket_name: str, key: str, name: str
) -> None:
    """Fetch and print one annotation's content.

    Args:
        s3_client: A boto3 S3 client.
        bucket_name: Bucket containing the object.
        key: Object key the annotation belongs to.
        name: Annotation name to fetch.
    """
    response = s3_client.get_object_annotation(
        Bucket=bucket_name, Key=key, AnnotationName=name
    )
    content = response["AnnotationPayload"].read().decode("utf-8")
    print(f"\n--- {name} ---")
    print(content)


def main() -> int:
    """Entry point. Returns a process exit code."""
    args = parse_args()
    s3_client = boto3.client("s3")

    try:
        names = list_annotation_names(s3_client, args.bucket_name, args.key)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            print(
                f"Object not found: s3://{args.bucket_name}/{args.key}",
                file=sys.stderr,
            )
        else:
            print(f"Failed to list annotations: {error}", file=sys.stderr)
        return 1
    except BotoCoreError as error:
        print(f"Failed to list annotations: {error}", file=sys.stderr)
        return 1

    if not names:
        print(
            f"No annotations found on s3://{args.bucket_name}/{args.key}. "
            "The enrichment Lambda may still be processing this object."
        )
        return 0

    print(
        f"Found {len(names)} annotation(s) on "
        f"s3://{args.bucket_name}/{args.key}"
    )
    for name in names:
        try:
            print_annotation(s3_client, args.bucket_name, args.key, name)
        except (ClientError, BotoCoreError) as error:
            print(
                f"\n--- {name} ---\nFailed to fetch annotation: {error}",
                file=sys.stderr,
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())

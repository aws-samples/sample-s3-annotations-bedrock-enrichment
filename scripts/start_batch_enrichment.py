"""Start an S3 Batch Operations job that runs the enrichment Lambda.

Builds a CSV manifest of object keys and version IDs, uploads it to the
data bucket, and creates a Batch Operations ``LambdaInvoke`` job
(invocation schema 2.0) that calls the same enrichment Lambda used for
new uploads. Two sources are supported:

- ``all``: every current object version under ``--prefix`` with the
  ``--suffix`` extension. Use this to backfill objects that existed
  before the pipeline. (For very large buckets, point a job at an S3
  Inventory report instead; see the README.)
- ``stale``: objects whose ``ai.provenance`` annotation records a prompt
  version other than ``--current-prompt-version``, read from the
  annotation table through Athena. Use this to re-enrich after a schema
  or prompt change.

Usage:
    python3 scripts/start_batch_enrichment.py --bucket-name <bucket> \\
        --function-arn <arn> --role-arn <arn> --source all --wait
    python3 scripts/start_batch_enrichment.py --bucket-name <bucket> \\
        --function-arn <arn> --role-arn <arn> --source stale \\
        --current-prompt-version financial-docs-v2 --wait
"""

import argparse
import sys
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))
import query_annotations  # noqa: E402

#: Prefix the Batch Operations role can read manifests from.
MANIFEST_PREFIX = "inventory/manifests/"

#: Prefix the Batch Operations role can write completion reports to.
REPORT_PREFIX = "batch-reports"

#: Job states after which polling stops.
TERMINAL_STATES = ("Complete", "Failed", "Cancelled")

#: Maximum time to wait for the job with --wait (seconds).
JOB_TIMEOUT_SECONDS = 900

POLL_INTERVAL_SECONDS = 10


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the enrichment Lambda over existing objects."
    )
    parser.add_argument("--bucket-name", required=True)
    parser.add_argument(
        "--function-arn",
        required=True,
        help="EnrichmentStack output EnrichmentFunctionArn",
    )
    parser.add_argument(
        "--role-arn",
        required=True,
        help="EnrichmentStack output BatchOperationsRoleArn",
    )
    parser.add_argument("--source", required=True, choices=["all", "stale"])
    parser.add_argument("--prefix", default="documents/")
    parser.add_argument("--suffix", default=".txt")
    parser.add_argument(
        "--current-prompt-version",
        help="Required for --source stale",
    )
    parser.add_argument(
        "--workgroup",
        default=query_annotations.DEFAULT_WORKGROUP,
        help="Athena workgroup for --source stale (QueryStack output)",
    )
    parser.add_argument(
        "--wait", action="store_true", help="Poll until the job finishes"
    )
    args = parser.parse_args()
    if not query_annotations.BUCKET_NAME_PATTERN.match(args.bucket_name):
        parser.error("--bucket-name is not a valid S3 bucket name")
    if args.source == "stale":
        if not args.current_prompt_version:
            parser.error("--source stale requires --current-prompt-version")
        if not query_annotations.PROMPT_VERSION_PATTERN.match(
            args.current_prompt_version
        ):
            parser.error("Invalid --current-prompt-version")
    return args


def current_versions(
    s3_client: "boto3.client", bucket: str, prefix: str, suffix: str
) -> list[tuple[str, str | None]]:
    """List the current version of every matching object.

    Returns:
        ``(key, version_id)`` pairs; ``version_id`` is ``None`` for
        objects written before versioning was enabled.
    """
    targets = []
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for version in page.get("Versions", []):
            if version["IsLatest"] and version["Key"].endswith(suffix):
                version_id = version["VersionId"]
                targets.append(
                    (version["Key"], None if version_id == "null" else version_id)
                )
    return targets


def stale_versions(
    bucket: str, current_prompt_version: str, workgroup: str
) -> list[tuple[str, str | None]]:
    """Return objects enriched with a different prompt version."""
    athena = boto3.client("athena")
    sql, params = query_annotations.build_query(
        "provenance",
        query_annotations.annotation_table_path(bucket),
        current_prompt_version,
    )
    execution_id = query_annotations.run_query(athena, sql, workgroup, params)
    rows = query_annotations.fetch_rows(athena, execution_id)[1:]
    return [(row[0], row[1] or None) for row in rows]


def write_manifest(
    s3_client: "boto3.client",
    bucket: str,
    targets: list[tuple[str, str | None]],
) -> tuple[str, str, list[str]]:
    """Upload a Batch Operations CSV manifest.

    Keys are URL-encoded, as the CSV manifest format requires. Version
    IDs are included only when every target has one.

    Returns:
        ``(manifest_key, etag, fields)``.
    """
    with_versions = all(version_id for _, version_id in targets)
    lines = []
    for key, version_id in targets:
        row = [bucket, urllib.parse.quote(key, safe="/")]
        if with_versions:
            row.append(version_id)
        lines.append(",".join(row))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_key = f"{MANIFEST_PREFIX}enrichment-{stamp}.csv"
    response = s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=("\n".join(lines) + "\n").encode("utf-8"),
        ContentType="text/csv",
    )
    fields = ["Bucket", "Key"] + (["VersionId"] if with_versions else [])
    return manifest_key, response["ETag"].strip('"'), fields


def create_job(
    account_id: str,
    bucket: str,
    function_arn: str,
    role_arn: str,
    manifest_key: str,
    etag: str,
    fields: list[str],
) -> str:
    """Create the LambdaInvoke Batch Operations job and return its ID."""
    s3control = boto3.client("s3control")
    response = s3control.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        Operation={
            "LambdaInvoke": {
                "FunctionArn": function_arn,
                "InvocationSchemaVersion": "2.0",
            }
        },
        Manifest={
            "Spec": {"Format": "S3BatchOperations_CSV_20180820", "Fields": fields},
            "Location": {
                "ObjectArn": f"arn:aws:s3:::{bucket}/{manifest_key}",
                "ETag": etag,
            },
        },
        Report={
            "Bucket": f"arn:aws:s3:::{bucket}",
            "Prefix": REPORT_PREFIX,
            "Format": "Report_CSV_20180820",
            "Enabled": True,
            "ReportScope": "AllTasks",
        },
        Priority=10,
        RoleArn=role_arn,
        ClientRequestToken=str(uuid.uuid4()),
        Description="Enrichment backfill / re-run",
    )
    return response["JobId"]


def wait_for_job(account_id: str, job_id: str) -> dict:
    """Poll the job until it reaches a terminal state."""
    s3control = boto3.client("s3control")
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
    while True:
        job = s3control.describe_job(AccountId=account_id, JobId=job_id)["Job"]
        print(f"  status: {job['Status']}  progress: {job.get('ProgressSummary', {})}")
        if job["Status"] in TERMINAL_STATES or time.monotonic() > deadline:
            return job
        time.sleep(POLL_INTERVAL_SECONDS)


def main() -> int:
    """Entry point. Returns a process exit code."""
    args = parse_args()
    s3_client = boto3.client("s3")
    try:
        if args.source == "all":
            targets = current_versions(
                s3_client, args.bucket_name, args.prefix, args.suffix
            )
        else:
            # The annotation table also holds rows for older versions;
            # re-enrich only versions that are still current.
            current = set(
                current_versions(
                    s3_client, args.bucket_name, args.prefix, args.suffix
                )
            )
            targets = [
                target
                for target in stale_versions(
                    args.bucket_name, args.current_prompt_version, args.workgroup
                )
                if target in current
            ]
        if not targets:
            print("No objects to enrich.")
            return 0
        print(f"{len(targets)} object(s) to enrich")

        manifest_key, etag, fields = write_manifest(
            s3_client, args.bucket_name, targets
        )
        print(f"Manifest: s3://{args.bucket_name}/{manifest_key}")

        account_id = boto3.client("sts").get_caller_identity()["Account"]
        job_id = create_job(
            account_id,
            args.bucket_name,
            args.function_arn,
            args.role_arn,
            manifest_key,
            etag,
            fields,
        )
        print(f"Created Batch Operations job {job_id}")
        if args.wait:
            job = wait_for_job(account_id, job_id)
            failed = job.get("ProgressSummary", {}).get("NumberOfTasksFailed", 0)
            if job["Status"] != "Complete" or failed:
                print(
                    f"Job ended {job['Status']} with {failed} failed task(s); "
                    f"see s3://{args.bucket_name}/{REPORT_PREFIX}/",
                    file=sys.stderr,
                )
                return 1
    except (ClientError, BotoCoreError, RuntimeError) as error:
        print(f"Batch enrichment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

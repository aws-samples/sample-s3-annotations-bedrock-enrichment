"""Query the S3 Metadata annotation table through Amazon Athena.

Runs a pre-built SQL query against the annotation table that S3
Metadata maintains for the enrichment bucket, polls for completion, and
prints the results as a formatted table. The annotation table lives in
the AWS-managed S3 Tables catalog (``s3tablescatalog/aws-s3``) in the
``b_{bucket-name}`` namespace, as a table named ``annotation``.

Annotation tables refresh within roughly one hour of annotation writes,
so this script reports a friendly message (instead of failing) when the
table does not exist yet or has no rows.

Usage:
    python3 scripts/query_annotations.py \
        --bucket-name my-bucket --query-type classification
    python3 scripts/query_annotations.py \
        --bucket-name my-bucket --query-type provenance \
        --current-prompt-version financial-docs-v2
"""

import argparse
import re
import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

#: Athena data catalog through which S3 Tables are read.
S3_TABLES_CATALOG = "s3tablescatalog/aws-s3"

#: Fixed name of the annotation table within the per-bucket namespace.
ANNOTATION_TABLE = "annotation"

#: Prefix used in annotation names (matches the cdk.json default).
ANNOTATION_PREFIX = "ai"

#: Allowed characters in a prompt version.
PROMPT_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

#: S3 general purpose bucket naming rules (lowercase letters, digits,
#: hyphens, and periods; 3-63 characters; letter or digit at each end).
BUCKET_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

#: Default Athena workgroup created by the QueryStack.
DEFAULT_WORKGROUP = "s3-enrich-annotation-queries"

#: Query templates. ``{table}`` is the validated table identifier; every
#: ``?`` is an Athena execution parameter.
QUERY_TEMPLATES = {
    "classification": """SELECT
    object_key,
    json_extract_scalar(text_value, '$.type') AS document_type,
    json_extract_scalar(text_value, '$.confidence') AS confidence
FROM {table}
WHERE name = ?
ORDER BY object_key""",
    "entities": """SELECT
    object_key,
    json_extract_scalar(entity, '$.name') AS entity_name,
    json_extract_scalar(entity, '$.type') AS entity_type
FROM {table}
CROSS JOIN UNNEST(
    CAST(json_parse(text_value) AS ARRAY(JSON))
) AS t(entity)
WHERE name = ?
ORDER BY object_key""",
    "provenance": """SELECT
    object_key,
    json_extract_scalar(text_value, '$.source_version_id') AS version_id,
    json_extract_scalar(text_value, '$.prompt_version') AS prompt_version,
    json_extract_scalar(text_value, '$.model_id') AS model_id,
    json_extract_scalar(text_value, '$.enriched_at') AS enriched_at
FROM {table}
WHERE name = ?{stale_filter}
ORDER BY object_key""",
    "all": """SELECT
    object_key,
    name,
    text_value
FROM {table}
ORDER BY object_key, name""",
}

#: Extra provenance filter used with --current-prompt-version.
STALE_FILTER = (
    "\n    AND json_extract_scalar(text_value, '$.prompt_version') <> ?"
)

#: Maximum time to wait for the Athena query to finish (seconds).
QUERY_TIMEOUT_SECONDS = 120

#: Delay between query-status polls (seconds).
POLL_INTERVAL_SECONDS = 2

#: Athena error hints meaning the annotation table does not exist yet.
TABLE_NOT_FOUND_HINTS = (
    "TABLE_NOT_FOUND",
    "does not exist",
    "EntityNotFoundException",
    "SCHEMA_NOT_FOUND",
    "not found",
)

#: Friendly message shown when the table is missing or empty.
NOT_POPULATED_MESSAGE = (
    "The annotation table has not been populated yet.\n"
    "S3 Metadata annotation tables refresh within roughly one hour of\n"
    "annotation writes. In the meantime, verify annotations directly\n"
    "with: python3 scripts/verify_annotations.py --bucket-name <bucket> "
    "--key <key>"
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Run a pre-built Athena query against the S3 Metadata "
            "annotation table and print the results."
        )
    )
    parser.add_argument(
        "--bucket-name",
        required=True,
        help="Name of the enrichment data bucket",
    )
    parser.add_argument(
        "--query-type",
        required=True,
        choices=["classification", "entities", "provenance", "all"],
        help="Which pre-built query to run",
    )
    parser.add_argument(
        "--current-prompt-version",
        help=(
            "For --query-type provenance: list only objects enriched with "
            "a different prompt version (candidates for re-enrichment)"
        ),
    )
    parser.add_argument(
        "--workgroup",
        default=DEFAULT_WORKGROUP,
        help=f"Athena workgroup (QueryStack output; default {DEFAULT_WORKGROUP})",
    )
    args = parser.parse_args()
    if not BUCKET_NAME_PATTERN.match(args.bucket_name):
        parser.error("--bucket-name is not a valid S3 bucket name")
    if args.current_prompt_version and not PROMPT_VERSION_PATTERN.match(
        args.current_prompt_version
    ):
        parser.error(
            "--current-prompt-version may contain only letters, digits, "
            "'.', '_', and '-'"
        )
    return args


def annotation_table_path(bucket_name: str) -> str:
    """Return the fully qualified Athena path of the annotation table.

    Args:
        bucket_name: The enrichment bucket name. S3 Metadata places the
            bucket's tables in a namespace named ``b_{bucket_name}``.

    Returns:
        The quoted ``"catalog"."namespace"."table"`` path.

    Raises:
        ValueError: If ``bucket_name`` isn't a valid S3 bucket name.
    """
    validate_bucket_name(bucket_name)
    return (
        f'"{S3_TABLES_CATALOG}"."b_{bucket_name}"."{ANNOTATION_TABLE}"'
    )


def validate_bucket_name(bucket_name: str) -> str:
    """Return ``bucket_name`` if it follows S3 bucket naming rules.

    The bucket name becomes part of the table identifier, which Athena
    can't take as a parameter, so it's checked before use.

    Raises:
        ValueError: If the name isn't a valid general purpose bucket name.
    """
    if not BUCKET_NAME_PATTERN.match(bucket_name) or ".." in bucket_name:
        raise ValueError(f"Invalid S3 bucket name: {bucket_name!r}")
    return bucket_name


def build_query(
    query_type: str,
    table_path: str,
    current_prompt_version: str | None = None,
) -> tuple[str, list[str]]:
    """Return the SQL and Athena execution parameters for a query type.

    Every value in the query (annotation names, prompt version) is passed
    as an Athena execution parameter (``?``), not inserted into the SQL
    text. The only interpolated part is the table identifier, which Athena
    cannot parameterize; ``annotation_table_path`` builds it from a bucket
    name that ``validate_bucket_name`` has already checked.

    Args:
        query_type: One of ``classification``, ``entities``,
            ``provenance``, or ``all``.
        table_path: Fully qualified Athena path of the annotation table,
            from ``annotation_table_path``.
        current_prompt_version: For ``provenance``, exclude objects already
            enriched with this prompt version. Must match
            ``PROMPT_VERSION_PATTERN``.

    Returns:
        ``(sql, execution_parameters)``.

    Raises:
        ValueError: If ``current_prompt_version`` is invalid.
    """
    template = QUERY_TEMPLATES[query_type]
    params: list[str] = []
    if query_type != "all":
        params.append(sql_literal(f"{ANNOTATION_PREFIX}.{query_type}"))
    if query_type == "provenance":
        if current_prompt_version:
            if not PROMPT_VERSION_PATTERN.match(current_prompt_version):
                raise ValueError("Invalid prompt version")
            template = template.replace("{stale_filter}", STALE_FILTER)
            params.append(sql_literal(current_prompt_version))
        else:
            template = template.replace("{stale_filter}", "")
    # Safe: table_path is built only from a validated bucket name and
    # fixed catalog/table names; all values are execution parameters.
    sql = template.replace("{table}", table_path)  # nosec B608
    return sql, params


def sql_literal(value: str) -> str:
    """Return ``value`` as a quoted SQL string literal for a parameter.

    Athena substitutes execution parameters as SQL literals, so string
    values need single quotes. Embedded quotes are doubled.
    """
    return "'" + value.replace("'", "''") + "'"


def run_query(
    athena_client: "boto3.client",
    sql: str,
    workgroup: str,
    params: list[str] | None = None,
) -> str:
    """Start the query and poll until it finishes.

    Args:
        athena_client: A boto3 Athena client.
        sql: The SQL to execute.
        workgroup: The Athena workgroup to run in.
        params: Athena execution parameters for the ``?`` placeholders.

    Returns:
        The query execution ID on success.

    Raises:
        RuntimeError: If the query fails, is cancelled, or times out.
            The message contains Athena's state-change reason.
    """
    request = {"QueryString": sql, "WorkGroup": workgroup}
    if params:
        request["ExecutionParameters"] = params
    start_response = athena_client.start_query_execution(**request)
    execution_id = start_response["QueryExecutionId"]

    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        execution = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            return execution_id
        if state in ("FAILED", "CANCELLED"):
            reason = execution["Status"].get(
                "StateChangeReason", "no reason provided"
            )
            raise RuntimeError(reason)
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"Query did not finish within {QUERY_TIMEOUT_SECONDS} seconds"
    )


def fetch_rows(
    athena_client: "boto3.client", execution_id: str
) -> list[list[str]]:
    """Fetch all result rows (including the header row) for a query.

    Args:
        athena_client: A boto3 Athena client.
        execution_id: The completed query execution ID.

    Returns:
        A list of rows, each a list of cell strings. The first row is
        the column-header row.
    """
    rows: list[list[str]] = []
    paginator = athena_client.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=execution_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append(
                [cell.get("VarCharValue", "") for cell in row["Data"]]
            )
    return rows


def print_table(rows: list[list[str]]) -> None:
    """Print rows as a formatted, width-aligned table.

    Args:
        rows: Result rows; the first row is treated as the header.
    """
    widths = [
        max(len(row[column]) for row in rows)
        for column in range(len(rows[0]))
    ]
    header, *data_rows = rows

    def format_row(row: list[str]) -> str:
        return " | ".join(
            cell.ljust(width) for cell, width in zip(row, widths)
        )

    print(format_row(header))
    print("-+-".join("-" * width for width in widths))
    for row in data_rows:
        print(format_row(row))


def is_table_not_found(message: str) -> bool:
    """Return True if an Athena error message means the table is missing.

    Args:
        message: The Athena state-change reason or exception text.

    Returns:
        True when the message matches a known table-not-found hint.
    """
    lowered = message.lower()
    return any(hint.lower() in lowered for hint in TABLE_NOT_FOUND_HINTS)


def main() -> int:
    """Entry point. Returns a process exit code."""
    args = parse_args()
    athena_client = boto3.client("athena")

    table_path = annotation_table_path(args.bucket_name)
    sql, params = build_query(
        args.query_type, table_path, args.current_prompt_version
    )
    workgroup = args.workgroup

    print(f"Running '{args.query_type}' query against {table_path}")
    print(f"Workgroup: {workgroup}\n")

    try:
        execution_id = run_query(athena_client, sql, workgroup, params)
    except RuntimeError as error:
        if is_table_not_found(str(error)):
            print(NOT_POPULATED_MESSAGE)
            return 0
        print(f"Athena query failed: {error}", file=sys.stderr)
        return 1
    except ClientError as error:
        message = str(error)
        if "WorkGroup" in message and "not found" in message.lower():
            print(
                f"Athena workgroup '{workgroup}' was not found. Deploy "
                "the QueryStack (set enable_query_stack to true in "
                "cdk.json) before running this script.",
                file=sys.stderr,
            )
        elif is_table_not_found(message):
            print(NOT_POPULATED_MESSAGE)
            return 0
        else:
            print(f"Athena query failed: {error}", file=sys.stderr)
        return 1
    except BotoCoreError as error:
        print(f"Athena query failed: {error}", file=sys.stderr)
        return 1

    try:
        rows = fetch_rows(athena_client, execution_id)
    except (ClientError, BotoCoreError) as error:
        print(f"Failed to fetch query results: {error}", file=sys.stderr)
        return 1

    # Athena always returns a header row; anything beyond it is data.
    if len(rows) <= 1:
        print(
            "The query returned no rows.\n\n" + NOT_POPULATED_MESSAGE
        )
        return 0

    print_table(rows)
    print(f"\n{len(rows) - 1} row(s) returned.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

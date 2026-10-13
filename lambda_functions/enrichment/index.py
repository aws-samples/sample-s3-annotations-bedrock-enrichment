"""Enrichment Lambda: analyze S3 documents with Bedrock, write S3 annotations.

The same handler serves two triggers:

- **S3 event notifications** (``Records``) for objects as they arrive.
- **S3 Batch Operations** (``invocationSchemaVersion``) for backfilling
  objects that already exist, typically driven by an S3 Inventory
  manifest.

For each object the handler:

1. Reads the object (the exact version, when a version ID is supplied)
   and captures its ETag.
2. Sends the (truncated) content to the Bedrock Converse API.
3. Parses the model's JSON response.
4. Writes one annotation per field in ``ANNOTATION_FIELDS`` plus an
   ``{prefix}.provenance`` annotation recording the model ID, prompt
   version, and timestamp. Every write targets the same version ID and
   is conditional on the captured ETag (``ObjectIfMatch``), so a
   replaced object is never annotated with stale results.

For S3 events the handler never raises for content-level failures
(missing object, non-JSON model output, individual annotation-write
errors); it logs and returns a summary dict instead, avoiding pointless
retries. For Batch Operations it reports a per-task result code.

Configuration comes from environment variables so unit tests can patch it:

- ``MODEL_ID``: Bedrock model identifier (default ``amazon.nova-pro-v1:0``).
- ``MAX_CONTENT_LENGTH``: max characters sent to Bedrock (default ``10000``).
- ``ANNOTATION_PREFIX``: prefix for annotation names (default ``ai``).
"""

import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone
from typing import Any, NamedTuple, Optional

import boto3
from botocore.exceptions import ClientError

try:
    # Lambda runtime: the function directory is on sys.path.
    from prompts import ANNOTATION_FIELDS, PROMPT_VERSION, SYSTEM_PROMPT
except ImportError:
    # Package-style import (e.g. unit tests importing the package).
    from .prompts import ANNOTATION_FIELDS, PROMPT_VERSION, SYSTEM_PROMPT

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Module-level clients are reused across warm invocations and can be
# patched by unit tests (e.g. mock.patch("index.s3")).
s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime")

DEFAULT_MODEL_ID = "amazon.nova-pro-v1:0"
DEFAULT_MAX_CONTENT_LENGTH = 10000
DEFAULT_ANNOTATION_PREFIX = "ai"

#: Annotation-name prefixes reserved by S3.
RESERVED_PREFIXES = ("aws", "s3")

#: Maximum length of the plain-text summary annotation.
MAX_SUMMARY_LENGTH = 500

#: Error codes meaning the object (or the requested version) is gone.
MISSING_OBJECT_CODES = ("NoSuchKey", "NoSuchVersion")

#: Error codes that Batch Operations should retry rather than fail.
RETRYABLE_ERROR_CODES = (
    "ThrottlingException",
    "ServiceUnavailableException",
    "ModelNotReadyException",
    "SlowDown",
    "ServiceUnavailable",
    "InternalError",
    "RequestTimeout",
)

TRIGGER_S3_EVENT = "s3-event"
TRIGGER_BATCH_OPERATIONS = "batch-operations"


class ObjectRef(NamedTuple):
    """The S3 object (and optionally the exact version) to enrich."""

    bucket: str
    key: str
    version_id: Optional[str]


class ProcessResult(NamedTuple):
    """Outcome of enriching one object."""

    written: int
    skipped: int
    status: str  # "ok", "missing", or "invalid_model_output"


def _get_model_id() -> str:
    """Return the configured Bedrock model ID."""
    return os.environ.get("MODEL_ID", DEFAULT_MODEL_ID)


def _get_annotation_prefix() -> str:
    """Return the annotation prefix, refusing reserved ``aws``/``s3`` prefixes.

    Falls back to the default prefix with a warning if the configured value
    starts with a reserved prefix.
    """
    prefix = os.environ.get("ANNOTATION_PREFIX", DEFAULT_ANNOTATION_PREFIX)
    if prefix.lower().startswith(RESERVED_PREFIXES):
        logger.warning(
            "ANNOTATION_PREFIX %r starts with a reserved prefix %s; "
            "falling back to %r",
            prefix,
            RESERVED_PREFIXES,
            DEFAULT_ANNOTATION_PREFIX,
        )
        return DEFAULT_ANNOTATION_PREFIX
    return prefix


def _analyze_content(content: str) -> dict[str, Any] | None:
    """Send document content to Bedrock and parse the JSON response.

    Truncates ``content`` to MAX_CONTENT_LENGTH characters before invoking
    the Converse API. Returns the parsed dict, or ``None`` if the model
    response is not a JSON object.
    """
    max_length = int(
        os.environ.get("MAX_CONTENT_LENGTH", DEFAULT_MAX_CONTENT_LENGTH)
    )
    truncated = content[:max_length]

    response = bedrock_runtime.converse(
        modelId=_get_model_id(),
        system=[{"text": SYSTEM_PROMPT}],
        messages=[{"role": "user", "content": [{"text": truncated}]}],
        inferenceConfig={"temperature": 0.1, "maxTokens": 2048},
    )
    response_text = response["output"]["message"]["content"][0]["text"]

    try:
        analysis = json.loads(response_text)
    except json.JSONDecodeError:
        analysis = None
    if not isinstance(analysis, dict):
        # Log only metadata: the response can repeat document content,
        # including personal data, so it never goes to CloudWatch Logs.
        logger.error(
            "Bedrock returned a response that is not a JSON object; "
            "skipping annotations (response length: %d characters)",
            len(response_text),
        )
        return None
    return analysis


def _build_annotations(analysis: dict[str, Any], prefix: str) -> dict[str, str]:
    """Build annotation payloads from a parsed Bedrock analysis.

    Writes one ``{prefix}.{field}`` annotation per field in
    ``ANNOTATION_FIELDS`` that the model returned. String values are
    stored as plain text (the summary is capped at 500 characters) and
    all other values as JSON. Fields missing from the response are
    logged and left out.
    """
    annotations: dict[str, str] = {}
    for field in ANNOTATION_FIELDS:
        if field not in analysis:
            logger.warning("Model response is missing field %r", field)
            continue
        value = analysis[field]
        if isinstance(value, str):
            payload = value[:MAX_SUMMARY_LENGTH] if field == "summary" else value
        else:
            payload = json.dumps(value)
        annotations[f"{prefix}.{field}"] = payload
    return annotations


def _build_provenance(ref: ObjectRef, etag: str, trigger: str) -> str:
    """Build the provenance payload recording how the enrichment was made.

    Readers can query this annotation to find objects enriched with an
    older model or prompt version and re-run them, and to tell model
    output apart from human-reviewed labels stored under another prefix.
    """
    return json.dumps(
        {
            "model_id": _get_model_id(),
            "prompt_version": PROMPT_VERSION,
            "enriched_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            ),
            "source_etag": etag,
            "source_version_id": ref.version_id,
            "trigger": trigger,
        }
    )


def _write_annotations(
    ref: ObjectRef, etag: str, annotations: dict[str, str]
) -> tuple[int, int]:
    """Write annotations with conditional (ObjectIfMatch=ETag) puts.

    When ``ref.version_id`` is set, every annotation lands on that exact
    version. A failure on one annotation is logged and does not block the
    remaining writes. Returns ``(written, skipped)`` counts.
    """
    written = 0
    skipped = 0
    for name, body in annotations.items():
        params: dict[str, Any] = {
            "Bucket": ref.bucket,
            "Key": ref.key,
            "AnnotationName": name,
            "AnnotationPayload": body.encode("utf-8"),
            "ObjectIfMatch": etag,
        }
        if ref.version_id:
            params["VersionId"] = ref.version_id
        try:
            s3.put_object_annotation(**params)
            written += 1
            logger.info(
                "Wrote annotation %s for s3://%s/%s", name, ref.bucket, ref.key
            )
        except Exception:
            skipped += 1
            logger.exception(
                "Failed to write annotation %s for s3://%s/%s; continuing",
                name,
                ref.bucket,
                ref.key,
            )
    return written, skipped


def _process_object(ref: ObjectRef, trigger: str) -> ProcessResult:
    """Enrich one object and write its annotations.

    Raises ``ClientError`` for errors other than a missing object or
    version so callers can decide whether to retry.
    """
    logger.info(
        "Processing s3://%s/%s (version %s)", ref.bucket, ref.key, ref.version_id
    )

    get_params: dict[str, Any] = {"Bucket": ref.bucket, "Key": ref.key}
    if ref.version_id:
        get_params["VersionId"] = ref.version_id
    try:
        response = s3.get_object(**get_params)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in MISSING_OBJECT_CODES:
            logger.warning(
                "Object s3://%s/%s no longer exists; skipping", ref.bucket, ref.key
            )
            return ProcessResult(0, 0, "missing")
        raise

    content = response["Body"].read().decode("utf-8")
    etag = response["ETag"]

    analysis = _analyze_content(content)
    if analysis is None:
        return ProcessResult(0, len(ANNOTATION_FIELDS) + 1, "invalid_model_output")

    prefix = _get_annotation_prefix()
    annotations = _build_annotations(analysis, prefix)
    annotations[f"{prefix}.provenance"] = _build_provenance(
        ref, etag, trigger
    )
    written, skipped = _write_annotations(ref, etag, annotations)
    return ProcessResult(written, skipped, "ok")


def _ref_from_s3_record(record: dict[str, Any]) -> ObjectRef:
    """Build an ObjectRef from an S3 event notification record.

    ``versionId`` is present only when the bucket has versioning enabled.
    """
    s3_object = record["s3"]["object"]
    return ObjectRef(
        bucket=record["s3"]["bucket"]["name"],
        key=urllib.parse.unquote_plus(s3_object["key"]),
        version_id=s3_object.get("versionId") or None,
    )


def _ref_from_batch_task(task: dict[str, Any]) -> ObjectRef:
    """Build an ObjectRef from an S3 Batch Operations task.

    Schema 2.0 supplies ``s3Bucket`` (a bucket name); schema 1.0 supplies
    ``s3BucketArn``. Keys arrive URL-encoded.
    """
    bucket = task.get("s3Bucket") or task["s3BucketArn"].split(":::", 1)[-1]
    return ObjectRef(
        bucket=bucket,
        key=urllib.parse.unquote_plus(task["s3Key"]),
        version_id=task.get("s3VersionId") or None,
    )


def _handle_s3_event(event: dict[str, Any]) -> dict[str, Any]:
    """Process every record in an S3 event notification."""
    total_written = 0
    total_skipped = 0
    for record in event.get("Records", []):
        result = _process_object(_ref_from_s3_record(record), TRIGGER_S3_EVENT)
        total_written += result.written
        total_skipped += result.skipped

    summary = {
        "annotations_written": total_written,
        "annotations_skipped": total_skipped,
    }
    logger.info("Enrichment complete: %s", summary)
    return summary


def _batch_task_result(task: dict[str, Any]) -> dict[str, str]:
    """Enrich one Batch Operations task and map the outcome to a result code."""
    try:
        result = _process_object(_ref_from_batch_task(task), TRIGGER_BATCH_OPERATIONS)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "Unknown")
        result_code = (
            "TemporaryFailure" if code in RETRYABLE_ERROR_CODES else "PermanentFailure"
        )
        logger.exception("Batch task %s failed with %s", task.get("taskId"), code)
        return {
            "taskId": task["taskId"],
            "resultCode": result_code,
            "resultString": f"{code}: {error}",
        }

    if result.status == "missing":
        result_code, message = "PermanentFailure", "Object or version not found"
    elif result.status == "invalid_model_output":
        result_code, message = "PermanentFailure", "Model response was not valid JSON"
    elif result.skipped:
        # Partial writes are retried; the retry re-reads the object.
        result_code = "TemporaryFailure"
        message = f"Wrote {result.written} annotations, {result.skipped} failed"
    else:
        result_code, message = "Succeeded", f"Wrote {result.written} annotations"
    return {"taskId": task["taskId"], "resultCode": result_code, "resultString": message}


def _handle_batch_operations(event: dict[str, Any]) -> dict[str, Any]:
    """Process an S3 Batch Operations invocation and return its results."""
    results = [_batch_task_result(task) for task in event.get("tasks", [])]
    logger.info("Batch Operations results: %s", results)
    return {
        "invocationSchemaVersion": event["invocationSchemaVersion"],
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": event["invocationId"],
        "results": results,
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point for S3 events and S3 Batch Operations jobs."""
    if "invocationSchemaVersion" in event:
        return _handle_batch_operations(event)
    return _handle_s3_event(event)

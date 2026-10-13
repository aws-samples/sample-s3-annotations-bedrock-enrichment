#!/usr/bin/env python3
"""CDK app entry point for the s3-annotations-bedrock-enrichment project.

Reads configuration from CDK context (``cdk.json`` or ``--context``
overrides), instantiates the stacks in dependency order
(StorageStack -> EnrichmentStack -> QueryStack), passes the bucket
reference from the StorageStack to the dependent stacks, and tags every
resource with ``Project: s3-annotations-enrichment``.

Context variables and defaults:
    project_prefix (str): Resource name prefix. Default ``s3-enrich``.
    model_id (str): Bedrock foundation-model ID.
        Default ``amazon.nova-pro-v1:0``.
    max_content_length (int): Max document characters sent to Bedrock.
        Default ``10000``.
    annotation_prefix (str): Prefix for annotation names. Default ``ai``.
    lambda_reserved_concurrency (int): Reserved concurrent executions.
        Default ``10``.
    lambda_memory_mb (int): Lambda memory size in MB. Default ``1024``.
    lambda_timeout_minutes (int): Lambda timeout in minutes. Default ``5``.
    event_filter_prefix (str): S3 key prefix filter for object-created
        events. Default ``documents/``.
    event_filter_suffix (str): S3 key suffix filter for object-created
        events. Default ``.txt``.
    enable_annotation_tables (bool): Configure the S3 Metadata
        annotation table. Default ``True``.
    enable_query_stack (bool): Deploy the optional QueryStack with
        Athena resources. Default ``False``.
    use_kms_encryption (bool): Encrypt the bucket with a KMS key
        instead of S3-managed keys. Default ``False``.
"""

from typing import Any, Optional

import aws_cdk as cdk

from stacks.enrichment_stack import EnrichmentStack
from stacks.query_stack import QueryStack
from stacks.storage_stack import StorageStack

PROJECT_TAG = "s3-annotations-enrichment"


def get_context(app: cdk.App, key: str, default: Any) -> Any:
    """Read a context value, falling back to a documented default.

    Args:
        app: The CDK app whose context is queried.
        key: Context variable name.
        default: Value returned when the key is absent.

    Returns:
        The context value, or ``default`` when the key is not set.
    """
    value: Optional[Any] = app.node.try_get_context(key)
    return default if value is None else value


def as_bool(value: Any) -> bool:
    """Coerce a context value to a boolean.

    Values from ``cdk.json`` arrive as native booleans, while
    ``--context key=value`` CLI overrides arrive as strings, so both
    forms are accepted.

    Args:
        value: The raw context value.

    Returns:
        The boolean interpretation of ``value``.
    """
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


def main() -> None:
    """Synthesize the CDK app."""
    app = cdk.App()

    project_prefix: str = get_context(app, "project_prefix", "s3-enrich")
    model_id: str = get_context(app, "model_id", "amazon.nova-pro-v1:0")
    max_content_length = int(get_context(app, "max_content_length", 10000))
    annotation_prefix: str = get_context(app, "annotation_prefix", "ai")
    reserved_concurrency = int(
        get_context(app, "lambda_reserved_concurrency", 10)
    )
    memory_mb = int(get_context(app, "lambda_memory_mb", 1024))
    timeout_minutes = int(get_context(app, "lambda_timeout_minutes", 5))
    event_filter_prefix: str = get_context(
        app, "event_filter_prefix", "documents/"
    )
    event_filter_suffix: str = get_context(app, "event_filter_suffix", ".txt")
    enable_annotation_tables = as_bool(
        get_context(app, "enable_annotation_tables", True)
    )
    enable_query_stack = as_bool(
        get_context(app, "enable_query_stack", False)
    )
    use_kms_encryption = as_bool(
        get_context(app, "use_kms_encryption", False)
    )

    storage_stack = StorageStack(
        app,
        "StorageStack",
        project_prefix=project_prefix,
        enable_annotation_tables=enable_annotation_tables,
        use_kms_encryption=use_kms_encryption,
        description="S3 bucket and annotation table for AI enrichment",
    )

    enrichment_stack = EnrichmentStack(
        app,
        "EnrichmentStack",
        bucket=storage_stack.bucket,
        model_id=model_id,
        max_content_length=max_content_length,
        annotation_prefix=annotation_prefix,
        reserved_concurrency=reserved_concurrency,
        memory_mb=memory_mb,
        timeout_minutes=timeout_minutes,
        event_filter_prefix=event_filter_prefix,
        event_filter_suffix=event_filter_suffix,
        description="Enrichment Lambda wired to S3 object-created events",
    )
    enrichment_stack.add_dependency(storage_stack)

    if enable_query_stack:
        query_stack = QueryStack(
            app,
            "QueryStack",
            project_prefix=project_prefix,
            bucket=storage_stack.bucket,
            annotation_prefix=annotation_prefix,
            description="Athena workgroup and named annotation queries",
        )
        query_stack.add_dependency(enrichment_stack)

    cdk.Tags.of(app).add("Project", PROJECT_TAG)

    app.synth()


if __name__ == "__main__":
    main()

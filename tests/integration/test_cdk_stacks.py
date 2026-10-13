"""CDK assertion tests for the enrichment pipeline stacks.

Synthesizes the StorageStack and EnrichmentStack with the default
context values from ``cdk.json`` and asserts on the generated
CloudFormation templates using ``aws_cdk.assertions``:

- Bucket: EventBridge notifications enabled, all public access
  blocked, SSL-enforcing bucket policy.
- Lambda: Python 3.13 runtime, 300-second timeout, reserved
  concurrency set.
- IAM: the Lambda role policy grants ``s3:PutObjectAnnotation``,
  never grants ``s3:DeleteObjectAnnotation``, scopes
  ``bedrock:InvokeModel`` to the exact foundation-model ARN, and
  contains no wildcard-only resources.
- Event notification: targets the Lambda with the configured
  prefix/suffix filters. The notification custom resource lands in
  the EnrichmentStack template (the bucket is re-imported by name
  there), so it is asserted against that stack.

Validates: Requirements 8.3
"""

import json
from pathlib import Path
from typing import Any, Dict, Tuple

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from stacks.enrichment_stack import EnrichmentStack
from stacks.storage_stack import StorageStack

# Default context values mirroring cdk.json.
MODEL_ID = "amazon.nova-pro-v1:0"
PROJECT_PREFIX = "s3-enrich"
EVENT_FILTER_PREFIX = "documents/"
EVENT_FILTER_SUFFIX = ".txt"
RESERVED_CONCURRENCY = 10
TIMEOUT_MINUTES = 5


@pytest.fixture(scope="module")
def templates() -> Tuple[Template, Template]:
    """Synthesize the stacks once and expose both templates.

    Returns:
        Tuple of (StorageStack template, EnrichmentStack template).
    """
    app = cdk.App()
    storage_stack = StorageStack(
        app,
        "StorageStack",
        project_prefix=PROJECT_PREFIX,
        enable_annotation_tables=True,
        use_kms_encryption=False,
    )
    enrichment_stack = EnrichmentStack(
        app,
        "EnrichmentStack",
        bucket=storage_stack.bucket,
        model_id=MODEL_ID,
        max_content_length=10000,
        annotation_prefix="ai",
        reserved_concurrency=RESERVED_CONCURRENCY,
        memory_mb=1024,
        timeout_minutes=TIMEOUT_MINUTES,
        event_filter_prefix=EVENT_FILTER_PREFIX,
        event_filter_suffix=EVENT_FILTER_SUFFIX,
    )
    return (
        Template.from_stack(storage_stack),
        Template.from_stack(enrichment_stack),
    )


@pytest.fixture(scope="module")
def storage_template(templates: Tuple[Template, Template]) -> Template:
    """The synthesized StorageStack template."""
    return templates[0]


@pytest.fixture(scope="module")
def enrichment_template(templates: Tuple[Template, Template]) -> Template:
    """The synthesized EnrichmentStack template."""
    return templates[1]


def _lambda_role_policy_document(
    enrichment_template: Template,
) -> Dict[str, Any]:
    """Locate the enrichment Lambda role's default policy document.

    The EnrichmentStack template contains other IAM policies (for the
    bucket-notification custom-resource handler), so the Lambda role
    policy is identified by its ``DocumentReadAndAnnotate`` statement
    Sid.

    Args:
        enrichment_template: The synthesized EnrichmentStack template.

    Returns:
        The PolicyDocument of the Lambda role's default policy.
    """
    policies = enrichment_template.find_resources("AWS::IAM::Policy")
    for resource in policies.values():
        document = resource["Properties"]["PolicyDocument"]
        sids = {
            statement.get("Sid")
            for statement in document["Statement"]
            if isinstance(statement, dict)
        }
        if "DocumentReadAndAnnotate" in sids:
            return document
    pytest.fail(
        "Lambda role default policy (Sid=DocumentReadAndAnnotate) "
        "not found in the EnrichmentStack template"
    )


class TestBucket:
    """Assertions on the StorageStack bucket configuration."""

    def test_eventbridge_notifications_enabled(
        self, storage_template: Template
    ) -> None:
        """The bucket has EventBridge notifications enabled."""
        storage_template.has_resource_properties(
            "Custom::S3BucketNotifications",
            {
                "NotificationConfiguration": {
                    "EventBridgeConfiguration": {},
                },
            },
        )

    def test_bucket_name_includes_account_and_region(
        self, storage_template: Template
    ) -> None:
        """The bucket name can't be pre-registered by another account."""
        buckets = storage_template.find_resources("AWS::S3::Bucket")
        name = json.dumps(next(iter(buckets.values()))["Properties"]["BucketName"])
        assert "AWS::AccountId" in name
        assert "AWS::Region" in name
        assert f"{PROJECT_PREFIX}-enrichment-" in name

    def test_versioning_enabled(self, storage_template: Template) -> None:
        """Versioning is on so annotations land on the analyzed version."""
        storage_template.has_resource_properties(
            "AWS::S3::Bucket",
            {"VersioningConfiguration": {"Status": "Enabled"}},
        )

    def test_public_access_fully_blocked(
        self, storage_template: Template
    ) -> None:
        """All four public-access-block flags are enabled."""
        storage_template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
            },
        )

    def test_ssl_enforced_by_bucket_policy(
        self, storage_template: Template
    ) -> None:
        """The bucket policy denies all requests over insecure transport."""
        storage_template.has_resource_properties(
            "AWS::S3::BucketPolicy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Effect": "Deny",
                                    "Action": "s3:*",
                                    "Principal": {"AWS": "*"},
                                    "Condition": {
                                        "Bool": {
                                            "aws:SecureTransport": "false"
                                        }
                                    },
                                }
                            )
                        ]
                    ),
                },
            },
        )


class TestLambdaFunction:
    """Assertions on the enrichment Lambda configuration."""

    def test_runtime_timeout_and_reserved_concurrency(
        self, enrichment_template: Template
    ) -> None:
        """Runtime is python3.13 with a 300 s timeout and capped concurrency."""
        enrichment_template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Runtime": "python3.13",
                "Timeout": TIMEOUT_MINUTES * 60,
                "ReservedConcurrentExecutions": RESERVED_CONCURRENCY,
            },
        )


class TestLambdaDependencies:
    """The pinned SDK ships as a layer instead of a synth-time pip call."""

    def test_function_uses_dependencies_layer(
        self, enrichment_template: Template
    ) -> None:
        enrichment_template.resource_count_is("AWS::Lambda::LayerVersion", 1)
        functions = enrichment_template.find_resources("AWS::Lambda::Function")
        enrichment = [
            resource
            for logical_id, resource in functions.items()
            if logical_id.startswith("EnrichmentFunction")
        ]
        assert len(enrichment) == 1
        assert len(enrichment[0]["Properties"]["Layers"]) == 1

    def test_stack_module_has_no_subprocess(self) -> None:
        source = Path(__file__).resolve().parents[2] / "stacks" / "enrichment_stack.py"
        assert "subprocess" not in source.read_text()


class TestLambdaIamPolicy:
    """Assertions on the Lambda role's least-privilege policy."""

    def test_put_object_annotation_granted(
        self, enrichment_template: Template
    ) -> None:
        """The role can write annotations on the document prefix."""
        document = _lambda_role_policy_document(enrichment_template)
        actions = [
            action
            for statement in document["Statement"]
            for action in (
                statement["Action"]
                if isinstance(statement["Action"], list)
                else [statement["Action"]]
            )
        ]
        assert "s3:PutObjectAnnotation" in actions

    def test_delete_object_annotation_absent_everywhere(
        self, enrichment_template: Template
    ) -> None:
        """No resource in the stack grants s3:DeleteObjectAnnotation."""
        template_json = json.dumps(enrichment_template.to_json())
        assert "s3:DeleteObjectAnnotation" not in template_json

    def test_bedrock_invoke_scoped_to_model_arn(
        self, enrichment_template: Template
    ) -> None:
        """bedrock:InvokeModel is scoped to the exact foundation-model ARN."""
        document = _lambda_role_policy_document(enrichment_template)
        bedrock_statements = [
            statement
            for statement in document["Statement"]
            if statement.get("Sid") == "BedrockInvokeModel"
        ]
        assert len(bedrock_statements) == 1
        statement = bedrock_statements[0]
        assert statement["Action"] == "bedrock:InvokeModel"
        # The resource is an Fn::Join of partition/region intrinsics
        # ending in the exact foundation-model ARN suffix.
        resource_json = json.dumps(statement["Resource"])
        assert f"foundation-model/{MODEL_ID}" in resource_json

    def test_no_wildcard_only_resources(
        self, enrichment_template: Template
    ) -> None:
        """No statement in the Lambda role policy uses a bare wildcard."""
        document = _lambda_role_policy_document(enrichment_template)
        for statement in document["Statement"]:
            resources = statement["Resource"]
            if not isinstance(resources, list):
                resources = [resources]
            for resource in resources:
                assert resource != "*", (
                    f"Wildcard-only resource found in statement "
                    f"{statement.get('Sid', '<no Sid>')}"
                )


class TestVersionAndBackfillPermissions:
    """Assertions on version-aware reads and the backfill role."""

    def test_lambda_can_read_specific_versions(
        self, enrichment_template: Template
    ) -> None:
        """The Lambda role grants s3:GetObjectVersion for versioned reads."""
        document = _lambda_role_policy_document(enrichment_template)
        statement = next(
            s for s in document["Statement"]
            if s.get("Sid") == "DocumentReadAndAnnotate"
        )
        assert "s3:GetObjectVersion" in statement["Action"]
        # Version-targeted annotation writes need their own action.
        assert "s3:PutObjectVersionAnnotation" in statement["Action"]

    def test_batch_operations_role_trust_and_invoke(
        self, enrichment_template: Template
    ) -> None:
        """A role trusted by S3 Batch Operations can invoke the Lambda."""
        enrichment_template.has_resource_properties(
            "AWS::IAM::Role",
            {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        Match.object_like(
                            {
                                "Principal": {
                                    "Service": "batchoperations.s3.amazonaws.com"
                                },
                            }
                        )
                    ],
                },
            },
        )
        enrichment_template.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Sid": "InvokeEnrichmentFunction",
                                    "Action": "lambda:InvokeFunction",
                                }
                            )
                        ]
                    ),
                },
            },
        )


class TestAnnotationTableRole:
    """Assertions on the role S3 Metadata uses to populate the table."""

    def test_role_trusted_by_metadata_service(
        self, storage_template: Template
    ) -> None:
        """The annotation-table role is trusted by metadata.s3.amazonaws.com."""
        storage_template.has_resource_properties(
            "AWS::IAM::Role",
            {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        Match.object_like(
                            {"Principal": {"Service": "metadata.s3.amazonaws.com"}}
                        )
                    ],
                },
            },
        )


class TestEventNotification:
    """Assertions on the S3 object-created event wiring."""

    def test_notification_targets_lambda_with_filters(
        self, enrichment_template: Template
    ) -> None:
        """The notification targets the Lambda with prefix/suffix filters.

        ``Match.array_with`` is order-sensitive and CDK does not
        guarantee filter-rule ordering, so the rules are compared as a
        set after extracting the notification resource.
        """
        notifications = enrichment_template.find_resources(
            "Custom::S3BucketNotifications"
        )
        assert len(notifications) == 1
        resource = next(iter(notifications.values()))
        configurations = resource["Properties"][
            "NotificationConfiguration"
        ]["LambdaFunctionConfigurations"]
        assert len(configurations) == 1
        configuration = configurations[0]

        assert configuration["Events"] == ["s3:ObjectCreated:*"]
        assert "LambdaFunctionArn" in configuration

        filter_rules = {
            (rule["Name"], rule["Value"])
            for rule in configuration["Filter"]["Key"]["FilterRules"]
        }
        assert filter_rules == {
            ("prefix", EVENT_FILTER_PREFIX),
            ("suffix", EVENT_FILTER_SUFFIX),
        }

    def test_lambda_invoke_permission_for_s3(
        self, enrichment_template: Template
    ) -> None:
        """S3 is permitted to invoke the enrichment Lambda."""
        enrichment_template.has_resource_properties(
            "AWS::Lambda::Permission",
            {
                "Action": "lambda:InvokeFunction",
                "Principal": "s3.amazonaws.com",
            },
        )


@pytest.fixture(scope="module")
def query_template() -> Template:
    """The synthesized QueryStack template."""
    from stacks.query_stack import QueryStack

    app = cdk.App()
    storage = StorageStack(app, "QStorage", project_prefix=PROJECT_PREFIX)
    stack = QueryStack(
        app, "QQuery", project_prefix=PROJECT_PREFIX, bucket=storage.bucket
    )
    return Template.from_stack(stack)


class TestQueryStack:
    """Assertions on the optional QueryStack named queries."""

    def test_named_queries_are_resolved_by_cloudformation(
        self, query_template: Template
    ) -> None:
        """Queries use Fn::Sub, and the entity search takes a parameter."""
        queries = query_template.find_resources("AWS::Athena::NamedQuery")
        assert len(queries) == 3
        bodies = [json.dumps(q["Properties"]["QueryString"]) for q in queries.values()]
        assert all("Fn::Sub" in body for body in bodies)
        assert any("LIKE ?" in body for body in bodies)

    def test_invalid_annotation_prefix_rejected(self) -> None:
        from stacks.query_stack import QueryStack

        app = cdk.App()
        storage = StorageStack(app, "QStorage2", project_prefix=PROJECT_PREFIX)
        with pytest.raises(ValueError):
            QueryStack(
                app,
                "QQuery2",
                project_prefix=PROJECT_PREFIX,
                bucket=storage.bucket,
                annotation_prefix="ai'; DROP",
            )

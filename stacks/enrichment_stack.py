"""EnrichmentStack: Lambda function, S3 event wiring, and least-privilege IAM.

Provisions the Python 3.13 enrichment Lambda function, subscribes it to
S3 ``OBJECT_CREATED`` events on the enrichment data bucket (filtered by
prefix/suffix), and attaches explicit least-privilege IAM statements:
object read and annotation read/write scoped to the document prefix,
and ``bedrock:InvokeModel`` scoped to the exact foundation-model ARN.
``s3:DeleteObjectAnnotation`` and wildcard resources are deliberately
omitted. Also creates the role that S3 Batch Operations assumes to
backfill existing objects through the same Lambda.
"""

from pathlib import Path
from typing import Any

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
)
from constructs import Construct

PROJECT_ROOT = Path(__file__).resolve().parent.parent

LAMBDA_CODE_PATH = PROJECT_ROOT / "lambda_functions" / "enrichment"

#: Lambda layer contents built by the ``app`` command in cdk.json. The
#: Lambda runtime's bundled boto3 predates the S3 object-annotation APIs,
#: so the pinned boto3 from the function's requirements.txt ships as a
#: layer. Layers load before the runtime's own libraries.
LAMBDA_DEPENDENCIES_PATH = PROJECT_ROOT / ".build" / "lambda-dependencies"

#: Command that builds the layer (also run automatically by cdk.json).
BUILD_DEPENDENCIES_COMMAND = (
    "python3 -m pip install --quiet --no-input --only-binary=:all: "
    "--upgrade --requirement lambda_functions/enrichment/requirements.txt "
    "--target .build/lambda-dependencies/python"
)

#: Where the backfill job reads its S3 Inventory manifest from.
BATCH_MANIFEST_PREFIX = "inventory/"

#: Where the backfill job writes its completion report.
BATCH_REPORT_PREFIX = "batch-reports/"


def dependencies_layer_code() -> lambda_.Code:
    """Return the asset for the dependencies layer.

    Raises:
        FileNotFoundError: If the layer hasn't been built yet. Running
            ``cdk synth`` or ``cdk deploy`` builds it through cdk.json.
    """
    if not (LAMBDA_DEPENDENCIES_PATH / "python").is_dir():
        raise FileNotFoundError(
            f"{LAMBDA_DEPENDENCIES_PATH / 'python'} not found. Build the "
            f"Lambda dependencies first: {BUILD_DEPENDENCIES_COMMAND}"
        )
    return lambda_.Code.from_asset(str(LAMBDA_DEPENDENCIES_PATH))


class EnrichmentStack(Stack):
    """Enrichment Lambda wired to S3 object-created events.

    Attributes:
        function: The enrichment Lambda function, exposed for
            cross-stack references and assertion tests.
        batch_operations_role: Role S3 Batch Operations assumes to run
            backfill jobs that invoke ``function`` for existing objects.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.Bucket,
        model_id: str,
        max_content_length: int,
        annotation_prefix: str,
        reserved_concurrency: int,
        memory_mb: int,
        timeout_minutes: int,
        event_filter_prefix: str,
        event_filter_suffix: str,
        **kwargs: Any,
    ) -> None:
        """Initialize the EnrichmentStack.

        Args:
            scope: The CDK app or parent construct.
            construct_id: Logical ID of this stack.
            bucket: The enrichment data bucket from the StorageStack.
            model_id: Bedrock foundation-model ID used for enrichment
                (for example ``amazon.nova-pro-v1:0``).
            max_content_length: Maximum number of document characters
                sent to Bedrock.
            annotation_prefix: Prefix for annotation names (must not
                start with the reserved ``aws`` or ``s3`` prefixes).
            reserved_concurrency: Reserved concurrent executions for
                the Lambda, rate-limiting Bedrock calls.
            memory_mb: Lambda memory size in MB.
            timeout_minutes: Lambda timeout in minutes.
            event_filter_prefix: S3 key prefix filter for the
                object-created event notification (for example
                ``documents/``).
            event_filter_suffix: S3 key suffix filter for the
                object-created event notification (for example
                ``.txt``).
            **kwargs: Additional Stack keyword arguments (env, etc.).
        """
        super().__init__(scope, construct_id, **kwargs)

        self.function = self._create_function(
            model_id=model_id,
            max_content_length=max_content_length,
            annotation_prefix=annotation_prefix,
            reserved_concurrency=reserved_concurrency,
            memory_mb=memory_mb,
            timeout_minutes=timeout_minutes,
        )
        self._wire_event_notification(
            bucket, event_filter_prefix, event_filter_suffix
        )
        self._grant_least_privilege(bucket, event_filter_prefix, model_id)
        self.batch_operations_role = self._create_batch_operations_role(bucket)
        self._create_outputs()

    def _create_function(
        self,
        *,
        model_id: str,
        max_content_length: int,
        annotation_prefix: str,
        reserved_concurrency: int,
        memory_mb: int,
        timeout_minutes: int,
    ) -> lambda_.Function:
        """Create the Python 3.13 enrichment Lambda function.

        Args:
            model_id: Bedrock foundation-model ID.
            max_content_length: Content truncation limit in characters.
            annotation_prefix: Prefix for annotation names.
            reserved_concurrency: Reserved concurrent executions.
            memory_mb: Memory size in MB.
            timeout_minutes: Timeout in minutes.

        Returns:
            The configured Lambda function.
        """
        dependencies = lambda_.LayerVersion(
            self,
            "EnrichmentDependencies",
            code=dependencies_layer_code(),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Pinned boto3 with the S3 object-annotation APIs",
        )
        return lambda_.Function(
            self,
            "EnrichmentFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=lambda_.Code.from_asset(
                str(LAMBDA_CODE_PATH),
                exclude=["__pycache__", "*.pyc", "requirements.txt"],
            ),
            layers=[dependencies],
            memory_size=memory_mb,
            timeout=Duration.minutes(timeout_minutes),
            reserved_concurrent_executions=reserved_concurrency,
            environment={
                "MODEL_ID": model_id,
                "MAX_CONTENT_LENGTH": str(max_content_length),
                "ANNOTATION_PREFIX": annotation_prefix,
            },
            description=(
                "Analyzes uploaded documents with Amazon Bedrock and "
                "writes the results back as S3 object annotations"
            ),
        )

    def _wire_event_notification(
        self,
        bucket: s3.Bucket,
        event_filter_prefix: str,
        event_filter_suffix: str,
    ) -> None:
        """Subscribe the Lambda to S3 object-created events.

        The bucket is re-imported by name so the notification custom
        resource is created in this stack. Calling
        ``add_event_notification`` on the bucket construct directly
        would place the notification resource in the StorageStack,
        creating a cyclic cross-stack reference (StorageStack would
        need the Lambda ARN while this stack needs the bucket ARN).

        Args:
            bucket: The enrichment data bucket.
            event_filter_prefix: Key prefix filter for the notification.
            event_filter_suffix: Key suffix filter for the notification.
        """
        imported_bucket = s3.Bucket.from_bucket_name(
            self, "NotificationBucket", bucket.bucket_name
        )
        imported_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.function),
            s3.NotificationKeyFilter(
                prefix=event_filter_prefix,
                suffix=event_filter_suffix,
            ),
        )

    def _grant_least_privilege(
        self, bucket: s3.Bucket, event_filter_prefix: str, model_id: str
    ) -> None:
        """Attach explicit least-privilege IAM statements to the role.

        Explicit statements are used instead of ``grant_read_write`` so
        the policy contains only the enumerated actions on scoped
        resources: no ``s3:DeleteObjectAnnotation`` and no wildcard
        resources.

        Args:
            bucket: The enrichment data bucket.
            event_filter_prefix: Key prefix scoping the object-level
                permissions (for example ``documents/``).
            model_id: Bedrock foundation-model ID used to build the
                exact model ARN.
        """
        self.function.add_to_role_policy(
            iam.PolicyStatement(
                sid="DocumentReadAndAnnotate",
                actions=[
                    "s3:GetObject",
                    # Needed to read a specific version when the event or
                    # Batch Operations task carries a version ID.
                    "s3:GetObjectVersion",
                    "s3:PutObjectAnnotation",
                    # PutObjectAnnotation with a VersionId is authorized
                    # as s3:PutObjectVersionAnnotation.
                    "s3:PutObjectVersionAnnotation",
                ],
                resources=[f"{bucket.bucket_arn}/{event_filter_prefix}*"],
            )
        )
        self.function.add_to_role_policy(
            iam.PolicyStatement(
                sid="BedrockInvokeModel",
                actions=["bedrock:InvokeModel"],
                resources=[
                    f"arn:{self.partition}:bedrock:{self.region}::"
                    f"foundation-model/{model_id}"
                ],
            )
        )

    def _create_batch_operations_role(self, bucket: s3.Bucket) -> iam.Role:
        """Create the role S3 Batch Operations assumes to backfill objects.

        A backfill job reads an S3 Inventory manifest, invokes the same
        enrichment Lambda once per listed object, and writes a
        completion report. The role is scoped to the sample layout: the
        manifest under ``inventory/`` and reports under
        ``batch-reports/`` in the data bucket. Point these at your own
        inventory destination if it lives elsewhere.

        Args:
            bucket: The enrichment data bucket.

        Returns:
            The IAM role trusted by S3 Batch Operations.
        """
        role = iam.Role(
            self,
            "BatchOperationsRole",
            assumed_by=iam.ServicePrincipal(
                "batchoperations.s3.amazonaws.com",
                conditions={
                    "StringEquals": {"aws:SourceAccount": self.account},
                },
            ),
            description=(
                "Assumed by S3 Batch Operations to invoke the enrichment "
                "Lambda for existing objects"
            ),
        )
        role.add_to_policy(
            iam.PolicyStatement(
                sid="InvokeEnrichmentFunction",
                actions=["lambda:InvokeFunction"],
                resources=[
                    self.function.function_arn,
                    f"{self.function.function_arn}:*",
                ],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadInventoryManifest",
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[f"{bucket.bucket_arn}/{BATCH_MANIFEST_PREFIX}*"],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                sid="WriteCompletionReport",
                actions=["s3:PutObject"],
                resources=[f"{bucket.bucket_arn}/{BATCH_REPORT_PREFIX}*"],
            )
        )
        return role

    def _create_outputs(self) -> None:
        """Export the Lambda function and Batch Operations role ARNs."""
        CfnOutput(
            self,
            "BatchOperationsRoleArn",
            value=self.batch_operations_role.role_arn,
            description="Role for S3 Batch Operations backfill jobs",
        )
        CfnOutput(
            self,
            "EnrichmentFunctionName",
            value=self.function.function_name,
            description="Name of the enrichment Lambda function",
        )
        CfnOutput(
            self,
            "EnrichmentFunctionArn",
            value=self.function.function_arn,
            description="ARN of the enrichment Lambda function",
        )

"""StorageStack: S3 bucket, annotation table configuration, and IAM role.

Provisions the enrichment data bucket with EventBridge notifications,
SSL enforcement, blocked public access, and server-side encryption.
Optionally enables the S3 Metadata annotation table via a custom
resource (CloudFormation has no native resource for
``CreateBucketMetadataConfiguration``) together with a dedicated IAM
role that the S3 metadata service assumes to populate the Apache
Iceberg table.
"""

from typing import Any, Optional

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3,
    custom_resources as cr,
)
from constructs import Construct


class StorageStack(Stack):
    """S3 bucket with optional S3 Metadata annotation table configuration.

    Attributes:
        bucket: The enrichment data bucket, exposed for cross-stack
            references (EnrichmentStack wires event notifications and
            IAM policies against it).
        annotation_table_role: The dedicated IAM role assumed by the S3
            metadata service to populate the annotation table. ``None``
            when annotation tables are disabled.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_prefix: str,
        enable_annotation_tables: bool = True,
        use_kms_encryption: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the StorageStack.

        Args:
            scope: The CDK app or parent construct.
            construct_id: Logical ID of this stack.
            project_prefix: Prefix used to name the bucket
                (``{project_prefix}-enrichment-{account}-{region}``).
            enable_annotation_tables: When True, configure the S3
                Metadata annotation table via a custom resource.
            use_kms_encryption: When True, use a KMS key for bucket
                encryption instead of S3-managed keys.
            **kwargs: Additional Stack keyword arguments (env, etc.).
        """
        super().__init__(scope, construct_id, **kwargs)

        self.encryption_key: Optional[kms.Key] = None
        self.bucket = self._create_bucket(project_prefix, use_kms_encryption)

        self.annotation_table_role: Optional[iam.Role] = None
        annotation_table_status = "DISABLED"
        if enable_annotation_tables:
            self.annotation_table_role = self._create_annotation_table_role()
            if self.encryption_key is not None:
                # The metadata service decrypts SSE-KMS annotations to
                # populate the table.
                self.encryption_key.grant_decrypt(self.annotation_table_role)
            self._configure_annotation_table(self.annotation_table_role)
            annotation_table_status = "ENABLED"

        self._create_outputs(annotation_table_status)

    def _create_bucket(
        self, project_prefix: str, use_kms_encryption: bool
    ) -> s3.Bucket:
        """Create the enrichment data bucket.

        Args:
            project_prefix: Prefix for the bucket name.
            use_kms_encryption: Whether to encrypt with a KMS key.

        Returns:
            The configured S3 bucket.
        """
        if use_kms_encryption:
            encryption_key: Optional[kms.Key] = kms.Key(
                self,
                "BucketEncryptionKey",
                enable_key_rotation=True,
                removal_policy=RemovalPolicy.DESTROY,
            )
            encryption = s3.BucketEncryption.KMS
        else:
            encryption_key = None
            encryption = s3.BucketEncryption.S3_MANAGED
        self.encryption_key = encryption_key

        return s3.Bucket(
            self,
            "EnrichmentDataBucket",
            # Account ID and Region make the name unique to the deployer,
            # so nobody can pre-register it (bucket squatting).
            bucket_name=f"{project_prefix}-enrichment-{self.account}-{self.region}",
            event_bridge_enabled=True,
            # Versioning lets the enrichment Lambda annotate the exact
            # version it analyzed (events and inventory carry versionId).
            versioned=True,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=encryption,
            encryption_key=encryption_key,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

    def _create_annotation_table_role(self) -> iam.Role:
        """Create the dedicated role for annotation table population.

        This role is intentionally separate from the Lambda execution
        role (separation of duties): the S3 metadata service assumes it
        to read annotations from the bucket and populate the annotation
        table. Trust and permissions follow the documented example for
        ``UpdateBucketMetadataAnnotationTableConfiguration``.

        Returns:
            The IAM role trusted by the S3 metadata service.
        """
        role = iam.Role(
            self,
            "AnnotationTableRole",
            assumed_by=iam.ServicePrincipal(
                "metadata.s3.amazonaws.com",
                conditions={
                    "StringEquals": {"aws:SourceAccount": self.account},
                    "ArnLike": {"aws:SourceArn": self.bucket.bucket_arn},
                },
            ),
            description=(
                "Assumed by the S3 metadata service to read object "
                "annotations and populate the annotation table"
            ),
        )

        role.add_to_policy(
            iam.PolicyStatement(
                sid="PermissionForGetAnnotation",
                actions=[
                    "s3:GetObjectAnnotation",
                    "s3:GetObjectVersionAnnotation",
                ],
                resources=[f"{self.bucket.bucket_arn}/*"],
                conditions={
                    "StringEquals": {"aws:ResourceAccount": self.account}
                },
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                sid="PermissionsForListBucket",
                actions=["s3:ListBucket", "s3:ListBucketVersions"],
                resources=[self.bucket.bucket_arn],
                conditions={
                    "StringEquals": {"aws:ResourceAccount": self.account}
                },
            )
        )
        return role

    def _configure_annotation_table(self, role: iam.Role) -> None:
        """Enable the S3 Metadata annotation table via a custom resource.

        CloudFormation has no native resource for
        ``CreateBucketMetadataConfiguration``, so an AwsCustomResource
        issues the SDK call on create and removes the configuration via
        ``DeleteBucketMetadataConfiguration`` on stack deletion.

        The API requires a ``JournalTableConfiguration`` in every
        metadata configuration (the journal table records object events
        and cannot be disabled); the annotation table rides alongside
        it. The delete call ignores ``MetadataConfigurationNotFound``
        so a failed create never wedges stack rollback or deletion.

        Args:
            role: The annotation-table role S3 uses to populate the
                Iceberg table.
        """
        create_call = cr.AwsSdkCall(
            service="S3",
            action="CreateBucketMetadataConfiguration",
            parameters={
                "Bucket": self.bucket.bucket_name,
                "MetadataConfiguration": {
                    "JournalTableConfiguration": {
                        "RecordExpiration": {
                            "Expiration": "ENABLED",
                            "Days": 30,
                        },
                    },
                    "AnnotationTableConfiguration": {
                        "ConfigurationState": "ENABLED",
                        "Role": role.role_arn,
                    },
                },
            },
            physical_resource_id=cr.PhysicalResourceId.of(
                f"{self.bucket.bucket_name}-metadata-configuration"
            ),
        )

        delete_call = cr.AwsSdkCall(
            service="S3",
            action="DeleteBucketMetadataConfiguration",
            parameters={"Bucket": self.bucket.bucket_name},
            ignore_error_codes_matching="MetadataConfigurationNotFound",
        )

        metadata_configuration = cr.AwsCustomResource(
            self,
            "AnnotationTableConfiguration",
            on_create=create_call,
            on_delete=delete_call,
            # The annotation table configuration API is newer than the SDK
            # bundled in the Lambda runtime, so the latest SDK is required.
            install_latest_aws_sdk=True,
            policy=cr.AwsCustomResourcePolicy.from_statements(
                [
                    iam.PolicyStatement(
                        actions=[
                            "s3:CreateBucketMetadataTableConfiguration",
                            "s3:DeleteBucketMetadataTableConfiguration",
                        ],
                        resources=[self.bucket.bucket_arn],
                    ),
                    # The metadata configuration provisions tables in
                    # the AWS-managed S3 Tables catalog (documented
                    # caller permissions for
                    # CreateBucketMetadataConfiguration).
                    iam.PolicyStatement(
                        actions=[
                            "s3tables:CreateTableBucket",
                            "s3tables:CreateNamespace",
                            "s3tables:GetTable",
                            "s3tables:CreateTable",
                            "s3tables:PutTablePolicy",
                            "s3tables:PutTableBucketPolicy",
                            "s3tables:PutTableEncryption",
                        ],
                        resources=[
                            self.format_arn(
                                service="s3tables",
                                resource="bucket",
                                resource_name="*",
                            ),
                        ],
                    ),
                    iam.PolicyStatement(
                        actions=["kms:DescribeKey"],
                        resources=[
                            self.format_arn(
                                service="kms",
                                resource="key",
                                resource_name="*",
                            ),
                        ],
                    ),
                    iam.PolicyStatement(
                        actions=["iam:PassRole"],
                        resources=[role.role_arn],
                    ),
                ]
            ),
        )
        metadata_configuration.node.add_dependency(self.bucket)
        metadata_configuration.node.add_dependency(role)

    def _create_outputs(self, annotation_table_status: str) -> None:
        """Export bucket name, bucket ARN, and annotation table status.

        Args:
            annotation_table_status: ``ENABLED`` or ``DISABLED``.
        """
        CfnOutput(
            self,
            "BucketName",
            value=self.bucket.bucket_name,
            description="Name of the enrichment data bucket",
        )
        CfnOutput(
            self,
            "BucketArn",
            value=self.bucket.bucket_arn,
            description="ARN of the enrichment data bucket",
        )
        CfnOutput(
            self,
            "AnnotationTableStatus",
            value=annotation_table_status,
            description="Whether the S3 Metadata annotation table is enabled",
        )

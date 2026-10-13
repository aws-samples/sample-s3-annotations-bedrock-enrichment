"""QueryStack: Athena workgroup and pre-built named queries.

Provisions an Athena workgroup with a query-results location in the
enrichment data bucket (under ``athena-results/``) and three named
queries that demonstrate SQL access to the S3 Metadata annotation
table: finding financial reports via the classification annotation,
searching extracted entities by name, and listing recent annotation
activity from the journal table.

The annotation and journal tables live in the AWS-managed S3 Tables
table bucket (``aws-s3``). Athena reaches them through the
``s3tablescatalog/aws-s3`` data catalog and the ``b_{bucket-name}``
namespace, with fixed table names ``annotation`` and ``journal``. The
tables populate within roughly one hour of annotation writes.

Querying requires the one-time S3 Tables analytics integration in the
account (Glue federated catalog, Lake Formation registration, and Lake
Formation grants to the querying principal); see the README.
"""

import re
from typing import Any

from aws_cdk import (
    CfnOutput,
    Fn,
    Stack,
    aws_athena as athena,
    aws_s3 as s3,
)
from constructs import Construct

#: Allowed annotation prefixes (substituted into the named queries).
ANNOTATION_PREFIX_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")

# Named-query templates. ${...} placeholders are resolved by AWS
# CloudFormation Fn::Sub at deploy time; see _create_named_queries.
FINANCIAL_REPORTS_SQL = """-- Find all financial reports
-- Filters the ${Prefix}.classification annotation on the document
-- types the enrichment model uses for financial content.
SELECT
    object_key,
    json_extract_scalar(text_value, '$.type') AS document_type,
    json_extract_scalar(text_value, '$.confidence') AS confidence
FROM ${AnnotationTable}
WHERE name = '${Prefix}.classification'
    AND json_extract_scalar(text_value, '$.type')
        IN ('regulatory_filing', 'research_report')
ORDER BY object_key;
"""

ENTITY_SEARCH_SQL = """-- Search entities by name
-- Run with one execution parameter: a LIKE pattern such as '%Acme%'.
SELECT
    object_key,
    json_extract_scalar(entity, '$.name') AS entity_name,
    json_extract_scalar(entity, '$.type') AS entity_type
FROM ${AnnotationTable}
CROSS JOIN UNNEST(
    CAST(json_parse(text_value) AS ARRAY(JSON))
) AS t(entity)
WHERE name = '${Prefix}.entities'
    AND json_extract_scalar(entity, '$.name') LIKE ?
ORDER BY object_key;
"""

RECENT_ANNOTATIONS_SQL = """-- Recent annotations (last 24 hours)
-- Lists annotation activity recorded in the S3 Metadata journal table.
SELECT
    bucket,
    key,
    record_type,
    record_timestamp
FROM ${JournalTable}
WHERE record_timestamp > (current_timestamp - INTERVAL '24' HOUR)
ORDER BY record_timestamp DESC;
"""


class QueryStack(Stack):
    """Athena workgroup and named queries over the annotation table.

    Attributes:
        workgroup: The Athena workgroup scoping the demo queries.
        annotation_table_path: Fully qualified Athena path of the
            annotation table, exposed for outputs and scripts.
    """

    #: Data catalog through which Athena reads S3 Tables.
    S3_TABLES_CATALOG = "s3tablescatalog/aws-s3"

    #: Fixed table names within the per-bucket metadata namespace.
    ANNOTATION_TABLE = "annotation"
    JOURNAL_TABLE = "journal"

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_prefix: str,
        bucket: s3.Bucket,
        annotation_prefix: str = "ai",
        **kwargs: Any,
    ) -> None:
        """Initialize the QueryStack.

        Args:
            scope: The CDK app or parent construct.
            construct_id: Logical ID of this stack.
            project_prefix: Prefix used to name the workgroup
                (``{project_prefix}-annotation-queries``).
            bucket: The enrichment data bucket from the StorageStack;
                query results are written under its ``athena-results/``
                prefix and metadata table names are derived from its
                name.
            annotation_prefix: Prefix used in annotation names (must
                match the EnrichmentStack configuration and
                ``ANNOTATION_PREFIX_PATTERN``; default ``ai``).

        Raises:
            ValueError: If ``annotation_prefix`` is not a simple
                identifier.
            **kwargs: Additional Stack keyword arguments (env, etc.).
        """
        super().__init__(scope, construct_id, **kwargs)

        if not ANNOTATION_PREFIX_PATTERN.match(annotation_prefix):
            raise ValueError(
                f"annotation_prefix {annotation_prefix!r} must match "
                f"{ANNOTATION_PREFIX_PATTERN.pattern}"
            )

        # S3 Metadata places each bucket's tables in a namespace named
        # ``b_{bucket-name}``. The bucket name includes the account and
        # Region, so it resolves at deploy time through CloudFormation.
        self.metadata_namespace = Fn.join("", ["b_", bucket.bucket_name])

        self.annotation_table_path = Fn.join(
            "",
            [f'"{self.S3_TABLES_CATALOG}"."', self.metadata_namespace,
             f'"."{self.ANNOTATION_TABLE}"'],
        )
        journal_table_path = Fn.join(
            "",
            [f'"{self.S3_TABLES_CATALOG}"."', self.metadata_namespace,
             f'"."{self.JOURNAL_TABLE}"'],
        )

        self.workgroup = self._create_workgroup(project_prefix, bucket)
        self._create_named_queries(
            annotation_prefix, journal_table_path
        )
        self._create_outputs()

    def _create_workgroup(
        self, project_prefix: str, bucket: s3.Bucket
    ) -> athena.CfnWorkGroup:
        """Create the Athena workgroup with a query-results location.

        Args:
            project_prefix: Prefix for the workgroup name.
            bucket: Bucket receiving query results under
                ``athena-results/``.

        Returns:
            The configured Athena workgroup.
        """
        return athena.CfnWorkGroup(
            self,
            "AnnotationQueriesWorkGroup",
            name=f"{project_prefix}-annotation-queries",
            description=(
                "Workgroup for querying the S3 Metadata annotation table "
                "produced by the enrichment pipeline"
            ),
            state="ENABLED",
            # Without this, the workgroup fails to delete on
            # `cdk destroy` once it has run any queries.
            recursive_delete_option=True,
            work_group_configuration=(
                athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                    enforce_work_group_configuration=True,
                    publish_cloud_watch_metrics_enabled=False,
                    result_configuration=(
                        athena.CfnWorkGroup.ResultConfigurationProperty(
                            output_location=(
                                f"s3://{bucket.bucket_name}/athena-results/"
                            ),
                        )
                    ),
                )
            ),
        )

    def _create_named_queries(
        self, annotation_prefix: str, journal_table_path: str
    ) -> None:
        """Create the three demo named queries.

        Args:
            annotation_prefix: Prefix used in annotation names.
            journal_table_path: Fully qualified Athena path of the
                journal table.
        """
        # The SQL is assembled by AWS CloudFormation (Fn::Sub) at deploy
        # time from deployment configuration only: the table path comes
        # from the stack's own bucket, and the annotation prefix is
        # validated above. No runtime or end-user input reaches these
        # strings. The entity search takes its search term as an Athena
        # execution parameter (?) rather than as text in the query.
        variables = {
            "AnnotationTable": self.annotation_table_path,
            "JournalTable": journal_table_path,
            "Prefix": annotation_prefix,
        }

        # Query 1: financial reports via the classification annotation.
        # "Financial reports" covers the regulatory_filing and
        # research_report document types produced by the classifier.
        financial_reports_sql = Fn.sub(FINANCIAL_REPORTS_SQL, variables)

        # Query 2: search extracted entities by name. Pass the search
        # pattern (for example '%Acme%') as the execution parameter.
        entity_search_sql = Fn.sub(ENTITY_SEARCH_SQL, variables)

        # Query 3: recent annotation activity from the journal table.
        recent_annotations_sql = Fn.sub(RECENT_ANNOTATIONS_SQL, variables)

        queries = [
            (
                "FinancialReportsQuery",
                "find-financial-reports",
                "Find all documents classified as financial reports",
                financial_reports_sql,
            ),
            (
                "EntitySearchQuery",
                "search-entities-by-name",
                "Search AI-extracted entities by name",
                entity_search_sql,
            ),
            (
                "RecentAnnotationsQuery",
                "recent-annotations",
                "Annotation activity from the journal table, last 24 hours",
                recent_annotations_sql,
            ),
        ]

        for construct_id, name, description, sql in queries:
            named_query = athena.CfnNamedQuery(
                self,
                construct_id,
                database=self.metadata_namespace,
                query_string=sql,
                name=name,
                description=description,
                work_group=self.workgroup.name,
            )
            # The workgroup must exist before its named queries.
            named_query.add_dependency(self.workgroup)

    def _create_outputs(self) -> None:
        """Export the workgroup name and annotation table path."""
        CfnOutput(
            self,
            "WorkGroupName",
            value=self.workgroup.name or "",
            description="Name of the Athena workgroup for demo queries",
        )
        CfnOutput(
            self,
            "AnnotationTablePath",
            value=self.annotation_table_path,
            description=(
                "Fully qualified Athena path of the S3 Metadata "
                "annotation table"
            ),
        )

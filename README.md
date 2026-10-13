# S3 Annotations Bedrock Enrichment

An event-driven AI enrichment pipeline built with the AWS CDK (Python). When documents land in an Amazon S3 bucket, an AWS Lambda function analyzes them with Amazon Bedrock (classification, summarization, entity extraction) and writes the results back to the objects as S3 annotations. S3 Metadata annotation tables then make the enriched metadata queryable with SQL through Amazon Athena, with no ETL pipeline to build or maintain.

> **Note:** This is sample code, for non-production usage. You should work with your security and legal teams to meet your organizational security, regulatory, and compliance requirements before deployment. Deploying it creates AWS resources that incur charges.

## Architecture

```
S3 Object Upload (documents/*.txt)
        |
        v  (S3 Event Notification)
AWS Lambda — enrichment function (Python 3.13)
        |
        |--> s3:GetObject            (read document content + ETag)
        |--> bedrock Converse API    (classify, summarize, extract entities)
        |--> s3:PutObjectAnnotation  x4, conditional on ETag (and version ID)
        |       - ai.classification  (JSON)
        |       - ai.summary         (plain text)
        |       - ai.entities        (JSON)
        |       - ai.provenance      (JSON: model, prompt version, timestamp)
        |
        v  (automatic, within ~1 hour)
S3 Metadata Annotation Table (Apache Iceberg)
        |
        |--> Amazon Athena           (SQL across annotations)
        |--> S3 Tables MCP Server    (natural-language agent queries)
```

The project is split into three CDK stacks:

| Stack | Responsibility |
|---|---|
| `StorageStack` | S3 bucket, EventBridge notifications, annotation table configuration (custom resource), annotation-table IAM role |
| `EnrichmentStack` | Lambda function, S3 event notification, least-privilege IAM, S3 Batch Operations role for backfill |
| `QueryStack` (optional) | Athena workgroup and pre-built named queries |

## Prerequisites

- An AWS account with credentials configured (`aws configure` or environment variables)
- [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) v2 installed (`npm install -g aws-cdk`)
- Python 3.13
- Amazon Bedrock model access enabled for `amazon.nova-pro-v1:0` in your deployment region (request access in the Bedrock console under Model access)

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Bootstrap your account/region for CDK (first time only)
cdk bootstrap

# 3. Deploy all stacks
cdk deploy --all

# 4. Upload the sample documents and wait for annotations
python3 scripts/upload_samples.py --bucket-name <bucket-from-stack-output> --wait

# 5. Verify the annotations on an object
python3 scripts/verify_annotations.py --bucket-name <bucket-from-stack-output> --key documents/contract_sample.txt
```

The bucket name is printed as the `StorageStack` `BucketName` output after deployment. It has the form `<project_prefix>-enrichment-<account-id>-<region>`, so it's unique to your account and Region and can't be pre-registered by someone else.

## How It Works

1. **Upload**: An object created under `documents/` with a `.txt` suffix triggers an S3 event notification.
2. **Analyze**: The Lambda function reads the object (the exact version, when the event carries a `versionId`), captures its ETag, truncates the content to `max_content_length` characters, and sends it to Amazon Bedrock through the Converse API with a system prompt that requests structured JSON output.
3. **Annotate**: The function writes four annotations to the same object version using conditional writes (`ObjectIfMatch` set to the captured ETag, so a replaced object is never annotated with stale results):
   - `ai.classification` — JSON with document `type` and `confidence`
   - `ai.summary` — plain-text summary under 500 characters
   - `ai.entities` — JSON array of `{name, type}` entities
   - `ai.provenance` — JSON with `model_id`, `prompt_version`, `enriched_at`, source ETag and version ID, and trigger

The schema and prompt in `lambda_functions/enrichment/prompts.py` are the parts to change for your own documents: edit `ANNOTATION_FIELDS`, `DOCUMENT_TYPES`, `ENTITY_TYPES`, and `SYSTEM_PROMPT`, and bump `PROMPT_VERSION`.
4. **Query**: S3 Metadata surfaces the annotations in an Apache Iceberg annotation table that refreshes within about an hour, queryable through Athena or the S3 Tables MCP server.

## Verifying Results

**Immediately** (seconds after upload), read annotations straight off the object:

```bash
python3 scripts/verify_annotations.py --bucket-name <bucket> --key documents/contract_sample.txt
```

**After ~1 hour**, once the annotation table has refreshed, query it with Athena (requires the optional QueryStack, see below).

Querying S3 Tables through Athena also requires the one-time [S3 Tables integration with AWS analytics services](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html) in your account: the easiest path is the "Enable integration" button in the S3 console under Table buckets. It creates the `s3tablescatalog` AWS Glue federated catalog and registers S3 Tables with AWS Lake Formation; you then grant your querying principal Lake Formation `DESCRIBE`/`SELECT` permissions on the `b_{bucket-name}` database and its tables.

```bash
python3 scripts/query_annotations.py --bucket-name <bucket> --query-type classification
python3 scripts/query_annotations.py --bucket-name <bucket> --query-type entities
python3 scripts/query_annotations.py --bucket-name <bucket> --query-type all
```

If the table has not populated yet, the script reports this clearly instead of failing.

## Backfilling Existing Objects

The same Lambda handles S3 Batch Operations invocations, so you can enrich objects that were in the bucket before the pipeline existed. The `EnrichmentStack` outputs `BatchOperationsRoleArn`, a role that can invoke the function, read a manifest under `inventory/`, and write a report under `batch-reports/` in the data bucket (edit `BATCH_MANIFEST_PREFIX`/`BATCH_REPORT_PREFIX` in `stacks/enrichment_stack.py` if your inventory lives elsewhere).

Point a job at a CSV S3 Inventory report's `manifest.json`:

```bash
aws s3control create-job \
  --account-id <account-id> \
  --operation '{"LambdaInvoke":{"FunctionArn":"<enrichment-function-arn>","InvocationSchemaVersion":"2.0"}}' \
  --manifest '{"Spec":{"Format":"S3InventoryReport_CSV_20161130"},"Location":{"ObjectArn":"arn:aws:s3:::<bucket>/inventory/<path>/manifest.json","ETag":"<manifest-etag>"}}' \
  --report '{"Bucket":"arn:aws:s3:::<bucket>","Prefix":"batch-reports","Format":"Report_CSV_20180820","Enabled":true,"ReportScope":"FailedTasksOnly"}' \
  --priority 10 \
  --role-arn <batch-operations-role-arn> \
  --no-confirmation-required
```

Each task's provenance annotation records `"trigger": "batch-operations"`. Throttling and partial writes return `TemporaryFailure` so Batch Operations retries them.

Without an inventory report, `scripts/start_batch_enrichment.py` builds a CSV manifest of keys and version IDs and creates the same job. Use `--source all` to backfill every current object under `documents/`, or `--source stale --current-prompt-version <version>` to re-enrich only current versions whose `ai.provenance` records a different prompt version (this reads the annotation table through Athena, so it needs the QueryStack and Lake Formation grants):

```bash
python3 scripts/start_batch_enrichment.py --bucket-name <bucket> \
  --function-arn <EnrichmentFunctionArn> --role-arn <BatchOperationsRoleArn> \
  --source stale --current-prompt-version financial-docs-v2 --wait
```

`scripts/query_annotations.py --query-type provenance` shows which model and prompt version produced each object version's results.

## Customization

All tunables live in `cdk.json` under `context` and can be overridden on the command line with `--context key=value`:

| Context variable | Default | Description |
|---|---|---|
| `project_prefix` | `s3-enrich` | Prefix for resource names. The bucket name adds your account ID and Region (`<project_prefix>-enrichment-<account-id>-<region>`); the workgroup is `<project_prefix>-annotation-queries` |
| `model_id` | `amazon.nova-pro-v1:0` | Bedrock foundation-model ID used for analysis |
| `max_content_length` | `10000` | Maximum document characters sent to Bedrock (content is truncated beyond this) |
| `annotation_prefix` | `ai` | Prefix for annotation names (must not start with reserved `aws` or `s3`) |
| `lambda_reserved_concurrency` | `10` | Reserved concurrent executions, rate-limits Bedrock calls under burst uploads |
| `lambda_memory_mb` | `1024` | Lambda memory size in MB |
| `lambda_timeout_minutes` | `5` | Lambda timeout in minutes |
| `event_filter_prefix` | `documents/` | S3 key prefix that triggers enrichment |
| `event_filter_suffix` | `.txt` | S3 key suffix that triggers enrichment |
| `enable_annotation_tables` | `true` | Configure the S3 Metadata annotation table on the bucket |
| `enable_query_stack` | `false` | Deploy the optional QueryStack (Athena workgroup and named queries) |
| `use_kms_encryption` | `false` | Encrypt the bucket with a KMS key instead of S3-managed keys |

For example, to deploy with the Athena query resources included:

```bash
cdk deploy --all --context enable_query_stack=true
```

## Cost Considerations

This demo uses pay-per-use services; there are no always-on compute costs.

- **Amazon Bedrock**: Charged per input/output token for each document analyzed. The three sample documents cost a fraction of a cent with Nova Pro.
- **AWS Lambda**: Charged per invocation and duration; free tier typically covers demo usage.
- **Amazon S3**: Each annotation write is billed as one PUT request (four per document here), and copying or replicating an object is billed one PUT per annotation copied. Annotation storage is billed at the S3 Standard rate regardless of the object's storage class. With S3 Metadata enabled, the annotation table adds a metadata-processed charge per GB of annotation data, journal updates per annotation write, and standard S3 Tables storage. See [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/).
- **Amazon Athena**: Charged per data scanned (roughly $5/TB); demo queries scan kilobytes.

Remember to run the clean-up step when finished to avoid ongoing storage charges.

## Limitations

- **Annotation table refresh latency**: S3 Metadata annotation tables refresh within approximately one hour of annotation writes. Use `verify_annotations.py` for immediate verification.
- **Annotation limits**: Up to 1,000 annotations per object, each up to 1 MiB.
- **Reserved prefixes**: Annotation names must not start with `aws` or `s3`; those prefixes are reserved.
- **No SSE-C support**: Annotations are not supported on objects encrypted with customer-provided keys (SSE-C).
- **Content truncation**: Documents are truncated to `max_content_length` characters (default 10,000) before being sent to Bedrock, so very long documents are analyzed from their leading content only.

## Clean Up

```bash
cdk destroy --all
```

The bucket is created with a `DESTROY` removal policy and auto-delete objects enabled, so all uploaded documents and annotations are removed with the stacks. Stack deletion calls `DeleteBucketMetadataConfiguration`, which stops table updates but leaves the journal and annotation tables in the AWS managed `aws-s3` table bucket. Delete them to stop S3 Tables storage charges:

```bash
for table in journal annotation; do
  aws s3tables delete-table \
    --table-bucket-arn arn:aws:s3tables:<region>:<account-id>:bucket/aws-s3 \
    --namespace b_<bucket-name> \
    --name "$table"
done
```

If you enabled the annotation table on an existing bucket and want to keep the rest of its metadata configuration, disable only the annotation table instead, then delete that table with the command above:

```bash
aws s3api update-bucket-metadata-annotation-table-configuration \
  --bucket <bucket> \
  --annotation-table-configuration '{"ConfigurationState":"DISABLED"}'
```

## Security

- **Least-privilege IAM**: The Lambda execution role is granted only `s3:GetObject`, `s3:GetObjectVersion`, `s3:PutObjectAnnotation`, and `s3:PutObjectVersionAnnotation` (required when the write targets a version ID) scoped to `documents/*`, plus `bedrock:InvokeModel` scoped to the exact foundation-model ARN. `s3:DeleteObjectAnnotation` is deliberately omitted, and no wildcard resources are used.
- **Conditional writes**: Every annotation write carries `ObjectIfMatch` with the ETag captured at read time, preventing annotation of objects that changed mid-flight.
- **Bucket hardening**: SSL enforced, all public access blocked, server-side encryption enabled (S3-managed keys by default, KMS via context).
- **Separation of duties**: The IAM role S3 assumes to populate the annotation table is separate from the Lambda execution role.
- **No secrets in code**: All configuration flows through CDK context and Lambda environment variables.

## Lambda Dependencies

The Lambda runtime's bundled boto3 predates the S3 object-annotation APIs, so the pinned `boto3` from `lambda_functions/enrichment/requirements.txt` ships as a Lambda layer. The `app` command in `cdk.json` installs it into `.build/lambda-dependencies/python` before every `cdk synth` or `cdk deploy`, using pre-built wheels only (`--only-binary=:all:`). If you run `app.py` directly, build the layer first:

```bash
python3 -m pip install --only-binary=:all: --upgrade \
  --requirement lambda_functions/enrichment/requirements.txt \
  --target .build/lambda-dependencies/python
```

## Running the Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The suite includes unit tests for the Lambda handler and system prompt (mocked boto3 clients) plus CDK assertion tests validating the synthesized templates.

## Security Issue Notifications

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.

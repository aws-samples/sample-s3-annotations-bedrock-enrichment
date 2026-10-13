"""Unit tests for the enrichment Lambda handler.

All boto3 clients are mocked; no real AWS calls are made.

Covers: happy path, content truncation, non-JSON Bedrock response,
partial annotation-write failure, NoSuchKey handling, configurable
annotation prefix, and ETag conditional writes.

Validates: Requirements 8.1 (covering 4.1-4.9)
"""

import io
import json
import os
from unittest import mock

import pytest
from botocore.exceptions import ClientError

import index

ETAG = '"d41d8cd98f00b204e9800998ecf8427e"'
ANALYSIS = {
    "classification": {"type": "contract", "confidence": 0.95},
    "summary": "A service agreement between two fictional companies.",
    "entities": [{"name": "Acme Corp", "type": "ORGANIZATION"}],
}


def make_event(
    bucket: str = "test-bucket", key: str = "documents/sample.txt", version_id=None
) -> dict:
    """Build a minimal S3 object-created event with one record."""
    s3_object = {"key": key}
    if version_id:
        s3_object["versionId"] = version_id
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": s3_object}}]}


def make_get_object_response(content: str = "Sample document text.", etag: str = ETAG) -> dict:
    """Build a mocked s3.get_object response with a readable body."""
    return {"Body": io.BytesIO(content.encode("utf-8")), "ETag": etag}


def make_converse_response(payload) -> dict:
    """Build a mocked Bedrock Converse response containing ``payload`` text."""
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return {"output": {"message": {"content": [{"text": text}]}}}


@pytest.fixture
def mock_s3():
    with mock.patch.object(index, "s3") as client:
        client.get_object.return_value = make_get_object_response()
        yield client


@pytest.fixture
def mock_bedrock():
    with mock.patch.object(index, "bedrock_runtime") as client:
        client.converse.return_value = make_converse_response(ANALYSIS)
        yield client


ALL_NAMES = {"ai.classification", "ai.summary", "ai.entities", "ai.provenance"}


def annotation_calls(mock_s3) -> dict:
    """Map annotation name to the kwargs of its put_object_annotation call."""
    return {
        call.kwargs["AnnotationName"]: call.kwargs
        for call in mock_s3.put_object_annotation.call_args_list
    }


class TestHappyPath:
    def test_writes_enrichment_and_provenance_annotations(self, mock_s3, mock_bedrock):
        result = index.handler(make_event(), None)

        assert result == {"annotations_written": 4, "annotations_skipped": 0}
        assert mock_s3.put_object_annotation.call_count == 4

        calls = annotation_calls(mock_s3)
        assert set(calls) == ALL_NAMES

        for kwargs in calls.values():
            assert kwargs["Bucket"] == "test-bucket"
            assert kwargs["Key"] == "documents/sample.txt"
            assert kwargs["ObjectIfMatch"] == ETAG
            # Unversioned event: no VersionId is sent.
            assert "VersionId" not in kwargs

        assert (
            json.loads(calls["ai.classification"]["AnnotationPayload"])
            == ANALYSIS["classification"]
        )
        assert (
            calls["ai.summary"]["AnnotationPayload"].decode("utf-8")
            == ANALYSIS["summary"]
        )
        assert (
            json.loads(calls["ai.entities"]["AnnotationPayload"])
            == ANALYSIS["entities"]
        )

    def test_url_encoded_key_is_decoded(self, mock_s3, mock_bedrock):
        index.handler(make_event(key="documents/my+file%20name.txt"), None)

        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="documents/my file name.txt"
        )

    def test_converse_called_with_configured_model_and_inference_config(
        self, mock_s3, mock_bedrock
    ):
        with mock.patch.dict(os.environ, {"MODEL_ID": "amazon.nova-lite-v1:0"}):
            index.handler(make_event(), None)

        kwargs = mock_bedrock.converse.call_args.kwargs
        assert kwargs["modelId"] == "amazon.nova-lite-v1:0"
        assert kwargs["inferenceConfig"] == {"temperature": 0.1, "maxTokens": 2048}
        assert kwargs["system"] == [{"text": index.SYSTEM_PROMPT}]


class TestContentTruncation:
    def test_content_truncated_before_converse(self, mock_s3, mock_bedrock):
        long_content = "x" * 500
        mock_s3.get_object.return_value = make_get_object_response(long_content)

        with mock.patch.dict(os.environ, {"MAX_CONTENT_LENGTH": "100"}):
            index.handler(make_event(), None)

        sent_text = mock_bedrock.converse.call_args.kwargs["messages"][0]["content"][0]["text"]
        assert sent_text == "x" * 100

    def test_short_content_not_modified(self, mock_s3, mock_bedrock):
        mock_s3.get_object.return_value = make_get_object_response("short text")

        with mock.patch.dict(os.environ, {"MAX_CONTENT_LENGTH": "100"}):
            index.handler(make_event(), None)

        sent_text = mock_bedrock.converse.call_args.kwargs["messages"][0]["content"][0]["text"]
        assert sent_text == "short text"


class TestNonJsonResponse:
    def test_raw_response_is_not_logged(self, mock_s3, mock_bedrock, caplog):
        document_excerpt = "Patient Jane Roe, account 000-00-0000"
        mock_bedrock.converse.return_value = make_converse_response(
            document_excerpt
        )

        with caplog.at_level("ERROR"):
            index.handler(make_event(), None)

        assert document_excerpt not in caplog.text
        assert f"response length: {len(document_excerpt)}" in caplog.text

    def test_no_annotations_written_and_no_exception(self, mock_s3, mock_bedrock):
        mock_bedrock.converse.return_value = make_converse_response(
            "Sorry, I cannot analyze this document."
        )

        result = index.handler(make_event(), None)

        mock_s3.put_object_annotation.assert_not_called()
        assert result["annotations_written"] == 0


class TestPartialWriteFailure:
    def test_one_failure_does_not_block_remaining_writes(self, mock_s3, mock_bedrock):
        mock_s3.put_object_annotation.side_effect = [
            ClientError(
                {"Error": {"Code": "PreconditionFailed", "Message": "ETag mismatch"}},
                "PutObjectAnnotation",
            ),
            {},
            {},
            {},
        ]

        result = index.handler(make_event(), None)

        assert mock_s3.put_object_annotation.call_count == 4
        assert result == {"annotations_written": 3, "annotations_skipped": 1}


class TestNoSuchKey:
    def test_missing_object_returns_gracefully(self, mock_s3, mock_bedrock):
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
        )

        result = index.handler(make_event(), None)

        assert result == {"annotations_written": 0, "annotations_skipped": 0}
        mock_bedrock.converse.assert_not_called()
        mock_s3.put_object_annotation.assert_not_called()

    def test_other_client_errors_are_raised(self, mock_s3, mock_bedrock):
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "GetObject"
        )

        with pytest.raises(ClientError):
            index.handler(make_event(), None)


class TestAnnotationPrefix:
    def test_custom_prefix_used_in_annotation_names(self, mock_s3, mock_bedrock):
        with mock.patch.dict(os.environ, {"ANNOTATION_PREFIX": "meta"}):
            index.handler(make_event(), None)

        names = {
            call.kwargs["AnnotationName"]
            for call in mock_s3.put_object_annotation.call_args_list
        }
        assert names == {
            "meta.classification",
            "meta.summary",
            "meta.entities",
            "meta.provenance",
        }

    def test_reserved_prefix_falls_back_to_default(self, mock_s3, mock_bedrock):
        with mock.patch.dict(os.environ, {"ANNOTATION_PREFIX": "s3meta"}):
            index.handler(make_event(), None)

        names = {
            call.kwargs["AnnotationName"]
            for call in mock_s3.put_object_annotation.call_args_list
        }
        assert names == ALL_NAMES


class TestConditionalWrites:
    def test_if_match_equals_get_object_etag(self, mock_s3, mock_bedrock):
        etag = '"custom-etag-value"'
        mock_s3.get_object.return_value = make_get_object_response(etag=etag)

        index.handler(make_event(), None)

        assert mock_s3.put_object_annotation.call_count == 4
        for call in mock_s3.put_object_annotation.call_args_list:
            assert call.kwargs["ObjectIfMatch"] == etag


class TestProvenance:
    def test_provenance_records_model_prompt_and_source(self, mock_s3, mock_bedrock):
        with mock.patch.dict(os.environ, {"MODEL_ID": "amazon.nova-lite-v1:0"}):
            index.handler(make_event(version_id="v123"), None)

        provenance = json.loads(
            annotation_calls(mock_s3)["ai.provenance"]["AnnotationPayload"]
        )
        assert provenance["model_id"] == "amazon.nova-lite-v1:0"
        assert provenance["prompt_version"] == index.PROMPT_VERSION
        assert provenance["source_etag"] == ETAG
        assert provenance["source_version_id"] == "v123"
        assert provenance["trigger"] == "s3-event"
        # ISO 8601 UTC timestamp.
        assert provenance["enriched_at"].endswith("+00:00")

    def test_missing_model_field_is_not_written(self, mock_s3, mock_bedrock):
        mock_bedrock.converse.return_value = make_converse_response(
            {"classification": ANALYSIS["classification"], "summary": "s"}
        )

        index.handler(make_event(), None)

        assert set(annotation_calls(mock_s3)) == {
            "ai.classification",
            "ai.summary",
            "ai.provenance",
        }


class TestVersionedObjects:
    def test_version_id_used_for_read_and_every_write(self, mock_s3, mock_bedrock):
        index.handler(make_event(version_id="v123"), None)

        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="documents/sample.txt", VersionId="v123"
        )
        calls = annotation_calls(mock_s3)
        assert set(calls) == ALL_NAMES
        for kwargs in calls.values():
            assert kwargs["VersionId"] == "v123"

    def test_missing_version_returns_gracefully(self, mock_s3, mock_bedrock):
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchVersion", "Message": "Not found"}}, "GetObject"
        )

        result = index.handler(make_event(version_id="gone"), None)

        assert result == {"annotations_written": 0, "annotations_skipped": 0}
        mock_bedrock.converse.assert_not_called()


def make_batch_event(
    key: str = "documents/sample.txt",
    version_id=None,
    schema: str = "2.0",
) -> dict:
    """Build an S3 Batch Operations Lambda invocation with one task."""
    task = {"taskId": "task-1", "s3Key": key, "s3VersionId": version_id}
    if schema == "2.0":
        task["s3Bucket"] = "test-bucket"
    else:
        task["s3BucketArn"] = "arn:aws:s3:::test-bucket"
    return {
        "invocationSchemaVersion": schema,
        "invocationId": "invocation-1",
        "job": {"id": "job-1"},
        "tasks": [task],
    }


class TestBatchOperations:
    def test_successful_task_reports_succeeded(self, mock_s3, mock_bedrock):
        result = index.handler(
            make_batch_event(key="documents/my+file%20name.txt", version_id="v9"), None
        )

        assert result["invocationSchemaVersion"] == "2.0"
        assert result["invocationId"] == "invocation-1"
        assert result["treatMissingKeysAs"] == "PermanentFailure"
        assert result["results"] == [
            {
                "taskId": "task-1",
                "resultCode": "Succeeded",
                "resultString": "Wrote 4 annotations",
            }
        ]
        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="documents/my file name.txt", VersionId="v9"
        )
        provenance = json.loads(
            annotation_calls(mock_s3)["ai.provenance"]["AnnotationPayload"]
        )
        assert provenance["trigger"] == "batch-operations"

    def test_schema_1_bucket_arn_is_supported(self, mock_s3, mock_bedrock):
        result = index.handler(make_batch_event(schema="1.0"), None)

        assert result["results"][0]["resultCode"] == "Succeeded"
        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="documents/sample.txt"
        )

    def test_throttling_reports_temporary_failure(self, mock_s3, mock_bedrock):
        mock_bedrock.converse.side_effect = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Slow down"}},
            "Converse",
        )

        result = index.handler(make_batch_event(), None)

        assert result["results"][0]["resultCode"] == "TemporaryFailure"

    def test_access_denied_reports_permanent_failure(self, mock_s3, mock_bedrock):
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "GetObject"
        )

        result = index.handler(make_batch_event(), None)

        assert result["results"][0]["resultCode"] == "PermanentFailure"

    def test_invalid_model_output_reports_permanent_failure(self, mock_s3, mock_bedrock):
        mock_bedrock.converse.return_value = make_converse_response("not json")

        result = index.handler(make_batch_event(), None)

        assert result["results"][0]["resultCode"] == "PermanentFailure"
        mock_s3.put_object_annotation.assert_not_called()

    def test_partial_write_reports_temporary_failure(self, mock_s3, mock_bedrock):
        mock_s3.put_object_annotation.side_effect = [
            ClientError(
                {"Error": {"Code": "SlowDown", "Message": "Slow down"}},
                "PutObjectAnnotation",
            ),
            {},
            {},
            {},
        ]

        result = index.handler(make_batch_event(), None)

        assert result["results"][0]["resultCode"] == "TemporaryFailure"

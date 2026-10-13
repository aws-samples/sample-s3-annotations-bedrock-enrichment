"""Unit tests for the Athena query builder in scripts/query_annotations.py.

Validates that values are passed as execution parameters, that the table
identifier is built only from a valid bucket name, and that the
workgroup and parameters reach StartQueryExecution.
"""

import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import query_annotations as qa  # noqa: E402

TABLE = qa.annotation_table_path("my-bucket-123456789012-us-east-1")


@pytest.mark.parametrize("query_type", ["classification", "entities", "provenance"])
def test_annotation_name_is_a_parameter(query_type):
    sql, params = qa.build_query(query_type, TABLE)

    assert "WHERE name = ?" in sql
    assert params == [f"'ai.{query_type}'"]
    assert f"ai.{query_type}" not in sql


def test_stale_prompt_version_is_a_parameter():
    sql, params = qa.build_query("provenance", TABLE, "financial-docs-v2")

    assert "'$.prompt_version') <> ?" in sql
    assert "financial-docs-v2" not in sql
    assert params == ["'ai.provenance'", "'financial-docs-v2'"]


def test_all_query_has_no_parameters():
    sql, params = qa.build_query("all", TABLE)

    assert params == []
    assert TABLE in sql


def test_invalid_prompt_version_rejected():
    with pytest.raises(ValueError):
        qa.build_query("provenance", TABLE, "v1' OR '1'='1")


@pytest.mark.parametrize(
    "name",
    ['bad"name', "Upper-Case", "a", "my..bucket", "-leading", "x" * 64, 'b"."x'],
)
def test_invalid_bucket_names_rejected(name):
    with pytest.raises(ValueError):
        qa.annotation_table_path(name)


def test_table_path_format():
    assert TABLE == (
        '"s3tablescatalog/aws-s3"."b_my-bucket-123456789012-us-east-1"."annotation"'
    )


def test_sql_literal_escapes_quotes():
    assert qa.sql_literal("o'neil") == "'o''neil'"


def test_run_query_passes_parameters_and_workgroup():
    client = mock.Mock()
    client.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    assert qa.run_query(client, "SELECT ?", "wg", ["'x'"]) == "q1"
    client.start_query_execution.assert_called_once_with(
        QueryString="SELECT ?", WorkGroup="wg", ExecutionParameters=["'x'"]
    )


def test_run_query_omits_empty_parameters():
    client = mock.Mock()
    client.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    qa.run_query(client, "SELECT 1", "wg")
    client.start_query_execution.assert_called_once_with(
        QueryString="SELECT 1", WorkGroup="wg"
    )

"""Unit tests for the enrichment system prompt.

Validates: Requirements 8.2 (covering 5.1, 5.2, 5.3)
"""

from prompts import SYSTEM_PROMPT

MAX_PROMPT_LENGTH = 2000

REQUIRED_FIELDS = ("classification", "summary", "entities")

DOCUMENT_TYPES = (
    "contract",
    "regulatory_filing",
    "research_report",
    "internal_memo",
    "technical_spec",
    "correspondence",
    "other",
)

ENTITY_TYPES = ("PERSON", "ORGANIZATION", "DATE", "AMOUNT", "LOCATION", "TOPIC")


def test_system_prompt_is_non_empty():
    assert isinstance(SYSTEM_PROMPT, str)
    assert SYSTEM_PROMPT.strip()


def test_system_prompt_is_under_2000_characters():
    assert len(SYSTEM_PROMPT) < MAX_PROMPT_LENGTH


def test_system_prompt_contains_required_json_fields():
    for field in REQUIRED_FIELDS:
        assert field in SYSTEM_PROMPT, f"Missing required field: {field}"


def test_system_prompt_lists_all_document_types():
    for doc_type in DOCUMENT_TYPES:
        assert doc_type in SYSTEM_PROMPT, f"Missing document type: {doc_type}"


def test_system_prompt_lists_all_entity_types():
    for entity_type in ENTITY_TYPES:
        assert entity_type in SYSTEM_PROMPT, f"Missing entity type: {entity_type}"


def test_system_prompt_requests_json_only_output():
    assert "JSON" in SYSTEM_PROMPT


def test_prompt_version_is_set():
    from prompts import PROMPT_VERSION

    assert isinstance(PROMPT_VERSION, str) and PROMPT_VERSION.strip()


def test_annotation_fields_match_prompt_schema():
    from prompts import ANNOTATION_FIELDS

    assert set(ANNOTATION_FIELDS) == set(REQUIRED_FIELDS)
    for field in ANNOTATION_FIELDS:
        assert f'"{field}"' in SYSTEM_PROMPT

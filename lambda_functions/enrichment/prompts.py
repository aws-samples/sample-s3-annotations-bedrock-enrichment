"""Enrichment schema and system prompt for the Bedrock enrichment Lambda.

This module is the part of the pipeline you customize for your own
documents. It defines:

- ``PROMPT_VERSION``: an identifier written into the ``ai.provenance``
  annotation. Bump it whenever you change the schema or instructions so
  you can tell which objects were enriched with which prompt, and
  re-run enrichment selectively.
- ``ANNOTATION_FIELDS``: the top-level JSON fields the handler writes
  back, one annotation per field.
- ``DOCUMENT_TYPES`` and ``ENTITY_TYPES``: the domain vocabulary the
  model is constrained to. Replace these with the categories and
  fields that matter in your domain.
- ``SYSTEM_PROMPT``: the instruction sent to the Bedrock Converse API,
  built from the values above.
"""

#: Identifier for this schema and prompt. Bump on every change.
PROMPT_VERSION = "financial-docs-v1"

#: Top-level fields in the model's JSON response. The handler writes each
#: one as an ``{prefix}.{field}`` annotation; strings are stored as plain
#: text and everything else as JSON. Keep this in sync with SYSTEM_PROMPT.
ANNOTATION_FIELDS = ("classification", "summary", "entities")

#: Allowed values for ``classification.type``.
DOCUMENT_TYPES = (
    "contract",
    "regulatory_filing",
    "research_report",
    "internal_memo",
    "technical_spec",
    "correspondence",
    "other",
)

#: Allowed values for ``entities[].type``.
ENTITY_TYPES = ("PERSON", "ORGANIZATION", "DATE", "AMOUNT", "LOCATION", "TOPIC")

SYSTEM_PROMPT = f"""You are a document analysis assistant. Analyze the provided document and return a JSON object with exactly these fields:
{{
  "classification": {{"type": "<document_type>", "confidence": <0.0-1.0>}},
  "summary": "<concise summary under 500 characters>",
  "entities": [{{"name": "<entity_name>", "type": "{'|'.join(ENTITY_TYPES)}"}}]
}}
Document types: {', '.join(DOCUMENT_TYPES)}.
Return ONLY the JSON object. No markdown formatting, no explanation."""

"""Shared pytest configuration for the test suite.

Makes the enrichment Lambda importable as a top-level module (mirroring
the Lambda runtime, where the function directory is on sys.path) and
ensures boto3 module-level client creation succeeds without real AWS
configuration.
"""

import os
import sys
from pathlib import Path

# Ensure module-level boto3 clients in index.py can be constructed
# without a configured region or credentials. No real AWS calls are
# made; all clients are mocked in the tests.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

# Put the Lambda function directory on sys.path so `import index` and
# `import prompts` work exactly as they do in the Lambda runtime.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAMBDA_DIR = PROJECT_ROOT / "lambda_functions" / "enrichment"
sys.path.insert(0, str(LAMBDA_DIR))

# Put the project root on sys.path so the CDK assertion tests can
# import the stack modules (`from stacks... import ...`).
sys.path.insert(0, str(PROJECT_ROOT))

# The EnrichmentStack packages pinned boto3 as a Lambda layer that
# cdk.json builds before synth. Assertion tests don't need its contents,
# so make sure the directory exists for Code.from_asset.
(PROJECT_ROOT / ".build" / "lambda-dependencies" / "python").mkdir(
    parents=True, exist_ok=True
)

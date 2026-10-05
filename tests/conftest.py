"""Keep unit-test configuration independent of production credentials."""

import os

os.environ.setdefault("INVEST_TOKEN", "unit-test-token")

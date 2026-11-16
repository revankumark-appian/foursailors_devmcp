"""Make the repo-root modules (appian_webhook_server, appian_mcp_client,
mock_jira_app) importable from the tests directory."""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

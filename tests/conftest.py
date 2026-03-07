"""Pytest fixtures for openclaw-crm tests."""
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture
def mock_sheet_result():
    """Create a mock sheet result."""
    def _make(success=True, data=None):
        result = MagicMock()
        result.success = success
        result.data = data or {}
        return result
    return _make


@pytest.fixture
def mock_read_sheet(mock_sheet_result):
    """Mock read_sheet function."""
    with patch('openclaw_crm.pipeline.read_sheet') as mock:
        yield mock


@pytest.fixture
def mock_append_sheet(mock_sheet_result):
    """Mock append_sheet function."""
    with patch('openclaw_crm.pipeline.append_sheet') as mock:
        yield mock


@pytest.fixture
def mock_update_sheet(mock_sheet_result):
    """Mock update_sheet function."""
    with patch('openclaw_crm.pipeline.update_sheet') as mock:
        yield mock


@pytest.fixture
def mock_config():
    """Mock config to return test spreadsheet ID."""
    with patch('openclaw_crm.pipeline.get_spreadsheet_id', return_value='test-sheet-id'):
        yield


@pytest.fixture
def sample_pipeline_data():
    """Sample pipeline data from Google Sheets."""
    return {
        "values": [
            ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
             "Service", "First Contact", "Last Contact", "Next Action",
             "Due Date", "Notes", "Slack Channel", "Proposal Link",
             "Owner", "Upwork URL", "Probability",
             "Referred By", "Network Parent", "Network Notes", "Signal Date"],
            ["Acme Corp", "John Doe", "upwork", "lead", "15000", "fixed",
             "Web Dev", "2026-01-01", "2026-03-01", "Follow up",
             "", "Test deal", "", "",
             "owner1", "", "=IFS(D2=\"lead\",0.1,...)",
             "", "", "", ""],
            ["Tech Inc", "Jane Smith", "network", "proposal", "25000", "hourly",
             "Consulting", "2026-01-15", "2026-03-05", "Send proposal",
             "2026-03-15", "Important", "", "https://prop.ly/abc",
             "owner2", "", "=IFS(D3=\"proposal\",0.5,...)",
             "Acme Corp", "Acme Corp", "Great fit", "2026-01-15"],
        ]
    }

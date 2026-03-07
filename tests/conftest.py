"""Test fixtures and configuration for openclaw-crm tests."""

import pytest
from unittest.mock import MagicMock, patch
from openclaw_crm.sheets import SheetsBackend, SheetResult


class MockBackend(SheetsBackend):
    """Mock SheetsBackend for testing without real Google Sheets calls."""

    def __init__(self):
        self._data = {}
        self.calls = []

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        self.calls.append(("read", spreadsheet_id, range_))
        if range_ in self._data:
            return SheetResult(success=True, data=self._data[range_])
        return SheetResult(success=True, data={"values": []})

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.calls.append(("append", spreadsheet_id, range_, values))
        if range_ not in self._data:
            self._data[range_] = {"values": [["Client", "Contact", "Source", "Stage", "Budget"]]}
        self._data[range_]["values"].append(values[0])
        return SheetResult(success=True, data={"updatedRange": range_})

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.calls.append(("update", spreadsheet_id, range_, values))
        return SheetResult(success=True, data={"updatedRange": range_})

    def set_data(self, range_: str, values: dict):
        """Set mock data for a given range."""
        self._data[range_] = values


@pytest.fixture
def mock_backend():
    """Provide a MockBackend fixture."""
    return MockBackend()


@pytest.fixture
def sample_pipeline_data():
    """Sample pipeline data for testing."""
    return {
        "values": [
            ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
             "Service", "First Contact", "Last Contact", "Next Action",
             "Due Date", "Notes", "Slack Channel", "Proposal Link",
             "Owner", "Upwork URL", "Probability",
             "Referred By", "Network Parent", "Network Notes", "Signal Date"],
            ["Acme Corp", "John Doe", "upwork", "lead", "$5000", "fixed",
             "Web Dev", "2026-01-01", "2026-03-01", "Follow up", "",
             "Interested in SEO", "", "",
             "Owner1", "https://upwork.com", "=IFS(D2=\"lead\",0.1,...)",
             "", "", "", ""],
            ["TechStart", "Jane Smith", "network", "won", "$10000", "hourly",
             "Mobile App", "2025-06-01", "2026-02-15", "", "",
             "Great client", "", "",
             "Owner2", "", "=IFS(D3=\"won\",1,...)",
             "John Doe", "John Doe", "Great referral", "2025-06-01"],
        ]
    }


@pytest.fixture
def empty_pipeline_data():
    """Empty pipeline data for edge case testing."""
    return {
        "values": [
            ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type"],
        ]
    }

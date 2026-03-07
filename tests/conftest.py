"""Pytest fixtures for openclaw-crm tests."""
from __future__ import annotations

from typing import Any
import pytest

from openclaw_crm.sheets import SheetsBackend, SheetResult, set_backend


class MockBackend(SheetsBackend):
    """Mock backend for testing without real Google Sheets calls."""

    def __init__(self, data: dict[str, list[list[str]]] | None = None):
        self.data = data or {}
        self.read_calls: list[tuple[str, str]] = []
        self.append_calls: list[tuple[str, str, list[list[str]]]] = []
        self.update_calls: list[tuple[str, str, list[list[str]]]] = []

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        self.read_calls.append((spreadsheet_id, range_))
        if range_ in self.data:
            return SheetResult(success=True, data={"values": self.data[range_]})
        return SheetResult(success=False, data=None, error="Range not found")

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.append_calls.append((spreadsheet_id, range_, values))
        if range_ not in self.data:
            self.data[range_] = []
        self.data[range_].extend(values)
        return SheetResult(success=True, data={"appended": len(values)})

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.update_calls.append((spreadsheet_id, range_, values))
        return SheetResult(success=True, data={"updated": len(values)})


@pytest.fixture
def mock_backend():
    """Provide a fresh MockBackend for each test."""
    return MockBackend()


@pytest.fixture
def mock_pipeline_data():
    """Provide sample pipeline data for testing."""
    return {
        "Pipeline!A:U": [
            ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
             "Service", "First Contact", "Last Contact", "Next Action",
             "Due Date", "Notes", "Slack Channel", "Proposal Link",
             "Owner", "Upwork URL", "Probability",
             "Referred By", "Network Parent", "Network Notes", "Signal Date"],
            ["Acme Corp", "John", "upwork", "lead", "15000", "fixed",
             "Web Dev", "2026-01-01", "2026-03-01", "Follow up",
             "", "", "", "",
             "", "", "=IFS(D2=\"lead\",0.1,...)",
             "", "", "", ""],
            ["Tech Inc", "Jane", "network", "proposal", "25000", "hourly",
             "Consulting", "2026-01-15", "2026-03-05", "Send quote",
             "", "", "", "",
             "", "", "=IFS(D3=\"proposal\",0.5,...)",
             "Acme Corp", "Acme Corp", "Referral from Acme", ""],
        ],
    }


@pytest.fixture
def mock_backend_with_data(mock_pipeline_data):
    """Provide MockBackend with sample data preloaded."""
    return MockBackend(mock_pipeline_data)


@pytest.fixture(autouse=True)
def reset_backend():
    """Reset the global backend after each test."""
    yield
    # Reset to None so next test gets fresh state
    import openclaw_crm.sheets as sheets_module
    sheets_module._backend = None

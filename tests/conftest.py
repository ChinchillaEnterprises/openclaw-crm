"""Pytest fixtures for OpenClaw CRM tests."""
from __future__ import annotations

from unittest.mock import MagicMock
import pytest

from openclaw_crm.sheets import SheetsBackend, SheetResult, set_backend


class MockBackend(SheetsBackend):
    """Mock backend for testing without real Google Sheets calls."""

    def __init__(self):
        self._data: dict[str, list[list[str]]] = {}
        self.read_calls: list[tuple[str, str]] = []
        self.append_calls: list[tuple[str, str, list[list[str]]]] = []
        self.update_calls: list[tuple[str, str, list[list[str]]]] = []

    def _set_sheet_data(self, range_: str, values: list[list[str]]) -> None:
        self._data[range_] = values

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        self.read_calls.append((spreadsheet_id, range_))
        if range_ in self._data:
            return SheetResult(success=True, data={"values": self._data[range_]})
        return SheetResult(success=True, data={"values": []})

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.append_calls.append((spreadsheet_id, range_, values))
        if range_ not in self._data:
            self._data[range_] = []
        # Add headers if empty
        if not self._data[range_] and values:
            self._data[range_].append(values[0])
        self._data[range_].extend(values)
        return SheetResult(success=True, data={"updatedRows": len(values)})

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.update_calls.append((spreadsheet_id, range_, values))
        return SheetResult(success=True, data={"updatedCells": sum(len(v) for v in values)})


@pytest.fixture
def mock_backend():
    """Fixture providing a MockBackend and setting it as the global backend."""
    backend = MockBackend()
    set_backend(backend)
    yield backend
    # Reset backend after test
    set_backend(None)


@pytest.fixture
def sample_pipeline_data():
    """Fixture providing sample pipeline data."""
    headers = [
        "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
        "Service", "First Contact", "Last Contact", "Next Action",
        "Due Date", "Notes", "Slack Channel", "Proposal Link",
        "Owner", "Upwork URL", "Probability",
        "Referred By", "Network Parent", "Network Notes", "Signal Date",
    ]
    rows = [
        headers,
        ["Acme Corp", "John Doe", "upwork", "lead", "5000", "fixed", "Web Dev", "2026-01-01", "2026-03-01", "Review lead", "", "Notes", "", "", "Owner", "", "0.1", "", "", "", ""],
        ["Beta Inc", "Jane Smith", "network", "qualifying", "10000", "hourly", "Mobile App", "2026-01-15", "2026-03-05", "Follow up", "", "", "", "", "Owner", "", "0.25", "John Doe", "John Doe", "Referral", "2026-01-15"],
        ["Gamma LLC", "Bob Wilson", "upwork", "won", "7500", "fixed", "API Dev", "2026-02-01", "2026-03-01", "", "", "", "", "", "Owner", "", "1.0", "", "", "", ""],
    ]
    return rows


@pytest.fixture
def mock_backend_with_data(mock_backend, sample_pipeline_data):
    """Fixture providing a MockBackend pre-loaded with pipeline data."""
    mock_backend._set_sheet_data("Pipeline!A:U", sample_pipeline_data)
    return mock_backend

"""Test fixtures for openclaw-crm."""

from __future__ import annotations

from unittest.mock import MagicMock

from openclaw_crm.sheets import SheetResult, SheetsBackend


class MockBackend(SheetsBackend):
    """Mock SheetsBackend for testing."""

    def __init__(self):
        self._data: dict[str, list[list[str]]] = {}
        self.calls: list[tuple[str, list]] = []

    def _set_sheet(self, spreadsheet_id: str, range_: str, data: list[list[str]]) -> None:
        key = f"{spreadsheet_id}:{range_}"
        self._data[key] = data

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        self.calls.append(("read", [spreadsheet_id, range_]))
        key = f"{spreadsheet_id}:{range_}"
        if key in self._data:
            return SheetResult(success=True, data={"values": self._data[key]})
        return SheetResult(success=False, data=None, error="Not found")

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.calls.append(("append", [spreadsheet_id, range_, values]))
        key = f"{spreadsheet_id}:{range_}"
        if key not in self._data:
            self._data[key] = []
        self._data[key].append(values[0])
        return SheetResult(success=True, data={"updatedRows": 1})

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        self.calls.append(("update", [spreadsheet_id, range_, values]))
        key = f"{spreadsheet_id}:{range_}"
        if key in self._data:
            self._data[key] = values
        return SheetResult(success=True, data={"updatedCells": len(values) * len(values[0])})


# Sample data for tests
SAMPLE_HEADERS = [
    "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
    "Service", "First Contact", "Last Contact", "Next Action",
    "Due Date", "Notes", "Slack Channel", "Proposal Link",
    "Owner", "Upwork URL", "Probability",
    "Referred By", "Network Parent", "Network Notes", "Signal Date",
]

SAMPLE_PIPELINE_DATA = [
    SAMPLE_HEADERS,
    ["Acme Corp", "John Doe", "upwork", "lead", "15000", "fixed", "SEO", "2026-01-01", "2026-02-01", "Follow up", "", "Notes", "", "", "Alice", "", "=IFS(D2=\"lead\",0.1,...)", "", "", "", ""],
    ["Tech Inc", "Jane Smith", "network", "proposal", "25000", "hourly", "Marketing", "2026-01-15", "2026-02-15", "Send proposal", "", "", "", "", "Bob", "", "=IFS(D3=\"proposal\",0.5,...)", "Acme Corp", "Acme Corp", "Referral", "2026-01-10"],
    ["Old Co", "Bob Wilson", "inbound", "won", "5000", "fixed", "Consulting", "2025-12-01", "2026-01-01", "", "", "", "", "", "Alice", "", "=IFS(D4=\"won\",1,...)", "", "", "", ""],
]

SAMPLE_EMPTY_DATA = [
    SAMPLE_HEADERS,
]

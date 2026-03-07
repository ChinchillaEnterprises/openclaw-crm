"""Pytest configuration and fixtures."""

import pytest
from unittest.mock import Mock

from openclaw_crm.sheets import SheetsBackend, SheetResult


class MockBackend(SheetsBackend):
    """Mock backend for testing without real Google Sheets."""
    
    def __init__(self):
        self.data = {}
    
    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        key = f"{spreadsheet_id}:{range_}"
        return SheetResult(success=True, data=self.data.get(key, {"values": []}))
    
    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        key = f"{spreadsheet_id}:{range_}"
        if key not in self.data:
            self.data[key] = {"values": []}
        self.data[key]["values"].extend(values)
        return SheetResult(success=True, data={"values": values})
    
    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        return SheetResult(success=True, data={"values": values})


@pytest.fixture
def mock_backend():
    """Provide a mock backend for tests."""
    return MockBackend()

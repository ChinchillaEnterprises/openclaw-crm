import pytest
from openclaw_crm.sheets import SheetsBackend, SheetResult, set_backend

class MockBackend(SheetsBackend):
    def __init__(self):
        self.data = {} # Key: (spreadsheet_id, range) -> list[list[str]]
        # For simplicity in tests, we'll just use the range name as the key
        # In the real app, we might need more complex range parsing.
        # But here we'll assume the range is "Pipeline!A:U" or similar.
        self.sheets = {} # sheet_name -> list[list[str]]

    def _get_sheet_name(self, range_):
        if "!" in range_:
            return range_.split("!")[0].strip("'")
        return "Sheet1"

    def read(self, spreadsheet_id, range_):
        sheet_name = self._get_sheet_name(range_)
        rows = self.sheets.get(sheet_name, [])
        return SheetResult(success=True, data={"values": rows})

    def append(self, spreadsheet_id, range_, values):
        sheet_name = self._get_sheet_name(range_)
        if sheet_name not in self.sheets:
            self.sheets[sheet_name] = []
        self.sheets[sheet_name].extend(values)
        return SheetResult(success=True, data={})

    def update(self, spreadsheet_id, range_, values):
        sheet_name = self._get_sheet_name(range_)
        # range_ might be "Pipeline!A2:U2"
        # We need to extract the row index.
        # This is a very simple mock for testing pipeline.py
        import re
        match = re.search(r"A(\d+):", range_)
        if match:
            row_idx = int(match.group(1)) - 1
            if sheet_name in self.sheets and 0 <= row_idx < len(self.sheets[sheet_name]):
                self.sheets[sheet_name][row_idx] = values[0]
                return SheetResult(success=True, data={})
        return SheetResult(success=False, data=None, error="Update failed in mock")

@pytest.fixture
def mock_backend():
    backend = MockBackend()
    set_backend(backend)
    return backend

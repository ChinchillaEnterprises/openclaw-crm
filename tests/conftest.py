import pytest
from openclaw_crm.sheets import SheetsBackend, SheetResult, set_backend


class MockBackend(SheetsBackend):
    def __init__(self):
        self.data = {}

    def read(self, spreadsheet_id, range_):
        # Very simple range support for tests
        # range_ can be 'SheetName!A:F' or just 'A:F'
        if "!" in range_:
            sheet_name, range_part = range_.split("!", 1)
            sheet_name = sheet_name.replace("'", "")
        else:
            sheet_name, range_part = "Sheet1", range_
        
        values = self.data.get(sheet_name, [])
        return SheetResult(success=True, data={"values": values})

    def append(self, spreadsheet_id, range_, values):
        if "!" in range_:
            sheet_name = range_.split("!", 1)[0].replace("'", "")
        else:
            sheet_name = "Sheet1"
            
        if sheet_name not in self.data:
            self.data[sheet_name] = []
        self.data[sheet_name].extend(values)
        return SheetResult(success=True, data={"updates": {"updatedRows": len(values)}})

    def update(self, spreadsheet_id, range_, values):
        if "!" in range_:
            sheet_name, range_part = range_.split("!", 1)
            sheet_name = sheet_name.replace("'", "")
        else:
            sheet_name, range_part = "Sheet1", range_
            
        # Extract row from range like 'SheetName!A2:F2'
        import re
        match = re.search(r'[A-Z](\d+)', range_part)
        if match:
            row_idx = int(match.group(1)) - 1
            if sheet_name not in self.data:
                self.data[sheet_name] = []
            
            # Pad with empty rows if needed
            while len(self.data[sheet_name]) <= row_idx:
                self.data[sheet_name].append([])
            
            self.data[sheet_name][row_idx] = values[0]
            return SheetResult(success=True, data={"updates": {"updatedRows": 1}})
        
        return SheetResult(success=False, data=None, error="Invalid range for update")


@pytest.fixture
def mock_backend():
    backend = MockBackend()
    set_backend(backend)
    return backend

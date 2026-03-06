from __future__ import annotations

from typing import Any

from openclaw_crm.sheets import SheetsBackend, SheetResult


class GspreadBackend(SheetsBackend):
    """Google Sheets backend using the gspread library.

    An alternative to the default gws CLI backend that uses the gspread
    Python library for Google Sheets access.

    Usage:
        pip install "openclaw-crm[gspread]"

        from openclaw_crm.backends.gspread_backend import GspreadBackend
        from openclaw_crm.sheets import set_backend

        # Using a service account credentials file
        backend = GspreadBackend("path/to/credentials.json")
        set_backend(backend)
    """

    def __init__(self, credentials_path: str):
        import gspread

        self.gc = gspread.service_account(filename=credentials_path)

    def _get_worksheet(self, spreadsheet_id: str, range_: str) -> Any:
        """Open a spreadsheet by ID and return the worksheet from the range."""
        sh = self.gc.open_by_key(spreadsheet_id)
        table_name = range_.split("!")[0].strip("'")
        return sh.worksheet(table_name)

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        try:
            ws = self._get_worksheet(spreadsheet_id, range_)
            values = ws.get_all_values()
            return SheetResult(success=True, data={"values": values})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        try:
            ws = self._get_worksheet(spreadsheet_id, range_)
            for row in values:
                ws.append_row(row, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updatedRows": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        try:
            ws = self._get_worksheet(spreadsheet_id, range_)
            # Parse cell range from the range_ string (e.g., "'Pipeline'!A2:U2")
            if "!" in range_:
                cell_range = range_.split("!")[1]
            else:
                cell_range = "A1"
            ws.update(cell_range, values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updatedRows": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

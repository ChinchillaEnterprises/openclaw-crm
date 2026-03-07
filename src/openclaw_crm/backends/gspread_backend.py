from __future__ import annotations

from typing import Any

from openclaw_crm.sheets import SheetResult, SheetsBackend


class GspreadBackend(SheetsBackend):
    """Google Sheets backend using gspread.

    Requires service-account credentials JSON path.
    """

    def __init__(self, credentials_path: str):
        import gspread

        self.gc = gspread.service_account(filename=credentials_path)

    def _sheet(self, spreadsheet_id: str):
        sh = self.gc.open_by_key(spreadsheet_id)
        return sh

    def _split_range(self, range_: str) -> tuple[str, str]:
        if "!" not in range_:
            return "Sheet1", range_
        tab, a1 = range_.split("!", 1)
        return tab.strip("'"), a1

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        try:
            sh = self._sheet(spreadsheet_id)
            tab, a1 = self._split_range(range_)
            ws = sh.worksheet(tab)
            values = ws.get(a1)
            return SheetResult(success=True, data={"values": values})
        except Exception as exc:
            return SheetResult(success=False, data=None, error=str(exc))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        try:
            sh = self._sheet(spreadsheet_id)
            tab, _ = self._split_range(range_)
            ws = sh.worksheet(tab)
            ws.append_rows(values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updated": len(values)})
        except Exception as exc:
            return SheetResult(success=False, data=None, error=str(exc))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        try:
            sh = self._sheet(spreadsheet_id)
            tab, a1 = self._split_range(range_)
            ws = sh.worksheet(tab)
            ws.update(a1, values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updated": len(values)})
        except Exception as exc:
            return SheetResult(success=False, data=None, error=str(exc))

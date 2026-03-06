from __future__ import annotations

from typing import Any
import gspread

from openclaw_crm.sheets import SheetsBackend, SheetResult


class GspreadBackend(SheetsBackend):
    """
    Google Sheets backend using the gspread library.
    
    Usage:
    ```python
    from openclaw_crm.backends.gspread_backend import GspreadBackend
    from openclaw_crm.sheets import set_backend
    
    # Option 1: Use service account JSON file
    set_backend(GspreadBackend(credentials_path="service_account.json"))
    
    # Option 2: Use oauth2 credentials
    set_backend(GspreadBackend(oauth_credentials_path="credentials.json"))
    ```
    """
    def __init__(
        self,
        credentials_path: str | None = None,
        oauth_credentials_path: str | None = None,
        service_account: gspread.Client | None = None,
    ):
        if service_account:
            self.gc = service_account
        elif credentials_path:
            self.gc = gspread.service_account(filename=credentials_path)
        elif oauth_credentials_path:
            self.gc = gspread.oauth(credentials_filename=oauth_credentials_path)
        else:
            raise ValueError(
                "Must provide either credentials_path, oauth_credentials_path, or service_account"
            )

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        try:
            sh = self.gc.open_by_key(spreadsheet_id)
            if "!" in range_:
                worksheet_name, cell_range = range_.split("!", 1)
                worksheet_name = worksheet_name.strip("'")
                worksheet = sh.worksheet(worksheet_name)
                data = worksheet.get(cell_range)
            else:
                worksheet = sh.worksheet(range_)
                data = worksheet.get_all_values()
            return SheetResult(success=True, data={"values": data})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        try:
            sh = self.gc.open_by_key(spreadsheet_id)
            if "!" in range_:
                worksheet_name, cell_range = range_.split("!", 1)
                worksheet_name = worksheet_name.strip("'")
                worksheet = sh.worksheet(worksheet_name)
                response = worksheet.append_rows(values, value_input_option="USER_ENTERED")
            else:
                worksheet = sh.worksheet(range_)
                response = worksheet.append_rows(values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data=response)
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        try:
            sh = self.gc.open_by_key(spreadsheet_id)
            if "!" in range_:
                worksheet_name, cell_range = range_.split("!", 1)
                worksheet_name = worksheet_name.strip("'")
                worksheet = sh.worksheet(worksheet_name)
                response = worksheet.update(cell_range, values, value_input_option="USER_ENTERED")
            else:
                worksheet = sh.worksheet(range_)
                response = worksheet.update(range_, values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data=response)
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

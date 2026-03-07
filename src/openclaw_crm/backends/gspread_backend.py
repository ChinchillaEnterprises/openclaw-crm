"""Google Sheets backend using gspread library."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import gspread

from openclaw_crm.sheets import SheetsBackend, SheetResult


@dataclass
class GspreadBackend(SheetsBackend):
    """Google Sheets backend using gspread library.
    
    Requires Google Service Account credentials:
    - Set GOOGLE_SERVICE_ACCOUNT_JSON env var to path of JSON file
    - Or set GOOGLE_SERVICE_ACCOUNT_JSON_DATA env var with inline JSON
    """
    
    def __init__(self, service_account_json: str | None = None):
        """Initialize gspread client.
        
        Args:
            service_account_json: Path to service account JSON file.
                If None, reads from GOOGLE_SERVICE_ACCOUNT_JSON env var.
        """
        if service_account_json:
            self._client = gspread.service_account(service_account_json)
        else:
            json_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
            json_data = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_DATA")
            if json_data:
                import json
                self._client = gspread.service_account_from_dict(json.loads(json_data))
            elif json_path:
                self._client = gspread.service_account(json_path)
            else:
                # Default behavior - looks for credentials.json in home
                self._client = gspread.service_account()
    
    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read values from a Google Sheet.
        
        Args:
            spreadsheet_id: The Google Sheets ID (from URL)
            range_: The range to read (e.g., 'Sheet1!A1:D10')
            
        Returns:
            SheetResult with data containing {'values': [[row1], [row2], ...]}
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            # Parse sheet name and cell range
            if '!' in range_:
                sheet_name, cell_range = range_.split('!', 1)
                worksheet = sheet.worksheet(sheet_name)
                values = worksheet.get_values(range=cell_range)
            else:
                worksheet = sheet.worksheet(range_)
                values = worksheet.get_values()
            return SheetResult(success=True, data={"values": values})
        except gspread.exceptions.SpreadsheetNotFound:
            return SheetResult(success=False, data=None, error=f"Spreadsheet {spreadsheet_id} not found")
        except gspread.exceptions.WorksheetNotFound:
            return SheetResult(success=False, data=None, error=f"Worksheet not found: {range_}")
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Append rows to a Google Sheet.
        
        Args:
            spreadsheet_id: The Google Sheets ID
            range_: The range (e.g., 'Sheet1!A:D')
            values: List of rows to append
            
        Returns:
            SheetResult with success status
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            if '!' in range_:
                sheet_name, _ = range_.split('!', 1)
                worksheet = sheet.worksheet(sheet_name)
            else:
                worksheet = sheet.worksheet(range_)
            worksheet.append_rows(values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updated": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Update cells in a Google Sheet.
        
        Args:
            spreadsheet_id: The Google Sheets ID
            range_: The cell range (e.g., 'Sheet1!A1:D1')
            values: List of rows (should match range size)
            
        Returns:
            SheetResult with success status
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            if '!' in range_:
                sheet_name, cell_range = range_.split('!', 1)
                worksheet = sheet.worksheet(sheet_name)
            else:
                return SheetResult(success=False, data=None, error="Range must include sheet name")
            
            # Parse start cell (e.g., A1 -> A, 1)
            import re
            match = re.match(r'([A-Z]+)(\d+)', cell_range)
            if not match:
                return SheetResult(success=False, data=None, error=f"Invalid range: {range_}")
            
            start_col = match.group(1)
            start_row = int(match.group(2))
            
            # Update the range
            worksheet.update(values, range=f"{sheet_name}!{cell_range}")
            return SheetResult(success=True, data={"updated": True})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

"""Gspread backend for openclaw-crm."""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    import gspread
except ImportError:
    gspread = None

from .sheets import SheetsBackend, SheetResult


@dataclass
class GSpreadBackend(SheetsBackend):
    """Google Sheets backend implementation using gspread library."""
    
    def __init__(self, service_account_path: str | None = None):
        if gspread is None:
            raise ImportError("gspread is required. Install with: pip install gspread")
        
        # Use service account or OAuth
        if service_account_path:
            self.gc = gspread.service_account(service_account_path)
        else:
            # Try to use default credentials
            self.gc = gspread.oauth()
        
        self.client = self.gc
    
    def _get_worksheet(self, spreadsheet_id: str, range_: str):
        """Parse range and get worksheet."""
        # Parse range like "Sheet1!A1:D10" or just "Sheet1"
        if "!" in range_:
            sheet_name, cell_range = range_.split("!", 1)
        else:
            sheet_name = range_
            cell_range = None
        
        spreadsheet = self.gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.sheet1 if sheet_name == "Sheet1" else spreadsheet.worksheet(sheet_name)
        
        return worksheet, cell_range
    
    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read data from Google Sheets."""
        try:
            worksheet, cell_range = self._get_worksheet(spreadsheet_id, range_)
            
            if cell_range:
                data = worksheet.get(cell_range)
            else:
                data = worksheet.get_all_values()
            
            return SheetResult(success=True, data={"values": data})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Append rows to Google Sheets."""
        try:
            worksheet, _ = self._get_worksheet(spreadsheet_id, range_)
            
            for row in values:
                worksheet.append_row(row)
            
            return SheetResult(success=True, data={"rows_added": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Update cells in Google Sheets."""
        try:
            worksheet, cell_range = self._get_worksheet(spreadsheet_id, range_)
            
            if cell_range:
                worksheet.update(cell_range, values)
            else:
                # Update first row
                worksheet.update("A1", values[0])
            
            return SheetResult(success=True, data={"updated": True})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

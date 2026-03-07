"""Gspread backend for OpenCRM.

An alternative SheetsBackend implementation using the gspread library.
Install with: pip install openclaw-crm[gspread]
"""

from __future__ import annotations

from typing import Any

import gspread

from openclaw_crm.sheets import SheetResult, SheetsBackend


class GspreadBackend(SheetsBackend):
    """Google Sheets backend using gspread library."""

    def __init__(self, service_account_file: str | None = None):
        """Initialize the gspread client.

        Args:
            service_account_file: Path to Google service account JSON credentials.
                                  If None, uses default gspread authentication
                                  (OAuth or ADC).
        """
        if service_account_file:
            self._client = gspread.service_account(filename=service_account_file)
        else:
            self._client = gspread.oauth()

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read values from a spreadsheet range.

        Args:
            spreadsheet_id: The Google Sheets ID (from the URL).
            range_: The range to read (e.g., 'Sheet1!A1:B10').

        Returns:
            SheetResult with data containing the values.
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            values = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_,
            ).execute()
            return SheetResult(success=True, data=values.get("values", []))
        except gspread.exceptions.SpreadsheetNotFound:
            return SheetResult(
                success=False,
                data=None,
                error=f"Spreadsheet not found: {spreadsheet_id}",
            )
        except Exception as e:  # noqa: BLE001
            return SheetResult(success=False, data=None, error=str(e))

    def append(
        self,
        spreadsheet_id: str,
        range_: str,
        values: list[list[str]],
    ) -> SheetResult:
        """Append values to a spreadsheet.

        Args:
            spreadsheet_id: The Google Sheets ID.
            range_: The range (e.g., 'Sheet1!A1' - rows are appended after).
            values: List of rows to append.

        Returns:
            SheetResult with update info.
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            result = sheet.values().append(
                spreadsheetId=spreadsheet_id,
                range=range_,
                body={"values": values},
                valueInputOption="USER_ENTERED",
            ).execute()
            return SheetResult(success=True, data=result)
        except Exception as e:  # noqa: BLE001
            return SheetResult(success=False, data=None, error=str(e))

    def update(
        self,
        spreadsheet_id: str,
        range_: str,
        values: list[list[str]],
    ) -> SheetResult:
        """Update values in a spreadsheet range.

        Args:
            spreadsheet_id: The Google Sheets ID.
            range_: The range to update (e.g., 'Sheet1!A1:B2').
            values: List of rows to write.

        Returns:
            SheetResult with update info.
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            result = sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=range_,
                body={"values": values},
                valueInputOption="USER_ENTERED",
            ).execute()
            return SheetResult(success=True, data=result)
        except Exception as e:  # noqa: BLE001
            return SheetResult(success=False, data=None, error=str(e))

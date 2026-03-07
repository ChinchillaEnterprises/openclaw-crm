"""Gspread backend for Google Sheets.

Provides an alternative to the gws CLI backend using gspread library.

Usage:
    from openclaw_crm.backends import GspreadBackend
    from openclaw_crm.sheets import set_backend

    set_backend(GspreadBackend("credentials.json"))
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from openclaw_crm.sheets import SheetResult, SheetsBackend


@dataclass
class GspreadBackend(SheetsBackend):
    """Google Sheets backend using gspread library.

    Args:
        credentials_path: Path to Google service account JSON credentials.
                         Can also be set via GOOGLE_CREDENTIALS_PATH env var.
    """

    credentials_path: str | None = None
    _gc: gspread.client.Client | None = None

    def _get_client(self) -> gspread.client.Client:
        """Lazy initialization of gspread client."""
        if self._gc is not None:
            return self._gc

        creds_path = self.credentials_path or os.environ.get("GOOGLE_CREDENTIALS_PATH")
        if not creds_path:
            # Try default location
            creds_path = os.path.expanduser("~/.config/gspread/credentials.json")

        if not Path(creds_path).exists():
            raise FileNotFoundError(
                f"Credentials file not found: {creds_path}. "
                "Set GOOGLE_CREDENTIALS_PATH or provide credentials_path."
            )

        credentials = Credentials.from_service_account_file(
            creds_path,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        self._gc = gspread.authorize(credentials)
        return self._gc

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read data from a Google Sheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (from the URL).
            range_: The A1 notation of the values to retrieve (e.g., "Sheet1!A1:D10").

        Returns:
            SheetResult with success=True and data containing the values.
        """
        try:
            gc = self._get_client()
            sh = gc.open_by_key(spreadsheet_id)
            # Parse sheet name from range (e.g., "Pipeline!A1:U21" -> "Pipeline")
            sheet_name = range_.split("!")[0].strip("'")
            worksheet = sh.worksheet(sheet_name)
            values = worksheet.get_all_values()
            return SheetResult(success=True, data={"values": values})
        except FileNotFoundError as e:
            return SheetResult(success=False, data=None, error=str(e))
        except Exception as e:
            return SheetResult(success=False, data=None, error=f"gspread error: {e}")

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Append rows to a Google Sheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet.
            range_: The A1 notation (e.g., "Sheet1!A").
            values: List of rows to append.

        Returns:
            SheetResult with success status.
        """
        try:
            gc = self._get_client()
            sh = gc.open_by_key(spreadsheet_id)
            sheet_name = range_.split("!")[0].strip("'")
            worksheet = sh.worksheet(sheet_name)
            worksheet.append_rows(values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updated": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=f"gspread error: {e}")

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Update cells in a Google Sheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet.
            range_: The A1 notation (e.g., "Sheet1!A1:D5").
            values: 2D list of values to write.

        Returns:
            SheetResult with success status.
        """
        try:
            gc = self._get_client()
            sh = gc.open_by_key(spreadsheet_id)
            sheet_name = range_.split("!")[0].strip("'")
            worksheet = sh.worksheet(sheet_name)
            worksheet.update(range_, values, value_input_option="USER_ENTERED")
            return SheetResult(success=True, data={"updated": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=f"gspread error: {e}")

from __future__ import annotations

import os
from typing import Any

from openclaw_crm.sheets import SheetResult, SheetsBackend


class GspreadBackend(SheetsBackend):
    """
    Gspread backend for openclaw-crm.

    Uses gspread library to interact with Google Sheets API.
    Provides a Pythonic interface to Google Sheets without requiring the gws CLI.

    Configuration via environment variables or config file:
    - SPREADSHEET_ID: Google Spreadsheet ID (required)
    - GOOGLE_CREDENTIALS_PATH: Path to Google service account JSON file (required)
      or use GOOGLE_CREDENTIALS_JSON with JSON content directly

    Authentication:
    1. Create a Google Cloud Project
    2. Enable Google Sheets API
    3. Create a Service Account
    4. Download the JSON key file
    5. Share the spreadsheet with the service account email

    Example:
        from openclaw_crm.backends import GspreadBackend

        backend = GspreadBackend(
            spreadsheet_id="1BxiMVs0XRA5nFMdKbBdB_...",
            credentials_path="/path/to/credentials.json"
        )

        # Read data
        result = backend.read("spreadsheet_id", "Pipeline!A1:Z")
        if result.ok:
            print(result.data["values"])

        # Append data
        values = [["Header1", "Header2"], ["Value1", "Value2"]]
        result = backend.append("spreadsheet_id", "Pipeline", values)

        # Update data
        result = backend.update("spreadsheet_id", "Pipeline!A2:C", values)
    """

    def __init__(
        self,
        spreadsheet_id: str | None = None,
        credentials_path: str | None = None,
        credentials_json: str | None = None,
    ) -> None:
        """
        Initialize Gspread backend.

        Args:
            spreadsheet_id: Google Spreadsheet ID. If None, reads from env var SPREADSHEET_ID or config.
            credentials_path: Path to Google service account JSON file.
                If None, reads from env var GOOGLE_CREDENTIALS_PATH or config.
            credentials_json: Google service account JSON content as string.
                If None, reads from env var GOOGLE_CREDENTIALS_JSON.
        """
        self.spreadsheet_id = (
            spreadsheet_id
            or os.environ.get("SPREADSHEET_ID")
            or self._load_config().get("spreadsheet_id")
        )

        # Get credentials (priority: explicit parameter -> env var -> config)
        self.credentials_path = (
            credentials_path
            or os.environ.get("GOOGLE_CREDENTIALS_PATH")
            or self._load_config().get("credentials_path")
        )
        self.credentials_json = (
            credentials_json
            or os.environ.get("GOOGLE_CREDENTIALS_JSON")
            or self._load_config().get("credentials_json")
        )

        if not self.spreadsheet_id:
            raise ValueError(
                "SPREADSHEET_ID not configured. "
                "Set environment variable or add to config file."
            )
        if not self.credentials_path and not self.credentials_json:
            raise ValueError(
                "GOOGLE_CREDENTIALS_PATH or GOOGLE_CREDENTIALS_JSON not configured. "
                "Set environment variable or add to config file."
            )

        try:
            import gspread

            self.gspread = gspread

            # Initialize client
            if self.credentials_json:
                # Use JSON content directly
                import json

                credentials_dict = json.loads(self.credentials_json)
                self.client = gspread.service_account_from_dict(credentials_dict)
            else:
                # Use credentials file
                self.client = gspread.service_account(filename=self.credentials_path)

            # Open spreadsheet
            self.sheet = self.client.open_by_key(self.spreadsheet_id)

        except ImportError as e:
            raise ImportError(
                "gspread is not installed. Install with: pip install gspread"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Failed to initialize gspread client: {str(e)}") from e

    def _load_config(self) -> dict[str, str]:
        """Load gspread configuration from config file if available."""
        try:
            from openclaw_crm.config import load_config

            cfg = load_config()
            gspread_cfg = cfg.get("gspread", {})
            return {
                "spreadsheet_id": gspread_cfg.get("spreadsheet_id", ""),
                "credentials_path": gspread_cfg.get("credentials_path", ""),
                "credentials_json": gspread_cfg.get("credentials_json", ""),
            }
        except Exception:
            return {}

    def _extract_worksheet_and_range(
        self, range_: str
    ) -> tuple[Any, str, str | None]:
        """
        Extract worksheet and cell range from range string.

        Args:
            range_: Range string like "Sheet1!A1:Z" or "Sheet1!A1" or just "Sheet1"

        Returns:
            Tuple of (worksheet object, range without sheet name, original range)
        """
        # Split sheet name and range
        if "!" in range_:
            sheet_name, cell_range = range_.split("!", 1)
        else:
            sheet_name = range_
            cell_range = None

        # Strip quotes from sheet name
        sheet_name = sheet_name.strip("'\"")

        # Get worksheet
        try:
            worksheet = self.sheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            # Try to create the worksheet if it doesn't exist
            worksheet = self.sheet.add_worksheet(title=sheet_name, rows="1000", cols="26")

        return worksheet, cell_range, range_

    def _cell_range_to_row_col(
        self, cell_range: str
    ) -> tuple[int, int, int, int] | None:
        """
        Convert cell range (e.g., "A1:Z10") to row/col indices.

        Returns:
            Tuple of (start_row, start_col, end_row, end_col) or None if no range
        """
        if not cell_range or ":" not in cell_range:
            return None

        try:
            # Parse range like "A1:Z10"
            start, end = cell_range.split(":")
            start_row = int("".join(filter(str.isdigit, start)))
            start_col = self.gspread.utils.a1_to_column(start)
            end_row = int("".join(filter(str.isdigit, end)))
            end_col = self.gspread.utils.a1_to_column(end)
            return (start_row, start_col, end_row, end_col)
        except Exception:
            return None

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """
        Read data from spreadsheet.

        Args:
            spreadsheet_id: Google Spreadsheet ID (can be different from configured one)
            range_: Range to read (e.g., "Sheet1!A1:Z" or "Sheet1!A1" or "Sheet1")

        Returns:
            SheetResult with data in 2D array format
        """
        try:
            # Use provided spreadsheet_id if different from configured one
            if spreadsheet_id and spreadsheet_id != self.spreadsheet_id:
                sheet = self.client.open_by_key(spreadsheet_id)
            else:
                sheet = self.sheet

            # Extract worksheet and range
            worksheet, cell_range, _ = self._extract_worksheet_and_range(range_)

            # Read data
            if cell_range:
                # Read specific range
                parsed_range = self._cell_range_to_row_col(cell_range)
                if parsed_range:
                    start_row, start_col, end_row, end_col = parsed_range
                    values = worksheet.get_values(start_row, start_col, end_row, end_col)
                else:
                    # Invalid range, read all
                    values = worksheet.get_values()
            else:
                # Read all data
                values = worksheet.get_values()

            return SheetResult(success=True, data={"values": values})

        except Exception as e:
            return SheetResult(
                success=False, data=None, error=f"Gspread read error: {str(e)}"
            )

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """
        Append values to spreadsheet.

        Args:
            spreadsheet_id: Google Spreadsheet ID (can be different from configured one)
            range_: Range to append to (e.g., "Sheet1!A1:Z" or just "Sheet1")
            values: 2D array of values to append

        Returns:
            SheetResult indicating success or failure
        """
        try:
            # Use provided spreadsheet_id if different from configured one
            if spreadsheet_id and spreadsheet_id != self.spreadsheet_id:
                sheet = self.client.open_by_key(spreadsheet_id)
            else:
                sheet = self.sheet

            # Extract worksheet and range
            worksheet, cell_range, _ = self._extract_worksheet_and_range(range_)

            # Get starting cell
            if cell_range and ":" in cell_range:
                start_cell = cell_range.split(":")[0]
            else:
                start_cell = "A1"

            # Append values
            worksheet.append_rows(values, table_range=start_cell)

            return SheetResult(success=True, data={"updatedRows": len(values)})

        except Exception as e:
            return SheetResult(
                success=False, data=None, error=f"Gspread append error: {str(e)}"
            )

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """
        Update cells in spreadsheet.

        Args:
            spreadsheet_id: Google Spreadsheet ID (can be different from configured one)
            range_: Range to update (e.g., "Sheet1!A1:Z" or "Sheet1!A2:C10")
            values: 2D array of values to update

        Returns:
            SheetResult indicating success or failure

        Note:
            The values array should be the same size as the target range.
            For partial updates, specify the exact range you want to update.
        """
        try:
            # Use provided spreadsheet_id if different from configured one
            if spreadsheet_id and spreadsheet_id != self.spreadsheet_id:
                sheet = self.client.open_by_key(spreadsheet_id)
            else:
                sheet = self.sheet

            # Extract worksheet and range
            worksheet, cell_range, original_range = self._extract_worksheet_and_range(
                range_
            )

            # Get starting position
            if cell_range and ":" in cell_range:
                start_cell = cell_range.split(":")[0]
            elif cell_range:
                start_cell = cell_range
            else:
                # If no range specified, update from A1
                start_cell = "A1"

            # Update values
            worksheet.update(values, start_cell)

            return SheetResult(success=True, data={"updatedRows": len(values)})

        except Exception as e:
            return SheetResult(
                success=False, data=None, error=f"Gspread update error: {str(e)}"
            )

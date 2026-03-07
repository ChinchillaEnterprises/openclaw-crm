from __future__ import annotations

import os
from typing import Any

from openclaw_crm.sheets import SheetResult, SheetsBackend


class AirtableBackend(SheetsBackend):
    """
    Airtable backend for openclaw-crm.

    Uses pyairtable library to interact with Airtable API.
    Maps spreadsheet_id -> Airtable base ID
    Maps range -> Airtable table name

    Configuration via environment variables or config file:
    - AIRTABLE_BASE_ID: Airtable base ID (required)
    - AIRTABLE_API_TOKEN: Airtable API token (required)
    """

    def __init__(self, base_id: str | None = None, api_token: str | None = None) -> None:
        """
        Initialize Airtable backend.

        Args:
            base_id: Airtable base ID. If None, reads from env var AIRTABLE_BASE_ID or config.
            api_token: Airtable API token. If None, reads from env var AIRTABLE_API_TOKEN or config.
        """
        self.base_id = base_id or os.environ.get("AIRTABLE_BASE_ID") or self._load_config().get("base_id")
        self.api_token = api_token or os.environ.get("AIRTABLE_API_TOKEN") or self._load_config().get("api_token")

        if not self.base_id:
            raise ValueError("AIRTABLE_BASE_ID not configured. Set environment variable or add to config file.")
        if not self.api_token:
            raise ValueError("AIRTABLE_API_TOKEN not configured. Set environment variable or add to config file.")

        try:
            from pyairtable import Api

            self.api = Api(self.api_token)
            self.base = self.api.base(self.base_id)
        except ImportError as e:
            raise ImportError("pyairtable is not installed. Install with: pip install pyairtable") from e

    def _load_config(self) -> dict[str, str]:
        """Load Airtable configuration from config file if available."""
        try:
            from openclaw_crm.config import load_config

            cfg = load_config()
            airtable_cfg = cfg.get("airtable", {})
            return {
                "base_id": airtable_cfg.get("base_id", ""),
                "api_token": airtable_cfg.get("api_token", ""),
            }
        except Exception:
            return {}

    def _extract_table_name(self, range_: str) -> str:
        """
        Extract table name from range.

        Google Sheets ranges can be "Sheet1!A1:Z" or "Sheet1!A1" or just "Sheet1".
        Airtable uses table names directly.

        Args:
            range_: Spreadsheet range string

        Returns:
            Airtable table name
        """
        # Remove sheet reference (e.g., "Pipeline!A1:Z" -> "Pipeline")
        if "!" in range_:
            table_name = range_.split("!")[0]
        else:
            table_name = range_

        # Strip quotes
        table_name = table_name.strip("'\"")
        return table_name

    def _convert_sheets_values_to_airtable_records(self, values: list[list[str]], table: str) -> list[dict[str, Any]]:
        """
        Convert Google Sheets values (2D array) to Airtable records format.

        Args:
            values: 2D array of values from Google Sheets format
            table: Airtable table name (to infer field names)

        Returns:
            List of Airtable record dictionaries
        """
        if not values:
            return []

        # First row is headers, subsequent rows are data
        headers = values[0]
        records = []

        for row in values[1:]:
            record = {}
            for i, value in enumerate(row):
                if i < len(headers):
                    field_name = headers[i]
                    # Skip empty values
                    if value:
                        record[field_name] = value
            if record:  # Only add non-empty records
                records.append(record)

        return records

    def _convert_airtable_records_to_sheets_values(self, records: list[dict[str, Any]]) -> list[list[str]]:
        """
        Convert Airtable records to Google Sheets values (2D array).

        Args:
            records: List of Airtable record dictionaries

        Returns:
            2D array of values compatible with Google Sheets
        """
        if not records:
            return []

        # Get all unique field names from all records
        field_names = set()
        for record in records:
            field_names.update(record["fields"].keys())
        field_names = sorted(field_names)

        # Create header row
        result = [list(field_names)]

        # Add data rows
        for record in records:
            row = []
            for field in field_names:
                value = record["fields"].get(field, "")
                # Convert non-string values to strings
                row.append(str(value) if value is not None else "")
            result.append(row)

        return result

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """
        Read data from Airtable table.

        Args:
            spreadsheet_id: Ignored (Airtable uses base_id from config)
            range_: Table name (e.g., "Pipeline" or "Pipeline!A1:Z")

        Returns:
            SheetResult with data in Google Sheets 2D array format
        """
        table_name = self._extract_table_name(range_)

        try:
            table = self.base.table(table_name)
            records = table.all()

            # Convert to Google Sheets format
            values = self._convert_airtable_records_to_sheets_values(records)

            return SheetResult(success=True, data={"values": values})

        except Exception as e:
            return SheetResult(success=False, data=None, error=f"Airtable read error: {str(e)}")

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """
        Append values to Airtable table.

        Args:
            spreadsheet_id: Ignored (Airtable uses base_id from config)
            range_: Table name (e.g., "Pipeline!A1:Z")
            values: 2D array of values to append (first row = headers, subsequent rows = data)

        Returns:
            SheetResult indicating success or failure
        """
        table_name = self._extract_table_name(range_)

        try:
            # Convert to Airtable format
            records = self._convert_sheets_values_to_airtable_records(values, table_name)

            if not records:
                return SheetResult(success=True, data={"updatedRows": 0})

            table = self.base.table(table_name)
            created = table.batch_create(records)

            return SheetResult(success=True, data={"updatedRows": len(created)})

        except Exception as e:
            return SheetResult(success=False, data=None, error=f"Airtable append error: {str(e)}")

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """
        Update records in Airtable table.

        Args:
            spreadsheet_id: Ignored (Airtable uses base_id from config)
            range_: Table name (e.g., "Pipeline!A1:Z")
            values: 2D array of values to update

        Returns:
            SheetResult indicating success or failure

        Note:
            Airtable requires record IDs for updates. This implementation
            will replace all records in the table with new values.
            For more granular updates, you'd need to store record IDs.
        """
        table_name = self._extract_table_name(range_)

        try:
            table = self.base.table(table_name)
            all_records = table.all()

            # Convert new values to Airtable format
            new_records = self._convert_sheets_values_to_airtable_records(values, table_name)

            # Update existing records
            updated = 0
            for existing_record, new_record in zip(all_records, new_records):
                table.update(existing_record["id"], new_record)
                updated += 1

            # Append any additional records
            if len(new_records) > len(all_records):
                additional_records = new_records[len(all_records) :]
                created = table.batch_create(additional_records)
                updated += len(created)

            return SheetResult(success=True, data={"updatedRows": updated})

        except Exception as e:
            return SheetResult(success=False, data=None, error=f"Airtable update error: {str(e)}")

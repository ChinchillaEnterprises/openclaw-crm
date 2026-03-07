"""Airtable backend for OpenCRM.

An alternative SheetsBackend implementation using the Airtable API.
Install with: pip install openclaw-crm[airtable]
"""

from __future__ import annotations

from typing import Any

from pyairtable import Api

from openclaw_crm.sheets import SheetResult, SheetsBackend


# Column mapping: Sheets column names to Airtable field names
PIPELINE_FIELDS = [
    "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
    "Service", "First Contact", "Last Contact", "Next Action",
    "Due Date", "Notes", "Slack Channel", "Proposal Link",
    "Owner", "Upwork URL", "Probability",
    "Referred By", "Network Parent", "Network Notes", "Signal Date",
]


class AirtableBackend(SheetsBackend):
    """Airtable backend as an alternative to Google Sheets."""

    def __init__(self, base_id: str | None = None, api_token: str | None = None):
        """Initialize the Airtable client.

        Args:
            base_id: The Airtable base ID. If None, reads from AIRTABLE_BASE_ID env.
            api_token: The Airtable API token. If None, reads from AIRTABLE_API_TOKEN env.
        """
        import os
        self._base_id = base_id or os.environ.get("AIRTABLE_BASE_ID")
        self._api_token = api_token or os.environ.get("AIRTABLE_API_TOKEN")
        if not self._base_id or not self._api_token:
            raise ValueError(
                "Airtable credentials required. Pass base_id and api_token, "
                "or set AIRTABLE_BASE_ID and AIRTABLE_API_TOKEN env vars."
            )
        self._api = Api(self._api_token)

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read records from an Airtable table.

        Args:
            spreadsheet_id: The Airtable table name (e.g., 'Pipeline').
            range_: Not used for Airtable - included for interface compatibility.

        Returns:
            SheetResult with data containing the records as rows.
        """
        try:
            table = self._api.table(self._base_id, spreadsheet_id)
            records = table.all()
            # Convert to sheet format (headers + rows)
            if not records:
                return SheetResult(success=True, data={"values": [PIPELINE_FIELDS]})
            # Get field names from first record
            fields = list(records[0].get("fields", {}).keys())
            headers = fields
            rows = [headers]
            for record in records:
                row = [record.get("fields", {}).get(field, "") for field in fields]
                rows.append(row)
            return SheetResult(success=True, data={"values": rows})
        except Exception as e:  # noqa: BLE001
            return SheetResult(success=False, data=None, error=str(e))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Append a record to an Airtable table.

        Args:
            spreadsheet_id: The Airtable table name (e.g., 'Pipeline').
            range_: Not used for Airtable.
            values: List of rows (each row is a list of field values).

        Returns:
            SheetResult with creation info.
        """
        try:
            table = self._api.table(self._base_id, spreadsheet_id)
            # Create record from values
            record = values[0] if values else []
            fields = dict(zip(PIPELINE_FIELDS[:len(record)], record))
            created = table.create(fields)
            return SheetResult(success=True, data={"id": created.id, "fields": created.get("fields", {})})
        except Exception as e:  # noqa: BLE001
            return SheetResult(success=False, data=None, error=str(e))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Update a record in an Airtable table.

        Args:
            spreadsheet_id: The Airtable table name (e.g., 'Pipeline').
            range_: The record ID to update.
            values: List of rows with new values.

        Returns:
            SheetResult with update info.
        """
        try:
            table = self._api.table(self._base_id, spreadsheet_id)
            record_id = range_  # Use range_ as the record ID
            record = values[0] if values else []
            fields = dict(zip(PIPELINE_FIELDS[:len(record)], record))
            updated = table.update(record_id, fields)
            return SheetResult(success=True, data={"id": updated.id, "fields": updated.get("fields", {})})
        except Exception as e:  # noqa: BLE001
            return SheetResult(success=False, data=None, error=str(e))

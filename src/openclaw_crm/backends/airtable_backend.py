from __future__ import annotations

import os
from typing import Any

from openclaw_crm.sheets import SheetsBackend, SheetResult


class AirtableBackend(SheetsBackend):
    """Airtable-backed storage backend for openclaw-crm.

    Uses the pyairtable library to read/write CRM data to an Airtable base
    instead of Google Sheets.

    Configuration via environment variables:
        AIRTABLE_API_TOKEN: Your Airtable personal access token
        AIRTABLE_BASE_ID: The Airtable base ID (starts with "app")

    Usage:
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        from openclaw_crm.sheets import set_backend

        backend = AirtableBackend()
        set_backend(backend)
    """

    def __init__(
        self,
        api_token: str | None = None,
        base_id: str | None = None,
    ):
        self.api_token = api_token or os.environ.get("AIRTABLE_API_TOKEN", "")
        self.base_id = base_id or os.environ.get("AIRTABLE_BASE_ID", "")
        if not self.api_token:
            raise ValueError("AIRTABLE_API_TOKEN environment variable or api_token parameter is required")
        if not self.base_id:
            raise ValueError("AIRTABLE_BASE_ID environment variable or base_id parameter is required")

    def _get_table(self, range_: str) -> Any:
        """Extract table name from a Sheets-style range and return a pyairtable Table."""
        from pyairtable import Api

        table_name = range_.split("!")[0].strip("'")
        api = Api(self.api_token)
        return api.table(self.base_id, table_name)

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read all records from an Airtable table.

        The spreadsheet_id parameter is ignored (base_id is used from config).
        The range_ parameter is parsed to extract the table name.
        """
        try:
            table = self._get_table(range_)
            records = table.all()
            if not records:
                return SheetResult(success=True, data={"values": []})
            all_fields: list[str] = []
            for record in records:
                for key in record["fields"]:
                    if key not in all_fields:
                        all_fields.append(key)
            values = [all_fields]
            for record in records:
                row = [str(record["fields"].get(col, "")) for col in all_fields]
                values.append(row)
            return SheetResult(success=True, data={"values": values})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Append rows to an Airtable table.

        Assumes the first row of the table defines field names. Each row in
        values is mapped to those field names positionally.
        """
        try:
            table = self._get_table(range_)
            existing = table.all()
            if existing:
                field_names = list(existing[0]["fields"].keys())
            elif values:
                field_names = values[0]
                values = values[1:]
            else:
                return SheetResult(success=True, data={})

            for row in values:
                fields = {}
                for i, val in enumerate(row):
                    if i < len(field_names):
                        fields[field_names[i]] = val
                table.create(fields)
            return SheetResult(success=True, data={"updatedRows": len(values)})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Update records in an Airtable table.

        Matches records by the first column value, then updates the remaining fields.
        """
        try:
            table = self._get_table(range_)
            existing = table.all()
            if not existing:
                return SheetResult(success=False, data=None, error="No existing records to update")

            field_names = list(existing[0]["fields"].keys())
            updated = 0
            for row in values:
                if not row:
                    continue
                match_value = row[0]
                for record in existing:
                    first_field = field_names[0] if field_names else None
                    if first_field and str(record["fields"].get(first_field, "")) == match_value:
                        fields = {}
                        for i, val in enumerate(row[1:], start=1):
                            if i < len(field_names):
                                fields[field_names[i]] = val
                        table.update(record["id"], fields)
                        updated += 1
                        break
            return SheetResult(success=True, data={"updatedRows": updated})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

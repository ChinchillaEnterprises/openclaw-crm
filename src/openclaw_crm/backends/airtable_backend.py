"""Airtable backend for OpenCRM.

This module provides an AirtableBackend class that implements the SheetsBackend
interface, allowing OpenCRM to use Airtable as an alternative storage backend.

Configuration:
- AIRTABLE_BASE_ID: The Airtable base ID (env var)
- AIRTABLE_API_TOKEN: The Airtable API token (env var)
- Or via config file:
    airtable:
      base_id: "appXXXXXXXXXXXXXX"
      api_token: "patXXXXXXXXXXXXXX"
"""

from __future__ import annotations

import os
from typing import Any

from openclaw_crm.sheets import SheetResult, SheetsBackend

# Default column mapping from Google Sheets headers to Airtable field names
DEFAULT_COLUMN_MAP = {
    "Client": "Client",
    "Contact": "Contact",
    "Source": "Source",
    "Stage": "Stage",
    "Budget": "Budget",
    "Rate Type": "Rate Type",
    "Service": "Service",
    "First Contact": "First Contact",
    "Last Contact": "Last Contact",
    "Next Action": "Next Action",
    "Due Date": "Due Date",
    "Notes": "Notes",
    "Slack Channel": "Slack Channel",
    "Proposal Link": "Proposal Link",
    "Owner": "Owner",
    "Upwork URL": "Upwork URL",
    "Probability": "Probability",
    "Referred By": "Referred By",
    "Network Parent": "Network Parent",
    "Network Notes": "Network Notes",
    "Signal Date": "Signal Date",
}


class AirtableBackend(SheetsBackend):
    """Airtable backend implementation for OpenCRM.
    
    This backend maps the SheetsBackend interface to Airtable's API:
    - spreadsheet_id -> Airtable base ID
    - range_ -> Airtable table name (e.g., "Pipeline")
    """

    def __init__(
        self,
        base_id: str | None = None,
        api_token: str | None = None,
        column_map: dict[str, str] | None = None,
    ):
        """Initialize the Airtable backend.
        
        Args:
            base_id: Airtable base ID. If None, reads from AIRTABLE_BASE_ID env var
                    or from config file.
            api_token: Airtable API token. If None, reads from AIRTABLE_API_TOKEN env var
                      or from config file.
            column_map: Optional mapping from standard headers to Airtable field names.
        """
        self._base_id = base_id or os.environ.get("AIRTABLE_BASE_ID")
        self._api_token = api_token or os.environ.get("AIRTABLE_API_TOKEN")
        self._column_map = column_map or DEFAULT_COLUMN_MAP
        self._table_cache: dict[str, list[dict]] = {}
        self._record_id_cache: dict[str, dict[int, str]] = {}  # table_name -> {row_num -> record_id}

    def _get_client(self):
        """Get or create the Airtable client."""
        try:
            from pyairtable import Api
        except ImportError:
            return None
        
        if not self._api_token:
            self._load_config()
        
        if not self._api_token:
            raise ValueError(
                "Airtable API token not configured. "
                "Set AIRTABLE_API_TOKEN env var or configure in config file."
            )
        
        return Api(self._api_token)

    def _load_config(self) -> None:
        """Load configuration from config file."""
        # Avoid circular import
        from openclaw_crm.config import load_config
        
        cfg = load_config()
        airtable_cfg = cfg.get("airtable", {})
        
        if not self._base_id:
            self._base_id = airtable_cfg.get("base_id") or os.environ.get("AIRTABLE_BASE_ID")
        if not self._api_token:
            self._api_token = airtable_cfg.get("api_token") or os.environ.get("AIRTABLE_API_TOKEN")

    def _parse_range(self, range_: str) -> tuple[str, int | None]:
        """Parse range string like 'Pipeline!A:U' or 'Pipeline!A2:U100'.
        
        Args:
            range_: Range string in Google Sheets format.
            
        Returns:
            Tuple of (table_name, row_number_or_none).
        """
        if "!" in range_:
            table_name, cell_range = range_.split("!", 1)
            # Remove sheet name quotes if present
            table_name = table_name.strip("'")
            
            # Parse cell range (e.g., "A2:U100" or "A:U")
            if cell_range and cell_range[0].isalpha():
                # Extract row number from cell range
                parts = cell_range.split(":")
                if len(parts) > 1 and parts[0].strip():
                    # Get start row (e.g., "A2" -> 2)
                    start_cell = parts[0]
                    digits = "".join(c for c in start_cell if c.isdigit())
                    if digits:
                        row_num = int(digits)
                        return table_name, row_num
                elif parts[0].strip():
                    start_cell = parts[0]
                    if any(c.isdigit() for c in start_cell):
                        row_num = int("".join(c for c in start_cell if c.isdigit()))
                        return table_name, row_num
            return table_name, None
        return range_, None

    def read(self, base_id: str, table_name: str) -> SheetResult:
        """Read records from an Airtable table.
        
        Args:
            base_id: The Airtable base ID.
            table_name: The table name (e.g., "Pipeline").
            
        Returns:
            SheetResult with data in the same format as SheetsBackend:
            {"values": [headers, row1, row2, ...]}
        """
        try:
            api = self._get_client()
            if api is None:
                return SheetResult(
                    success=False,
                    data=None,
                    error="pyairtable not installed. Run: pip install pyairtable"
                )

            table = api.table(base_id, table_name)
            records = table.all()
            
            if not records:
                return SheetResult(success=True, data={"values": []})
            
            # Get field names from first record
            if records:
                fields = list(records[0].get("fields", {}).keys())
            else:
                fields = list(self._column_map.keys())
            
            # Reverse column map for Airtable -> standard
            at_to_standard = {v: k for k, v in self._column_map.items()}
            
            # Build rows with standard headers
            rows = [fields]  # Header row
            for record in records:
                row = [record.get("fields", {}).get(field, "") for field in fields]
                rows.append(row)
            
            # Cache record IDs for update operations
            self._cache_record_ids(table_name, records)
            
            return SheetResult(success=True, data={"values": rows})
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def _cache_record_ids(self, table_name: str, records: list[dict]) -> None:
        """Cache record IDs by row number for update operations."""
        self._record_id_cache[table_name] = {}
        for idx, record in enumerate(records, start=2):  # Row 2 is first data row
            self._record_id_cache[table_name][idx] = record.get("id", "")

    def append(self, base_id: str, table_name: str, values: list[list[str]]) -> SheetResult:
        """Append records to an Airtable table.
        
        Args:
            base_id: The Airtable base ID.
            table_name: The table name.
            values: List of rows to append. First row should be headers.
            
        Returns:
            SheetResult with success status and created record info.
        """
        try:
            api = self._get_client()
            if api is None:
                return SheetResult(
                    success=False,
                    data=None,
                    error="pyairtable not installed. Run: pip install pyairtable"
                )

            table = api.table(base_id, table_name)
            
            # Get headers from first row
            if not values or len(values) < 1:
                return SheetResult(success=False, data=None, error="No values to append")
            
            headers = values[0]
            
            # Create records from values (skip header row)
            for row in values[1:]:
                record = {}
                for idx, header in enumerate(headers):
                    if idx < len(row):
                        # Use column map to get Airtable field name
                        at_field = self._column_map.get(header, header)
                        record[at_field] = row[idx]
                
                if record:
                    table.create(record)
            
            return SheetResult(
                success=True,
                data={"created": len(values) - 1},
                error=""
            )
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def update(self, base_id: str, table_name: str, values: list[list[str]]) -> SheetResult:
        """Update a record in an Airtable table.
        
        Args:
            base_id: The Airtable base ID.
            table_name: The table name.
            values: List containing a single row to update [headers, row_data].
                    Row number should be included in the range parsed earlier.
            
        Returns:
            SheetResult with success status.
        """
        try:
            api = self._get_client()
            if api is None:
                return SheetResult(
                    success=False,
                    data=None,
                    error="pyairtable not installed. Run: pip install pyairtable"
                )

            # Parse the table name to get row number
            table_name, row_num = self._parse_range(table_name)
            
            if row_num is None:
                return SheetResult(
                    success=False,
                    data=None,
                    error="Row number required for update. Use format 'TableName!A2:Z2'"
                )

            # Get cached record ID
            if table_name not in self._record_id_cache:
                # Refresh cache
                self.read(base_id, table_name)
            
            record_id = self._record_id_cache.get(table_name, {}).get(row_num)
            
            if not record_id:
                return SheetResult(
                    success=False,
                    data=None,
                    error=f"Record not found for row {row_num}. Refresh data first."
                )

            table = api.table(base_id, table_name)
            
            # Build update record
            if len(values) < 2:
                return SheetResult(success=False, data=None, error="No values to update")
            
            headers = values[0]
            row_data = values[1]
            
            record = {}
            for idx, header in enumerate(headers):
                if idx < len(row_data):
                    at_field = self._column_map.get(header, header)
                    record[at_field] = row_data[idx]
            
            table.update(record_id, record)
            
            return SheetResult(success=True, data={"updated": 1}, error="")
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))


def get_airtable_backend(
    base_id: str | None = None,
    api_token: str | None = None,
) -> AirtableBackend:
    """Get an AirtableBackend instance.
    
    Args:
        base_id: Optional base ID override.
        api_token: Optional API token override.
        
    Returns:
        Configured AirtableBackend instance.
    """
    return AirtableBackend(base_id=base_id, api_token=api_token)

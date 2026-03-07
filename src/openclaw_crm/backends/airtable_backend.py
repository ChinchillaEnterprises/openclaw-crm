from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, List, Optional, Dict

try:
    from pyairtable import Api
    from pyairtable.formulas import match
except ImportError:
    Api = None

from openclaw_crm.sheets import SheetsBackend, SheetResult
from openclaw_crm.pipeline import HEADERS as PIPELINE_HEADERS

# Revenue Log headers are not exported in pipeline.py, define them here based on pipeline.py usage
REVENUE_HEADERS = ["Date", "Client", "Amount", "Description", "Status", "Invoice Link"]


class AirtableBackend(SheetsBackend):
    def __init__(self, api_key: str | None = None, base_id: str | None = None):
        self.api_key = api_key or os.getenv("AIRTABLE_API_KEY")
        self.base_id = base_id or os.getenv("AIRTABLE_BASE_ID")
        
        if not Api:
            raise ImportError("pyairtable is required for AirtableBackend. Install it with `pip install pyairtable`.")
            
        if not self.api_key:
            raise ValueError("AIRTABLE_API_KEY is required for AirtableBackend")
        if not self.base_id:
            raise ValueError("AIRTABLE_BASE_ID is required for AirtableBackend")
            
        self.api = Api(self.api_key)

    def _get_table_name(self, range_: str) -> str:
        # range_ is typically "Pipeline!A:U" or "'Revenue Log'!A:F"
        if "!" in range_:
            table_name = range_.split("!")[0]
            if table_name.startswith("'") and table_name.endswith("'"):
                return table_name[1:-1]
            return table_name
        return range_

    def _get_headers(self, table_name: str) -> List[str]:
        if table_name == "Pipeline":
            return PIPELINE_HEADERS
        elif table_name == "Revenue Log":
            return REVENUE_HEADERS
        return []

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        # spreadsheet_id is ignored, use self.base_id
        table_name = self._get_table_name(range_)
        headers = self._get_headers(table_name)
        
        try:
            table = self.api.table(self.base_id, table_name)
            # Fetch all records. Default sort is by creation time unless specified.
            # Using view="Grid view" if it exists would be safer but we can't assume view names.
            # We'll just fetch all.
            records = table.all()
            
            # Convert to list of lists
            rows = [headers]
            for record in records:
                fields = record.get("fields", {})
                row = [str(fields.get(h, "")) for h in headers]
                rows.append(row)
                
            return SheetResult(success=True, data={"values": rows})
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        table_name = self._get_table_name(range_)
        headers = self._get_headers(table_name)
        
        if not values:
             return SheetResult(success=True, data={"updates": {"updatedRows": 0}})

        try:
            table = self.api.table(self.base_id, table_name)
            created_records = []
            
            for row in values:
                # Map row values to fields
                fields = {}
                for i, val in enumerate(row):
                    if i < len(headers):
                        fields[headers[i]] = val
                
                record = table.create(fields)
                created_records.append(record)
                
            return SheetResult(success=True, data={"updates": {"updatedRows": len(created_records)}})
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        # range_ format: "Pipeline!A{row}:U{row}"
        # We need to parse the row index.
        # OpenClaw CRM assumes row 1 is headers, row 2 is first data row.
        # So row N corresponds to index N-2 in the records list (0-based).
        
        table_name = self._get_table_name(range_)
        headers = self._get_headers(table_name)
        
        # Extract row number from range
        try:
            # Example: "Pipeline!A5:U5" -> 5
            parts = range_.split("!")
            if len(parts) < 2:
                return SheetResult(success=False, data=None, error="Invalid range format")
            
            cell_range = parts[1]
            # Naive parsing: extract digits
            import re
            match = re.search(r"A(\d+):", cell_range)
            if not match:
                return SheetResult(success=False, data=None, error="Could not parse row number from range")
            
            row_num = int(match.group(1))
            record_index = row_num - 2 # Row 2 is index 0
            
            if record_index < 0:
                 return SheetResult(success=False, data=None, error="Invalid row number (must be >= 2)")

        except Exception as e:
            return SheetResult(success=False, data=None, error=f"Error parsing range: {str(e)}")

        if not values:
            return SheetResult(success=True, data={"updatedRows": 0})

        try:
            table = self.api.table(self.base_id, table_name)
            records = table.all() # Fetch all to find by index. Inefficient but necessary without ID.
            
            if record_index >= len(records):
                return SheetResult(success=False, data=None, error="Row index out of range")
                
            record_id = records[record_index]["id"]
            
            # Map values to fields
            row_values = values[0] # Usually updates one row at a time
            fields = {}
            for i, val in enumerate(row_values):
                if i < len(headers):
                    fields[headers[i]] = val
            
            table.update(record_id, fields)
            return SheetResult(success=True, data={"updatedRows": 1})
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

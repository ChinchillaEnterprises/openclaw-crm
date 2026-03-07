"""Airtable backend for OpenCRM."""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from airtable import airtable
except ImportError:
    airtable = None

from .sheets_backend import SheetsBackend, SheetResult


@dataclass
class AirtableConfig:
    """Configuration for Airtable backend."""
    base_id: str
    api_token: str
    table_name: str = "Pipeline"


class AirtableBackend(SheetsBackend):
    """Airtable backend implementation.
    
    Uses Airtable API as an alternative storage backend,
    allowing users to use Airtable instead of Google Sheets.
    """
    
    FIELD_MAPPING = {
        "name": "Name",
        "email": "Email", 
        "phone": "Phone",
        "company": "Company",
        "status": "Status",
        "notes": "Notes",
        "created_at": "Created",
        "updated_at": "Updated",
    }
    
    def __init__(self, config: AirtableConfig | None = None):
        if airtable is None:
            raise ImportError("pyairtable required. Install: pip install pyairtable")
        
        if config is None:
            config = AirtableConfig(
                base_id=os.environ.get("AIRTABLE_BASE_ID", ""),
                api_token=os.environ.get("AIRTABLE_API_TOKEN", ""),
                table_name=os.environ.get("AIRTABLE_TABLE_NAME", "Pipeline"),
            )
        
        if not config.base_id or not config.api_token:
            raise ValueError("Airtable base_id and api_token required")
        
        self.config = config
        self.client = airtable.Airtable(config.base_id, config.api_token)
    
    def _to_airtable_fields(self, values):
        records = []
        for row in values:
            fields = {}
            field_names = list(self.FIELD_MAPPING.values())
            for i, value in enumerate(row):
                if i < len(field_names):
                    fields[field_names[i]] = value
            if fields:
                records.append(fields)
        return records
    
    def _from_airtable_records(self, records):
        field_names = list(self.FIELD_MAPPING.values())
        result = [field_names]
        for record in records:
            fields = record.get("fields", {})
            row = [fields.get(fn, "") for fn in field_names]
            result.append(row)
        return result
    
    def read(self, spreadsheet_id=None, range_="A1:Z1000") -> SheetResult:
        try:
            records = self.client.get(self.config.table_name)
            all_records = records.get("records", [])
            while "offset" in records:
                records = self.client.get(self.config.table_name, params={"offset": records["offset"]})
                all_records.extend(records.get("records", []))
            data = self._from_airtable_records([{"fields": r.get("fields", {})} for r in all_records])
            return SheetResult(success=True, data=data)
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def append(self, spreadsheet_id=None, range_=None, values=None) -> SheetResult:
        if values is None:
            values = []
        try:
            records = self._to_airtable_fields(values)
            created = []
            for record in records:
                result = self.client.create(self.config.table_name, record)
                created.append(result.get("id", ""))
            return SheetResult(success=True, data={"created": len(created), "ids": created})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def update(self, spreadsheet_id=None, range_=None, values=None) -> SheetResult:
        if values is None or not values:
            return SheetResult(success=False, data=None, error="No values to update")
        try:
            updated = []
            field_names = list(self.FIELD_MAPPING.values())
            for row in values[1:]:
                if not row:
                    continue
                record_id = row[0]
                fields = {}
                for i in range(1, len(row)):
                    if i < len(field_names):
                        fields[field_names[i]] = row[i]
                if record_id and fields:
                    result = self.client.update(self.config.table_name, record_id, fields)
                    updated.append(result.get("id", ""))
            return SheetResult(success=True, data={"updated": len(updated), "ids": updated})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

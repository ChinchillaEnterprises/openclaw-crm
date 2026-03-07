"""Airtable backend for OpenCRM."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

try:
    from airtable import airtable
    from airtable.models import Field
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
    
    # Field mappings between Pipeline columns and Airtable fields
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
        """Initialize Airtable backend.
        
        Args:
            config: Airtable configuration. If None, reads from env vars:
                - AIRTABLE_BASE_ID
                - AIRTABLE_API_TOKEN
                - AIRTABLE_TABLE_NAME (default: "Pipeline")
        """
        if airtable is None:
            raise ImportError(
                "pyairtable is required for Airtable backend. "
                "Install with: pip install pyairtable"
            )
        
        if config is None:
            config = AirtableConfig(
                base_id=os.environ.get("AIRTABLE_BASE_ID", ""),
                api_token=os.environ.get("AIRTABLE_API_TOKEN", ""),
                table_name=os.environ.get("AIRTABLE_TABLE_NAME", "Pipeline"),
            )
        
        if not config.base_id or not config.api_token:
            raise ValueError(
                "Airtable base_id and api_token are required. "
                "Set via config or environment variables."
            )
        
        self.config = config
        self.client = airtable.Airtable(config.base_id, config.api_token)
    
    def _to_airtable_fields(self, values: list[list[str]]) -> list[dict]:
        """Convert sheet-like values to Airtable fields.
        
        Args:
            values: List of rows, each row is a list of cell values
            
        Returns:
            List of field dictionaries for Airtable records
        """
        records = []
        for row in values:
            fields = {}
            for i, value in enumerate(row):
                # Map column index to field name
                field_names = list(self.FIELD_MAPPING.values())
                if i < len(field_names):
                    fields[field_names[i]] = value
            if fields:
                records.append(fields)
        return records
    
    def _from_airtable_records(self, records: list[dict]) -> list[list[str]]:
        """Convert Airtable records to sheet-like values.
        
        Args:
            records: List of Airtable record dictionaries
            
        Returns:
            List of rows, each row is a list of cell values
        """
        field_names = list(self.FIELD_MAPPING.values())
        result = [field_names]  # Header row
        
        for record in records:
            fields = record.get("fields", {})
            row = [fields.get(fn, "") for fn in field_names]
            result.append(row)
        
        return result
    
    def read(self, spreadsheet_id: str = None, range_: str = "A1:Z1000") -> SheetResult:
        """Read records from Airtable.
        
        Args:
            spreadsheet_id: Ignored (use base_id from config)
            range_: Ignored (reads all records from table)
            
        Returns:
            SheetResult with data as list of rows
        """
        try:
            records = self.client.get(self.config.table_name)
            all_records = records.get("records", [])
            
            # Handle pagination
            while "offset" in records:
                records = self.client.get(
                    self.config.table_name,
                    params={"offset": records["offset"]}
                )
                all_records.extend(records.get("records", []))
            
            data = self._from_airtable_records([
                {"fields": r.get("fields", {})} for r in all_records
            ])
            
            return SheetResult(success=True, data=data)
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def append(self, spreadsheet_id: str = None, range_: str = None, 
               values: list[list[str]] = None) -> SheetResult:
        """Append records to Airtable.
        
        Args:
            spreadsheet_id: Ignored
            range_: Ignored
            values: List of rows to append
            
        Returns:
            SheetResult with created record IDs
        """
        if values is None:
            values = []
        
        try:
            records = self._to_airtable_fields(values)
            created = []
            
            for record in records:
                result = self.client.create(self.config.table_name, record)
                created.append(result.get("id", ""))
            
            return SheetResult(
                success=True,
                data={"created": len(created), "ids": created}
            )
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def update(self, spreadsheet_id: str = None, range_: str = None,
               values: list[list[str]] = None) -> SheetResult:
        """Update records in Airtable.
        
        Note: Airtable requires record IDs for updates. This method
        expects the first column to contain record IDs.
        
        Args:
            spreadsheet_id: Ignored
            range_: Ignored  
            values: List of rows, first column is record ID
            
        Returns:
            SheetResult with updated record info
        """
        if values is None or not values:
            return SheetResult(success=False, data=None, error="No values to update")
        
        try:
            updated = []
            
            # Skip header row
            for row in values[1:]:
                if not row:
                    continue
                
                record_id = row[0]
                fields = {}
                
                # Map remaining columns to fields
                field_names = list(self.FIELD_MAPPING.values())
                for i in range(1, len(row)):
                    if i < len(field_names):
                        fields[field_names[i]] = row[i]
                
                if record_id and fields:
                    result = self.client.update(self.config.table_name, record_id, fields)
                    updated.append(result.get("id", ""))
            
            return SheetResult(
                success=True,
                data={"updated": len(updated), "ids": updated}
            )
            
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))


# For backward compatibility
SheetsBackend = AirtableBackend

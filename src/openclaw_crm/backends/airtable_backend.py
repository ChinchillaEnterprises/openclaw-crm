"""Airtable backend for openclaw-crm."""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from pyairtable import Api
except ImportError:
    Api = None

from .sheets import SheetsBackend, SheetResult


@dataclass
class AirtableBackend(SheetsBackend):
    """Airtable backend implementation using pyairtable."""
    
    def __init__(self, api_key: str | None = None, base_id: str | None = None):
        if Api is None:
            raise ImportError("pyairtable is required. Install with: pip install pyairtable")
        
        self.api_key = api_key or os.environ.get("AIRTABLE_API_TOKEN")
        self.base_id = base_id or os.environ.get("AIRTABLE_BASE_ID")
        
        if not self.api_key or not self.base_id:
            raise ValueError("AIRTABLE_API_TOKEN and AIRTABLE_BASE_ID must be set")
        
        self.api = Api(self.api_key)
        self.table = self.api.table(self.base_id, "Pipeline")
    
    COLUMN_MAP = [
        "Client", "Contact", "Email", "Phone", "Budget", "Stage",
        "Probability", "Source", "Created", "Updated", "Notes",
        "Company", "Position", "Referral", "Competitor", "InvoiceDate",
        "InvoiceAmount", "InvoiceStatus", "LastContact", "NextAction", "Owner"
    ]
    
    def read(self, spreadsheet_id: str | None = None, range_: str | None = None) -> SheetResult:
        try:
            records = self.table.all()
            data = []
            for record in records:
                fields = record.get("fields", {})
                row = [fields.get(col, "") for col in self.COLUMN_MAP]
                data.append(row)
            return SheetResult(success=True, data={"values": data})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def append(self, spreadsheet_id: str | None = None, range_: str | None = None, 
               values: list[list[str]] | None = None) -> SheetResult:
        try:
            if not values or not values[0]:
                return SheetResult(success=False, data=None, error="No values")
            fields = {col: val for col, val in zip(self.COLUMN_MAP, values[0]) if val}
            record = self.table.create(fields)
            return SheetResult(success=True, data={"id": record.id})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))
    
    def update(self, spreadsheet_id: str | None = None, range_: str | None = None,
               values: list[list[str]] | None = None) -> SheetResult:
        try:
            if not values or not values[0]:
                return SheetResult(success=False, data=None, error="No values")
            record_id = values[0][0]
            fields = {col: val for col, val in zip(self.COLUMN_MAP, values[0][1:]) if val}
            record = self.table.update(record_id, fields)
            return SheetResult(success=True, data={"id": record.id})
        except Exception as e:
            return SheetResult(success=False, data=None, error=str(e))

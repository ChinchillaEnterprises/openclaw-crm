from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests

from openclaw_crm.sheets import SheetResult, SheetsBackend


@dataclass
class AirtableBackend(SheetsBackend):
    """Airtable backend implementation for CRM storage."""
    
    api_key: str = ""
    base_id: str = ""
    
    def __init__(self, api_key: str = None, base_id: str = None):
        self.api_key = api_key or os.environ.get("AIRTABLE_API_KEY", "")
        self.base_id = base_id or os.environ.get("AIRTABLE_BASE_ID", "")
    
    def _request(self, method: str, endpoint: str, data: dict = None) -> SheetResult:
        if not self.api_key:
            return SheetResult(success=False, data=None, error="AIRTABLE_API_KEY not set")
        if not self.base_id:
            return SheetResult(success=False, data=None, error="AIRTABLE_BASE_ID not set")
        
        url = f"https://api.airtable.com/v0/{self.base_id}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        
        try:
            response = requests.request(method, url, headers=headers, json=data, timeout=30)
            if response.status_code >= 400:
                return SheetResult(
                    success=False, 
                    data=None, 
                    error=f"Airtable API error: {response.status_code} - {response.text}"
                )
            return SheetResult(success=True, data=response.json())
        except requests.RequestException as e:
            return SheetResult(success=False, data=None, error=f"Request failed: {str(e)}")
    
    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        """Read records from an Airtable table.
        
        Args:
            spreadsheet_id: The table name in Airtable
            range_: Filter formula (e.g., "Name != ''")
        """
        params = {"maxRecords": 100}
        if range_:
            params["filterByFormula"] = range_
        
        return self._request("GET", spreadsheet_id, data=params)
    
    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Append records to an Airtable table.
        
        Args:
            spreadsheet_id: The table name
            range_: Not used (for interface compatibility)
            values: List of records where each record is a list of field values
        """
        if not values:
            return SheetResult(success=True, data={"records": []})
        
        # Convert values to Airtable format
        # Assuming first row is headers if provided
        fields = values[0] if len(values) > 0 else []
        records = [{"fields": dict(zip(fields, row))} for row in values[1:]] if len(values) > 1 else []
        
        return self._request("POST", spreadsheet_id, data={"records": records})
    
    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        """Update records in an Airtable table.
        
        Args:
            spreadsheet_id: The table name
            range_: Record ID to update
            values: Updated field values
        """
        if not values or not range_:
            return SheetResult(success=False, data=None, error="Record ID and values required")
        
        fields = values[0] if len(values) > 0 else {}
        record_id = range_
        
        return self._request("PATCH", spreadsheet_id, data={
            "records": [{"id": record_id, "fields": dict(zip(fields, values[1]))}]
        })

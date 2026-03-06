"""Backends for OpenCRM."""

from openclaw_crm.backends.airtable_backend import AirtableBackend
from openclaw_crm.backends.gspread_backend import GspreadBackend
from openclaw_crm.sheets import SheetResult, SheetsBackend

__all__ = ["AirtableBackend", "GspreadBackend", "SheetResult", "SheetsBackend"]

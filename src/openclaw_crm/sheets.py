from __future__ import annotations

import os
import json
import subprocess
from dataclasses import dataclass
from typing import Any


@dataclass
class SheetResult:
    success: bool
    data: Any
    error: str = ""


class SheetsBackend:
    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        raise NotImplementedError

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        raise NotImplementedError

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        raise NotImplementedError


class GWSBackend(SheetsBackend):
    def _run(self, args: list[str], timeout: int = 30) -> SheetResult:
        try:
            result = subprocess.run(
                ["gws"] + args,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if result.returncode != 0:
                return SheetResult(success=False, data=None, error=result.stderr.strip())
            data = json.loads(result.stdout) if result.stdout.strip() else {}
            return SheetResult(success=True, data=data)
        except FileNotFoundError:
            return SheetResult(success=False, data=None, error="gws CLI not installed — see README")
        except subprocess.TimeoutExpired:
            return SheetResult(success=False, data=None, error="gws command timed out")
        except json.JSONDecodeError:
            return SheetResult(success=True, data=result.stdout.strip())

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:
        return self._run([
            "sheets", "+read",
            "--spreadsheet", spreadsheet_id,
            "--range", range_,
        ])

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        return self._run([
            "sheets", "spreadsheets", "values", "append",
            "--params", json.dumps({
                "spreadsheetId": spreadsheet_id,
                "range": range_,
                "valueInputOption": "USER_ENTERED",
            }),
            "--json", json.dumps({"values": values}),
        ])

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
        return self._run([
            "sheets", "spreadsheets", "values", "update",
            "--params", json.dumps({
                "spreadsheetId": spreadsheet_id,
                "range": range_,
                "valueInputOption": "USER_ENTERED",
            }),
            "--json", json.dumps({"values": values}),
        ])


_backend: SheetsBackend | None = None


def set_backend(backend: SheetsBackend) -> None:
    global _backend
    _backend = backend


def get_backend() -> SheetsBackend:
    global _backend
    if _backend is None:
        if os.getenv("AIRTABLE_API_KEY") and os.getenv("AIRTABLE_BASE_ID"):
            try:
                from openclaw_crm.backends.airtable_backend import AirtableBackend
                _backend = AirtableBackend()
            except ImportError:
                # pyairtable not installed
                _backend = GWSBackend()
        else:
            _backend = GWSBackend()
    return _backend


def read_sheet(spreadsheet_id: str, range_: str) -> SheetResult:
    return get_backend().read(spreadsheet_id, range_)


def append_sheet(spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
    return get_backend().append(spreadsheet_id, range_, values)


def update_sheet(spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:
    return get_backend().update(spreadsheet_id, range_, values)

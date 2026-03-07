from __future__ import annotations

import os
from typing import Any

from openclaw_crm.config import load_config
from openclaw_crm.sheets import SheetResult, SheetsBackend


class AirtableBackend(SheetsBackend):
    """A SheetsBackend-compatible Airtable adapter.

    Expected range format follows existing code paths, e.g.:
      - "Pipeline!A:U"
      - "Pipeline!A2:U2"
    The tab name before `!` is treated as an Airtable table name.
    """

    DEFAULT_FIELD_MAP = {
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

    def __init__(
        self,
        *,
        api_token: str | None = None,
        base_id: str | None = None,
        field_map: dict[str, str] | None = None,
    ) -> None:
        cfg = load_config()
        airtable_cfg = cfg.get("airtable", {}) if isinstance(cfg, dict) else {}

        self.api_token = api_token or os.getenv("AIRTABLE_API_TOKEN") or airtable_cfg.get("api_token", "")
        self.base_id = base_id or os.getenv("AIRTABLE_BASE_ID") or airtable_cfg.get("base_id", "")
        self.field_map = {**self.DEFAULT_FIELD_MAP, **(field_map or airtable_cfg.get("field_map", {}))}

        self._table_cls = None

    def _validate_creds(self) -> str | None:
        if not self.api_token:
            return "Missing Airtable API token (AIRTABLE_API_TOKEN or airtable.api_token)"
        if not self.base_id:
            return "Missing Airtable base id (AIRTABLE_BASE_ID or airtable.base_id)"
        return None

    def _table_for_range(self, range_: str):
        err = self._validate_creds()
        if err:
            raise ValueError(err)

        table_name = range_.split("!", 1)[0].strip().strip("'")
        if not table_name:
            raise ValueError(f"Invalid range '{range_}': missing table name")

        if self._table_cls is None:
            try:
                from pyairtable import Table  # type: ignore
            except ImportError as exc:
                raise RuntimeError("pyairtable is not installed. Install with openclaw-crm[airtable]") from exc
            self._table_cls = Table

        return self._table_cls(self.api_token, self.base_id, table_name)

    def _row_to_record(self, headers: list[str], row: list[str]) -> dict[str, Any]:
        padded = row + [""] * (len(headers) - len(row))
        out: dict[str, Any] = {}
        for idx, header in enumerate(headers):
            field_name = self.field_map.get(header, header)
            out[field_name] = padded[idx]
        return out

    def _record_to_row(self, headers: list[str], fields: dict[str, Any]) -> list[str]:
        row: list[str] = []
        for header in headers:
            field_name = self.field_map.get(header, header)
            value = fields.get(field_name, "")
            row.append("" if value is None else str(value))
        return row

    def read(self, spreadsheet_id: str, range_: str) -> SheetResult:  # noqa: ARG002
        try:
            table = self._table_for_range(range_)
            records = table.all()

            headers = list(self.DEFAULT_FIELD_MAP.keys())
            rows = [headers]
            for rec in records:
                rows.append(self._record_to_row(headers, rec.get("fields", {})))

            return SheetResult(success=True, data={"values": rows})
        except Exception as exc:
            return SheetResult(success=False, data=None, error=str(exc))

    def append(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:  # noqa: ARG002
        try:
            table = self._table_for_range(range_)
            if not values:
                return SheetResult(success=True, data={"created": []})

            headers = list(self.DEFAULT_FIELD_MAP.keys())
            created = []
            for row in values:
                payload = {"fields": self._row_to_record(headers, row)}
                created.append(table.create(payload["fields"]))

            return SheetResult(success=True, data={"created": created})
        except Exception as exc:
            return SheetResult(success=False, data=None, error=str(exc))

    def update(self, spreadsheet_id: str, range_: str, values: list[list[str]]) -> SheetResult:  # noqa: ARG002
        """Update first N records in table for compatibility with row updates.

        Existing code updates one row at a time. We map that behavior by:
        1) fetching records in current order,
        2) updating first len(values) records.
        """
        try:
            table = self._table_for_range(range_)
            if not values:
                return SheetResult(success=True, data={"updated": []})

            headers = list(self.DEFAULT_FIELD_MAP.keys())
            records = table.all()
            if len(records) < len(values):
                return SheetResult(
                    success=False,
                    data=None,
                    error=f"Not enough records to update: have {len(records)}, need {len(values)}",
                )

            updated = []
            for idx, row in enumerate(values):
                rec_id = records[idx]["id"]
                payload = self._row_to_record(headers, row)
                updated.append(table.update(rec_id, payload))

            return SheetResult(success=True, data={"updated": updated})
        except Exception as exc:
            return SheetResult(success=False, data=None, error=str(exc))

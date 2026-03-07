from __future__ import annotations

from openclaw_crm import pipeline
from openclaw_crm.sheets import SheetResult


def test_parse_rows_handles_short_rows():
    result = SheetResult(
        success=True,
        data={"values": [["Client", "Stage", "Budget"], ["Acme", "lead"], ["Globex", "won", "$1000"]]},
    )
    rows = pipeline._parse_rows(result)
    assert rows[0]["Budget"] == ""
    assert rows[1]["Client"] == "Globex"


def test_get_pipeline_summary_counts_and_weighted_value(monkeypatch):
    sample = SheetResult(
        success=True,
        data={
            "values": [
                ["Client", "Stage", "Budget", "Last Contact", "Referred By"],
                ["A", "lead", "1000", "2026-03-01", ""],
                ["B", "proposal", "$2000", "2026-02-01", "RefX"],
                ["C", "won", "3000", "2026-03-01", ""],
            ]
        },
    )

    monkeypatch.setattr(pipeline, "get_spreadsheet_id", lambda: "sid")
    monkeypatch.setattr(pipeline, "read_sheet", lambda *_: sample)

    summary = pipeline.get_pipeline_summary()
    assert summary["total_deals"] == 2
    assert summary["won_deals"] == 1
    assert summary["network_count"] == 1
    assert summary["top_referrer"] == "RefX"
    assert summary["total_weighted_value"] == 1100.0


def test_move_stage_updates_matching_client(monkeypatch):
    rows = SheetResult(
        success=True,
        data={
            "values": [
                ["Client", "Stage", "Last Contact"],
                ["Acme", "lead", "2026-03-01"],
            ]
        },
    )
    updates: list[tuple[str, list[list[str]]]] = []

    monkeypatch.setattr(pipeline, "get_spreadsheet_id", lambda: "sid")
    monkeypatch.setattr(pipeline, "read_sheet", lambda *_: rows)
    monkeypatch.setattr(pipeline, "update_sheet", lambda _sid, rng, vals: updates.append((rng, vals)) or SheetResult(True, {}))

    out = pipeline.move_stage("Acme", "proposal")
    assert out["ok"] is True
    assert updates
    assert updates[0][0] == "Pipeline!A2:U2"
    assert updates[0][1][0][1] == "proposal"

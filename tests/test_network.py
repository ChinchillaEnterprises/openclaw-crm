from __future__ import annotations

from openclaw_crm import network
from openclaw_crm.sheets import SheetResult


def test_get_pending_signals_filters_new(monkeypatch):
    data = SheetResult(
        success=True,
        data={
            "values": [
                ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"],
                ["2026-03-01", "X", "tg", "hello", "Acme", "new"],
                ["2026-03-01", "Y", "tg", "old", "Globex", "dismissed"],
            ]
        },
    )
    monkeypatch.setattr(network, "get_spreadsheet_id", lambda: "sid")
    monkeypatch.setattr(network, "read_sheet", lambda *_: data)

    pending = network.get_pending_signals()
    assert len(pending) == 1
    assert pending[0]["Mentioned Company"] == "Acme"


def test_get_network_value_aggregates_direct_and_network(monkeypatch):
    pipeline_rows = SheetResult(
        success=True,
        data={
            "values": [
                ["Client", "Budget", "Network Parent", "Referred By"],
                ["Acme", "1000", "", ""],
                ["Client2", "$500", "Acme", ""],
                ["Client3", "250", "", "Acme"],
            ]
        },
    )
    monkeypatch.setattr(network, "get_spreadsheet_id", lambda: "sid")
    monkeypatch.setattr(network, "read_sheet", lambda *_: pipeline_rows)

    out = network.get_network_value("acme")
    assert out["direct_value"] == 1000.0
    assert out["network_value"] == 750.0
    assert out["total"] == 1750.0


def test_check_competitor_guard_rejects_active_duplicate(monkeypatch):
    pipeline_rows = SheetResult(
        success=True,
        data={
            "values": [
                ["Client", "Stage"],
                ["Acme", "proposal"],
            ]
        },
    )
    client_rows = SheetResult(
        success=True,
        data={
            "values": [
                ["Client", "Status"],
                ["Globex", "active"],
            ]
        },
    )

    def fake_read(_sid, rng):
        return client_rows if rng.startswith("Clients!") else pipeline_rows

    monkeypatch.setattr(network, "get_spreadsheet_id", lambda: "sid")
    monkeypatch.setattr(network, "read_sheet", fake_read)

    assert network.check_competitor_guard("Acme", "X") is False
    assert network.check_competitor_guard("NewCo", "X") is True

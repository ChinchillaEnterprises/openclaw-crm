from __future__ import annotations

import pytest
from datetime import date, timedelta
from openclaw_crm.pipeline import (
    get_pipeline,
    create_deal,
    move_stage,
    get_pipeline_summary,
    get_stale_deals,
    HEADERS
)

@pytest.fixture
def setup_pipeline_data(mock_backend):
    # Setup initial data for pipeline tests
    mock_backend.sheets["Pipeline"] = [
        HEADERS, # Headers
        ["Client A", "Contact A", "upwork", "lead", "1000", "fixed", "Development", date.today().isoformat(), date.today().isoformat(), "Review lead", "", "", "", "", "Owner A", "", "formula", "", "", "", ""],
        ["Client B", "Contact B", "upwork", "won", "5000", "fixed", "Maintenance", date.today().isoformat(), date.today().isoformat(), "None", "", "", "", "", "Owner B", "", "formula", "", "", "", ""],
        ["Client C", "Contact C", "network", "proposal", "2000", "fixed", "Design", date.today().isoformat(), (date.today() - timedelta(days=10)).isoformat(), "Follow up", "", "", "", "", "Owner C", "", "formula", "Referrer A", "Referrer A", "Notes", date.today().isoformat()],
    ]
    return mock_backend

def test_get_pipeline_active_only(setup_pipeline_data):
    deals = get_pipeline(active_only=True)
    assert len(deals) == 2
    assert "Client A" in [d["Client"] for d in deals]
    assert "Client C" in [d["Client"] for d in deals]
    assert "Client B" not in [d["Client"] for d in deals]

def test_get_pipeline_all(setup_pipeline_data):
    deals = get_pipeline(active_only=False)
    assert len(deals) == 3

def test_create_deal(mock_backend):
    # Ensure starting with at least headers if MockBackend is clean
    if "Pipeline" not in mock_backend.sheets:
        mock_backend.sheets["Pipeline"] = [HEADERS]
    
    deal_data = {
        "client": "Client D",
        "contact": "Contact D",
        "source": "upwork",
        "stage": "lead",
        "budget": "1500",
        "rate_type": "fixed",
        "service": "Consulting"
    }
    result = create_deal(deal_data)
    assert result["ok"] is True
    assert result["client"] == "Client D"
    
    deals = get_pipeline(active_only=False)
    assert any(d["Client"] == "Client D" for d in deals)
    
    # Check if network source is automatically set if referred_by is present
    deal_data_network = {
        "client": "Client E",
        "referred_by": "Referrer B"
    }
    create_deal(deal_data_network)
    deals = get_pipeline(active_only=False)
    deal_e = next(d for d in deals if d["Client"] == "Client E")
    assert deal_e["Source"] == "network"

def test_move_stage(setup_pipeline_data):
    result = move_stage("Client A", "proposal")
    assert result["ok"] is True
    assert result["stage"] == "proposal"
    
    deals = get_pipeline(active_only=True)
    deal_a = next(d for d in deals if d["Client"] == "Client A")
    assert deal_a["Stage"] == "proposal"

def test_move_stage_not_found(setup_pipeline_data):
    result = move_stage("NonExistent", "won")
    assert result["ok"] is False
    assert "not found" in result["error"].lower()

def test_get_pipeline_summary(setup_pipeline_data):
    summary = get_pipeline_summary()
    assert summary["total_deals"] == 2 # Only active deals: Client A (lead), Client C (proposal)
    assert summary["won_deals"] == 1 # Client B is won
    assert summary["by_stage"]["lead"] == 1
    assert summary["by_stage"]["proposal"] == 1
    # Client A: 1000 * 0.10 = 100
    # Client C: 2000 * 0.50 = 1000
    # Total weighted: 1100
    assert summary["total_weighted_value"] == 1100.0
    assert summary["stale_count"] == 1 # Client C is 10 days since last contact
    assert summary["network_count"] == 1 # Client C is network
    assert summary["top_referrer"] == "Referrer A"

def test_get_stale_deals(setup_pipeline_data):
    # Client C is 10 days stale
    stale = get_stale_deals([7, 14])
    assert len(stale[7]) == 1
    assert stale[7][0]["Client"] == "Client C"
    assert len(stale[14]) == 0

def test_edge_cases_empty_sheet(mock_backend):
    mock_backend.sheets["Pipeline"] = []
    deals = get_pipeline()
    assert len(deals) == 0
    
    summary = get_pipeline_summary()
    assert summary["total_deals"] == 0
    assert summary["total_weighted_value"] == 0

def test_edge_cases_only_headers(mock_backend):
    mock_backend.sheets["Pipeline"] = [HEADERS]
    deals = get_pipeline()
    assert len(deals) == 0
    
    summary = get_pipeline_summary()
    assert summary["total_deals"] == 0

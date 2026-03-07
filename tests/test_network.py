import pytest
from datetime import datetime
from openclaw_crm.network import (
    add_signal, promote_signal, dismiss_signal, 
    get_network_tree, get_network_value, check_competitor_guard,
    SIGNAL_HEADERS
)
from openclaw_crm.pipeline import HEADERS as PIPELINE_HEADERS

@pytest.fixture
def setup_sheets(mock_backend):
    # Setup headers for required sheets
    mock_backend.data["Network Signals"] = [SIGNAL_HEADERS]
    mock_backend.data["Pipeline"] = [PIPELINE_HEADERS]
    mock_backend.data["Clients"] = [["Client", "Status", "Other"]]
    return mock_backend

def test_add_signal(setup_sheets):
    signal = {
        "source_client": "Client A",
        "channel": "slack",
        "signal_text": "Looking for CRM",
        "mentioned_company": "Target Co"
    }
    res = add_signal(signal)
    assert res["ok"] is True
    assert res["status"] == "new"
    
    signals = setup_sheets.data["Network Signals"]
    assert len(signals) == 2
    assert signals[1][1] == "Client A"
    assert signals[1][4] == "Target Co"
    assert signals[1][5] == "new"

def test_promote_signal(setup_sheets):
    # Add a signal first
    setup_sheets.data["Network Signals"].append([
        "2026-03-07T10:00:00", "Client A", "slack", "Need help", "Target Co", "new"
    ])
    
    res = promote_signal(2) # Row 2 (first data row)
    assert res["ok"] is True
    assert res["deal"]["client"] == "Target Co"
    
    # Check signal status updated
    assert setup_sheets.data["Network Signals"][1][5] == "promoted"
    
    # Check deal created in Pipeline
    pipeline = setup_sheets.data["Pipeline"]
    assert len(pipeline) == 2
    assert pipeline[1][0] == "Target Co"
    assert pipeline[1][17] == "Client A" # Referred By

def test_promote_signal_already_promoted(setup_sheets):
    setup_sheets.data["Network Signals"].append([
        "2026-03-07T10:00:00", "Client A", "slack", "Need help", "Target Co", "promoted"
    ])
    
    res = promote_signal(2)
    assert res["ok"] is False
    assert "already promoted" in res["error"]

def test_dismiss_signal(setup_sheets):
    setup_sheets.data["Network Signals"].append([
        "2026-03-07T10:00:00", "Client A", "slack", "Need help", "Target Co", "new"
    ])
    
    res = dismiss_signal(2)
    assert res["ok"] is True
    assert setup_sheets.data["Network Signals"][1][5] == "dismissed"

def test_get_network_tree(setup_sheets):
    setup_sheets.data["Pipeline"].extend([
        ["Child 1", "", "network", "lead", "$1000", "", "", "", "", "", "", "", "", "", "", "", "", "Parent A", "Parent A", "", ""],
        ["Child 2", "", "network", "lead", "$2000", "", "", "", "", "", "", "", "", "", "", "", "", "Parent A", "Parent A", "", ""],
        ["Other", "", "direct", "lead", "$500", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    ])
    
    tree = get_network_tree()
    assert "Parent A" in tree
    assert len(tree["Parent A"]) == 2
    assert tree["Parent A"][0]["client"] == "Child 1"
    
    tree_single = get_network_tree("Parent A")
    assert list(tree_single.keys()) == ["Parent A"]
    assert len(tree_single["Parent A"]) == 2

def test_get_network_value(setup_sheets):
    setup_sheets.data["Pipeline"].extend([
        ["Client A", "", "direct", "won", "$5000", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["Referral 1", "", "network", "lead", "$1000", "", "", "", "", "", "", "", "", "", "", "", "", "Client A", "Client A", "", ""],
    ])
    
    val = get_network_value("Client A")
    assert val["direct_value"] == 5000.0
    assert val["network_value"] == 1000.0
    assert val["total"] == 6000.0

def test_check_competitor_guard(setup_sheets):
    # Add existing won client to Pipeline
    setup_sheets.data["Pipeline"].append(
        ["Existing Co", "", "", "won", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    )
    # Add active client to Clients sheet
    setup_sheets.data["Clients"].append(
        ["Active Co", "active", ""]
    )
    
    # Existing in Pipeline
    assert check_competitor_guard("Existing Co", "Some Source") is False
    # Existing in Clients
    assert check_competitor_guard("Active Co", "Some Source") is False
    # New company
    assert check_competitor_guard("New Co", "Some Source") is True
    
    # Case insensitive
    assert check_competitor_guard("existing co", "Some Source") is False

def test_promote_signal_atomic_order(setup_sheets, monkeypatch):
    """Verify that deal is created before signal is marked as promoted."""
    setup_sheets.data["Network Signals"].append([
        "2026-03-07T10:00:00", "Client A", "slack", "Need help", "Target Co", "new"
    ])
    
    order = []
    
    # Use monkeypatch to wrap the functions in openclaw_crm.network
    import openclaw_crm.network
    
    original_create_deal = openclaw_crm.network.create_deal
    original_update_sheet = openclaw_crm.network.update_sheet

    def mock_create_deal(deal):
        order.append("create_deal")
        return original_create_deal(deal)
    
    def mock_update_sheet(sid, range_, values):
        order.append("update_sheet")
        return original_update_sheet(sid, range_, values)

    monkeypatch.setattr(openclaw_crm.network, "create_deal", mock_create_deal)
    monkeypatch.setattr(openclaw_crm.network, "update_sheet", mock_update_sheet)
    
    promote_signal(2)
    
    assert order == ["create_deal", "update_sheet"]

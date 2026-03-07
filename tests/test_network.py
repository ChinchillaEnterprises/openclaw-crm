from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.network import (
    add_signal,
    get_pending_signals,
    _get_all_signals,
    promote_signal,
    dismiss_signal,
    get_network_tree,
    get_network_value,
    check_competitor_guard,
)


@pytest.fixture
def mock_spreadsheet_id():
    """Mock spreadsheet ID."""
    return "test_spreadsheet_id"


@pytest.fixture
def sample_signal_headers():
    """Sample signal headers."""
    return ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"]


@pytest.fixture
def sample_signals(sample_signal_headers):
    """Sample signals data."""
    return [
        sample_signal_headers,
        ["2024-01-01T10:00:00", "Acme", "slack", "Need help with mobile app", "TechCorp", "new"],
        ["2024-01-02T11:00:00", "Acme", "email", "Looking for cloud migration", "CloudXYZ", "new"],
        ["2024-01-03T12:00:00", "TechCorp", "slack", "Need database help", "DataInc", "promoted"],
        ["2024-01-04T13:00:00", "StartupXYZ", "email", "Not interested", "NoGood", "dismissed"],
    ]


@pytest.fixture
def sample_pipeline_headers():
    """Sample pipeline headers."""
    return [
        "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
        "Service", "First Contact", "Last Contact", "Next Action",
        "Due Date", "Notes", "Slack Channel", "Proposal Link",
        "Owner", "Upwork URL", "Probability",
        "Referred By", "Network Parent", "Network Notes", "Signal Date",
    ]


@pytest.fixture
def sample_pipeline(sample_pipeline_headers):
    """Sample pipeline data."""
    return [
        sample_pipeline_headers,
        ["Acme", "John Doe", "upwork", "lead", "15000", "fixed", "Web Dev", "2024-01-01", "2024-01-05", "Email", "", "", "", "", "", "", "", "", "", "", ""],
        ["TechCorp", "Jane Smith", "network", "proposal", "25000", "hourly", "Mobile App", "2024-01-02", "2024-01-10", "Call", "", "", "", "", "", "", "", "", "Acme", "Acme", "referral", "2024-01-10"],
        ["CloudXYZ", "Bob Wilson", "network", "lead", "18000", "retainer", "Cloud Migration", "2024-01-03", "2024-01-11", "Meeting", "", "", "", "", "", "", "", "", "Acme", "Acme", "referral", "2024-01-11"],
        ["DataInc", "Alice Brown", "network", "negotiation", "40000", "fixed", "Database", "2024-01-04", "2024-01-12", "Review", "", "", "", "", "", "", "", "", "TechCorp", "TechCorp", "referral", "2024-01-12"],
        ["StartupXYZ", "Charlie", "inbound", "won", "50000", "retainer", "Consulting", "2024-01-05", "2024-01-15", "Meeting", "", "", "", "", "", "", "", "", "", "", ""],
    ]


class TestAddSignal:
    """Test add_signal function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.append_sheet")
    def test_add_signal_basic(self, mock_append, mock_sid):
        """Test adding a basic signal."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=True)

        signal_data = {
            "source_client": "Acme",
            "channel": "slack",
            "signal_text": "Need help with mobile app",
            "mentioned_company": "TechCorp",
        }

        result = add_signal(signal_data)

        assert result["ok"] is True
        assert result["status"] == "new"
        mock_append.assert_called_once()

        # Check that row was created correctly
        call_args = mock_append.call_args
        assert call_args[0][2][0][1] == "Acme"  # Source Client
        assert call_args[0][2][0][2] == "slack"  # Channel
        assert call_args[0][2][0][3] == "Need help with mobile app"  # Signal Text
        assert call_args[0][2][0][4] == "TechCorp"  # Mentioned Company
        assert call_args[0][2][0][5] == "new"  # Status

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.append_sheet")
    def test_add_signal_with_timestamp(self, mock_append, mock_sid):
        """Test adding signal with custom timestamp."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=True)

        custom_timestamp = "2024-01-01T12:00:00"
        signal_data = {
            "timestamp": custom_timestamp,
            "source_client": "Acme",
            "signal_text": "Test signal",
        }

        add_signal(signal_data)

        # Check that custom timestamp was used
        call_args = mock_append.call_args
        assert call_args[0][2][0][0] == custom_timestamp

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.append_sheet")
    def test_add_signal_failure(self, mock_append, mock_sid):
        """Test adding signal when append fails."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=False, error="API error")

        signal_data = {"source_client": "Acme", "signal_text": "Test"}

        result = add_signal(signal_data)

        assert result["ok"] is False


class TestGetPendingSignals:
    """Test get_pending_signals function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_pending_signals(self, mock_read, mock_sid, sample_signals):
        """Test getting pending signals."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})

        pending = get_pending_signals()

        assert len(pending) == 2
        assert all(s["Status"].lower() == "new" for s in pending)
        assert pending[0]["Source Client"] == "Acme"
        assert pending[1]["Source Client"] == "Acme"

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_pending_signals_empty(self, mock_read, mock_sid):
        """Test getting pending signals when none exist."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": []})

        pending = get_pending_signals()

        assert pending == []

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_pending_signals_case_insensitive(self, mock_read, mock_sid, sample_signals):
        """Test that status matching is case-insensitive."""
        # Create signals with different case for status
        signals_with_cases = sample_signals[:2] + [
            sample_signals[0][:5] + ["NEW"],
            sample_signals[0][:5] + ["New"],
        ]

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": signals_with_cases})
        mock_sid.return_value = "test_id"

        pending = get_pending_signals()

        assert len(pending) == 4  # All should match regardless of case


class TestGetAllSignals:
    """Test _get_all_signals function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_all_signals_success(self, mock_read, mock_sid, sample_signals):
        """Test getting all signals successfully."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})

        data, headers = _get_all_signals()

        assert len(data) == 4
        assert len(headers) == 6
        assert headers[0] == "Timestamp"
        assert headers[5] == "Status"

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_all_signals_empty(self, mock_read, mock_sid):
        """Test getting all signals when empty."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": []})

        data, headers = _get_all_signals()

        assert data == []
        assert headers == ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"]

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_all_signals_unsuccessful(self, mock_read, mock_sid):
        """Test getting all signals when read fails."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=False, data=None)

        data, headers = _get_all_signals()

        assert data == []
        assert headers == []


class TestPromoteSignal:
    """Test promote_signal function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    @patch("openclaw_crm.network.update_sheet")
    @patch("openclaw_crm.network.create_deal")
    def test_promote_signal_success(self, mock_create_deal, mock_update, mock_read, mock_sid, sample_signals):
        """Test promoting a signal successfully."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})
        mock_update.return_value = SheetResult(success=True)
        mock_create_deal.return_value = {"ok": True, "row": 5}

        result = promote_signal(2)

        assert result["ok"] is True
        assert result["signal_row"] == 2
        assert result["deal"]["ok"] is True
        mock_create_deal.assert_called_once()
        mock_update.assert_called_once()

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    @patch("openclaw_crm.network.update_sheet")
    @patch("openclaw_crm.network.create_deal")
    def test_promote_signal_with_overrides(self, mock_create_deal, mock_update, mock_read, mock_sid, sample_signals):
        """Test promoting signal with deal overrides."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})
        mock_update.return_value = SheetResult(success=True)
        mock_create_deal.return_value = {"ok": True, "row": 5}

        overrides = {"stage": "qualifying", "budget": "20000"}
        result = promote_signal(2, overrides)

        assert result["ok"] is True

        # Check that overrides were passed to create_deal
        call_args = mock_create_deal.call_args
        assert call_args[0][0]["stage"] == "qualifying"
        assert call_args[0][0]["budget"] == "20000"

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_promote_signal_out_of_range(self, mock_read, mock_sid, sample_signals):
        """Test promoting signal with invalid row."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})

        result = promote_signal(100)

        assert result["ok"] is False
        assert "out of range" in result["error"]

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_promote_signal_already_promoted(self, mock_read, mock_sid, sample_signals):
        """Test promoting an already promoted signal."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})

        result = promote_signal(4)  # Row 4 is already promoted

        assert result["ok"] is False
        assert "already promoted" in result["error"]

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    @patch("openclaw_crm.network.update_sheet")
    @patch("openclaw_crm.network.create_deal")
    def test_promote_signal_deal_creation_fails(self, mock_create_deal, mock_update, mock_read, mock_sid, sample_signals):
        """Test promoting signal when deal creation fails."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})
        mock_create_deal.return_value = {"ok": False, "error": "Failed"}

        result = promote_signal(2)

        assert result["ok"] is False
        assert "Deal creation failed" in result["error"]
        mock_update.assert_not_called()


class TestDismissSignal:
    """Test dismiss_signal function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    @patch("openclaw_crm.network.update_sheet")
    def test_dismiss_signal_success(self, mock_update, mock_read, mock_sid, sample_signals):
        """Test dismissing a signal successfully."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})
        mock_update.return_value = SheetResult(success=True)

        result = dismiss_signal(2)

        assert result["ok"] is True
        mock_update.assert_called_once()

        # Check that status was updated to "dismissed"
        call_args = mock_update.call_args
        updated_row = call_args[0][2][0]
        assert updated_row[5] == "dismissed"  # Status column

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_dismiss_signal_out_of_range(self, mock_read, mock_sid, sample_signals):
        """Test dismissing signal with invalid row."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})

        result = dismiss_signal(100)

        assert result["ok"] is False
        assert "out of range" in result["error"]

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    @patch("openclaw_crm.network.update_sheet")
    def test_dismiss_signal_failure(self, mock_update, mock_read, mock_sid, sample_signals):
        """Test dismissing signal when update fails."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_signals})
        mock_update.return_value = SheetResult(success=False, error="API error")

        result = dismiss_signal(2)

        assert result["ok"] is False


class TestGetNetworkTree:
    """Test get_network_tree function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_tree_all(self, mock_read, mock_sid, sample_pipeline):
        """Test getting complete network tree."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        tree = get_network_tree()

        assert "Acme" in tree
        assert "TechCorp" in tree

        # Check Acme's referrals
        acme_refs = tree["Acme"]
        assert len(acme_refs) == 2
        assert any(r["client"] == "TechCorp" for r in acme_refs)
        assert any(r["client"] == "CloudXYZ" for r in acme_refs)

        # Check TechCorp's referrals
        tech_refs = tree["TechCorp"]
        assert len(tech_refs) == 1
        assert tech_refs[0]["client"] == "DataInc"

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_tree_root(self, mock_read, mock_sid, sample_pipeline):
        """Test getting network tree for specific root."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        tree = get_network_tree(root="Acme")

        assert "Acme" in tree
        assert len(tree["Acme"]) == 2
        assert "TechCorp" not in tree

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_tree_case_insensitive(self, mock_read, mock_sid, sample_pipeline):
        """Test that root matching is case-insensitive."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        tree = get_network_tree(root="acme")

        assert "Acme" in tree  # Returns with original case
        assert len(tree["Acme"]) == 2

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_tree_empty(self, mock_read, mock_sid):
        """Test getting network tree when empty."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": []})

        tree = get_network_tree()

        assert tree == {}


class TestGetNetworkValue:
    """Test get_network_value function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_value_with_referrals(self, mock_read, mock_sid, sample_pipeline):
        """Test getting network value for client with referrals."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        value = get_network_value("Acme")

        assert value["client"] == "Acme"
        # Direct: 15000 (Acme itself)
        # Network: 25000 (TechCorp) + 18000 (CloudXYZ) = 43000
        # Total: 58000
        # However, looking at the code more carefully:
        # - Direct: sum of budgets where Client == "Acme" = 15000
        # - Network: sum where Network Parent == "Acme" = 25000 + 18000 = 43000
        assert value["direct_value"] == 15000
        assert value["network_value"] == 43000
        assert value["total"] == 58000

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_value_with_indirect_referrals(self, mock_read, mock_sid, sample_pipeline):
        """Test getting network value including indirect referrals."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        value = get_network_value("TechCorp")

        # TechCorp: Direct = 25000
        # Network (referred by TechCorp): DataInc = 40000
        # Total = 65000
        assert value["direct_value"] == 25000
        assert value["network_value"] == 40000
        assert value["total"] == 65000

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_value_no_referrals(self, mock_read, mock_sid, sample_pipeline):
        """Test getting network value for client with no referrals."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        value = get_network_value("StartupXYZ")

        # StartupXYZ has direct value but no referrals
        assert value["direct_value"] == 50000
        assert value["network_value"] == 0
        assert value["total"] == 50000

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_value_case_insensitive(self, mock_read, mock_sid, sample_pipeline):
        """Test that client matching is case-insensitive."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.return_value = SheetResult(success=True, data={"values": sample_pipeline})

        value = get_network_value("acme")

        assert value["client"] == "acme"  # Preserves input case
        assert value["total"] == 58000

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_value_invalid_budget(self, mock_read, mock_sid, sample_pipeline):
        """Test handling invalid budget values."""
        mock_sid.return_value = "test_id"

        # Add a deal with invalid budget
        from openclaw_crm.sheets import SheetResult

        pipeline_with_invalid = sample_pipeline[:3] + [
            sample_pipeline[0][:4] + ["invalid", "fixed", "Service", "2024-01-01", "2024-01-01", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
        ]
        mock_read.return_value = SheetResult(success=True, data={"values": pipeline_with_invalid})

        value = get_network_value("Acme")

        # Should handle invalid budget gracefully
        assert value["direct_value"] >= 0
        assert value["network_value"] >= 0


class TestCheckCompetitorGuard:
    """Test check_competitor_guard function."""

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_safe(self, mock_read, mock_sid, sample_pipeline):
        """Test that safe company passes competitor guard."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        # First call for pipeline, second for clients (which returns empty)
        mock_read.side_effect = [
            SheetResult(success=True, data={"values": sample_pipeline}),
            SheetResult(success=True, data={"values": []}),
        ]

        is_safe = check_competitor_guard("NewCompany", "Acme")

        assert is_safe is True

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_existing_client(self, mock_read, mock_sid, sample_pipeline):
        """Test that existing client fails competitor guard."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.side_effect = [
            SheetResult(success=True, data={"values": sample_pipeline}),
            SheetResult(success=True, data={"values": []}),
        ]

        is_safe = check_competitor_guard("Acme", "TechCorp")

        assert is_safe is False

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_case_insensitive(self, mock_read, mock_sid, sample_pipeline):
        """Test that matching is case-insensitive."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult
        mock_read.side_effect = [
            SheetResult(success=True, data={"values": sample_pipeline}),
            SheetResult(success=True, data={"values": []}),
        ]

        is_safe = check_competitor_guard("acme", "TechCorp")

        assert is_safe is False

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_ignores_lost_deals(self, mock_read, mock_sid, sample_pipeline):
        """Test that lost deals are ignored in competitor check."""
        # Modify pipeline to have a lost deal
        pipeline_with_lost = sample_pipeline + [
            sample_pipeline[0][:4] + ["LostCorp", "contact", "inbound", "lost", "10000", "fixed", "Service", "2024-01-01", "2024-01-01", "", "", "", "", "", "", "", "", "", "", "", ""]
        ]

        from openclaw_crm.sheets import SheetResult
        mock_read.side_effect = [
            SheetResult(success=True, data={"values": pipeline_with_lost}),
            SheetResult(success=True, data={"values": []}),
        ]
        mock_sid.return_value = "test_id"

        # LostCorp should not trigger competitor guard
        is_safe = check_competitor_guard("LostCorp", "Acme")

        assert is_safe is True

    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_with_active_clients(self, mock_read, mock_sid, sample_pipeline):
        """Test that active clients from Clients sheet are checked."""
        clients_data = [
            ["Client", "Contact", "Status", "Budget", "Notes"],
            ["ExistingClient", "John", "active", "50000", "Notes"],
            ["PausedClient", "Jane", "paused", "30000", "Notes"],
            ["LostClient", "Bob", "inactive", "20000", "Notes"],
        ]

        from openclaw_crm.sheets import SheetResult
        mock_read.side_effect = [
            SheetResult(success=True, data={"values": sample_pipeline}),
            SheetResult(success=True, data={"values": clients_data}),
        ]
        mock_sid.return_value = "test_id"

        # Should fail for active client
        assert check_competitor_guard("ExistingClient", "Acme") is False

        # Should fail for paused client
        assert check_competitor_guard("PausedClient", "Acme") is False

        # Should pass for inactive client
        assert check_competitor_guard("LostClient", "Acme") is True

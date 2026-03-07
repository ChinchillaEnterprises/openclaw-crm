"""Unit tests for network.py"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

import sys
sys.path.insert(0, 'src')

from openclaw_crm import network


@pytest.fixture
def mock_sheet_result():
    """Create a mock sheet result."""
    def _make(success=True, data=None):
        result = MagicMock()
        result.success = success
        result.data = data or {}
        return result
    return _make


@pytest.fixture
def mock_config():
    """Mock config to return test spreadsheet ID."""
    with patch('openclaw_crm.network.get_spreadsheet_id', return_value='test-sheet-id'):
        yield


@pytest.fixture
def sample_signals_data():
    """Sample network signals data."""
    return {
        "values": [
            ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"],
            ["2026-03-01T10:00:00", "Acme Corp", "Slack", "Looking for help with React", "Tech Startup", "new"],
            ["2026-03-02T11:00:00", "Big Co", "Email", "Needs consulting", "Small Biz", "promoted"],
        ]
    }


@pytest.fixture
def sample_pipeline_data():
    """Sample pipeline data for network tests."""
    return {
        "values": [
            ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
             "Service", "First Contact", "Last Contact", "Next Action",
             "Due Date", "Notes", "Slack Channel", "Proposal Link",
             "Owner", "Upwork URL", "Probability",
             "Referred By", "Network Parent", "Network Notes", "Signal Date"],
            ["Acme Corp", "John", "network", "won", "50000", "fixed",
             "Dev", "2026-01-01", "2026-03-01", "",
             "", "", "", "", "", "", "",
             "", "", "", ""],
            ["Tech Startup", "Jane", "network", "lead", "15000", "hourly",
             "Consulting", "2026-02-01", "2026-03-05", "Follow up",
             "", "", "", "", "", "", "",
             "Acme Corp", "Acme Corp", "Great referral", "2026-03-01"],
        ]
    }


class TestAddSignal:
    """Tests for add_signal function."""

    def test_add_signal_basic(self, mock_config):
        """Test adding a basic signal."""
        with patch('openclaw_crm.network.append_sheet') as mock_append:
            mock_append.return_value = MagicMock(success=True)
            signal = {
                "source_client": "Test Client",
                "channel": "Slack",
                "signal_text": "Need help",
                "mentioned_company": "New Co"
            }
            result = network.add_signal(signal)
            assert result["ok"] is True
            assert result["status"] == "new"


class TestGetPendingSignals:
    """Tests for get_pending_signals function."""

    def test_get_pending_signals(self, mock_config, sample_signals_data):
        """Test getting pending signals."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=sample_signals_data
            )
            signals = network.get_pending_signals()
            assert len(signals) == 1
            assert signals[0]["Source Client"] == "Acme Corp"
            assert signals[0]["Status"] == "new"


class TestPromoteSignal:
    """Tests for promote_signal function."""

    def test_promote_signal_basic(self, mock_config, sample_signals_data, sample_pipeline_data):
        """Test promoting a signal to a deal."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            with patch('openclaw_crm.network.create_deal') as mock_create:
                with patch('openclaw_crm.network.update_sheet') as mock_update:
                    # First call: read signals
                    # Second call: read pipeline (for create_deal)
                    mock_read.side_effect = [
                        MagicMock(success=True, data=sample_signals_data),
                        MagicMock(success=True, data=sample_pipeline_data),
                    ]
                    mock_create.return_value = {"ok": True, "client": "New Co"}
                    mock_update.return_value = MagicMock(success=True)

                    result = network.promote_signal(2)  # Row 2 in signals sheet
                    assert result["ok"] is True
                    assert result["signal_row"] == 2


class TestDismissSignal:
    """Tests for dismiss_signal function."""

    def test_dismiss_signal(self, mock_config, sample_signals_data):
        """Test dismissing a signal."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            with patch('openclaw_crm.network.update_sheet') as mock_update:
                mock_read.return_value = MagicMock(
                    success=True,
                    data=sample_signals_data
                )
                mock_update.return_value = MagicMock(success=True)

                result = network.dismiss_signal(2)
                assert result["ok"] is True


class TestNetworkTree:
    """Tests for get_network_tree function."""

    def test_network_tree_basic(self, mock_config, sample_pipeline_data):
        """Test getting network tree."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=sample_pipeline_data
            )
            tree = network.get_network_tree()
            assert "Acme Corp" in tree
            assert len(tree["Acme Corp"]) == 1
            assert tree["Acme Corp"][0]["client"] == "Tech Startup"

    def test_network_tree_with_root(self, mock_config, sample_pipeline_data):
        """Test getting tree for specific root."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=sample_pipeline_data
            )
            tree = network.get_network_tree(root="Acme Corp")
            assert "Acme Corp" in tree


class TestNetworkValue:
    """Tests for get_network_value function."""

    def test_network_value_direct(self, mock_config, sample_pipeline_data):
        """Test getting direct network value."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=sample_pipeline_data
            )
            value = network.get_network_value("Tech Startup")
            assert value["direct_value"] == 15000
            assert value["network_value"] == 0
            assert value["total"] == 15000

    def test_network_value_network(self, mock_config, sample_pipeline_data):
        """Test getting network value (from referrals)."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=sample_pipeline_data
            )
            value = network.get_network_value("Acme Corp")
            assert value["direct_value"] == 50000
            assert value["network_value"] == 15000
            assert value["total"] == 65000


class TestCompetitorGuard:
    """Tests for check_competitor_guard function."""

    def test_competitor_guard_allowed(self, mock_config, sample_pipeline_data):
        """Test company is allowed (not a competitor)."""
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=sample_pipeline_data
            )
            result = network.check_competitor_guard("New Company", "Acme Corp")
            assert result is True

    def test_competitor_guard_blocked(self, mock_config):
        """Test company is blocked (is already a client in active stage)."""
        # Pipeline with a company in negotiation stage
        pipeline_data = {
            "values": [
                ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
                 "Service", "First Contact", "Last Contact", "Next Action",
                 "Due Date", "Notes", "Slack Channel", "Proposal Link",
                 "Owner", "Upwork URL", "Probability",
                 "Referred By", "Network Parent", "Network Notes", "Signal Date"],
                ["Acme Corp", "John", "network", "negotiation", "50000", "fixed",
                 "Dev", "2026-01-01", "2026-03-01", "",
                 "", "", "", "", "", "", "",
                 "", "", "", ""],
            ]
        }
        with patch('openclaw_crm.network.read_sheet') as mock_read:
            mock_read.return_value = MagicMock(
                success=True,
                data=pipeline_data
            )
            # Acme Corp is in negotiation - can't sell to Acme Corp again
            result = network.check_competitor_guard("Acme Corp", "Some Other Client")
            assert result is False

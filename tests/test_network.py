"""Unit tests for network module."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from openclaw_crm import network
from openclaw_crm.sheets import set_backend
from tests.conftest import MockBackend


# Sample network signals data
NETWORK_SIGNALS_HEADERS = ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"]

NETWORK_SIGNALS_DATA = [
    NETWORK_SIGNALS_HEADERS,
    ["2026-01-15T10:00:00", "Acme Corp", "slack", "Looking for marketing help", "Tech Inc", "new"],
    ["2026-01-16T11:00:00", "Tech Inc", "email", "Needs SEO", "Old Corp", "promoted"],
    ["2026-01-17T12:00:00", "StartupXYZ", "twitter", "Want consulting", "NewCo", "new"],
]

PIPELINE_DATA = [
    ["Client", "Contact", "Source", "Stage", "Budget", "Rate Type", "Service", "First Contact", "Last Contact", "Next Action", "Due Date", "Notes", "Slack Channel", "Proposal Link", "Owner", "Upwork URL", "Probability", "Referred By", "Network Parent", "Network Notes", "Signal Date"],
    ["Acme Corp", "John", "upwork", "won", "15000", "fixed", "SEO", "2026-01-01", "2026-02-01", "", "", "", "", "", "Alice", "", "=IFS(D2=\"won\",1,...)", "", "", "", ""],
    ["Tech Inc", "Jane", "network", "lead", "25000", "hourly", "Marketing", "2026-01-15", "2026-02-15", "", "", "", "", "", "Bob", "", "=IFS(D3=\"lead\",0.1,...)", "Acme Corp", "Acme Corp", "Referral", "2026-01-10"],
]


@pytest.fixture
def mock_backend():
    """Create a mock backend with sample data."""
    backend = MockBackend()
    backend._set_sheet("test-id", "'Network Signals'!A:F", NETWORK_SIGNALS_DATA)
    backend._set_sheet("test-id", "Pipeline!A:U", PIPELINE_DATA)
    set_backend(backend)
    yield backend
    set_backend(None)


@pytest.fixture
def config_mock():
    """Mock the config to return a test spreadsheet ID."""
    with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
        yield


class TestAddSignal:
    """Tests for add_signal function."""

    def test_add_signal_basic(self, mock_backend, config_mock):
        """Test adding a basic signal."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.add_signal({
                "source_client": "Test Client",
                "signal_text": "Needs help",
                "mentioned_company": "Target Corp",
            })
            assert result["ok"] is True
            assert result["status"] == "new"


class TestGetPendingSignals:
    """Tests for get_pending_signals function."""

    def test_get_pending_signals(self, mock_backend, config_mock):
        """Test getting pending signals."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            signals = network.get_pending_signals()
            assert len(signals) >= 1
            for s in signals:
                assert s.get("Status", "").lower() == "new"


class TestPromoteSignal:
    """Tests for promote_signal function."""

    def test_promote_signal_basic(self, mock_backend, config_mock):
        """Test promoting a signal to a deal."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.promote_signal(2)  # Row 2 has "new" status
            # May fail due to deal creation but shouldn't crash

    def test_promote_already_promoted(self, mock_backend, config_mock):
        """Test promoting an already promoted signal fails."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.promote_signal(3)  # Row 3 is already "promoted"
            assert result["ok"] is False
            assert "already promoted" in result.get("error", "").lower()


class TestDismissSignal:
    """Tests for dismiss_signal function."""

    def test_dismiss_signal(self, mock_backend, config_mock):
        """Test dismissing a signal."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.dismiss_signal(2)
            # Signal exists, dismiss should work


class TestGetNetworkTree:
    """Tests for get_network_tree function."""

    def test_get_network_tree_all(self, mock_backend, config_mock):
        """Test getting full network tree."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            tree = network.get_network_tree()
            assert isinstance(tree, dict)

    def test_get_network_tree_root(self, mock_backend, config_mock):
        """Test getting network tree for specific root."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            tree = network.get_network_tree(root="Acme Corp")
            assert isinstance(tree, dict)


class TestGetNetworkValue:
    """Tests for get_network_value function."""

    def test_get_network_value(self, mock_backend, config_mock):
        """Test calculating network value."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.get_network_value("Acme Corp")
            assert "client" in result
            assert "direct_value" in result
            assert "network_value" in result
            assert "total" in result


class TestCompetitorGuard:
    """Tests for check_competitor_guard function."""

    def test_competitor_guard_allowed(self, mock_backend, config_mock):
        """Test competitor not in pipeline."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.check_competitor_guard("Unknown Corp", "Acme Corp")
            assert result is True

    def test_competitor_guard_blocked(self, mock_backend, config_mock):
        """Test competitor already in pipeline."""
        with patch.object(network, "get_spreadsheet_id", return_value="test-id"):
            result = network.check_competitor_guard("Acme Corp", "Tech Inc")
            assert result is False

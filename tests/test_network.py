"""Tests for network module (spider network)."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from openclaw_crm import network


class MockSheetResult:
    def __init__(self, success=True, data=None, error=""):
        self.success = success
        self.data = data or {}
        self.error = error


class TestNetwork:
    """Test cases for network module."""
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.append_sheet")
    def test_add_signal(self, mock_append, mock_get_id):
        """Test adding a new signal."""
        mock_get_id.return_value = "test_spreadsheet"
        mock_append.return_value = MockSheetResult(success=True)
        
        signal = {
            "source_client": "Acme Corp",
            "channel": "Twitter",
            "signal_text": "Looking for CRM solution",
            "mentioned_company": "Beta Inc",
        }
        
        result = network.add_signal(signal)
        
        assert result["ok"] is True
        assert result["status"] == "new"
        mock_append.assert_called_once()
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_pending_signals(self, mock_read, mock_get_id):
        """Test getting pending signals."""
        mock_get_id.return_value = "test_spreadsheet"
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"],
                ["2024-01-01", "Acme", "Twitter", "Needs help", "Beta", "new"],
                ["2024-01-02", "Gamma", "LinkedIn", "Inquiry", "Delta", "promoted"],
            ]
        })
        
        result = network.get_pending_signals()
        
        assert len(result) == 1
        assert result[0]["Source Client"] == "Acme"
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_tree(self, mock_read, mock_get_id):
        """Test building network tree."""
        mock_get_id.return_value = "test_spreadsheet"
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Client", "Stage", "Budget", "Network Parent", "Referred By"],
                ["Acme", "won", "10000", "", ""],
                ["Beta", "lead", "5000", "Acme", "Acme"],
                ["Gamma", "proposal", "8000", "Acme", "Acme"],
            ]
        })
        
        tree = network.get_network_tree()
        
        assert "Acme" in tree
        assert len(tree["Acme"]) == 2
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_get_network_value(self, mock_read, mock_get_id):
        """Test calculating network value."""
        mock_get_id.return_value = "test_spreadsheet"
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Client", "Budget", "Network Parent"],
                ["Acme", "10000", ""],
                ["Beta", "5000", "Acme"],
                ["Gamma", "8000", "Acme"],
            ]
        })
        
        result = network.get_network_value("Acme")
        
        assert result["direct_value"] == 10000
        assert result["network_value"] == 13000
        assert result["total"] == 23000
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_allows(self, mock_read, mock_get_id):
        """Test competitor guard allows safe company."""
        mock_get_id.return_value = "test_spreadsheet"
        
        # First call for pipeline
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Client", "Stage"],
                ["Acme Corp", "won"],
            ]
        })
        
        result = network.check_competitor_guard("New Company", "Acme Corp")
        
        assert result is True
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_check_competitor_guard_blocks(self, mock_read, mock_get_id):
        """Test competitor guard blocks competitor."""
        mock_get_id.return_value = "test_spreadsheet"
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Client", "Stage"],
                ["Competitor Inc", "won"],
            ]
        })
        
        result = network.check_competitor_guard("Competitor Inc", "Acme Corp")
        
        assert result is False
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_promote_signal_atomic(self, mock_read, mock_get_id):
        """Test atomic promote: deal created before signal marked promoted."""
        mock_get_id.return_value = "test_spreadsheet"
        
        # First read returns the signal
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"],
                ["2024-01-01", "Acme", "Twitter", "Needs CRM", "Beta Inc", "new"],
            ]
        })
        
        with patch("openclaw_crm.network.create_deal") as mock_create, \
             patch("openclaw_crm.network.update_sheet") as mock_update:
            mock_create.return_value = {"ok": True, "id": "deal_123"}
            mock_update.return_value = MockSheetResult(success=True)
            
            result = network.promote_signal(2)
            
            assert result["ok"] is True
            # Verify create_deal was called BEFORE update_sheet
            assert mock_create.called
            assert mock_update.called
    
    @patch("openclaw_crm.network.get_spreadsheet_id")
    @patch("openclaw_crm.network.read_sheet")
    def test_promote_signal_already_promoted(self, mock_read, mock_get_id):
        """Test re-promote guard prevents double promotion."""
        mock_get_id.return_value = "test_spreadsheet"
        mock_read.return_value = MockSheetResult(success=True, data={
            "values": [
                ["Timestamp", "Source Client", "Channel", "Signal Text", "Mentioned Company", "Status"],
                ["2024-01-01", "Acme", "Twitter", "Needs CRM", "Beta Inc", "promoted"],
            ]
        })
        
        result = network.promote_signal(2)
        
        assert result["ok"] is False
        assert "already promoted" in result["error"]

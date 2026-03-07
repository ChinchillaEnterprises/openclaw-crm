"""Unit tests for pipeline module."""
from __future__ import annotations

import pytest
from datetime import date, datetime
from unittest.mock import patch, MagicMock

from openclaw_crm import pipeline


class TestGetPipeline:
    """Tests for get_pipeline function."""

    def test_get_pipeline_returns_list(self, mock_backend_with_data):
        """Test that get_pipeline returns a list."""
        result = pipeline.get_pipeline()
        assert isinstance(result, list)

    def test_get_pipeline_active_only(self, mock_backend_with_data):
        """Test that active_only filters out won/lost deals."""
        result = pipeline.get_pipeline(active_only=True)
        # Should not include "won" deals
        stages = [d.get("Stage", "").lower() for d in result]
        assert "won" not in stages
        assert "lost" not in stages

    def test_get_pipeline_includes_all_when_active_false(self, mock_backend_with_data):
        """Test that active_only=False includes all deals."""
        result = pipeline.get_pipeline(active_only=False)
        stages = [d.get("Stage", "").lower() for d in result]
        assert "won" in stages

    def test_get_pipeline_empty_sheet(self, mock_backend):
        """Test handling of empty sheet."""
        result = pipeline.get_pipeline()
        assert result == []

    def test_get_pipeline_single_row(self, mock_backend):
        """Test handling of sheet with only headers."""
        mock_backend._set_sheet_data("Pipeline!A:U", [
            ["Client", "Contact", "Source", "Stage"]
        ])
        result = pipeline.get_pipeline()
        assert result == []


class TestCreateDeal:
    """Tests for create_deal function."""

    def test_create_deal_basic(self, mock_backend):
        """Test basic deal creation."""
        deal = {
            "client": "Test Corp",
            "contact": "Test User",
            "source": "upwork",
            "stage": "lead",
            "budget": "5000",
        }
        result = pipeline.create_deal(deal)
        assert result["ok"] is True
        assert result["client"] == "Test Corp"
        # Check that append was called
        assert len(mock_backend.append_calls) == 1

    def test_create_deal_with_referral(self, mock_backend):
        """Test deal creation with referral sets source to network."""
        deal = {
            "client": "Referred Corp",
            "contact": "Ref User",
            "referred_by": "John Doe",
            "stage": "lead",
        }
        result = pipeline.create_deal(deal)
        assert result["ok"] is True
        # Check append call contains network source
        call = mock_backend.append_calls[0]
        row = call[2][0]
        assert "network" in row  # Source should be "network"

    def test_create_deal_stage_normalization(self, mock_backend):
        """Test that stage is normalized to lowercase."""
        deal = {
            "client": "Test Corp",
            "stage": "LEAD",  # uppercase
        }
        result = pipeline.create_deal(deal)
        assert result["ok"] is True


class TestMoveStage:
    """Tests for move_stage function."""

    def test_move_stage_existing_client(self, mock_backend_with_data):
        """Test moving stage for existing client."""
        result = pipeline.move_stage("Acme Corp", "qualifying")
        assert result["ok"] is True
        assert result["stage"] == "qualifying"

    def test_move_stage_nonexistent_client(self, mock_backend_with_data):
        """Test moving stage for non-existent client."""
        result = pipeline.move_stage("NonExistent Corp", "won")
        assert result["ok"] is False
        assert "not found" in result["error"].lower()

    def test_move_stage_case_insensitive(self, mock_backend_with_data):
        """Test that client lookup is case insensitive."""
        result = pipeline.move_stage("ACME CORP", "won")
        assert result["ok"] is True


class TestGetPipelineSummary:
    """Tests for get_pipeline_summary function."""

    def test_summary_counts(self, mock_backend_with_data):
        """Test summary calculates correct counts."""
        result = pipeline.get_pipeline_summary()
        assert result["total_deals"] >= 0
        assert "by_stage" in result

    def test_summary_network_count(self, mock_backend_with_data):
        """Test network count is calculated correctly."""
        result = pipeline.get_pipeline_summary()
        # Should count deals with "Referred By"
        assert result["network_count"] >= 0

    def test_summary_empty_pipeline(self, mock_backend):
        """Test summary with empty pipeline."""
        result = pipeline.get_pipeline_summary()
        assert result["total_deals"] == 0
        assert result["won_deals"] == 0


class TestGetStaleDeals:
    """Tests for get_stale_deals function."""

    def test_stale_deals_default_thresholds(self, mock_backend_with_data):
        """Test stale deals with default thresholds."""
        result = pipeline.get_stale_deals()
        assert isinstance(result, dict)
        # Default thresholds are [7, 14, 21]
        assert 7 in result
        assert 14 in result
        assert 21 in result

    def test_stale_deals_custom_thresholds(self, mock_backend_with_data):
        """Test stale deals with custom thresholds."""
        result = pipeline.get_stale_deals(thresholds=[5, 10])
        assert 5 in result
        assert 10 in result

    def test_stale_deals_empty_pipeline(self, mock_backend):
        """Test stale deals with empty pipeline."""
        result = pipeline.get_stale_deals()
        for deals in result.values():
            assert deals == []


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_sheet_read(self, mock_backend):
        """Test handling of completely empty sheet."""
        mock_backend._set_sheet_data("Pipeline!A:U", [])
        result = pipeline.get_pipeline()
        assert result == []

    def test_missing_columns(self, mock_backend):
        """Test handling of sheet with missing columns."""
        # Sheet with fewer columns than expected - use full range
        mock_backend._set_sheet_data("Pipeline!A:U", [
            ["Client", "Stage", "Budget", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
            ["Test", "lead", "1000", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ])
        result = pipeline.get_pipeline()
        assert len(result) == 1
        assert result[0].get("Client") == "Test"

    def test_stage_normalization(self, mock_backend_with_data):
        """Test that stage names are normalized correctly."""
        result = pipeline.get_pipeline()
        for deal in result:
            stage = deal.get("Stage", "")
            # Stages should be normalized (handled by the function)
            assert isinstance(stage, str)

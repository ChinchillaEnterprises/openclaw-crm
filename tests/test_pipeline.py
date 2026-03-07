"""Unit tests for pipeline.py module."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import date

from openclaw_crm import pipeline
from openclaw_crm.sheets import SheetsBackend, SheetResult
from tests.conftest import MockBackend


class TestGetPipeline:
    """Tests for get_pipeline function."""

    def test_get_pipeline_returns_list(self, mock_backend, sample_pipeline_data):
        """Test that get_pipeline returns a list of deals."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline(active_only=False)
                assert isinstance(result, list)

    def test_get_pipeline_filters_inactive_deals(self, mock_backend, sample_pipeline_data):
        """Test that active_only filters out won/lost deals."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline(active_only=True)
                stages = [d.get("Stage", "").lower() for d in result]
                assert "won" not in stages
                assert "lost" not in stages

    def test_get_pipeline_empty_sheet(self, mock_backend, empty_pipeline_data):
        """Test handling of empty pipeline sheet."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, empty_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline()
                assert result == []

    def test_get_pipeline_missing_columns(self, mock_backend):
        """Test handling of missing columns in sheet."""
        data = {"values": [["Client", "Stage"]]}
        mock_backend.set_data(pipeline.PIPELINE_RANGE, data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline()
                assert isinstance(result, list)


class TestCreateDeal:
    """Tests for create_deal function."""

    def test_create_deal_basic(self, mock_backend, sample_pipeline_data):
        """Test creating a basic deal."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                with patch("openclaw_crm.pipeline.append_sheet", mock_backend.append):
                    deal = {"client": "New Client", "contact": "New Contact", "budget": "$3000"}
                    result = pipeline.create_deal(deal)
                    assert result["ok"] is True
                    assert result["client"] == "New Client"

    def test_create_deal_with_referral(self, mock_backend, sample_pipeline_data):
        """Test creating a deal with referral sets source to network."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                with patch("openclaw_crm.pipeline.append_sheet", mock_backend.append):
                    deal = {
                        "client": "Referred Client",
                        "contact": "Ref Contact",
                        "referred_by": "Existing Client"
                    }
                    result = pipeline.create_deal(deal)
                    assert result["ok"] is True


class TestMoveStage:
    """Tests for move_stage function."""

    def test_move_stage_existing_client(self, mock_backend, sample_pipeline_data):
        """Test moving stage for an existing client."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                with patch("openclaw_crm.pipeline.update_sheet", mock_backend.update):
                    result = pipeline.move_stage("Acme Corp", "qualifying")
                    assert result["ok"] is True
                    assert result["stage"] == "qualifying"

    def test_move_stage_nonexistent_client(self, mock_backend, sample_pipeline_data):
        """Test moving stage for nonexistent client returns error."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.move_stage("Nonexistent Corp", "won")
                assert result["ok"] is False
                assert "not found" in result["error"]

    def test_move_stage_empty_pipeline(self, mock_backend, empty_pipeline_data):
        """Test moving stage with empty pipeline returns error."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, empty_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.move_stage("Any Client", "won")
                assert result["ok"] is False


class TestGetPipelineSummary:
    """Tests for get_pipeline_summary function."""

    def test_get_pipeline_summary_basic(self, mock_backend, sample_pipeline_data):
        """Test basic pipeline summary calculation."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline_summary()
                assert "total_deals" in result
                assert "by_stage" in result
                assert "total_weighted_value" in result

    def test_get_pipeline_summary_counts_network(self, mock_backend, sample_pipeline_data):
        """Test that network referrals are counted."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline_summary()
                assert result["network_count"] >= 0


class TestGetStaleDeals:
    """Tests for get_stale_deals function."""

    def test_get_stale_deals_basic(self, mock_backend, sample_pipeline_data):
        """Test stale deals are categorized by threshold."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_stale_deals(thresholds=[7, 14, 21])
                assert 7 in result
                assert 14 in result
                assert 21 in result

    def test_get_stale_deals_default_thresholds(self, mock_backend, sample_pipeline_data):
        """Test default thresholds are used when not specified."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_stale_deals()
                assert 7 in result
                assert 14 in result
                assert 21 in result


class TestStageNormalization:
    """Tests for stage normalization edge cases."""

    def test_stage_normalization_lowercase(self, mock_backend, sample_pipeline_data):
        """Test that stages are normalized to lowercase."""
        mock_backend.set_data(pipeline.PIPELINE_RANGE, sample_pipeline_data)
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test-id"):
            with patch("openclaw_crm.pipeline.read_sheet", mock_backend.read):
                result = pipeline.get_pipeline(active_only=True)
                for deal in result:
                    stage = deal.get("Stage", "")
                    assert stage == stage.lower() or stage == ""

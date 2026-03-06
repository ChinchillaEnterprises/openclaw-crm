"""Unit tests for pipeline.py"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import date

import sys
sys.path.insert(0, 'src')

from openclaw_crm import pipeline


class TestParseRows:
    """Tests for _parse_rows function."""

    def test_parse_rows_empty(self, mock_sheet_result):
        """Test parsing empty result."""
        result = mock_sheet_result(success=False)
        assert pipeline._parse_rows(result) == []

    def test_parse_rows_no_data(self, mock_sheet_result):
        """Test parsing result without data."""
        result = mock_sheet_result(success=True, data={})
        assert pipeline._parse_rows(result) == []

    def test_parse_rows_no_headers(self, mock_sheet_result):
        """Test parsing result with only data rows."""
        result = mock_sheet_result(success=True, data={"values": [["Acme"]]})
        assert pipeline._parse_rows(result) == []

    def test_parse_rows_valid(self, sample_pipeline_data):
        """Test parsing valid data."""
        result = MagicMock()
        result.success = True
        result.data = sample_pipeline_data
        parsed = pipeline._parse_rows(result)
        assert len(parsed) == 2
        assert parsed[0]["Client"] == "Acme Corp"
        assert parsed[0]["Stage"] == "lead"
        assert parsed[1]["Client"] == "Tech Inc"
        assert parsed[1]["Stage"] == "proposal"


class TestDaysSince:
    """Tests for _days_since function."""

    def test_empty_string(self):
        """Test empty string returns 999."""
        assert pipeline._days_since("") == 999

    def test_none_value(self):
        """Test None returns 999."""
        assert pipeline._days_since(None) == 999

    def test_valid_date(self):
        """Test valid date returns correct days."""
        today = date.today()
        days = pipeline._days_since(today.isoformat())
        assert days == 0

    def test_invalid_format(self):
        """Test invalid date format returns 999."""
        assert pipeline._days_since("not-a-date") == 999


class TestGetPipeline:
    """Tests for get_pipeline function."""

    def test_get_pipeline_active_only(self, mock_read_sheet, mock_config, sample_pipeline_data):
        """Test getting active pipeline deals."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )
        deals = pipeline.get_pipeline(active_only=True)
        assert len(deals) == 2  # Both are active
        assert all(d.get("Stage") not in ("won", "lost") for d in deals)

    def test_get_pipeline_all(self, mock_read_sheet, mock_config, sample_pipeline_data):
        """Test getting all pipeline deals including closed."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )
        deals = pipeline.get_pipeline(active_only=False)
        assert len(deals) == 2


class TestCreateDeal:
    """Tests for create_deal function."""

    def test_create_deal_basic(self, mock_read_sheet, mock_append_sheet, mock_config, sample_pipeline_data):
        """Test creating a basic deal."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )
        mock_append_sheet.return_value = MagicMock(success=True)

        deal = {
            "client": "New Client",
            "budget": "10000",
            "source": "upwork",
            "stage": "lead"
        }
        result = pipeline.create_deal(deal)
        assert result["ok"] is True
        assert result["client"] == "New Client"
        assert mock_append_sheet.called


class TestMoveStage:
    """Tests for move_stage function."""

    def test_move_stage_existing(self, mock_read_sheet, mock_update_sheet, mock_config, sample_pipeline_data):
        """Test moving an existing deal to new stage."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )
        mock_update_sheet.return_value = MagicMock(success=True)

        result = pipeline.move_stage("Acme Corp", "qualifying")
        assert result["ok"] is True
        assert result["stage"] == "qualifying"

    def test_move_stage_not_found(self, mock_read_sheet, mock_config, sample_pipeline_data):
        """Test moving non-existent deal."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )

        result = pipeline.move_stage("Non Existent", "won")
        assert result["ok"] is False
        assert "not found" in result["error"]


class TestPipelineSummary:
    """Tests for get_pipeline_summary function."""

    def test_summary_basic(self, mock_read_sheet, mock_config, sample_pipeline_data):
        """Test basic pipeline summary."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )
        summary = pipeline.get_pipeline_summary()
        assert summary["total_deals"] == 2
        assert "lead" in summary["by_stage"]
        assert "proposal" in summary["by_stage"]
        assert summary["network_count"] == 1  # Tech Inc referred by Acme


class TestStaleDeals:
    """Tests for get_stale_deals function."""

    def test_stale_deals(self, mock_read_sheet, mock_config, sample_pipeline_data):
        """Test getting stale deals."""
        mock_read_sheet.return_value = MagicMock(
            success=True,
            data=sample_pipeline_data
        )
        buckets = pipeline.get_stale_deals()
        assert 7 in buckets
        assert 14 in buckets
        assert 21 in buckets

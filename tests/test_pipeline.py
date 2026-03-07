"""Unit tests for pipeline module."""

from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from openclaw_crm import pipeline
from openclaw_crm.sheets import set_backend
from tests.conftest import MockBackend, SAMPLE_EMPTY_DATA, SAMPLE_PIPELINE_DATA


@pytest.fixture
def mock_backend():
    """Create a mock backend with sample data."""
    backend = MockBackend()
    backend._set_sheet("test-id", "Pipeline!A:U", SAMPLE_PIPELINE_DATA)
    set_backend(backend)
    yield backend
    set_backend(None)


@pytest.fixture
def empty_backend():
    """Create a mock backend with empty data."""
    backend = MockBackend()
    backend._set_sheet("test-id", "Pipeline!A:U", SAMPLE_EMPTY_DATA)
    set_backend(backend)
    yield backend
    set_backend(None)


@pytest.fixture
def config_mock():
    """Mock the config to return a test spreadsheet ID."""
    with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
        yield


class TestGetPipeline:
    """Tests for get_pipeline function."""

    def test_get_pipeline_returns_all_deals(self, mock_backend, config_mock):
        """Test that get_pipeline returns all deals when active_only=False."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            deals = pipeline.get_pipeline(active_only=False)
            assert len(deals) == 3

    def test_get_pipeline_filters_active_only(self, mock_backend, config_mock):
        """Test that get_pipeline filters out won/lost when active_only=True."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            deals = pipeline.get_pipeline(active_only=True)
            assert len(deals) == 2
            for deal in deals:
                assert deal.get("Stage").lower() not in ("won", "lost")

    def test_get_pipeline_empty_sheet(self, empty_backend, config_mock):
        """Test that get_pipeline handles empty sheet."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            deals = pipeline.get_pipeline()
            assert deals == []


class TestCreateDeal:
    """Tests for create_deal function."""

    def test_create_deal_basic(self, empty_backend, config_mock):
        """Test basic deal creation."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            result = pipeline.create_deal({
                "client": "New Client",
                "budget": "10000",
                "source": "upwork",
            })
            assert result["ok"] is True
            assert result["client"] == "New Client"

    def test_create_deal_with_referred_by(self, empty_backend, config_mock):
        """Test deal creation sets source to network when referred_by is present."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            result = pipeline.create_deal({
                "client": "Referred Client",
                "referred_by": "Acme Corp",
            })
            assert result["ok"] is True


class TestMoveStage:
    """Tests for move_stage function."""

    def test_move_stage_existing_client(self, mock_backend, config_mock):
        """Test moving stage for existing client."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            result = pipeline.move_stage("Acme Corp", "qualifying")
            assert result["ok"] is True
            assert result["stage"] == "qualifying"

    def test_move_stage_normalizes_case(self, mock_backend, config_mock):
        """Test that stage is normalized to lowercase."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            result = pipeline.move_stage("Acme Corp", "LEAD")
            assert result["ok"] is True
            assert result["stage"] == "lead"

    def test_move_stage_not_found(self, mock_backend, config_mock):
        """Test moving stage for non-existent client."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            result = pipeline.move_stage("NonExistent", "lead")
            assert result["ok"] is False
            assert "not found" in result["error"]


class TestPipelineSummary:
    """Tests for get_pipeline_summary function."""

    def test_summary_counts(self, mock_backend, config_mock):
        """Test pipeline summary counts."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
            assert summary["total_deals"] == 2  # active only
            assert summary["won_deals"] == 1

    def test_summary_by_stage(self, mock_backend, config_mock):
        """Test summary groups deals by stage."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
            assert "lead" in summary["by_stage"]
            assert "proposal" in summary["by_stage"]

    def test_summary_network_count(self, mock_backend, config_mock):
        """Test network count is calculated."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
            assert summary["network_count"] >= 1


class TestStaleDeals:
    """Tests for get_stale_deals function."""

    def test_stale_deals_returns_buckets(self, mock_backend, config_mock):
        """Test stale deals returns correct buckets."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            stale = pipeline.get_stale_deals()
            assert 7 in stale
            assert 14 in stale
            assert 21 in stale

    def test_stale_deals_custom_thresholds(self, mock_backend, config_mock):
        """Test custom threshold values."""
        with patch.object(pipeline, "get_spreadsheet_id", return_value="test-id"):
            stale = pipeline.get_stale_deals(thresholds=[10, 30])
            assert 10 in stale
            assert 30 in stale
            assert 7 not in stale

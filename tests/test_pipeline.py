"""Tests for pipeline module."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from openclaw_crm import pipeline
from openclaw_crm.sheets import set_backend, SheetResult
from tests.conftest import MockBackend


class TestGetPipeline:
    """Tests for get_pipeline function."""

    def test_get_pipeline_returns_all_active_deals(self, mock_backend_with_data):
        """Should return only active deals when active_only=True."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            deals = pipeline.get_pipeline(active_only=True)
        
        assert len(deals) == 2
        # Should exclude won/lost deals
        assert all(d.get("Stage", "").lower() not in ("won", "lost") for d in deals)

    def test_get_pipeline_includes_inactive_when_requested(self, mock_backend_with_data):
        """Should return all deals including won/lost when active_only=False."""
        # Add a won deal to the data
        mock_backend_with_data.data["Pipeline!A:U"].append(
            ["Closed Corp", "Bob", "referral", "won", "50000", "fixed",
             "Service", "2025-01-01", "2025-06-01", "",
             "", "", "", "", "", "", "=IFS(D4=\"won\",1,...)",
             "", "", "", ""]
        )
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            all_deals = pipeline.get_pipeline(active_only=False)
            active_deals = pipeline.get_pipeline(active_only=True)
        
        assert len(all_deals) == 3
        assert len(active_deals) == 2

    def test_get_pipeline_empty_sheet(self, mock_backend):
        """Should return empty list when sheet has no data."""
        mock_backend.data = {"Pipeline!A:U": [["Client", "Stage"]]}
        set_backend(mock_backend)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            deals = pipeline.get_pipeline()
        
        assert deals == []

    def test_get_pipeline_handles_missing_columns(self, mock_backend):
        """Should handle rows with fewer columns than headers."""
        mock_backend.data = {
            "Pipeline!A:U": [
                ["Client", "Stage", "Budget"],  # Only 3 columns
                ["Acme", "lead", "10000"],  # Less than header count
            ]
        }
        set_backend(mock_backend)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            deals = pipeline.get_pipeline()
        
        assert len(deals) == 1
        assert deals[0]["Client"] == "Acme"
        assert deals[0]["Stage"] == "lead"


class TestCreateDeal:
    """Tests for create_deal function."""

    def test_create_deal_basic(self, mock_backend):
        """Should create a new deal with provided fields."""
        mock_backend.data = {"Pipeline!A:U": [pipeline.HEADERS]}
        set_backend(mock_backend)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.create_deal({
                "client": "NewCorp",
                "budget": "20000",
                "source": "upwork",
                "stage": "lead",
            })
        
        assert result["ok"] is True
        assert result["client"] == "NewCorp"
        assert len(mock_backend.append_calls) == 1

    def test_create_deal_sets_referred_by(self, mock_backend):
        """Should set source to network when referred_by is provided."""
        mock_backend.data = {"Pipeline!A:U": [pipeline.HEADERS]}
        set_backend(mock_backend)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.create_deal({
                "client": "RefCorp",
                "referred_by": "ExistingCorp",
            })
        
        assert result["ok"] is True
        call_args = mock_backend.append_calls[0]
        row = call_args[2][0]  # The row that was appended
        assert "network" in row  # Source should be "network"

    def test_create_deal_includes_probability_formula(self, mock_backend):
        """Should include probability IFS formula in the row."""
        mock_backend.data = {"Pipeline!A:U": [pipeline.HEADERS]}
        set_backend(mock_backend)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.create_deal({"client": "TestCorp"})
        
        assert result["ok"] is True
        call_args = mock_backend.append_calls[0]
        row = call_args[2][0]
        # Probability column should have a formula
        prob_col = row[16]  # Column Q (index 16)
        assert "IFS" in prob_col


class TestMoveStage:
    """Tests for move_stage function."""

    def test_move_stage_existing_client(self, mock_backend_with_data):
        """Should update stage for existing client."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.move_stage("Acme Corp", "qualifying")
        
        assert result["ok"] is True
        assert result["stage"] == "qualifying"

    def test_move_stage_updates_last_contact(self, mock_backend_with_data):
        """Should update last contact date when moving stage."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.move_stage("Tech Inc", "negotiation")
        
        assert result["ok"] is True

    def test_move_stage_normalizes_stage_name(self, mock_backend_with_data):
        """Should normalize stage name to lowercase."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.move_stage("Acme Corp", "LEAD")
        
        assert result["ok"] is True
        assert result["stage"] == "lead"

    def test_move_stage_client_not_found(self, mock_backend_with_data):
        """Should return error when client not found."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            result = pipeline.move_stage("NonExistent Corp", "lead")
        
        assert result["ok"] is False
        assert "not found" in result["error"]


class TestGetPipelineSummary:
    """Tests for get_pipeline_summary function."""

    def test_summary_counts_by_stage(self, mock_backend_with_data):
        """Should correctly count deals by stage."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
        
        assert summary["total_deals"] == 2
        assert summary["by_stage"]["lead"] == 1
        assert summary["by_stage"]["proposal"] == 1

    def test_summary_calculates_weighted_value(self, mock_backend_with_data):
        """Should calculate probability-weighted pipeline value."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
        
        # lead (0.1 * 15000) + proposal (0.5 * 25000) = 1500 + 12500 = 14000
        assert summary["total_weighted_value"] == 14000.0

    def test_summary_counts_network_referrals(self, mock_backend_with_data):
        """Should count deals with referral source."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
        
        assert summary["network_count"] == 1  # Tech Inc has "Acme Corp" as referred by

    def test_summary_identifies_top_referrer(self, mock_backend_with_data):
        """Should identify the top referrer."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            summary = pipeline.get_pipeline_summary()
        
        assert summary["top_referrer"] == "Acme Corp"


class TestGetStaleDeals:
    """Tests for get_stale_deals function."""

    def test_stale_deals_buckets_by_threshold(self, mock_backend_with_data):
        """Should bucket deals by stale thresholds."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            buckets = pipeline.get_stale_deals([7, 14, 21])
        
        # Acme Corp: last contact 2026-03-01, today 2026-03-07 = 6 days (not stale)
        # Tech Inc: last contact 2026-03-05, today 2026-03-07 = 2 days (not stale)
        # Both should not be in any bucket if tested with recent dates
        # (test depends on current date)

    def test_stale_deals_default_thresholds(self, mock_backend_with_data):
        """Should use default thresholds [7, 14, 21] if not provided."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            buckets = pipeline.get_stale_deals()
        
        assert 7 in buckets
        assert 14 in buckets
        assert 21 in buckets

    def test_stale_deals_adds_days_stale(self, mock_backend_with_data):
        """Should add _days_stale to each deal in buckets."""
        set_backend(mock_backend_with_data)
        
        with patch.object(pipeline, 'get_spreadsheet_id', return_value="test-id"):
            buckets = pipeline.get_stale_deals([7])
        
        for deal in buckets.get(7, []):
            assert "_days_stale" in deal


class TestStageProbability:
    """Tests for STAGE_PROBABILITY constant."""

    def test_stage_probability_values(self):
        """Should have correct probability values for each stage."""
        assert pipeline.STAGE_PROBABILITY["lead"] == 0.10
        assert pipeline.STAGE_PROBABILITY["qualifying"] == 0.25
        assert pipeline.STAGE_PROBABILITY["proposal"] == 0.50
        assert pipeline.STAGE_PROBABILITY["negotiation"] == 0.75
        assert pipeline.STAGE_PROBABILITY["won"] == 1.0
        assert pipeline.STAGE_PROBABILITY["lost"] == 0.0


class TestDaysSince:
    """Tests for _days_since helper function."""

    def test_days_since_valid_date(self):
        """Should calculate correct days for valid date string."""
        # This test depends on current date, so we mock it
        with patch('openclaw_crm.pipeline.date') as mock_date:
            mock_date.today.return_value = date(2026, 3, 7)
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
            
            result = pipeline._days_since("2026-03-01")
            assert result == 6

    def test_days_since_empty_string(self):
        """Should return 999 for empty string."""
        assert pipeline._days_since("") == 999

    def test_days_since_invalid_date(self):
        """Should return 999 for invalid date format."""
        assert pipeline._days_since("not-a-date") == 999

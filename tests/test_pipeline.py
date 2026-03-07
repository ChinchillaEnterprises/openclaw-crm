from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.pipeline import (
    _parse_rows,
    _days_since,
    get_pipeline,
    create_deal,
    update_deal,
    move_stage,
    get_pipeline_summary,
    get_stale_deals,
    get_overdue_invoices,
)


@pytest.fixture
def mock_spreadsheet_id():
    """Mock spreadsheet ID."""
    return "test_spreadsheet_id"


@pytest.fixture
def sample_headers():
    """Sample headers matching pipeline.py HEADERS."""
    return [
        "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
        "Service", "First Contact", "Last Contact", "Next Action",
        "Due Date", "Notes", "Slack Channel", "Proposal Link",
        "Owner", "Upwork URL", "Probability",
        "Referred By", "Network Parent", "Network Notes", "Signal Date",
    ]


@pytest.fixture
def sample_values(sample_headers):
    """Sample values for testing."""
    return [
        sample_headers,
        ["Acme", "John Doe", "upwork", "lead", "15000", "fixed", "Web Dev", "2024-01-01", "2024-01-05", "Email", "", "", "", "", "", "", "", "", "", "", ""],
        ["TechCorp", "Jane Smith", "network", "proposal", "25000", "hourly", "Mobile App", "2024-01-02", "2024-01-10", "Call", "", "", "", "", "", "", "", "", "Acme", "Acme", "referral", "2024-01-10"],
        ["StartupXYZ", "Bob Wilson", "inbound", "won", "50000", "retainer", "Consulting", "2024-01-03", "2024-01-15", "Meeting", "", "", "", "", "", "", "", "", "", "", ""],
    ]


@pytest.fixture
def sample_sheet_result(sample_values):
    """Sample successful SheetResult."""
    from openclaw_crm.sheets import SheetResult

    return SheetResult(success=True, data={"values": sample_values})


@pytest.fixture
def sample_sheet_result_empty():
    """Sample empty SheetResult."""
    from openclaw_crm.sheets import SheetResult

    return SheetResult(success=True, data={"values": []})


@pytest.fixture
def sample_sheet_result_no_headers():
    """Sample SheetResult with only headers."""
    from openclaw_crm.sheets import SheetResult

    headers = [
        "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
        "Service", "First Contact", "Last Contact", "Next Action",
        "Due Date", "Notes", "Slack Channel", "Proposal Link",
        "Owner", "Upwork URL", "Probability",
        "Referred By", "Network Parent", "Network Notes", "Signal Date",
    ]
    return SheetResult(success=True, data={"values": [headers]})


class TestParseRows:
    """Test _parse_rows function."""

    def test_parse_rows_success(self, sample_sheet_result):
        """Test parsing successful result."""
        rows = _parse_rows(sample_sheet_result)

        assert len(rows) == 3
        assert rows[0]["Client"] == "Acme"
        assert rows[0]["Stage"] == "lead"
        assert rows[1]["Client"] == "TechCorp"
        assert rows[1]["Referred By"] == "Acme"
        assert rows[2]["Client"] == "StartupXYZ"

    def test_parse_rows_empty(self, sample_sheet_result_empty):
        """Test parsing empty result."""
        rows = _parse_rows(sample_sheet_result_empty)
        assert rows == []

    def test_parse_rows_no_headers(self, sample_sheet_result_no_headers):
        """Test parsing result with no data rows."""
        rows = _parse_rows(sample_sheet_result_no_headers)
        assert rows == []

    def test_parse_rows_unsuccessful(self):
        """Test parsing unsuccessful result."""
        from openclaw_crm.sheets import SheetResult

        result = SheetResult(success=False, data=None)
        rows = _parse_rows(result)
        assert rows == []

    def test_parse_rows_missing_values(self):
        """Test parsing with missing values in rows."""
        from openclaw_crm.sheets import SheetResult

        values = [
            ["Client", "Contact", "Stage"],
            ["Acme", "John", "lead"],
            ["TechCorp"],  # Missing Contact and Stage
        ]
        result = SheetResult(success=True, data={"values": values})
        rows = _parse_rows(result)

        assert len(rows) == 2
        assert rows[0] == {"Client": "Acme", "Contact": "John", "Stage": "lead"}
        assert rows[1] == {"Client": "TechCorp", "Contact": "", "Stage": ""}


class TestDaysSince:
    """Test _days_since function."""

    def test_days_since_valid_date(self):
        """Test calculating days since valid date."""
        # Create a date 5 days ago
        test_date = (date.today() - timedelta(days=5)).isoformat()
        days = _days_since(test_date)

        assert days == 5

    def test_days_since_today(self):
        """Test calculating days since today."""
        today_str = date.today().isoformat()
        days = _days_since(today_str)

        assert days == 0

    def test_days_since_empty_string(self):
        """Test with empty string."""
        days = _days_since("")
        assert days == 999

    def test_days_since_none(self):
        """Test with None."""
        days = _days_since(None)
        assert days == 999

    def test_days_since_invalid_format(self):
        """Test with invalid date format."""
        days = _days_since("01/05/2024")
        assert days == 999

    def test_days_since_whitespace(self):
        """Test with whitespace in date string."""
        test_date = f"  {(date.today() - timedelta(days=3)).isoformat()}  "
        days = _days_since(test_date)

        assert days == 3


class TestGetPipeline:
    """Test get_pipeline function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_active_only(self, mock_read, mock_sid, sample_sheet_result, sample_values):
        """Test getting active pipeline (won/lost filtered out)."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        deals = get_pipeline(active_only=True)

        assert len(deals) == 2
        assert all(d["Stage"].lower() not in ("won", "lost") for d in deals)
        assert deals[0]["Client"] == "Acme"
        assert deals[1]["Client"] == "TechCorp"

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_all(self, mock_read, mock_sid, sample_sheet_result):
        """Test getting all deals including won/lost."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        deals = get_pipeline(active_only=False)

        assert len(deals) == 3
        assert any(d["Stage"].lower() == "won" for d in deals)

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_empty(self, mock_read, mock_sid, sample_sheet_result_empty):
        """Test getting pipeline when empty."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        deals = get_pipeline(active_only=True)

        assert deals == []


class TestCreateDeal:
    """Test create_deal function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.append_sheet")
    def test_create_deal_basic(self, mock_append, mock_read, mock_sid, sample_sheet_result_empty):
        """Test creating a basic deal."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=True)

        deal_data = {
            "client": "Test Corp",
            "contact": "Test User",
            "budget": "20000",
            "service": "Web Development",
        }

        result = create_deal(deal_data)

        assert result["ok"] is True
        assert result["row"] == 2
        assert result["client"] == "Test Corp"
        mock_append.assert_called_once()

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.append_sheet")
    def test_create_deal_with_referral(self, mock_append, mock_read, mock_sid, sample_sheet_result_empty):
        """Test creating a deal with referral."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=True)

        deal_data = {
            "client": "Referred Corp",
            "contact": "Test User",
            "referred_by": "Acme",
            "network_notes": "Met at conference",
        }

        result = create_deal(deal_data)

        assert result["ok"] is True

        # Check that source was set to "network"
        appended_row = mock_append.call_args[0][2][0]
        assert appended_row[2] == "network"  # Source column
        assert appended_row[17] == "Acme"  # Referred By column

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.append_sheet")
    def test_create_deal_appends_to_existing(self, mock_append, mock_read, mock_sid, sample_sheet_result):
        """Test creating a deal when pipeline already has deals."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=True)

        deal_data = {"client": "New Corp", "contact": "New User"}

        result = create_deal(deal_data)

        assert result["row"] == 5  # 3 existing + 1 for headers + 1 for new

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.append_sheet")
    def test_create_deal_failure(self, mock_append, mock_read, mock_sid, sample_sheet_result_empty):
        """Test creating a deal when append fails."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        from openclaw_crm.sheets import SheetResult
        mock_append.return_value = SheetResult(success=False, error="API error")

        deal_data = {"client": "Test Corp"}

        result = create_deal(deal_data)

        assert result["ok"] is False


class TestUpdateDeal:
    """Test update_deal function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_update_deal_success(self, mock_update, mock_read, mock_sid, sample_sheet_result):
        """Test updating a deal successfully."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        from openclaw_crm.sheets import SheetResult
        mock_update.return_value = SheetResult(success=True)

        result = update_deal(2, {"Stage": "proposal", "Budget": "20000"})

        assert result["ok"] is True
        mock_update.assert_called_once()

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_update_deal_row_out_of_range(self, mock_update, mock_read, mock_sid, sample_sheet_result):
        """Test updating with row out of range."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        result = update_deal(100, {"Stage": "proposal"})

        assert result["ok"] is False
        assert "out of range" in result["error"]
        mock_update.assert_not_called()

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_update_deal_invalid_key(self, mock_update, mock_read, mock_sid, sample_sheet_result):
        """Test updating with invalid key."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        from openclaw_crm.sheets import SheetResult
        mock_update.return_value = SheetResult(success=True)

        result = update_deal(2, {"InvalidKey": "value"})

        # Should still succeed, just ignore invalid key
        assert result["ok"] is True


class TestMoveStage:
    """Test move_stage function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_move_stage_success(self, mock_update, mock_read, mock_sid, sample_sheet_result):
        """Test moving a deal to a new stage successfully."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        from openclaw_crm.sheets import SheetResult
        mock_update.return_value = SheetResult(success=True)

        result = move_stage("Acme", "proposal")

        assert result["ok"] is True
        assert result["client"] == "Acme"
        assert result["stage"] == "proposal"
        assert result["row"] == 2
        mock_update.assert_called_once()

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_move_stage_client_not_found(self, mock_update, mock_read, mock_sid, sample_sheet_result):
        """Test moving stage for non-existent client."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        result = move_stage("NonExistent", "proposal")

        assert result["ok"] is False
        assert "not found" in result["error"]
        mock_update.assert_not_called()

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_move_stage_no_pipeline_data(self, mock_update, mock_read, mock_sid, sample_sheet_result_empty):
        """Test moving stage when pipeline is empty."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        result = move_stage("Acme", "proposal")

        assert result["ok"] is False
        assert "No pipeline data" in result["error"]

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    @patch("openclaw_crm.pipeline.update_sheet")
    def test_move_stage_case_insensitive(self, mock_update, mock_read, mock_sid, sample_sheet_result):
        """Test that client matching is case-insensitive."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        from openclaw_crm.sheets import SheetResult
        mock_update.return_value = SheetResult(success=True)

        result = move_stage("acme", "PROPOSAL")

        assert result["ok"] is True
        assert result["client"] == "Acme"  # Original case returned


class TestGetPipelineSummary:
    """Test get_pipeline_summary function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_summary_basic(self, mock_read, mock_sid, sample_sheet_result):
        """Test getting basic pipeline summary."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        summary = get_pipeline_summary()

        assert summary["total_deals"] == 2  # Excluding won
        assert summary["won_deals"] == 1
        assert summary["by_stage"]["lead"] == 1
        assert summary["by_stage"]["proposal"] == 1
        assert summary["network_count"] == 1
        assert summary["top_referrer"] == "Acme"

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_summary_weighted_value(self, mock_read, mock_sid, sample_sheet_result):
        """Test weighted pipeline value calculation."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        summary = get_pipeline_summary()

        # Expected: 15000 * 0.1 (lead) + 25000 * 0.5 (proposal) = 1500 + 12500 = 14000
        assert summary["total_weighted_value"] == 14000

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_summary_empty(self, mock_read, mock_sid, sample_sheet_result_empty):
        """Test getting summary when pipeline is empty."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        summary = get_pipeline_summary()

        assert summary["total_deals"] == 0
        assert summary["won_deals"] == 0
        assert summary["total_weighted_value"] == 0
        assert summary["network_count"] == 0
        assert summary["top_referrer"] == ""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_pipeline_summary_invalid_budget(self, mock_read, mock_sid, sample_values):
        """Test handling invalid budget values."""
        from openclaw_crm.sheets import SheetResult

        # Add a deal with invalid budget
        invalid_values = sample_values + [
            sample_values[0][:4] + ["invalid", "fixed", "Service", "2024-01-01", "2024-01-01", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
        ]
        mock_sid.return_value = "test_id"
        mock_read.return_value = SheetResult(success=True, data={"values": invalid_values})

        summary = get_pipeline_summary()

        # Should handle invalid budget gracefully (treat as 0)
        assert summary["total_weighted_value"] >= 0


class TestGetStaleDeals:
    """Test get_stale_deals function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_stale_deals_default_thresholds(self, mock_read, mock_sid, sample_sheet_result):
        """Test getting stale deals with default thresholds."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        buckets = get_stale_deals()

        assert 7 in buckets
        assert 14 in buckets
        assert 21 in buckets

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_stale_deals_custom_thresholds(self, mock_read, mock_sid, sample_sheet_result):
        """Test getting stale deals with custom thresholds."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        buckets = get_stale_deals(thresholds=[5, 10, 15])

        assert 5 in buckets
        assert 10 in buckets
        assert 15 in buckets
        assert 7 not in buckets

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_stale_deals_populates_days_stale(self, mock_read, mock_sid, sample_sheet_result):
        """Test that stale deals have _days_stale attribute."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result

        buckets = get_stale_deals()

        # All deals should have _days_stale if they're old enough
        for threshold, deals in buckets.items():
            for deal in deals:
                assert "_days_stale" in deal
                assert deal["_days_stale"] >= threshold


class TestGetOverdueInvoices:
    """Test get_overdue_invoices function."""

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_overdue_invoices(self, mock_read, mock_sid):
        """Test getting overdue invoices."""
        mock_sid.return_value = "test_id"

        # Create sample revenue data
        from openclaw_crm.sheets import SheetResult

        old_date = (date.today() - timedelta(days=40)).isoformat()
        recent_date = date.today().isoformat()

        values = [
            ["Client", "Amount", "Status", "Date", "Notes", "Invoice Link"],
            ["ClientA", "5000", "sent", old_date, "", ""],
            ["ClientB", "3000", "paid", old_date, "", ""],
            ["ClientC", "2000", "sent", recent_date, "", ""],
        ]

        mock_read.return_value = SheetResult(success=True, data={"values": values})

        overdue = get_overdue_invoices()

        assert len(overdue) == 1
        assert overdue[0]["Client"] == "ClientA"
        assert overdue[0]["_days_overdue"] == 40

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_overdue_invoices_empty(self, mock_read, mock_sid, sample_sheet_result_empty):
        """Test getting overdue invoices when no invoices."""
        mock_sid.return_value = "test_id"
        mock_read.return_value = sample_sheet_result_empty

        overdue = get_overdue_invoices()

        assert overdue == []

    @patch("openclaw_crm.pipeline.get_spreadsheet_id")
    @patch("openclaw_crm.pipeline.read_sheet")
    def test_get_overdue_invoices_exactly_30_days(self, mock_read, mock_sid):
        """Test that invoices exactly 30 days old are not overdue."""
        mock_sid.return_value = "test_id"

        from openclaw_crm.sheets import SheetResult

        exactly_30_days = (date.today() - timedelta(days=30)).isoformat()
        values = [
            ["Client", "Amount", "Status", "Date", "Notes", "Invoice Link"],
            ["ClientA", "5000", "sent", exactly_30_days, "", ""],
        ]

        mock_read.return_value = SheetResult(success=True, data={"values": values})

        overdue = get_overdue_invoices()

        assert len(overdue) == 0  # Not overdue (must be > 30 days)

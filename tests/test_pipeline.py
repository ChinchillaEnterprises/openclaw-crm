"""Tests for pipeline module."""

import pytest
from unittest.mock import patch, Mock

from openclaw_crm import pipeline
from openclaw_crm.sheets import SheetResult


class TestPipeline:
    """Test cases for pipeline module."""
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_get_pipeline(self, mock_get_backend):
        """Test getting pipeline data."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={
            "values": [
                ["Client", "Stage", "Budget", "Probability"],
                ["Acme", "lead", "10000", "25"],
                ["Beta", "proposal", "5000", "50"],
            ]
        })
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.get_pipeline()
            
            assert len(result) == 2
            assert result[0]["Client"] == "Acme"
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_create_deal(self, mock_get_backend):
        """Test creating a new deal."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={
            "values": [["Client", "Stage", "Budget"]]
        })
        mock_backend.append.return_value = SheetResult(success=True, data={})
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.create_deal({
                "client": "NewCorp",
                "budget": "15000",
                "stage": "lead"
            })
            
            assert result["ok"] is True
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_move_stage(self, mock_get_backend):
        """Test moving deal to different stage."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={
            "values": [
                ["Client", "Stage", "Budget"],
                ["Acme", "lead", "10000"],
            ]
        })
        mock_backend.update.return_value = SheetResult(success=True, data={})
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.move_stage("Acme", "proposal")
            
            assert result["ok"] is True
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_get_pipeline_summary(self, mock_get_backend):
        """Test pipeline summary calculation."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={
            "values": [
                ["Client", "Stage", "Budget", "Probability"],
                ["Acme", "lead", "10000", "25"],
                ["Beta", "proposal", "5000", "50"],
                ["Gamma", "won", "20000", "100"],
            ]
        })
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.get_pipeline_summary()
            
            assert "lead" in result["by_stage"]
            assert result["by_stage"]["lead"]["count"] == 1
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_get_stale_deals(self, mock_get_backend):
        """Test finding stale deals."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={
            "values": [
                ["Client", "Stage", "LastContact"],
                ["Acme", "lead", "2024-01-01"],
            ]
        })
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.get_stale_deals(7)
            
            assert isinstance(result, list)
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_empty_pipeline(self, mock_get_backend):
        """Test edge case: empty pipeline."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={"values": []})
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.get_pipeline()
            
            assert result == []
    
    @patch("openclaw_crm.pipeline.get_backend")
    def test_stage_normalization(self, mock_get_backend):
        """Test stage name normalization."""
        mock_backend = Mock()
        mock_backend.read.return_value = SheetResult(success=True, data={
            "values": [
                ["Client", "Stage"],
                ["Acme", "LEAD"],
            ]
        })
        mock_get_backend.return_value = mock_backend
        
        with patch("openclaw_crm.pipeline.get_spreadsheet_id", return_value="test"):
            result = pipeline.get_pipeline()
            
            # Stage should be normalized
            assert result[0]["Stage"].lower() == "lead"

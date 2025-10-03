"""
Tests for enhanced Phase 1: Goal & KPIs with Domain Compatibility
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

from app.services.phase1_goal_kpis import Phase1Service
from app.services.domain_packs import DOMAIN_PACKS, get_domain_pack, suggest_domain
from app.models.schemas import (
    DomainSelection, GoalDefinition, KPIDefinition, DomainType, GoalType, KPIType
)


class TestDomainPacks:
    """Test domain pack functionality"""
    
    def test_domain_packs_structure(self):
        """Test that domain packs have correct structure"""
        assert len(DOMAIN_PACKS) == 5
        assert "logistics" in DOMAIN_PACKS
        assert "healthcare" in DOMAIN_PACKS
        assert "emarketing" in DOMAIN_PACKS
        assert "retail" in DOMAIN_PACKS
        assert "finance" in DOMAIN_PACKS
    
    def test_domain_pack_content(self):
        """Test domain pack content"""
        logistics = DOMAIN_PACKS["logistics"]
        assert logistics.name == "logistics"
        assert len(logistics.kpis) > 0
        assert len(logistics.expected_columns) > 0
        assert len(logistics.expected_features) > 0
        assert logistics.description is not None
    
    def test_get_domain_pack(self):
        """Test getting domain pack by name"""
        pack = get_domain_pack("logistics")
        assert pack.name == "logistics"
        
        with pytest.raises(ValueError):
            get_domain_pack("nonexistent")
    
    def test_suggest_domain(self):
        """Test domain suggestion based on columns"""
        # Test with logistics columns
        logistics_cols = ["shipment_id", "order_id", "carrier", "origin", "destination"]
        suggestions = suggest_domain(logistics_cols)
        
        assert "logistics" in suggestions
        assert suggestions["logistics"] > 0.5  # Should have high match
        
        # Test with healthcare columns
        healthcare_cols = ["patient_id", "admission_ts", "discharge_ts", "department"]
        suggestions = suggest_domain(healthcare_cols)
        
        assert "healthcare" in suggestions
        assert suggestions["healthcare"] > 0.5


class TestPhase1ServiceEnhanced:
    """Test enhanced Phase 1 service with domain compatibility"""
    
    @pytest.fixture
    def service(self):
        """Create Phase1Service instance"""
        return Phase1Service()
    
    @pytest.fixture
    def sample_columns(self):
        """Sample columns for testing"""
        return ["shipment_id", "order_id", "carrier", "origin", "destination", "pickup_date"]
    
    def test_domain_compatibility_ok(self, service, sample_columns):
        """Test domain compatibility with high match"""
        result = service.check_domain_compatibility("logistics", sample_columns)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.status == "OK"
        assert result.data.match_percentage >= 0.7
        assert len(result.data.matched_columns) > 0
        assert len(result.data.suggestions) == 0  # No suggestions for OK status
    
    def test_domain_compatibility_warn(self, service):
        """Test domain compatibility with partial match"""
        partial_cols = ["shipment_id", "carrier"]  # Only 2 out of 10 logistics columns
        result = service.check_domain_compatibility("logistics", partial_cols)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.status == "WARN"
        assert 0.3 <= result.data.match_percentage < 0.7
        assert len(result.data.suggestions) > 0
    
    def test_domain_compatibility_stop(self, service):
        """Test domain compatibility with low match"""
        unrelated_cols = ["random_col1", "random_col2", "random_col3"]
        result = service.check_domain_compatibility("logistics", unrelated_cols)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.status == "STOP"
        assert result.data.match_percentage < 0.3
        assert len(result.data.suggestions) > 0
    
    def test_domain_compatibility_invalid_domain(self, service, sample_columns):
        """Test domain compatibility with invalid domain"""
        result = service.check_domain_compatibility("nonexistent", sample_columns)
        
        assert result.status == "error"
        assert "not found" in result.message
    
    def test_domain_compatibility_empty_columns(self, service):
        """Test domain compatibility with empty columns"""
        result = service.check_domain_compatibility("logistics", [])
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.status == "STOP"
        assert result.data.match_percentage == 0.0


class TestPhase1Integration:
    """Test Phase 1 integration with existing functionality"""
    
    @pytest.fixture
    def service(self):
        """Create Phase1Service instance"""
        return Phase1Service()
    
    def test_save_domain_selection(self, service):
        """Test saving domain selection"""
        domain_selection = DomainSelection(
            domain=DomainType.RETAIL,
            subdomain="e-commerce",
            industry_context="Online retail platform"
        )
        
        result = service.save_domain_selection(domain_selection)
        assert result.status == "success"
        assert "retail" in result.message.lower()
    
    def test_add_goal(self, service):
        """Test adding a goal"""
        goal = GoalDefinition(
            goal_id="test_goal_1",
            title="Test Goal",
            description="This is a test goal for unit testing",
            goal_type=GoalType.PREDICTION,
            domain=DomainType.RETAIL,
            priority=3,
            target_metric="accuracy",
            target_value=0.85,
            success_criteria="Achieve 85% accuracy in prediction model"
        )
        
        result = service.add_goal(goal)
        assert result.status == "success"
        assert "added successfully" in result.message
    
    def test_add_kpi(self, service):
        """Test adding a KPI"""
        kpi = KPIDefinition(
            kpi_id="test_kpi_1",
            name="Test KPI",
            description="This is a test KPI for unit testing",
            kpi_type=KPIType.ACCURACY,
            target_value=0.85,
            threshold_min=0.8,
            threshold_max=0.9,
            unit="percentage",
            calculation_method="Correct predictions / Total predictions",
            is_primary=True
        )
        
        result = service.add_kpi(kpi)
        assert result.status == "success"
        assert "added successfully" in result.message
    
    def test_validate_config(self, service):
        """Test configuration validation"""
        result = service.validate_config()
        assert result.status in ["success", "warning"]
        assert "data" in result.dict()
        
        if result.data:
            assert "domain_selected" in result.data
            assert "goals_count" in result.data
            assert "kpis_count" in result.data
            assert "is_complete" in result.data


class TestDomainCompatibilityWorkflow:
    """Test domain compatibility workflow scenarios"""
    
    @pytest.fixture
    def service(self):
        """Create Phase1Service instance"""
        return Phase1Service()
    
    def test_logistics_workflow(self, service):
        """Test complete logistics workflow"""
        logistics_cols = [
            "shipment_id", "order_id", "carrier", "origin", "destination",
            "pickup_date", "delivery_date", "status", "transit_time", "dwell_time"
        ]
        
        result = service.check_domain_compatibility("logistics", logistics_cols)
        
        assert result.status == "success"
        assert result.data.status == "OK"
        assert result.data.match_percentage == 1.0  # Perfect match
        assert len(result.data.matched_columns) == 10
        assert len(result.data.missing_columns) == 0
    
    def test_healthcare_workflow(self, service):
        """Test complete healthcare workflow"""
        healthcare_cols = [
            "patient_id", "admission_ts", "discharge_ts", "department",
            "diagnosis", "procedure", "los_days", "age", "gender"
        ]
        
        result = service.check_domain_compatibility("healthcare", healthcare_cols)
        
        assert result.status == "success"
        assert result.data.status == "OK"
        assert result.data.match_percentage == 1.0  # Perfect match
    
    def test_mixed_domain_workflow(self, service):
        """Test workflow with mixed domain columns"""
        mixed_cols = [
            "shipment_id", "patient_id", "campaign_id", "order_id", "account_id"
        ]
        
        result = service.check_domain_compatibility("logistics", mixed_cols)
        
        assert result.status == "success"
        assert result.data.status in ["WARN", "STOP"]  # Partial match
        assert len(result.data.suggestions) > 0  # Should suggest alternatives


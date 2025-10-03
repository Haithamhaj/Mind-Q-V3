"""
Tests for GoalKPIsService - Phase 1 Goal & KPIs
"""

import pytest
from app.services.phase1_goal_kpis import GoalKPIsService, GoalKPIsResult, DomainCompatibilityResult


class TestGoalKPIsService:
    """Test GoalKPIsService functionality"""
    
    def test_auto_domain_suggestion(self):
        """Test automatic domain suggestion when no domain specified"""
        columns = ["shipment_id", "order_id", "carrier", "origin", "destination"]
        
        service = GoalKPIsService(columns=columns, domain=None)
        result = service.run()
        
        assert isinstance(result, GoalKPIsResult)
        assert result.domain == "logistics"  # Should auto-suggest logistics
        assert len(result.kpis) > 0
        assert isinstance(result.compatibility, DomainCompatibilityResult)
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
    
    def test_specified_domain_compatible(self):
        """Test with specified compatible domain"""
        columns = ["shipment_id", "order_id", "carrier", "origin", "destination"]
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
        assert len(result.compatibility.matched_columns) > 0
        assert len(result.compatibility.missing_columns) >= 0
    
    def test_specified_domain_warn(self):
        """Test with specified domain that has partial compatibility"""
        columns = ["shipment_id", "carrier"]  # Only 2 out of 10 logistics columns
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status == "WARN"
        assert 0.3 <= result.compatibility.match_percentage < 0.7
        assert len(result.compatibility.suggestions) > 0
        assert "Consider" in result.compatibility.message
    
    def test_specified_domain_stop(self):
        """Test with specified domain that has low compatibility"""
        columns = ["random_col1", "random_col2", "random_col3"]
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status == "STOP"
        assert result.compatibility.match_percentage < 0.3
        assert len(result.compatibility.suggestions) > 0
        assert "incompatible" in result.compatibility.message
        assert "Use" in result.compatibility.message
    
    def test_healthcare_domain(self):
        """Test with healthcare domain"""
        columns = ["patient_id", "admission_ts", "discharge_ts", "department", "diagnosis"]
        
        service = GoalKPIsService(columns=columns, domain="healthcare")
        result = service.run()
        
        assert result.domain == "healthcare"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
        assert "healthcare" in result.kpis or any("health" in kpi.lower() for kpi in result.kpis)
    
    def test_retail_domain(self):
        """Test with retail domain"""
        columns = ["order_id", "customer_id", "order_date", "product_id", "quantity", "price"]
        
        service = GoalKPIsService(columns=columns, domain="retail")
        result = service.run()
        
        assert result.domain == "retail"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
        assert "retail" in result.kpis or any("retail" in kpi.lower() for kpi in result.kpis)
    
    def test_finance_domain(self):
        """Test with finance domain"""
        columns = ["account_id", "customer_id", "account_type", "balance", "open_date"]
        
        service = GoalKPIsService(columns=columns, domain="finance")
        result = service.run()
        
        assert result.domain == "finance"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
        assert "finance" in result.kpis or any("finance" in kpi.lower() for kpi in result.kpis)
    
    def test_emarketing_domain(self):
        """Test with e-marketing domain"""
        columns = ["campaign_id", "date", "channel", "spend", "impressions", "clicks"]
        
        service = GoalKPIsService(columns=columns, domain="emarketing")
        result = service.run()
        
        assert result.domain == "emarketing"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
        assert "emarketing" in result.kpis or any("marketing" in kpi.lower() for kpi in result.kpis)
    
    def test_empty_columns(self):
        """Test with empty columns list"""
        columns = []
        
        service = GoalKPIsService(columns=columns, domain=None)
        result = service.run()
        
        # Should still work, but with low compatibility
        assert isinstance(result, GoalKPIsResult)
        assert result.compatibility.status in ["WARN", "STOP"]
        assert result.compatibility.match_percentage == 0.0
    
    def test_mixed_domain_columns(self):
        """Test with columns from multiple domains"""
        columns = ["shipment_id", "patient_id", "campaign_id", "order_id", "account_id"]
        
        service = GoalKPIsService(columns=columns, domain=None)
        result = service.run()
        
        # Should auto-suggest the best matching domain
        assert isinstance(result, GoalKPIsResult)
        assert result.compatibility.status in ["OK", "WARN", "STOP"]
        assert len(result.compatibility.suggestions) > 0


class TestDomainCompatibilityResult:
    """Test DomainCompatibilityResult model"""
    
    def test_domain_compatibility_result_structure(self):
        """Test DomainCompatibilityResult structure"""
        result = DomainCompatibilityResult(
            status="OK",
            domain="logistics",
            match_percentage=0.8,
            matched_columns=["shipment_id", "order_id"],
            missing_columns=["carrier", "origin"],
            suggestions={"logistics": 0.8, "retail": 0.2},
            message="Domain 'logistics' is compatible (80.0% match)"
        )
        
        assert result.status == "OK"
        assert result.domain == "logistics"
        assert result.match_percentage == 0.8
        assert len(result.matched_columns) == 2
        assert len(result.missing_columns) == 2
        assert len(result.suggestions) == 2
        assert "compatible" in result.message


class TestGoalKPIsResult:
    """Test GoalKPIsResult model"""
    
    def test_goal_kpis_result_structure(self):
        """Test GoalKPIsResult structure"""
        compatibility = DomainCompatibilityResult(
            status="OK",
            domain="logistics",
            match_percentage=0.8,
            matched_columns=["shipment_id", "order_id"],
            missing_columns=["carrier", "origin"],
            suggestions={"logistics": 0.8},
            message="Domain 'logistics' is compatible (80.0% match)"
        )
        
        result = GoalKPIsResult(
            domain="logistics",
            kpis=["SLA_pct", "TransitTime_avg", "RTO_pct"],
            compatibility=compatibility
        )
        
        assert result.domain == "logistics"
        assert len(result.kpis) == 3
        assert isinstance(result.compatibility, DomainCompatibilityResult)
        assert result.compatibility.status == "OK"


class TestGoalKPIsServiceIntegration:
    """Test GoalKPIsService integration scenarios"""
    
    def test_logistics_workflow(self):
        """Test complete logistics workflow"""
        columns = [
            "shipment_id", "order_id", "carrier", "origin", "destination",
            "pickup_date", "delivery_date", "status", "transit_time", "dwell_time"
        ]
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage == 1.0  # Perfect match
        assert len(result.compatibility.matched_columns) == 10
        assert len(result.compatibility.missing_columns) == 0
        assert len(result.kpis) > 0
    
    def test_healthcare_workflow(self):
        """Test complete healthcare workflow"""
        columns = [
            "patient_id", "admission_ts", "discharge_ts", "department",
            "diagnosis", "procedure", "los_days", "age", "gender"
        ]
        
        service = GoalKPIsService(columns=columns, domain="healthcare")
        result = service.run()
        
        assert result.domain == "healthcare"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage == 1.0  # Perfect match
        assert len(result.compatibility.matched_columns) == 9
        assert len(result.compatibility.missing_columns) == 0
        assert len(result.kpis) > 0
    
    def test_auto_suggestion_workflow(self):
        """Test auto-suggestion workflow"""
        columns = ["campaign_id", "date", "channel", "spend", "impressions", "clicks", "conversions"]
        
        service = GoalKPIsService(columns=columns, domain=None)
        result = service.run()
        
        # Should auto-suggest e-marketing
        assert result.domain == "emarketing"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage >= 0.7
        assert len(result.kpis) > 0
    
    def test_warning_workflow(self):
        """Test warning workflow with partial compatibility"""
        columns = ["shipment_id", "carrier", "origin"]  # Partial logistics match
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status == "WARN"
        assert 0.3 <= result.compatibility.match_percentage < 0.7
        assert len(result.compatibility.suggestions) > 0
        assert "Consider" in result.compatibility.message
    
    def test_stop_workflow(self):
        """Test stop workflow with incompatible domain"""
        columns = ["random_col1", "random_col2", "random_col3"]
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status == "STOP"
        assert result.compatibility.match_percentage < 0.3
        assert len(result.compatibility.suggestions) > 0
        assert "incompatible" in result.compatibility.message
        assert "Use" in result.compatibility.message



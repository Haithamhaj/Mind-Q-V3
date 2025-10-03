import pytest
from datetime import datetime
from app.services.phase1_goal_kpis import Phase1Service
from app.models.schemas import (
    DomainSelection, GoalDefinition, KPIDefinition, 
    DomainType, GoalType, KPIType
)


def test_get_available_domains():
    """Test that all domains are available"""
    service = Phase1Service()
    domains = service.get_available_domains()
    
    assert len(domains) == 8
    domain_names = [d.domain.value for d in domains]
    expected_domains = ["finance", "healthcare", "retail", "manufacturing", 
                       "technology", "education", "government", "general"]
    
    for expected in expected_domains:
        assert expected in domain_names


def test_get_domain_info():
    """Test getting specific domain information"""
    service = Phase1Service()
    finance_info = service.get_domain_info(DomainType.FINANCE)
    
    assert finance_info is not None
    assert finance_info.domain == DomainType.FINANCE
    assert finance_info.name == "Finance"
    assert len(finance_info.common_goals) > 0
    assert len(finance_info.typical_kpis) > 0


def test_save_domain_selection():
    """Test saving domain selection"""
    service = Phase1Service()
    domain_selection = DomainSelection(
        domain=DomainType.RETAIL,
        subdomain="E-commerce",
        industry_context="Online retail platform"
    )
    
    result = service.save_domain_selection(domain_selection)
    
    assert result.status == "success"
    assert "retail" in result.message.lower()


def test_add_goal():
    """Test adding a business goal"""
    service = Phase1Service()
    goal = GoalDefinition(
        goal_id="goal_001",
        title="Customer Segmentation",
        description="Segment customers based on purchasing behavior and demographics",
        goal_type=GoalType.CLUSTERING,
        domain=DomainType.RETAIL,
        priority=3,
        target_metric="Segmentation Accuracy",
        success_criteria="Achieve 85% accuracy in customer segmentation"
    )
    
    result = service.add_goal(goal)
    
    assert result.status == "success"
    assert goal.goal_id in result.data["goal_id"]


def test_add_kpi():
    """Test adding a KPI"""
    service = Phase1Service()
    kpi = KPIDefinition(
        kpi_id="kpi_001",
        name="Customer Lifetime Value",
        description="Average revenue per customer over their lifetime",
        kpi_type=KPIType.BUSINESS_METRIC,
        target_value=1000.0,
        unit="USD",
        calculation_method="Sum of all customer purchases / Number of customers",
        is_primary=True
    )
    
    result = service.add_kpi(kpi)
    
    assert result.status == "success"
    assert kpi.kpi_id in result.data["kpi_id"]


def test_duplicate_goal_id():
    """Test that duplicate goal IDs are rejected"""
    service = Phase1Service()
    goal = GoalDefinition(
        goal_id="duplicate_goal",
        title="Test Goal",
        description="A test goal for duplicate testing",
        goal_type=GoalType.PREDICTION,
        domain=DomainType.GENERAL,
        priority=1,
        target_metric="Test Metric",
        success_criteria="Test success criteria"
    )
    
    # Add first goal
    result1 = service.add_goal(goal)
    assert result1.status == "success"
    
    # Try to add duplicate
    result2 = service.add_goal(goal)
    assert result2.status == "error"
    assert "already exists" in result2.message


def test_validate_config():
    """Test configuration validation"""
    service = Phase1Service()
    
    # Test empty configuration
    result = service.validate_config()
    assert result.status == "warning"
    assert not result.data["is_complete"]
    
    # Add domain selection
    domain_selection = DomainSelection(domain=DomainType.FINANCE)
    service.save_domain_selection(domain_selection)
    
    # Add goal
    goal = GoalDefinition(
        goal_id="test_goal",
        title="Test Goal",
        description="Test goal description",
        goal_type=GoalType.PREDICTION,
        domain=DomainType.FINANCE,
        priority=1,
        target_metric="Test Metric",
        success_criteria="Test criteria"
    )
    service.add_goal(goal)
    
    # Add KPI
    kpi = KPIDefinition(
        kpi_id="test_kpi",
        name="Test KPI",
        description="Test KPI description",
        kpi_type=KPIType.ACCURACY,
        calculation_method="Test calculation",
        is_primary=True
    )
    service.add_kpi(kpi)
    
    # Validate complete configuration
    result = service.validate_config()
    assert result.status == "success"
    assert result.data["is_complete"]


def test_get_config():
    """Test getting configuration"""
    service = Phase1Service()
    
    # Add some test data
    domain_selection = DomainSelection(domain=DomainType.HEALTHCARE)
    service.save_domain_selection(domain_selection)
    
    result = service.get_config()
    
    assert result.status == "success"
    assert result.data is not None
    assert "domain_selection" in result.data
    assert "goals" in result.data
    assert "kpis" in result.data

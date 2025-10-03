"""
Pytest configuration and fixtures
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000, 60000, 70000, 80000, 90000],
        'date': pd.date_range('2024-01-01', periods=5)
    })


@pytest.fixture
def sample_dataframe_with_issues():
    """Sample DataFrame with data quality issues"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', None, 'Charlie', None, 'Eve'],  # Missing values
        'age': [25, 30, 35, 40, 45],
        'salary': [50000, None, None, None, 90000],  # High missing %
        'date': pd.date_range('2024-01-01', periods=5)
    })


@pytest.fixture
def temp_artifacts_dir():
    """Temporary artifacts directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_goal():
    """Sample goal for testing"""
    from app.models.schemas import GoalDefinition, GoalType, DomainType
    
    return GoalDefinition(
        goal_id="test_goal_001",
        title="Test Prediction Goal",
        description="A test goal for unit testing purposes",
        goal_type=GoalType.PREDICTION,
        domain=DomainType.GENERAL,
        priority=3,
        target_metric="Test Accuracy",
        target_value=0.85,
        success_criteria="Achieve 85% accuracy in test predictions"
    )


@pytest.fixture
def sample_kpi():
    """Sample KPI for testing"""
    from app.models.schemas import KPIDefinition, KPIType
    
    return KPIDefinition(
        kpi_id="test_kpi_001",
        name="Test Accuracy",
        description="Test accuracy metric for unit testing",
        kpi_type=KPIType.ACCURACY,
        target_value=0.85,
        threshold_min=0.80,
        threshold_max=0.95,
        unit="percentage",
        calculation_method="Correct predictions / Total predictions",
        is_primary=True
    )


@pytest.fixture
def sample_domain_selection():
    """Sample domain selection for testing"""
    from app.models.schemas import DomainSelection, DomainType
    
    return DomainSelection(
        domain=DomainType.RETAIL,
        subdomain="E-commerce",
        industry_context="Online retail platform",
        data_sources=["Customer database", "Transaction logs"],
        regulatory_requirements=["GDPR compliance"],
        business_constraints=["Data privacy", "Real-time processing"]
    )

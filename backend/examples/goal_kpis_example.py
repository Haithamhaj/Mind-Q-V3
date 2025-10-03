"""
Example usage of GoalKPIsService - Phase 1 Goal & KPIs
"""

from app.services.phase1_goal_kpis import GoalKPIsService


def example_logistics():
    """Example with logistics domain"""
    print("=== Logistics Domain Example ===")
    
    columns = ["shipment_id", "order_id", "carrier", "origin", "destination"]
    service = GoalKPIsService(columns=columns, domain="logistics")
    result = service.run()
    
    print(f"Domain: {result.domain}")
    print(f"Status: {result.compatibility.status}")
    print(f"Match: {result.compatibility.match_percentage:.1%}")
    print(f"KPIs: {result.kpis}")
    print(f"Message: {result.compatibility.message}")
    print()


def example_healthcare():
    """Example with healthcare domain"""
    print("=== Healthcare Domain Example ===")
    
    columns = ["patient_id", "admission_ts", "discharge_ts", "department", "diagnosis"]
    service = GoalKPIsService(columns=columns, domain="healthcare")
    result = service.run()
    
    print(f"Domain: {result.domain}")
    print(f"Status: {result.compatibility.status}")
    print(f"Match: {result.compatibility.match_percentage:.1%}")
    print(f"KPIs: {result.kpis}")
    print(f"Message: {result.compatibility.message}")
    print()


def example_auto_suggestion():
    """Example with auto-suggestion"""
    print("=== Auto-Suggestion Example ===")
    
    columns = ["campaign_id", "date", "channel", "spend", "impressions", "clicks"]
    service = GoalKPIsService(columns=columns, domain=None)  # No domain specified
    result = service.run()
    
    print(f"Auto-suggested Domain: {result.domain}")
    print(f"Status: {result.compatibility.status}")
    print(f"Match: {result.compatibility.match_percentage:.1%}")
    print(f"KPIs: {result.kpis}")
    print(f"Message: {result.compatibility.message}")
    print()


def example_warning():
    """Example with warning status"""
    print("=== Warning Status Example ===")
    
    columns = ["shipment_id", "carrier"]  # Partial match
    service = GoalKPIsService(columns=columns, domain="logistics")
    result = service.run()
    
    print(f"Domain: {result.domain}")
    print(f"Status: {result.compatibility.status}")
    print(f"Match: {result.compatibility.match_percentage:.1%}")
    print(f"Matched Columns: {result.compatibility.matched_columns}")
    print(f"Missing Columns: {result.compatibility.missing_columns}")
    print(f"Suggestions: {result.compatibility.suggestions}")
    print(f"Message: {result.compatibility.message}")
    print()


def example_stop():
    """Example with stop status"""
    print("=== Stop Status Example ===")
    
    columns = ["random_col1", "random_col2", "random_col3"]
    service = GoalKPIsService(columns=columns, domain="logistics")
    result = service.run()
    
    print(f"Domain: {result.domain}")
    print(f"Status: {result.compatibility.status}")
    print(f"Match: {result.compatibility.match_percentage:.1%}")
    print(f"Suggestions: {result.compatibility.suggestions}")
    print(f"Message: {result.compatibility.message}")
    print()


if __name__ == "__main__":
    print("GoalKPIsService Examples")
    print("=" * 50)
    print()
    
    example_logistics()
    example_healthcare()
    example_auto_suggestion()
    example_warning()
    example_stop()
    
    print("All examples completed!")

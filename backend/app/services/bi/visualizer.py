from __future__ import annotations
import plotly.graph_objects as go
from typing import Dict


class Visualizer:
    """Generate Plotly charts from query results"""
    
    def __init__(self, language: str = "en"):
        self.language = language
    
    def generate(self, query_result: Dict, parsed_query) -> Dict:
        """Generate appropriate chart based on query type"""
        
        data = query_result.get("data", {})
        intent = parsed_query.intent
        entities = parsed_query.entities if isinstance(parsed_query.entities, dict) else {}
        
        if intent == "aggregate" and entities.get("dimension"):
            return self._bar_chart(data, entities)
        elif intent == "trend":
            return self._line_chart(data, entities)
        elif intent == "compare":
            return self._grouped_bar_chart(data, entities)
        else:
            return self._metric_card(data, entities)
    
    def _bar_chart(self, data: Dict, entities: Dict) -> Dict:
        """Create bar chart for dimension-based aggregation"""
        
        entities = entities if isinstance(entities, dict) else {}
        metric = entities.get("metric", "Value")
        dimension = entities.get("dimension", "Category")
        
        fig = go.Figure(data=[
            go.Bar(
                x=list(data.keys()),
                y=list(data.values()),
                marker_color='rgb(55, 83, 109)',
                text=[f"{v:.1f}" for v in data.values()],
                textposition='auto'
            )
        ])
        
        title = f"{metric} by {dimension}" if self.language == "en" else f"{metric} حسب {dimension}"
        
        fig.update_layout(
            title=title,
            xaxis_title=dimension,
            yaxis_title=metric,
            font=dict(family="Arial, sans-serif", size=14),
            template="plotly_white",
            height=400
        )
        
        return {
            "type": "bar",
            "config": fig.to_json(),
            "meta": {"metric": metric, "dimension": dimension},
            "data": data
        }
    
    def _line_chart(self, data: Dict, entities: Dict) -> Dict:
        """Create line chart for trends"""
        
        entities = entities if isinstance(entities, dict) else {}
        metric = entities.get("metric", "Value")
        
        fig = go.Figure(data=[
            go.Scatter(
                x=list(data.keys()),
                y=list(data.values()),
                mode='lines+markers',
                line=dict(color='rgb(55, 83, 109)', width=2),
                marker=dict(size=8)
            )
        ])
        
        title = f"{metric} Trend" if self.language == "en" else f"اتجاه {metric}"
        
        fig.update_layout(
            title=title,
            xaxis_title="Date" if self.language == "en" else "التاريخ",
            yaxis_title=metric,
            font=dict(family="Arial, sans-serif", size=14),
            template="plotly_white",
            height=400
        )
        
        return {
            "type": "line",
            "config": fig.to_json(),
            "meta": {"metric": metric},
            "data": data
        }
    
    def _grouped_bar_chart(self, data: Dict, entities: Dict) -> Dict:
        """Create grouped bar chart for comparisons"""
        
        entities = entities if isinstance(entities, dict) else {}
        metric = entities.get("metric", "Value")
        dimension = entities.get("dimension", "Category")
        
        print(f"🔍 _grouped_bar_chart data: {data}")
        print(f"🔍 _grouped_bar_chart data type: {type(data)}")
        
        categories = list(data.keys())
        means = []
        medians = []
        
        for k in categories:
            value = data[k]
            print(f"🔍 Key: {k}, Value: {value}, Type: {type(value)}")
            
            # Handle error cases
            if isinstance(value, str) and 'error' in value.lower():
                print(f"❌ Error in data: {value}")
                continue
            
            if isinstance(value, dict):
                means.append(value.get('mean', 0))
                medians.append(value.get('median', 0))
            else:
                # If value is not a dict (e.g., it's a number), use it directly
                try:
                    # Handle numpy types
                    if hasattr(value, 'item'):
                        value = value.item()  # Convert numpy types to Python types
                    
                    means.append(float(value) if value is not None else 0)
                    medians.append(float(value) if value is not None else 0)
                except (ValueError, TypeError) as e:
                    print(f"❌ Could not convert value {value} to float: {e}")
                    continue
        
        # Check if we have valid data
        if not means or not medians:
            print("❌ No valid data for grouped bar chart")
            return {
                "type": "metric",
                "value": "No data available",
                "title": f"{metric} Comparison by {dimension}",
                "data": data
            }
        
        fig = go.Figure(data=[
            go.Bar(name='Mean', x=categories, y=means),
            go.Bar(name='Median', x=categories, y=medians)
        ])
        
        title = f"{metric} Comparison by {dimension}" if self.language == "en" else f"مقارنة {metric} حسب {dimension}"
        
        fig.update_layout(
            title=title,
            barmode='group',
            font=dict(family="Arial, sans-serif", size=14),
            template="plotly_white",
            height=400
        )
        
        return {
            "type": "grouped_bar",
            "config": fig.to_json(),
            "meta": {"metric": metric, "dimension": dimension},
            "data": data
        }
    
    def _metric_card(self, data: Dict, entities: Dict) -> Dict:
        """Create simple metric card"""
        
        entities = entities if isinstance(entities, dict) else {}
        value = data.get("overall", "N/A")
        metric = entities.get("metric", "Metric")
        
        return {
            "type": "metric",
            "value": value,
            "title": metric,
            "data": data
        }

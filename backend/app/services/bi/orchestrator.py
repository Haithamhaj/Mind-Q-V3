from __future__ import annotations
import pandas as pd
from typing import Dict, Any, List, Callable
from pydantic import BaseModel

from .executor import Executor
from .stats_signals import build_signals
from .rule_recommender import recommend_from_signals
from .chart_explainer import explain_chart
from .query_parser import QueryParser
from .visualizer import Visualizer


class BIResponse(BaseModel):
    query: str
    parsed: Dict[str, Any]
    chart: Dict[str, Any]
    explanation: Dict[str, Any]
    recommendations: List[Dict]
    language: str
    signals_meta: Dict[str, Any]


class BIOrchestrator:
    """Main BI orchestrator - coordinates all components"""
    
    def __init__(
        self,
        df: pd.DataFrame,
        domain: str,
        time_window: str,
        llm_call: Callable[[str], str]
    ):
        self.df = df
        self.domain = domain
        self.time_window = time_window
        self.llm_call = llm_call
        # Initialize components
        self.executor = Executor(df)
        self.parser = QueryParser(domain, df)  # Pass dataframe to parser for dynamic analysis
        
        # Build signals once (cached)
        self.signals = build_signals(df, domain, time_window, key_cols=None)
    
    def process_question(self, user_question: str) -> BIResponse:
        """
        Process natural language question end-to-end
        
        Flow:
        1. Parse question (NL → structured)
        2. Execute query
        3. Generate chart
        4. Get rule-based recommendations
        5. Generate LLM explanation (with guardrails)
        6. Return complete response
        """
        
        # Step 1: Parse question
        parsed_query = self.parser.parse(user_question, self.llm_call)
        
        # Debug: Print parsed query details
        print(f"🔍 Parsed Query: {parsed_query}")
        print(f"🔍 Entities type: {type(parsed_query.entities)}")
        print(f"🔍 Entities value: {parsed_query.entities}")
        print(f"🔍 Filters type: {type(parsed_query.filters)}")
        print(f"🔍 Filters value: {parsed_query.filters}")
        
        lang = parsed_query.language
        intent = parsed_query.intent
        entities = parsed_query.entities if isinstance(parsed_query.entities, dict) else {}
        filters = parsed_query.filters if isinstance(parsed_query.filters, dict) else {}
        metric = entities.get("metric") if entities else None
        dimension = entities.get("dimension") if entities else None
        agg = parsed_query.aggregation
        
        if intent == "aggregate":
            data = self.executor.aggregate(metric, agg, filters, dimension)
        elif intent == "compare":
            data = self.executor.compare(metric, dimension, filters)
        elif intent == "trend":
            data = self.executor.trend(metric, "D", filters)
        elif intent == "overview":
            data = self.executor.overview()
        else:
            data = {"error": "unknown intent"}
        
        query_result = {"data": data}
        
        visualizer = Visualizer(language=lang)
        chart = visualizer.generate(query_result, parsed_query)
        
        # Step 4: Rule-based recommendations (pre-LLM)
        pre_recs = recommend_from_signals(self.signals)
        
        # Step 5: LLM explanation with guardrails
        try:
            explanation = explain_chart(self.signals, chart, lang, self.llm_call)
        except Exception as e:
            # Fallback if LLM fails
            explanation = {
                "summary": f"Data analysis for {metric}" if lang == "en" else f"تحليل البيانات لـ{metric}",
                "findings": ["Analysis completed"],
                "recommendation": "Review the chart for insights"
            }
        
        chart = self._convert_numpy_types(chart)
        
        return BIResponse(
            query=user_question,
            parsed=parsed_query.dict(),
            chart=chart,
            explanation=explanation,
            recommendations=pre_recs,
            language=lang,
            signals_meta=self.signals["meta"]
        )
    
    def _convert_numpy_types(self, obj):
        """Convert numpy types to Python types for JSON serialization"""
        import numpy as np
        
        if isinstance(obj, dict):
            return {key: self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return obj
    
    def get_signals(self) -> Dict[str, Any]:
        """Get complete signals JSON"""
        return self.signals
    
    def get_kpis(self) -> Dict[str, Any]:
        """Get KPIs only"""
        return self.signals.get("kpis", {})

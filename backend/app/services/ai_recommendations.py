"""
AI-Powered Automated Recommendations Service
Generates intelligent recommendations based on comprehensive data analysis from all phases
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import pandas as pd
from pathlib import Path
from pydantic import BaseModel

from ..config import settings
from .bi.llm_client import llm_client


class Recommendation(BaseModel):
    title: str
    description: str
    type: str = "data_insight"  # "data_insight", "best_practice", "hidden_pattern", "user_check", "future_action", "warning"
    category: str = "general"  # "data_quality", "performance", "insights", "optimization", "compliance"
    severity: str = "medium"  # "high", "medium", "low"
    priority: int = 3  # 1-5 (1 = highest priority)
    actionable: bool = True
    estimated_impact: str = "medium"  # "high", "medium", "low"
    implementation_effort: str = "medium"  # "easy", "medium", "complex"
    confidence: float = 0.8  # 0.0 to 1.0
    related_phases: List[str] = []
    business_value: str = ""
    evidence: str = ""  # Supporting data/metrics for the recommendation
    verification_needed: bool = False  # Whether user should verify this finding


class RecommendationAnalysis(BaseModel):
    domain: str
    total_phases_completed: int
    data_quality_score: float
    overall_health_score: float
    recommendations: List[Recommendation]
    summary: str
    next_steps: List[str]
    generated_at: datetime


class AIRecommendationsService:
    def __init__(self, domain: str, phase_results: Dict[str, Any]):
        self.domain = domain
        self.phase_results = phase_results
        self.artifacts_dir = settings.artifacts_dir
    
    def generate_recommendations(self) -> RecommendationAnalysis:
        """Generate comprehensive AI-powered recommendations"""
        try:
            # Analyze all available data
            analysis_context = self._build_analysis_context()
            
            # Generate recommendations using AI
            ai_recommendations = self._get_ai_recommendations(analysis_context)
            
            # Calculate health scores
            data_quality_score = self._calculate_data_quality_score()
            overall_health_score = self._calculate_overall_health_score()
            
            # Generate summary and next steps
            summary = self._generate_summary(ai_recommendations)
            next_steps = self._generate_next_steps(ai_recommendations)
            
            return RecommendationAnalysis(
                domain=self.domain,
                total_phases_completed=len(self.phase_results),
                data_quality_score=data_quality_score,
                overall_health_score=overall_health_score,
                recommendations=ai_recommendations,
                summary=summary,
                next_steps=next_steps,
                generated_at=datetime.utcnow()
            )
            
        except Exception as e:
            print(f"Error generating recommendations: {e}")
            # Return minimal recommendations on error
            return self._get_fallback_recommendations()
    
    def _build_analysis_context(self) -> str:
        """Build comprehensive context for AI analysis"""
        context_parts = []
        
        # Domain context
        context_parts.append(f"DOMAIN ANALYSIS: {self.domain.upper()}")
        context_parts.append("=" * 50)
        
        # Phase results summary
        context_parts.append("PHASE RESULTS SUMMARY:")
        for phase_id, result in self.phase_results.items():
            if result.get('status') == 'success':
                context_parts.append(f"SUCCESS {phase_id}: {result.get('message', 'Completed successfully')}")
            else:
                context_parts.append(f"FAILED {phase_id}: {result.get('error', 'Failed')}")
        
        context_parts.append("")
        
        # Data quality insights
        if 'phase0' in self.phase_results:
            quality_data = self.phase_results['phase0'].get('data', {})
            if quality_data:
                context_parts.append("DATA QUALITY INSIGHTS:")
                context_parts.append(f"- Total records: {quality_data.get('total_records', 'N/A')}")
                context_parts.append(f"- Duplicate rate: {quality_data.get('duplicate_rate', 'N/A')}%")
                context_parts.append(f"- Missing data rate: {quality_data.get('missing_data_rate', 'N/A')}%")
                context_parts.append("")
        
        # KPI insights
        if 'phase1' in self.phase_results:
            kpi_data = self.phase_results['phase1'].get('data', {})
            if kpi_data:
                context_parts.append("BUSINESS KPIs:")
                kpis = kpi_data.get('kpis', [])
                for kpi in kpis[:5]:  # Show top 5 KPIs
                    context_parts.append(f"- {kpi}")
                context_parts.append("")
        
        # Schema insights
        if 'phase3' in self.phase_results:
            schema_data = self.phase_results['phase3'].get('data', {})
            if schema_data:
                context_parts.append("DATA SCHEMA:")
                validation = schema_data.get('validation', {})
                context_parts.append(f"- Schema compliance: {validation.get('compliance_rate', 'N/A')}%")
                context_parts.append(f"- Data type issues: {validation.get('type_issues', 'N/A')}")
                context_parts.append("")
        
        # Profiling insights
        if 'phase4' in self.phase_results:
            profile_data = self.phase_results['phase4'].get('data', {})
            if profile_data:
                context_parts.append("DATA PROFILING:")
                stats = profile_data.get('statistics', {})
                context_parts.append(f"- Columns analyzed: {stats.get('total_columns', 'N/A')}")
                context_parts.append(f"- Outliers detected: {stats.get('outlier_count', 'N/A')}")
                context_parts.append("")
        
        # Missing data insights
        if 'phase5' in self.phase_results:
            missing_data = self.phase_results['phase5'].get('data', {})
            if missing_data:
                context_parts.append("MISSING DATA ANALYSIS:")
                missing_stats = missing_data.get('missing_data_stats', {})
                context_parts.append(f"- Columns with missing data: {missing_stats.get('columns_with_missing', 'N/A')}")
                context_parts.append(f"- Overall missing rate: {missing_stats.get('overall_missing_rate', 'N/A')}%")
                context_parts.append("")
        
        # Correlation insights
        if 'phase9' in self.phase_results:
            correlation_data = self.phase_results['phase9'].get('data', {})
            if correlation_data:
                context_parts.append("CORRELATION ANALYSIS:")
                correlations = correlation_data.get('correlations', [])
                context_parts.append(f"- Strong correlations found: {len([c for c in correlations if abs(c.get('correlation', 0)) > 0.7])}")
                context_parts.append("")
        
        # Business validation insights
        if 'phase9.5' in self.phase_results:
            validation_data = self.phase_results['phase9.5'].get('data', {})
            if validation_data:
                context_parts.append("BUSINESS VALIDATION:")
                business_rules = validation_data.get('business_rules', {})
                context_parts.append(f"- Rules validated: {business_rules.get('total_rules', 'N/A')}")
                context_parts.append(f"- Compliance rate: {business_rules.get('compliance_rate', 'N/A')}%")
                context_parts.append("")
        
        return "\n".join(context_parts)
    
    def _get_ai_recommendations(self, analysis_context: str) -> List[Recommendation]:
        """Use AI to generate intelligent recommendations"""
        try:
            prompt = f"""You are a senior data analyst and business intelligence expert. Based on the comprehensive data analysis below, generate 8-12 specific recommendations across 6 different types for improving data quality, business insights, and operational efficiency.

ANALYSIS CONTEXT:
{analysis_context}

Generate recommendations in these 6 categories (provide 1-2 recommendations per type):

1. **DATA INSIGHTS** - Evidence-based findings from the analysis:
   - Specific metrics, trends, or patterns discovered
   - Include supporting evidence/metrics in the description
   - High confidence based on actual data

2. **BEST PRACTICES** - Industry standards and proven methodologies:
   - Worldwide best practices for this domain/industry
   - Benchmarking against industry standards
   - Proven methodologies from similar organizations

3. **HIDDEN PATTERNS** - Anomalies or unexpected findings that need investigation:
   - Unusual correlations, outliers, or anomalies
   - Segments or sub-groups that behave differently
   - Set verification_needed=true for user validation

4. **USER CHECKS** - Things the user should verify or validate:
   - Data accuracy concerns
   - Business rule exceptions
   - Threshold validations
   - Set verification_needed=true

5. **FUTURE ACTIONS** - Strategic roadmap items and next steps:
   - New data collection needs
   - Process improvements
   - System enhancements
   - Monitoring setup

6. **WARNINGS** - Risks, compliance issues, or urgent concerns:
   - Data quality risks
   - Compliance violations
   - Security concerns
   - Operational risks

For each recommendation, provide:
- Clear, actionable title
- Detailed description with evidence where applicable
- Type (data_insight, best_practice, hidden_pattern, user_check, future_action, warning)
- Category (data_quality, performance, insights, optimization, compliance)
- Severity (high, medium, low)
- Priority (1-5, where 1 is highest)
- Whether it's immediately actionable
- Estimated impact (high, medium, low)
- Implementation effort (easy, medium, complex)
- Confidence level (0.0 to 1.0)
- Related phases that would benefit
- Business value explanation
- Evidence (supporting data/metrics for data insights)
- Verification needed (true for hidden patterns and user checks)

Return ONLY a JSON array of recommendations in this exact format:
[
  {{
    "title": "Recommendation Title",
    "description": "Detailed description with evidence",
    "type": "data_insight",
    "category": "data_quality",
    "severity": "high",
    "priority": 1,
    "actionable": true,
    "estimated_impact": "high",
    "implementation_effort": "medium",
    "confidence": 0.9,
    "related_phases": ["phase5", "phase6"],
    "business_value": "Explanation of business value",
    "evidence": "Supporting metrics or data",
    "verification_needed": false
  }}
]

Focus on practical, implementable recommendations that will drive real business value:"""

            response = llm_client.call(prompt, max_tokens=800)
            
            if response and isinstance(response, str):
                # Extract JSON from response
                import re
                json_match = re.search(r'\[.*?\]', response, re.DOTALL)
                if json_match:
                    recommendations_json = json_match.group(0)
                    recommendations_data = json.loads(recommendations_json)
                    
                    if isinstance(recommendations_data, list):
                        recommendations = []
                        for rec_data in recommendations_data:
                            if isinstance(rec_data, dict):
                                recommendation = Recommendation(**rec_data)
                                recommendations.append(recommendation)
                        
                        print(f"AI generated {len(recommendations)} recommendations")
                        return recommendations
            
            print("AI recommendation generation failed, using fallback")
            return self._get_fallback_recommendations()
            
        except Exception as e:
            print(f"AI recommendation error: {e}")
            return self._get_fallback_recommendations()
    
    def _calculate_data_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)"""
        try:
            score = 100.0
            
            # Deduct for data quality issues
            if 'phase0' in self.phase_results:
                quality_data = self.phase_results['phase0'].get('data', {})
                duplicate_rate = quality_data.get('duplicate_rate', 0)
                missing_rate = quality_data.get('missing_data_rate', 0)
                
                # Deduct points for quality issues
                score -= (duplicate_rate * 0.5)  # Each 1% duplicate = -0.5 points
                score -= (missing_rate * 0.3)    # Each 1% missing = -0.3 points
            
            return max(0.0, min(100.0, score))
            
        except Exception as e:
            print(f"Error calculating data quality score: {e}")
            return 75.0  # Default score
    
    def _calculate_overall_health_score(self) -> float:
        """Calculate overall pipeline health score (0-100)"""
        try:
            score = 0.0
            total_weight = 0.0
            
            # Weight different phases based on importance
            phase_weights = {
                'phase0': 0.25,  # Data quality
                'phase1': 0.15,  # Business goals
                'phase3': 0.15,  # Schema validation
                'phase4': 0.20,  # Data profiling
                'phase5': 0.15,  # Missing data
                'phase9': 0.10   # Correlation analysis
            }
            
            for phase_id, weight in phase_weights.items():
                if phase_id in self.phase_results:
                    result = self.phase_results[phase_id]
                    if result.get('status') == 'success':
                        score += 100 * weight
                    else:
                        score += 50 * weight  # Partial credit for attempted phases
                    total_weight += weight
            
            # Normalize score
            if total_weight > 0:
                score = score / total_weight
            
            return max(0.0, min(100.0, score))
            
        except Exception as e:
            print(f"Error calculating health score: {e}")
            return 70.0  # Default score
    
    def _generate_summary(self, recommendations: List[Recommendation]) -> str:
        """Generate executive summary of recommendations"""
        high_priority = [r for r in recommendations if r.priority <= 2]
        actionable = [r for r in recommendations if r.actionable]
        
        summary = f"Analysis of {len(self.phase_results)} completed phases reveals {len(recommendations)} key opportunities. "
        summary += f"{len(high_priority)} high-priority recommendations identified, with {len(actionable)} immediately actionable items. "
        
        if recommendations:
            top_category = max(set(r.category for r in recommendations), 
                             key=lambda x: len([r for r in recommendations if r.category == x]))
            summary += f"Primary focus areas include {top_category.replace('_', ' ')} improvements."
        
        return summary
    
    def _generate_next_steps(self, recommendations: List[Recommendation]) -> List[str]:
        """Generate prioritized next steps"""
        steps = []
        
        # Sort by priority
        sorted_recs = sorted(recommendations, key=lambda x: x.priority)
        
        for i, rec in enumerate(sorted_recs[:5], 1):  # Top 5 next steps
            if rec.actionable:
                steps.append(f"{i}. {rec.title} ({rec.implementation_effort.title()} effort)")
        
        return steps
    
    def _get_fallback_recommendations(self) -> List[Recommendation]:
        """Fallback recommendations when AI fails"""
        return [
            Recommendation(
                title="Complete Data Quality Assessment",
                description="Run comprehensive data quality checks to identify and address data issues",
                type="data_insight",
                category="data_quality",
                severity="high",
                priority=1,
                actionable=True,
                estimated_impact="high",
                implementation_effort="medium",
                confidence=0.9,
                related_phases=["phase0", "phase5"],
                business_value="Ensures data reliability for accurate business decisions",
                evidence="Data quality issues detected in pipeline analysis",
                verification_needed=False
            ),
            Recommendation(
                title="Implement Data Monitoring",
                description="Set up automated monitoring for data quality and pipeline health",
                type="future_action",
                category="performance",
                severity="medium",
                priority=2,
                actionable=True,
                estimated_impact="medium",
                implementation_effort="complex",
                confidence=0.8,
                related_phases=["phase0", "phase4"],
                business_value="Prevents data quality degradation and enables proactive issue resolution",
                evidence="No monitoring system currently in place",
                verification_needed=False
            ),
            Recommendation(
                title="Verify Data Accuracy",
                description="Manual validation of key metrics and calculations",
                type="user_check",
                category="data_quality",
                severity="medium",
                priority=3,
                actionable=True,
                estimated_impact="medium",
                implementation_effort="easy",
                confidence=0.7,
                related_phases=["phase0", "phase4"],
                business_value="Ensures data integrity for business decisions",
                evidence="Automated checks may miss edge cases",
                verification_needed=True
            )
        ]


def generate_ai_recommendations(domain: str, phase_results: Dict[str, Any]) -> RecommendationAnalysis:
    """Main function to generate AI-powered recommendations"""
    service = AIRecommendationsService(domain, phase_results)
    return service.generate_recommendations()

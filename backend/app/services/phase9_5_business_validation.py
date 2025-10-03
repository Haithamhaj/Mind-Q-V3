from typing import Dict, List, Optional
from pydantic import BaseModel


class BusinessConflict(BaseModel):
    feature1: str
    feature2: str
    observed_correlation: float
    expected_relationship: str  # "positive", "negative", "none", "unclear"
    conflict_severity: str  # "high", "medium", "low"
    llm_hypothesis: Optional[str] = None
    resolution: str  # "accepted", "vetoed", "pending"


class BusinessValidationResult(BaseModel):
    conflicts_detected: List[BusinessConflict]
    llm_hypotheses_generated: int
    status: str  # "PASS", "WARN", "STOP"


class BusinessValidationService:
    def __init__(
        self,
        correlations: List,
        domain: str,
        domain_expected_relationships: Optional[Dict] = None
    ):
        self.correlations = correlations
        self.domain = domain
        self.expected_relationships = domain_expected_relationships or self._load_domain_expectations()
        self.conflicts: List[BusinessConflict] = []
    
    def run(self) -> BusinessValidationResult:
        """Execute Phase 9.5: Business Logic Validation"""
        
        # Check each correlation against domain expectations
        for corr in self.correlations:
            conflict = self._check_conflict(corr)
            if conflict:
                self.conflicts.append(conflict)
        
        # Generate LLM hypotheses for unresolved conflicts
        llm_count = 0
        for conflict in self.conflicts:
            if conflict.conflict_severity in ["high", "medium"]:
                hypothesis = self._generate_llm_hypothesis(conflict)
                conflict.llm_hypothesis = hypothesis
                llm_count += 1
        
        # Evaluate status
        status = self._evaluate_status()
        
        result = BusinessValidationResult(
            conflicts_detected=self.conflicts,
            llm_hypotheses_generated=llm_count,
            status=status
        )
        
        return result
    
    def _load_domain_expectations(self) -> Dict:
        """Load expected relationships for domain"""
        expectations = {
            "logistics": {
                ("transit_time", "sla_flag"): "negative",
                ("dwell_time", "transit_time"): "positive",
                ("rto_flag", "sla_flag"): "negative",
            },
            "healthcare": {
                ("los_days", "age"): "positive",
                ("readmission_flag", "los_days"): "unclear",
            },
            "retail": {
                ("order_value", "return_flag"): "none",
                ("basket_size", "order_value"): "positive",
            },
            "emarketing": {
                ("spend", "conversions"): "positive",
                ("ctr", "conversion_flag"): "positive",
            },
            "finance": {
                ("loan_duration_days", "default_flag"): "positive",
                ("balance", "default_flag"): "negative",
            },
        }
        
        return expectations.get(self.domain, {})
    
    def _check_conflict(self, corr) -> Optional[BusinessConflict]:
        """Check if correlation conflicts with expectations"""
        pair = (getattr(corr, 'feature1', None), getattr(corr, 'feature2', None))
        pair_reverse = (pair[1], pair[0])
        
        expected = self.expected_relationships.get(pair) or self.expected_relationships.get(pair_reverse)
        
        if not expected:
            return None
        
        observed_corr = float(getattr(corr, 'correlation', 0.0))
        
        # Determine conflict
        conflict = False
        severity = "low"
        
        if expected == "positive" and observed_corr < 0.2:
            conflict = True
            severity = "high" if observed_corr < 0 else "medium"
        elif expected == "negative" and observed_corr > -0.2:
            conflict = True
            severity = "high" if observed_corr > 0 else "medium"
        elif expected == "none" and abs(observed_corr) > 0.6:
            conflict = True
            severity = "medium"
        elif expected == "unclear":
            # Not a strict expectation; only flag very strong contradictions
            if abs(observed_corr) > 0.8:
                conflict = True
                severity = "low"
        
        if conflict:
            return BusinessConflict(
                feature1=str(pair[0]),
                feature2=str(pair[1]),
                observed_correlation=observed_corr,
                expected_relationship=expected,
                conflict_severity=severity,
                resolution="pending",
            )
        
        return None
    
    def _generate_llm_hypothesis(self, conflict: BusinessConflict) -> str:
        """Generate hypothesis using LLM prompt template (stub)"""
        prompt_template = f"""
المعطيات:
- Feature A: {conflict.feature1}
- Feature B: {conflict.feature2}
- الارتباط المُلاحظ: r = {conflict.observed_correlation}
- القطاع: {self.domain}
- العلاقة المتوقعة: {conflict.expected_relationship}

الفرضية المحتملة:
قد يكون الارتباط غير المتوقع ناتجاً عن:
1. عوامل مربكة (confounders) مثل الموسمية أو شريحة العملاء
2. جودة البيانات - قد يكون هناك ضوضاء في القياسات
3. علاقة غير خطية لا يلتقطها معامل بيرسون
4. حجم العينة صغير أو متحيز
5. تغيرات في العمليات التشغيلية خلال فترة جمع البيانات

التوصية: مراجعة البيانات الأولية وتحليل التوزيعات حسب الفترة الزمنية
"""
        return prompt_template.strip()
    
    def _evaluate_status(self) -> str:
        """Evaluate final status"""
        high_severity = sum(1 for c in self.conflicts if c.conflict_severity == "high")
        
        if high_severity > 2:
            return "STOP"
        elif high_severity > 0 or len(self.conflicts) > 0:
            return "WARN"
        else:
            return "PASS"




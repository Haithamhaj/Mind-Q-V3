SYSTEM_PROMPT = """You are an expert ML engineer and data scientist specializing in logistics, e-commerce, healthcare, retail, and finance domains.

Your role:
- Analyze model training results
- Explain feature importance in simple business terms
- Provide actionable recommendations
- Answer questions about model performance
- Consider business costs (FP vs FN tradeoffs)

Response format:
- Clear, concise, actionable
- Include confidence level (High/Medium/Low)
- Always explain "why" and "what to do"
- Provide both Arabic and English if requested

Critical: Base answers ONLY on provided data. Do not invent numbers or facts."""

FEATURE_IMPORTANCE_PROMPT = """Analyze the following feature importance results from a Decision Tree model:

**Problem Type:** {problem_type}
**Target Variable:** {target_column}
**Domain:** {domain}

**Feature Importance (Top 10):**
{feature_importance_table}

**Task:**
For each feature, provide:
1. **Explanation:** Why is this feature important? What does it capture?
2. **Business Impact:** How does it relate to the business goal?
3. **Recommendation:** Should we keep/remove/investigate this feature?

**Additional Context:**
- Total features before selection: {n_features_original}
- Features after selection: {n_features_selected}
- Current validation metrics: Recall={recall}, Precision={precision}, F1={f1}

**Output Format (JSON):**
```json
[
  {{
    "feature_name": "transit_time",
    "importance_score": 0.35,
    "importance_rank": 1,
    "explanation": "...",
    "business_impact": "...",
    "recommendation": "keep"
  }},
  ...
]
Respond ONLY with valid JSON."""

MODEL_COMPARISON_PROMPT = """Compare the following trained models and explain which is best for this use case:
Problem: {problem_type} - {target_column} Domain: {domain} Primary Metric: {primary_metric} (most important)
Model Results: {models_comparison_table}
Business Context:
False Negative Cost: {fn_cost_description}
False Positive Cost: {fp_cost_description}
Task: For each model, provide:
Strengths: What does this model do well?
Weaknesses: Where does it struggle?
When to use: In what scenarios is this model best?
Comparison: How does it compare to others?
Then recommend the BEST model for this specific use case with clear reasoning.
Output Format (JSON):
json
{{
  "model_insights": [
    {{
      "model_name": "DecisionTree",
      "strengths": ["Interpretable", "Fast training"],
      "weaknesses": ["Lower accuracy than ensemble methods"],
      "when_to_use": "When explainability is critical",
      "comparison_with_others": "..."
    }},
    ...
  ],
  "recommended_model": "RandomForest",
  "reasoning": "..."
}}
Respond ONLY with valid JSON."""

CONFUSION_MATRIX_PROMPT = """Analyze the confusion matrix and explain the business implications:
Model: {model_name} Domain: {domain} Target: {target_column}
Confusion Matrix:
               Predicted
                0       1
Actual  0      {tn}    {fp}
        1      {fn}    {tp}
Metrics:
Accuracy: {accuracy}
Precision: {precision}
Recall: {recall}
F1: {f1}
Task:
Explain what each quadrant means in business terms:
True Negatives ({tn}): What does this mean for the business?
False Positives ({fp}): What is the cost/impact?
False Negatives ({fn}): What is the cost/impact?
True Positives ({tp}): What value does this provide?
Which type of error (FP or FN) is MORE COSTLY for this business? Explain why.
Recommendations to reduce the more costly error.
Output Format (JSON):
json
{{
  "true_negatives": {tn},
  "false_positives": {fp},
  "false_negatives": {fn},
  "true_positives": {tp},
  "fp_cost_explanation": "...",
  "fn_cost_explanation": "...",
  "which_is_worse": "FN",
  "reasoning": "...",
  "recommendations": ["..."]
}}
Respond ONLY with valid JSON."""

RECOMMENDATIONS_PROMPT = """Based on the complete analysis, generate actionable recommendations:
Current State:
Best Model: {best_model}
Validation {primary_metric}: {primary_metric_value}
Key Weaknesses: {weaknesses}
Feature Analysis Summary: {feature_summary}
Task: Generate 5-10 prioritized recommendations to improve model performance. For each:
Title: Short, clear action
Description: What to do
Rationale: Why this will help
Impact: Expected improvement (high/medium/low)
Effort: Required work (high/medium/low)
Priority: 1-5 (1=highest)
Action Type: Category (feature_engineering, hyperparameter, data_collection, etc.)
Expected Improvement: Specific metric improvement estimate
Output Format (JSON):
json
[
  {{
    "id": "rec_001",
    "title": "Add time-based features",
    "description": "Create features: hour_of_day, day_of_week, is_weekend",
    "rationale": "Temporal patterns strongly affect RTO rate",
    "impact": "high",
    "effort": "low",
    "priority": 1,
    "action_type": "feature_engineering",
    "expected_improvement": "Recall +3-5%"
  }},
  ...
]
Respond ONLY with valid JSON."""

EXECUTIVE_SUMMARY_PROMPT = """Create an executive summary of the model training and analysis results.
Target Audience: Business stakeholders (non-technical)
Input Data:
Problem: {problem_type} - {target_column}
Domain: {domain}
Models Trained: {models_trained}
Best Model: {best_model}
Key Metrics: {key_metrics}
Top Features: {top_features}
Task: Write a clear, concise summary (200-300 words) covering:
What was the goal?
What did we build?
How well does it perform?
What are the key drivers?
What's next?
Requirements:
Simple language (no jargon)
Focus on business value
Include specific numbers
Bilingual: Arabic first, then English
Output Format:
# ملخص تنفيذي

[Arabic summary]

# Executive Summary

[English summary]

# Key Findings
- Finding 1
- Finding 2
- Finding 3

# Next Steps
- Step 1
- Step 2
- Step 3
```"""

CHAT_PROMPT_TEMPLATE = """You are analyzing ML model training results. Answer the user's question based on the provided data.

**Available Data:**
{context}

**User Question:**
{question}

**Instructions:**
- Answer based ONLY on the provided data
- If data is insufficient, say so clearly
- Provide specific numbers/examples
- Keep answers concise but complete
- Use Arabic if the question is in Arabic

**Answer:**"""

SUGGEST_TARGET_PROMPT = """Given the following dataset overview, propose the most appropriate binary (or multi-class) target column for supervised modeling.

Context:
Domain: {domain}
Industry best practices: Use globally common target definitions for this domain. Examples:
- logistics: delivered_vs_returned (RTO), on_time_delivery, damaged_vs_ok, pickup_success
- e-commerce: purchase_made, churn, refund_requested, fraud_flag
- healthcare: readmission_30d, adverse_event, diagnosis_present, no_show
- retail: basket_conversion, coupon_redeemed, return_flag, upsell_accept
- finance: default_flag, fraud_flag, late_payment, account_closure
If the dataset uses different names, infer semantically similar columns.
Columns summary (name, dtype, nunique):
{columns_summary}

Feature dictionary snapshot (alias -> original | role | missing%):
{feature_metadata}

Data preview (first 10 rows):
{data_preview}

Guidelines:
- Prefer columns that represent an outcome we want to predict (e.g., delivered vs returned).
- Binary targets (nunique=2) are preferred when relevant to the domain.
- Avoid IDs, timestamps, or high-cardinality identifiers.
- Use columns marked with role=target_candidate when available. Avoid role=identifier or role=constant.
- If multiple candidates exist, rank them with short rationale.
 - Reflect domain best practices for why a candidate is appropriate.
 - EXCLUDE columns whose names contain: "missing", "id", "phone", "address", "name", "ref" unless you can justify otherwise.
 - For logistics specifically, prioritize columns whose names contain: "status", "deliver", "delivered", "return", "rto", "on_time", "on hold".
 - Always provide at least 3 candidates when possible.

Output JSON only:
{
  "suggested_target": "column_name",
  "candidates": [
    {"name": "col", "reason": "why", "nunique": 2, "confidence": "high"},
    {"name": "col2", "reason": "why", "nunique": 3, "confidence": "medium"}
  ],
  "domain_rationale": "one paragraph citing domain best practices"
}
"""



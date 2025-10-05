from __future__ import annotations
import json
import re
from typing import Dict, Callable

# Forbidden causal terms
FORBIDDEN_TERMS = [
    r"\bbecause of\b",
    r"\bcaused by\b",
    r"\bproves\b",
    r"\bcause\b",
    r"\bresult of\b",
    r"\bdue to\b"
]


def _contains_forbidden(text: str) -> bool:
    """Check if text contains forbidden causal language"""
    return any(re.search(p, text, flags=re.IGNORECASE) for p in FORBIDDEN_TERMS)


def explain_chart(
    signals: Dict,
    chart: Dict,
    lang: str,
    llm_call: Callable[[str], str]
) -> Dict:
    """
    Generate LLM explanation with strict guardrails
    
    Guardrails:
    1. Must mention n and time_window
    2. No causal language
    3. Association only
    4. Return structured JSON: {summary, findings, recommendation}
    """
    
    signals_json = json.dumps(signals, ensure_ascii=False, indent=2)
    
    if lang == "ar":
        prompt = f"""أنت محلل بيانات خبير. دورك: شرح الرسم البياني فقط باستخدام البيانات المقدمة.

**قيود صارمة:**
- لا تحسب أي أرقام. استخدم فقط ما في JSON.
- اذكر حجم العينة n={signals['meta']['n']} والفترة الزمنية "{signals['meta']['time_window']}".
- ارتباطات فقط - لا تستخدم لغة سببية (بسبب، نتيجة لـ، يثبت).
- أعد JSON بالمفاتيح: summary, findings (قائمة), recommendation.

**السياق:**
{signals_json}

**الرسم البياني:**
{json.dumps({"type": chart.get("type"), "meta": chart.get("meta", {})}, ensure_ascii=False)}

أعد JSON فقط (بدون نص إضافي):
"""
    else:
        prompt = f"""You are an expert data analyst. Your role: Explain the chart using ONLY the provided data.

**Strict constraints:**
- DO NOT compute any numbers. Use ONLY what's in the JSON.
- Mention sample size n={signals['meta']['n']} and time window "{signals['meta']['time_window']}".
- Association only - NO causal language (because of, caused by, proves, due to).
- Return JSON with keys: summary, findings (list), recommendation.

**Context:**
{signals_json}

**Chart:**
{json.dumps({"type": chart.get("type"), "meta": chart.get("meta", {})}, ensure_ascii=False)}

Return ONLY JSON (no additional text):
"""
    
    raw = llm_call(prompt).strip()
    
    # Clean markdown if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    
    out = json.loads(raw)
    
    # Guardrail checks
    full_text = " ".join([
        out.get("summary", ""),
        *out.get("findings", []),
        out.get("recommendation", "")
    ])
    
    # Must mention n
    assert str(signals["meta"]["n"]) in full_text, "Explanation must mention sample size n"
    
    # Must mention time_window
    assert signals["meta"]["time_window"] in full_text, "Explanation must mention time window"
    
    # No forbidden causal terms
    assert not _contains_forbidden(full_text), "Explanation contains forbidden causal language"
    
    return out

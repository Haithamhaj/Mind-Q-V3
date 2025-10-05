# BI Layer - Complete Documentation

## Overview

Complete Business Intelligence layer with:
- Natural language queries (Arabic + English)
- Domain-specific KPIs
- Statistical signals (quality, distributions, trends)
- Rule-based recommendations
- LLM explanations with strict guardrails (Gemini, Claude, GPT-4)
- Interactive Plotly visualizations

## Architecture
```
User Question (NL)
    ↓
Query Parser (LLM) → Structured query
    ↓
Signals Builder → stats.json (meta, KPIs, quality, distributions, trends)
    ↓
Executor → Run query (aggregate/compare/trend)
    ↓
Visualizer → Generate Plotly chart
    ↓
Rule Recommender → Pre-LLM recommendations
    ↓
Chart Explainer (LLM + guardrails) → Natural language explanation
    ↓
Response → Complete BI response
```

## Components

### 1. Metrics Registry (`metrics_registry.py`)
- Domain-specific KPI definitions
- Supports: logistics, healthcare, retail, emarketing, finance
- Fallback handling for missing columns

### 2. Stats Signals (`stats_signals.py`)
- Builds complete `signals.json`:
  - **meta**: domain, time_window, n
  - **kpis**: domain-specific metrics
  - **quality**: missing%, duplicates%, orphans%
  - **distributions**: skew, kurtosis, outliers (IQR), quantiles (p90, p95)
  - **trends**: slope analysis over time

### 3. Query Executor (`executor.py`)
- Advanced aggregations: mean, median, sum, count, min, max, std, p90, p95
- Filters: ==, in, range, like (regex)
- Query types: aggregate, compare, trend

### 4. Rule Recommender (`rule_recommender.py`)
- Pre-LLM recommendations based on:
  - KPI thresholds
  - Trend patterns
  - Domain-specific rules
- Returns top 3 with severity (high/medium/low)

### 5. Chart Explainer (`chart_explainer.py`)
- LLM generates explanations with **strict guardrails**:
  - Must mention n and time_window
  - No causal language (because, caused by, proves)
  - Association only
  - Structured output: {summary, findings[], recommendation}

### 6. Query Parser (`query_parser.py`)
- Converts NL questions → structured queries
- Detects language (Arabic/English)
- Maps to domain entities (metrics, dimensions)

### 7. Visualizer (`visualizer.py`)
- Generates Plotly charts:
  - Bar charts (dimension-based)
  - Line charts (trends)
  - Grouped bar charts (comparisons)
  - Metric cards (single values)

### 8. Orchestrator (`orchestrator.py`)
- Main coordinator
- Executes full pipeline
- Caches signals for performance

## LLM Configuration

### Supported Providers:
- **Google Gemini**: `gemini-1.5-pro` (Recommended - Fast & Cost-effective)
- **Anthropic Claude**: `claude-3-5-sonnet-20241022`
- **OpenAI GPT-4**: `gpt-4`

### Environment Variables:
```bash
LLM_PROVIDER=gemini  # or "anthropic" or "openai"
GEMINI_API_KEY=your_gemini_api_key_here
ANTHROPIC_API_KEY=your_anthropic_api_key_here
OPENAI_API_KEY=your_openai_api_key_here
```

### Getting API Keys:
- **Gemini**: https://makersuite.google.com/app/apikey
- **Claude**: https://console.anthropic.com/
- **OpenAI**: https://platform.openai.com/

### Testing LLM Integration:
```bash
cd backend
python test_gemini.py
```

## API Endpoints

### 1. POST `/api/v1/bi/ask`
Natural language query

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/bi/ask \
  -F "question=What is the average transit time for DHL?" \
  -F "domain=logistics" \
  -F "time_window=2024-01-01..2024-12-31"
```

**Response:**
```json
{
  "query": "What is the average transit time for DHL?",
  "parsed": {...},
  "chart": {
    "type": "bar",
    "config": "...",
    "data": {"DHL": 27.5}
  },
  "explanation": {
    "summary": "DHL shows 27.5h average (n=1000, 2024-Q1)",
    "findings": ["..."],
    "recommendation": "..."
  },
  "recommendations": [...],
  "language": "en",
  "signals_meta": {...}
}
```

### 2. GET `/api/v1/bi/signals`
Get complete signals JSON

### 3. GET `/api/v1/bi/kpis`
Get KPIs only

### 4. GET `/api/v1/bi/recommendations`
Get rule-based recommendations

## Supported Domains
- **Logistics**: SLA%, RTO%, FAS%, avg_transit_h
- **Healthcare**: avg_los_days, readmission_30d_pct, bed_occupancy_pct
- **Retail**: GMV, AOV, return_pct, basket_size
- **E-marketing**: CTR%, conversion%, CAC, ROAS
- **Finance**: NPL%, avg_balance, liquidity_ratio

## Question Examples

**English:**
- "What is the average transit time for DHL?"
- "Compare SLA performance by carrier"
- "Show me transit time trend over time"

**Arabic:**
- "ما متوسط وقت الشحن لشركة DHL؟"
- "قارن أداء SLA حسب الناقل"
- "أظهر لي اتجاه وقت الشحن عبر الوقت"

## LLM Guardrails
**Mandatory checks:**
- ✅ Mentions sample size (n)
- ✅ Mentions time window
- ✅ Association language only (no causation)
- ✅ Structured JSON output

**❌ Forbidden terms:** because of, caused by, proves, due to

## Development

**Run tests:**
```bash
pytest backend/tests/test_bi_complete.py -v
```

**Test API:**
```bash
# English question
curl -X POST http://localhost:8000/api/v1/bi/ask \
  -F "question=Average transit time by carrier" \
  -F "domain=logistics"

# Arabic question
curl -X POST http://localhost:8000/api/v1/bi/ask \
  -F "question=ما متوسط وقت الشحن؟" \
  -F "domain=logistics"
```

**Performance Notes:**
- Signals cached per domain (rebuilt only on data change)
- Rule recommendations execute without LLM (fast)
- LLM calls only for parsing + explanation (~2 API calls/question)
- Recommended: Implement caching for frequent questions

**Security:**
- Rate limit /ask endpoint (max 100/hour per user)
- Validate all user inputs
- Sanitize filters to prevent injection
- Never expose raw SQL or data structure to users

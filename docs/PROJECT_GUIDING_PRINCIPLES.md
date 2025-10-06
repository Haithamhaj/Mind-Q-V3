# Project Guiding Principles

> This document is the single source of truth for vision, design principles, and domain alignment across the Mind-Q V3 platform. Every new phase, feature, or integration must be validated against these guidelines before implementation.

## 1. Vision

Build a modular, intelligent data system that transforms raw business data into structured insight, prediction, and strategy driven by advanced analytics and large language models (LLMs). The system serves commercial sectors by optimizing profitability, cost efficiency, and operational intelligence within each domain.

## 2. Core Purpose

- Enable every stage of the data-to-decision pipeline: ingestion -> EDA -> ML readiness -> insight generation -> business recommendation.
- Integrate AI reasoning to enhance interpretation and business value, not replace human judgment.
- Maintain flexibility for new phases, industries, and data sources without core redesign.

## 3. Domain Focus — Unified Commercial Framework

All supported sectors share a common business logic built around five universal financial levers.

| Lever            | Definition                                            | Example Metrics                                   |
|------------------|--------------------------------------------------------|---------------------------------------------------|
| Sales / Revenue  | Income from operations, recurring or transactional.    | GMV, sales growth %, average order value, revenue per client |
| Cost             | Operational or variable expenses influencing profit.   | Fulfillment cost, CAC, logistics spend, labor cost |
| Investment       | Capital or resource allocation to create future value. | CapEx, R&D, technology spend                      |
| Budget           | Planned spending limits and resource allocation.       | Monthly Opex, campaign budget, category budget    |
| Target / KPI     | Quantified business objectives defining success.       | ROI %, margin %, SLA %, conversion %, occupancy % |

Each sector expresses these levers through domain-specific KPIs and contextual analytics.

### A. Logistics
- Objective: Deliver goods efficiently at minimal total cost.
- Sales: delivery volume x rate
- Cost: fuel, storage, last-mile operations
- Investment: fleet or routing technology
- Budget: route cost ceilings
- Targets: SLA >= 95 %, RTO <= 5 %, cost per kilometer decreasing

### B. Healthcare
- Objective: Balance throughput, quality, and efficiency.
- Sales: revenue per bed or procedure
- Cost: staffing, consumables
- Investment: IT, facility upgrades
- Budget: departmental caps
- Targets: occupancy %, average length of stay, success %

### C. E-Marketing
- Objective: Maximize campaign ROI under limited spend.
- Sales: conversions x average order value
- Cost: ad spend, platform fees
- Investment: marketing technology, data
- Budget: channel cap
- Targets: CTR %, ROAS >= target, CAC decreasing

### D. Retail / E-Commerce
- Objective: Optimize demand, product mix, and margin.
- Sales: transactions, GMV
- Cost: procurement, logistics, returns
- Investment: inventory, store technology
- Budget: category plan
- Targets: margin %, return rate decreasing, AOV increasing

### E. Finance
- Objective: Deploy capital efficiently while managing risk.
- Sales: interest, fees
- Cost: defaults, operations
- Investment: diversification, technology
- Budget: liquidity or risk limits
- Targets: ROI %, NPL %, liquidity ratio >= target

### F. Cross-Sector Intelligence Layer
- Map data signals to the five levers.
- Translate results into financial narratives (how it affects sales, cost, or ROI).
- Support cross-sector benchmarking.
- Preserve sector adapters for KPI context.

### G. Design Rule
Any new domain or module must define its Sales-Cost-Investment-Budget-Target map and align all analytics and prompts with these pillars.

## 4. System Philosophy

- Explainable by design: outputs must be interpretable by business users.
- Composable architecture: modules plug in without conflict.
- LLM-augmented: models handle explanation, reasoning, summarization.
- Data-centric: quality and structure outweigh algorithm complexity.
- Outcome-oriented: every output ties to measurable business impact.

## 5. Data and Intelligence Flow

1. Data foundation – ingestion, cleaning, validation.
2. Profiling and understanding – distributions, anomalies, relationships.
3. Feature engineering – automated yet explainable transformations.
4. Contextual intelligence – sector-aware reasoning using LLMs.
5. Model readiness – export consistent ML-ready datasets.
6. Insight generation – interpretive summaries and BI narratives.
7. Feedback loop – learn from results and refine logic.

## 6. AI and LLM Utilization

Use LLMs for:
- Explaining analytical results in natural business language.
- Generating hypotheses and identifying hidden patterns.
- Summarizing reports for executives.
- Auto-documenting processes and metrics.
- Supporting "why + how" decision reasoning.

AI outputs must always be:
- Traceable (source -> reasoning -> result).
- Verifiable (no hallucination).
- Aligned with financial and sector logic.

## 7. Business and Commercial Orientation

Every analytical output should answer:
- How can revenue increase?
- How can cost or inefficiency decrease?
- How can decision accuracy improve?

Each recommendation should include an impact estimate such as ROI %, cost saved, or efficiency gained.

## 8. Technical and Design Principles

- Modular, reproducible, scalable.
- Performance discipline: low latency, optimized queries.
- Security: anonymize sensitive data by default.
- Observability: logs, telemetry, confidence metrics for all AI calls.
- Version control: lock data schemas and transformation code.

## 9. Human-Centered AI

- Keep users in the decision loop.
- Explanations must educate, not obscure.
- Favor assistive AI that augments expertise rather than replaces it.

## 10. Innovation and Continuous Development

Encourage experimentation as long as:
- Core architecture stays stable.
- Interpretability and reliability are preserved.

Each new phase must document:
- Business value and KPIs impacted.
- Data needs and assumptions.
- Potential cross-sector benefits.

Prefer simplicity and measurable impact over theoretical sophistication.

## 11. Governance and Quality

- Single source of truth: unified configuration and schema repository.
- Validation gates: each phase passes quality checks before output.
- AI review: all model outputs scored for confidence and verified.
- Audit trail: retain reasoning and output logs for traceability.

## 12. Future-Ready Orientation

The framework must support:
- Addition of new sectors via Domain Packs.
- Predictive and prescriptive extensions (pricing, risk, policy simulation).
- Integration with BI dashboards and conversational agents.
- Hybrid intelligence (rules + generative reasoning).

## 13. Success Criteria

- Reusable, explainable, and sector-aware data pipelines.
- Business users understand what happened, why, and what to do next.
- Reduced time-to-insight and higher ROI per analysis cycle.
- Every phase delivers measurable commercial or analytical value.

## 14. Ethical and Operational Standards

- Transparency in data use and AI reasoning.
- Zero fabricated or unverifiable information.
- Uphold fairness, privacy, and integrity.
- Always align with client ethics and domain regulations.

## 15. Guiding Principle Summary

**Truth | Clarity | Impact**
- Every dataset must tell the truth.
- Every model must explain itself clearly.
- Every insight must lead to measurable business impact.

## 16. Cross-Lever to System Output Mapping

| Phase / Function        | Primary Lever | Typical Output                                  |
|-------------------------|---------------|--------------------------------------------------|
| Data profiling          | Cost / Quality| Data health metrics, missingness cost            |
| Feature engineering     | Investment    | Derived variables improving model ROI            |
| Correlation / validation| Target        | KPI alignment checks                             |
| Explain @ phase         | Sales / Cost  | Narrative on operational efficiency              |
| Explain @ run           | All           | End-to-end financial narrative                   |
| BI layer                | Budget / Target| Dashboards linking KPIs to strategy              |
| Feedback loop           | Investment / ROI| Learning from business outcomes                |

Each data phase must explicitly reference which lever it influences and how it contributes to measurable results.

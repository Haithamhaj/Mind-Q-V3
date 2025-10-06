# Mind‑Q V3 — System Logic Overview (No Code)

This paper explains how the system works step by step across phases, what rules, algorithms, and equations are used to make decisions, and why. The aim is to understand the project logic independent of implementation details. For the high‑level layout, see `docs/architecture.md` and `docs/phases.md`.


## General principles

- Each phase has its own business logic under `backend/app/services/` and produces intermediate artifacts saved in `backend/artifacts/` for traceability and visualization.
- Decisions are driven by explicit rules with WARN/STOP thresholds to prevent unsafe outputs.
- No database in the MVP; the pipeline relies on Parquet/JSON files inside `artifacts`.


## Phase map at a glance

See the table in `docs/phases.md`. Below is a deeper explanation of each phase and its outputs.

---

## Phase 0 — Quality Control (Foundation)

- Environment, layout, and service health checks to ensure a solid base before any data processing.


## Phase 1 — Goal & KPIs

- Capture business goals and target KPIs to be assessed later. No mandatory calculations here; it’s a requirements definition step.


## Phase 2 — Ingestion & Landing

- Read the source (CSV/Excel/Parquet/JSON) with recovery strategies for corrupted CSVs.
- Sanitize column names to a standard form (lowercase/underscores/remove symbols).
- Write to Parquet with Zstd compression and report compression ratio and ingestion time.
- Chunking rule: if file size > 1GB, use chunked writing instead of a single write.
- Typical outputs: `raw_ingested.parquet`, size/time/ratio metrics.

Compression ratio (approx.):

$\text{ratio} = \frac{\text{source\_size}}{\text{parquet\_size}}$.


## Phase 3 — Schema & Dtypes

- Type inference and casting:
  - IDs → `string`, timestamps → `datetime[UTC]`, numeric → `float`, low‑variance text → `category`.
  - Smart numeric parsing (e.g., "%") and date detection by name/pattern.
- Categorize columns into identifiers / datetimes / numeric / categorical and generate a Pandera‑style JSON schema.
- Compute violation rate and set status: if $\text{violation\_rate} > 2\%$ ⇒ WARN.


## Phase 4 — Profiling

- Use the full dataset for ML‑oriented accuracy.
- Numeric: mean, median, std, quartiles, skewness, kurtosis.
- Categorical: unique count, most common, top‑5 categories.
- Missingness: count and percentage per column.
- Outliers by robust IQR rule: lower/upper bound $= Q1 \pm 3\,IQR$.
- Preview top‑5 numeric correlation pairs.
- Persist top‑10 data quality issues to `artifacts/dq_report.json`.

Short definitions:

- $\text{skew}(x)$ and $\text{kurt}(x)$ as in SciPy.
- $\text{IQR} = Q3 - Q1$.


## Phase 5 — Missing Data

- Column‑level decision tree (abridged):
  1) Datetimes: no imputation; add a `missing` flag only.
  2) Numeric with low missing ($\le 5\%$): median.
  3) If `group_col` exists and missing up to 50%: group median.
  4) Large data ($n \ge 50{,}000$) with ≥3 correlated peers (|r|≥0.4): MICE.
  5) Very small data ($n<2000$): avoid KNN/MICE; use median (or group median if available).
  6) Default: KNN (k=5) for numeric.
  7) Categorical: mode/group‑mode, or set to "Unknown".
- After imputation: quality validation via PSI and KS.
- Status: PASS/WARN/STOP based on record completeness and validation failures.

Validation equations:

- PSI using common bins:

$$\text{PSI} = \sum_{b} (p_b - q_b)\,\ln\left(\frac{p_b}{q_b}\right)$$

where $p_b$ is the baseline distribution and $q_b$ is the post‑imputation distribution. Suggested WARN near $\approx 0.10$.

- Kolmogorov–Smirnov statistic:

$$D = \sup_x |F_{\text{orig}}(x) - F_{\text{imp}}(x)|$$

A preferred target is $D \le 0.10$.


## Phase 6 — Standardization

- Unicode normalization for text plus Arabic‑specific fixes: unify Alef/Ya/Ta‑Marbuta.
- Apply domain mappings (Logistics/Healthcare/Retail …). Example: unify carrier names and shipment statuses.
- Collapse rare categories to "Other" by frequency. For small data (≤1000 rows) enforce an effective minimum threshold of 3% to avoid tiny, unhelpful classes.


## Phase 7 — Feature Draft

- Domain‑oriented derivations:
  - Logistics: `transit_time` (hours), SLA flag ≤ 48h, RTO flag.
  - Healthcare: `los_days` (length of stay), `age_group` bins.
  - Retail/Marketing/Finance: relevant features (order value, CTR, loan duration, …).
- Cap extreme outliers for selected derived features by domain (e.g., `transit_time` ≤ 240h).


## Phase 7.5 — Encoding & Scaling

- Categorical encoding on TRAIN only:
  - Cardinality ≤ 50 ⇒ One‑Hot.
  - Cardinality > 50 with large data ($n>50{,}000$) and available target ⇒ Target Encoding.
  - Otherwise ⇒ simple Ordinal.
- Numeric scaling on TRAIN only, then transform VAL/TEST with the same fit:
  - Default: StandardScaler.
  - Finance domain: RobustScaler for heavy‑tailed distributions.
- Persist encoders/scalers to `artifacts/` (e.g., `scaler_numeric.joblib`).

z‑scoring:

$$z = \frac{x - \mu}{\sigma}$$


## Phase 8 — Merging & Keys

- Auto‑identify a common key (columns containing "id").
- Duplicates policy:
  - Duplicate rate > 10% ⇒ STOP.
  - Between 3% and 10% ⇒ keep latest record (prefer a timestamp).
  - < 3% ⇒ log only.
- Orphans after join (missing attached values) policy:
  - > 10% ⇒ STOP.
  - 2%–10% ⇒ quarantine and save orphans, then continue.
  - < 2% ⇒ warn only.
- Orphan tables are saved under `artifacts/` when applicable.


## Phase 9 — Correlations & Associations

- Numeric pairs: Pearson r with p‑value.
- Categorical pairs: contingency table, Chi‑square, then Cramér’s V.
- dtype cleaning and pairwise dropping of invalid rows.
- If many tests (>20), apply multiple‑testing correction: FDR‑BH or Bonferroni fallback.

Equations:

- Pearson:

$$r = \frac{\sum (x_i-\bar{x})(y_i-\bar{y})}{\sqrt{\sum (x_i-\bar{x})^2}\;\sqrt{\sum (y_i-\bar{y})^2}}$$

- Cramér’s V:

$$V = \sqrt{\frac{\chi^2}{n\,(\min(r,c)-1)}}$$

with $\chi^2$ from Chi‑square and $r,c$ the table dimensions.


## Phase 9.5 — Business Validation

- Expected relationship matrices per domain (positive/negative/none/unclear).
- Compare observed correlations against expectations and classify conflicts: high/medium/low.
- Generate text hypotheses (LLM) for non‑conforming cases to be reviewed by experts.
- Overall rule of thumb: more than two high‑severity conflicts ⇒ STOP; else WARN/PASS.


## Phase 10 — Packaging

- Collect core pieces (quality reports / standardization configs / schemas / merged data), create `provenance.json` and `changelog.md`, then ZIP bundle `eda_bundle.zip`.
- Create a short SHA‑256 provenance hash for integrity.


## Phase 10.5 — Train/Val/Test Split

- Two options:
  - Time‑based: sort by time; no shuffling.
  - Stratified: preserve target distribution if a target column exists.
- Default ratios: Train ≈ $(1-\text{test}-\text{val})$, Val ≈ $\frac{\text{val}}{1-\text{test}}$ of the train+val pool.
- Output the target distribution per split for auditing.


## Phase 11 — Advanced Exploration

- K‑Means clustering with a small grid search for the best $k$ via Silhouette.
- PCA to retain ≥90% variance when features are many.
- Isolation Forest anomaly detection at 5% contamination and record an `anomaly_flag`.

Silhouette:

$$s = \frac{b - a}{\max(a,b)}$$

where $a$ is the intra‑cluster distance and $b$ is the nearest inter‑cluster distance.


## Phase 11.5 — Feature Selection

- Model‑based ranking via Random Forest (classification/regression per target).
- RFE with logistic regression to select a target number of features.
- Merge the two lists, force‑include business‑approved features, then trim to `top_k`.
- Variance Inflation Factor (VIF) check with a target threshold < 5.

VIF:

$$\text{VIF}(x_j) = \frac{1}{1 - R_j^2}$$

where $R_j^2$ is from regressing feature $x_j$ on the remaining features.


## Phase 12 — Text Analysis

- Detect text columns: average length > 50 characters.
- Language detection (ar/en/mixed) using Arabic character ratio on a sample.
- Processing recommendation:
  - Rows > 500k: skip text analysis.
  - Text volume > 50MB: basic features only.
  - Otherwise: basic + sentiment (MVP).
- Basic features: lengths/words/sentences, numeric/special/Arabic ratios.


## Phase 13 — Monitoring & Drift

- Establish a numeric baseline per feature: mean/std and PSI/KS thresholds.
- Defaults: PSI warn 0.10, action 0.25; KS warn 0.10, action 0.20.
- Output: `drift_config.json` with feature list and baseline timestamp.

---

## Key artifacts

- Data quality: `dq_report.json`, `profile_summary.json`.
- Policies/specs: `imputation_policy.json`, `feature_spec.json`, `mapping_config.json`.
- Analytical results: `correlations.json`/`correlation_matrix.json`, `business_veto_report.json`.
- Processed data: `merged_data.parquet`, `train.parquet`, `validation.parquet`, `test.parquet`, and encoder/scaler files.
- Bundle: `eda_bundle.zip`, `provenance.json`, `changelog.md`.


## How to use this paper

- To understand what happens and why in each phase.
- No code here; implementation may evolve, but the decision logic and equations are the stable reference.
- For operational details and APIs, see: `docs/backend.md`, `docs/validation-and-testing.md`, and the service files under `backend/app/services/`.

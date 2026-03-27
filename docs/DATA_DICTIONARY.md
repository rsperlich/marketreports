# Data Dictionary — `firms_4.rds`

**Source:** Moody's / Bureau van Dijk (BvD) — Austrian corporate financials  
**Format:** R serialized data frame (RDS), loaded via `pyreadr`  
**Shape:** 7,819 rows × 30 columns (3,855 unique firms, all Austrian)  
**Snapshot:** 2023 only (single year, no time series)

---

## Row Structure & Duplication

The raw dataset has **massive row duplication** — a BvD export artifact where identical rows are repeated per firm. After deduplication: **3,855 unique firms**.

| Group | Rows | Unique Firms | Has Financials? |
|-------|------|-------------|-----------------|
| With consolidation code | 3,348 | 2,092 | C1/C2/U1/U2 rows: yes; LF rows: no |
| Without consolidation code | 4,471 | 1,763 | No (name + NACE + employees only) |
| **Zero overlap between groups** | | | |

### Consolidation Codes (BvD)
| Code | Meaning | Count | Has Financial Data |
|------|---------|-------|--------------------|
| C1 | Consolidated (IFRS/local) | 57 | Yes |
| C2 | Consolidated (companion) | 125 | Yes |
| U1 | Unconsolidated | 379 | Yes |
| U2 | Unconsolidated (companion) | 17 | Yes |
| LF | Limited financials | 2,770 | No (employees only) |

**Dedup strategy:** Pick highest-priority consolidation per firm (C1 > C2 > U1 > U2 > LF), then deduplicate by `bvd_listed`.

---

## Columns

### Identifiers
| Raw Column | Friendly Name | Type | Nulls | Description |
|-----------|---------------|------|-------|-------------|
| `bvd_listed` | `bvd_id` | str | 0 | BvD firm identifier (unique per firm) |
| `name_internat` | `firm_name` | str | 0 | International firm name |
| `country_iso_code` | `country` | str | 0 | ISO 3166-1 alpha-2 (**AT only**) |
| `isin_number` | `isin` | str | 6,895 | ISIN (listed firms only) |
| `lei_legal_entity_identifier_` | `lei` | str | 4,001 | Legal Entity Identifier |

### Time & Reporting
| Raw Column | Type | Nulls | Description |
|-----------|------|-------|-------------|
| `year` | object | 4,471 | Reporting year (2023 only) |
| `month` | object | 4,471 | Reporting month |
| `closing_date` | datetime | 4,471 | Financial closing date |
| `number_of_months` | object | 4,471 | Reporting period length |
| `accounting_practice` | str | 7,241 | "Local GAAP" or "IFRS" |
| `consolidation_code` | str | 4,471 | C1/C2/U1/U2/LF |
| `consolidation_rank` | object | 4,471 | BvD internal ranking |
| `accounting_rank` | object | 7,241 | BvD internal ranking |

### Industry Classification
| Raw Column | Friendly Name | Type | Nulls | Description |
|-----------|---------------|------|-------|-------------|
| `nace_rev_2_core_code_4_digits_` | `nace_4digit` | str | 1 | NACE Rev.2 core code (406 unique) |
| `nace_rev_2_primary_code_s_` | `nace_primary` | str | 1 | Primary NACE (405 unique) |
| `nace_rev_2_secondary_code_s_` | `nace_secondary` | str | 2,522 | Secondary NACE code |

### Financial Metrics (available for ~354-578 firms after dedup)
| Raw Column | Friendly Name | Type | Non-null | Range |
|-----------|---------------|------|----------|-------|
| `operating_revenue_turnover_` | `revenue` | float64 | 326 | 1.2M — 20.7B |
| `total_assets` | `total_assets` | float64 | 578 | 195K — 18.6B |
| `cash_flow` | `cash_flow` | float64 | 354 | -9.1M — 2.0B |
| `shareholders_funds` | `shareholders_funds` | float64 | 578 | -4.8M — 8.4B |
| `p_l_before_tax` | `pbt` | float64 | 354 | -10.8M — 1.6B |
| `p_l_for_period_net_income_` | `net_income` | float64 | 354 | -27.0M — 1.2B |
| `number_of_employees` | `employees` | float64 | 3,334 | 2 — 47,473 |

### Pre-computed Ratios (from BvD)
| Raw Column | Friendly Name | Type | Non-null | Range |
|-----------|---------------|------|----------|-------|
| `profit_margin_` | `profit_margin` | float64 | 305 | -29.3% — 94.3% |
| `current_ratio_x_` | `current_ratio` | float64 | 369 | 0.12 — 73.1 |
| `roe_using_p_l_before_tax_` | `roe` | float64 | 354 | -262.8% — 131.5% |
| `roce_using_p_l_before_tax_` | `roce` | float64 | 341 | -132.2% — 83.5% |
| `solvency_ratio_asset_based_` | `solvency_ratio` | float64 | 569 | -28.3% — 100.0% |
| `market_capitalisation_mil_` | `market_cap_mil` | float64 | 188 | 135.6M — 6,073.7M |
| `price_earning_ratio_x_` | `pe_ratio` | float64 | 169 | 2.3 — 67.2 |

### Derived Metrics (computed in pipeline)
| Name | Formula | Description |
|------|---------|-------------|
| `total_debt_proxy` | total_assets − shareholders_funds | Proxy for total debt |
| `debt_to_equity` | debt_proxy / shareholders_funds | Leverage ratio |
| `debt_to_assets` | debt_proxy / total_assets | Leverage ratio |
| `asset_turnover` | revenue / total_assets | Capital efficiency |
| `revenue_per_employee` | revenue / employees | Labor productivity |
| `net_income_margin` | net_income / revenue × 100 | Profitability |
| `nace_2digit` | nace_4digit[:2] | 2-digit sector code |
| `firm_size` | by employees: micro(<10), small(<50), medium(<250), large(250+) | Size category |

### Industry Aggregates (per NACE 4-digit)
| Metric | Description |
|--------|-------------|
| `firm_count` | Number of firms with revenue data |
| `total_industry_revenue` | Sum of all firm revenues |
| `avg_revenue`, `median_revenue` | Central tendency |
| `avg_roe`, `median_roe`, `avg_roce` | Profitability benchmarks |
| `avg_profit_margin` | Industry margin benchmark |
| `avg_solvency`, `avg_current_ratio` | Financial health benchmarks |
| `total_employees`, `avg_employees` | Employment metrics |
| `hhi` | Herfindahl-Hirschman Index (0–10,000) |
| `cr4`, `cr8` | Concentration ratios (top 4/8 firms share %) |

---

## Key Limitations

1. **Austria only** — no cross-border comparison possible with this dataset alone.
2. **Single year (2023)** — no trend/growth analysis, no CAGR computation.
3. **Sparse financials** — only ~578/3,855 firms (15%) have balance sheet data; ~326 (8%) have revenue.
4. **LF firms** — 2,770 firms with "Limited Financials" have only employee counts + NACE codes.
5. **No sub-national geography** — no city/region data.
6. **Revenue skew** — median revenue (612M) << mean (3.7B), heavy right skew from large firms.

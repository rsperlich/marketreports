# Knowledge Graph Schema

## Node Types

### `:Firm`
| Property | Type | Indexed | Description |
|----------|------|---------|-------------|
| `bvd_id` | String | **Yes** | BvD identifier (unique) |
| `name` | String | No | International firm name |
| `employees` | Float | No | Number of employees |
| `firm_size` | String | No | micro / small / medium / large |
| `isin` | String | No | ISIN number (listed firms) |
| `lei` | String | No | Legal Entity Identifier |
| `consolidation_code` | String | No | C1/C2/U1/U2/LF |
| `accounting_practice` | String | No | Local GAAP / IFRS |

### `:Country`
| Property | Type | Indexed | Description |
|----------|------|---------|-------------|
| `iso_code` | String | **Yes** | ISO 3166-1 alpha-2 (AT) |

### `:Industry`
| Property | Type | Indexed | Description |
|----------|------|---------|-------------|
| `nace_4digit` | String | **Yes** | NACE Rev.2 4-digit code |
| `nace_2digit` | String | **Yes** | 2-digit sector code |

### `:FinancialSnapshot`
| Property | Type | Description |
|----------|------|-------------|
| `bvd_id` | String | Links back to firm (indexed) |
| `year` | Integer | Reporting year (2023) |
| `revenue` | Float | Operating revenue/turnover |
| `total_assets` | Float | Total assets |
| `cash_flow` | Float | Cash flow |
| `shareholders_funds` | Float | Shareholders' funds |
| `pbt` | Float | Profit/loss before tax |
| `net_income` | Float | Net income |
| `profit_margin` | Float | Profit margin % |
| `current_ratio` | Float | Current ratio |
| `roe` | Float | Return on equity (pre-tax) % |
| `roce` | Float | Return on capital employed % |
| `solvency_ratio` | Float | Solvency ratio (asset-based) % |
| `market_cap_mil` | Float | Market cap in millions |
| `pe_ratio` | Float | Price/earnings ratio |
| `debt_to_equity` | Float | Derived leverage ratio |
| `debt_to_assets` | Float | Derived leverage ratio |
| `asset_turnover` | Float | Revenue / total assets |
| `revenue_per_employee` | Float | Revenue / employees |
| `net_income_margin` | Float | Net income / revenue % |

### `:IndustryMetrics`
| Property | Type | Description |
|----------|------|-------------|
| `nace_4digit` | String | Industry code (indexed) |
| `firm_count` | Integer | Firms with revenue data |
| `total_industry_revenue` | Float | Sum of revenues |
| `avg_revenue` | Float | Mean revenue |
| `median_revenue` | Float | Median revenue |
| `avg_roe` | Float | Mean ROE |
| `median_roe` | Float | Median ROE |
| `avg_roce` | Float | Mean ROCE |
| `avg_profit_margin` | Float | Mean profit margin |
| `avg_solvency` | Float | Mean solvency ratio |
| `avg_current_ratio` | Float | Mean current ratio |
| `total_employees` | Float | Total industry employment |
| `avg_employees` | Float | Mean employees per firm |
| `hhi` | Float | Herfindahl-Hirschman Index (0–10,000) |
| `cr4` | Float | Top-4 concentration ratio % |
| `cr8` | Float | Top-8 concentration ratio % |

## Relationships

```
(Firm)-[:HEADQUARTERED_IN]->(Country)
(Firm)-[:OPERATES_IN {role: 'primary'|'secondary'}]->(Industry)
(Firm)-[:HAS_FINANCIALS]->(FinancialSnapshot)
(Industry)-[:HAS_METRICS]->(IndustryMetrics)
```

## Indexes

| Index | Purpose |
|-------|---------|
| `Firm(bvd_id)` | Fast firm lookup |
| `Country(iso_code)` | Country matching |
| `Industry(nace_4digit)` | Industry lookup by 4-digit NACE |
| `Industry(nace_2digit)` | Sector-level queries |
| `FinancialSnapshot(bvd_id)` | Join financials to firms |
| `IndustryMetrics(nace_4digit)` | Industry metric lookup |

## Why These Indexes?

- **`Firm(bvd_id)`** — Every query starts from or joins to firms by their BvD ID.
- **`Country(iso_code)`** — Currently AT only, but critical when adding cross-border data.
- **`Industry(nace_4digit)`** — Primary query dimension for report generation.
- **`Industry(nace_2digit)`** — Sector-level aggregation queries (e.g., "all financial services").
- **`FinancialSnapshot(bvd_id)`** — Fast join when traversing `Firm → FinancialSnapshot`.
- **`IndustryMetrics(nace_4digit)`** — Direct lookup of pre-computed industry benchmarks.

## Diagram

```
┌──────────┐     HEADQUARTERED_IN     ┌──────────┐
│  Firm    │ ──────────────────────── │ Country  │
│ (3,855)  │                          │ (1: AT)  │
└────┬─────┘                          └──────────┘
     │
     │ OPERATES_IN
     │ {role: primary|secondary}
     ▼
┌──────────┐     HAS_METRICS     ┌─────────────────┐
│ Industry │ ──────────────────► │ IndustryMetrics  │
│  (406)   │                     │ (56 w/ revenue)  │
└──────────┘                     └─────────────────┘
     ▲
     │
     │ (via Firm)
     │
┌────┴─────┐
│ Financial│
│ Snapshot │
│ (~578)   │
└──────────┘
```

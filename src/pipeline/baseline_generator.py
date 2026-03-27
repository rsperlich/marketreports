"""
Baseline report generator — uses raw DataFrame summary instead of KG context.

This provides the comparison for RQ2: does the KG improve report quality
vs. feeding the LLM a naive data dump?
"""

import logging
from pathlib import Path

import pandas as pd

from src.config import get_model, get_openai_client

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a senior international business analyst specializing in cross-border \
market entry analysis for the Austrian market. You produce rigorous, \
data-grounded reports for corporate decision-makers evaluating market entry or \
competitive positioning in Austria.

Your reports must:
1. Reference specific firms, metrics, and figures from the provided data context.
2. Be structured, professional, and actionable.
3. Flag data limitations explicitly.

Never fabricate data. If a metric is not available, say so explicitly.
"""

BASELINE_TEMPLATE = """\
Generate a comprehensive market entry report for NACE industry {nace_4digit} \
in Austria based on the raw data summary below.

Structure your report as follows:

# Market Entry Report: NACE {nace_4digit} — Austria

## 1. Executive Summary
## 2. Market Size & Structure
## 3. Profitability Landscape
## 4. Capital Efficiency & Solvency
## 5. Competitive Concentration
## 6. Key Players
## 7. Risk Factors & Market Barriers
## 8. Recommendations

---

### Raw Data Summary:

{context}
"""


def build_baseline_context(nace_4digit: str, data_dir: str = "data") -> str:
    """Build a naive text summary directly from DataFrames — no KG involved."""
    data = Path(data_dir)
    firms = pd.read_parquet(data / "firms_cleaned.parquet")
    industry_agg = pd.read_parquet(data / "industry_aggregates.parquet")

    # Filter to target industry
    ind_firms = firms[firms["nace_4digit"] == nace_4digit]
    ind_agg = industry_agg[industry_agg["nace_4digit"] == nace_4digit]

    sections = []

    # Industry aggregate stats (just dump the row)
    if not ind_agg.empty:
        sections.append("## Industry Aggregate Statistics")
        row = ind_agg.iloc[0]
        for col in ind_agg.columns:
            val = row[col]
            if pd.notna(val):
                sections.append(f"- {col}: {val}")

    # Firm count
    sections.append(f"\n## Firms in Industry: {len(ind_firms)} total")

    # Dump all firms with financials — raw tabular
    with_fin = ind_firms[ind_firms["total_assets"].notna()]
    if not with_fin.empty:
        sections.append(f"\n## Firms with Financial Data ({len(with_fin)}):")
        cols = [
            "firm_name", "employees", "revenue", "total_assets", "net_income",
            "roe", "roce", "profit_margin", "current_ratio", "solvency_ratio",
            "market_cap_mil", "pe_ratio",
        ]
        existing_cols = [c for c in cols if c in with_fin.columns]
        sections.append(with_fin[existing_cols].to_string(index=False))

    # Firms without financials (just names + employees)
    no_fin = ind_firms[ind_firms["total_assets"].isna()]
    if not no_fin.empty:
        sections.append(f"\n## Firms without Financial Data ({len(no_fin)}):")
        name_cols = ["firm_name", "employees", "firm_size"]
        existing = [c for c in name_cols if c in no_fin.columns]
        # Limit to first 20
        sections.append(no_fin[existing].head(20).to_string(index=False))
        if len(no_fin) > 20:
            sections.append(f"... and {len(no_fin) - 20} more firms")

    context = "\n".join(sections)
    log.info(f"Baseline context: {len(context)} chars (~{len(context)//4} tokens)")
    return context


def generate_baseline_report(
    context: str,
    nace_4digit: str,
    model: str | None = None,
    temperature: float = 0.3,
) -> str:
    """Generate a baseline report from raw DataFrame context (no KG)."""
    if model is None:
        model = get_model()
    else:
        model = get_model(model)

    client = get_openai_client()

    user_prompt = BASELINE_TEMPLATE.format(nace_4digit=nace_4digit, context=context)

    log.info(f"Generating BASELINE report with {model} (temp={temperature})")
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=temperature,
        max_tokens=4096,
    )

    report = response.choices[0].message.content
    usage = response.usage
    log.info(
        f"Baseline: {len(report)} chars | "
        f"Tokens: {usage.prompt_tokens} in, {usage.completion_tokens} out"
    )
    return report

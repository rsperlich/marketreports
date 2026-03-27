"""
LLM-powered report generator using OpenAI API.

Takes structured KG context and generates a grounded market entry report.
"""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a senior international business analyst specializing in cross-border \
market entry analysis for the Austrian market. You produce rigorous, \
data-grounded reports for corporate decision-makers evaluating market entry or \
competitive positioning in Austria.

Your reports must:
1. Reference specific firms, metrics, and figures from the provided data context.
2. Interpret financial ratios in the context of industry benchmarks (e.g., HHI \
   thresholds: <1500 = unconcentrated, 1500-2500 = moderately concentrated, \
   >2500 = highly concentrated).
3. Be structured, professional, and actionable.
4. Flag data limitations explicitly (e.g., missing metrics, small sample sizes).
5. Use Austrian/European regulatory and business context where relevant.

Never fabricate data. If a metric is not available, say so explicitly.
"""

REPORT_TEMPLATE = """\
Generate a comprehensive market entry report for NACE industry {nace_4digit} \
in Austria based on the data context below.

Structure your report as follows:

# Market Entry Report: NACE {nace_4digit} — Austria

## 1. Executive Summary
Brief overview of key findings and market attractiveness assessment.

## 2. Market Size & Structure
Total industry revenue, number of firms, firm size distribution, employment.

## 3. Profitability Landscape
ROE, ROCE, profit margins — comparison of top players vs. industry averages.

## 4. Capital Efficiency & Solvency
Asset turnover, debt ratios, solvency ratios, current ratio analysis.

## 5. Competitive Concentration
HHI interpretation, CR4/CR8, market power distribution.

## 6. Key Players
Profile the top firms with specific financial metrics.

## 7. Risk Factors & Market Barriers
Based on concentration, capital requirements, regulatory environment.

## 8. Recommendations
Actionable insights for a potential market entrant.

---

### Data Context (from Knowledge Graph):

{context}
"""


def generate_report(
    context: str,
    nace_4digit: str,
    model: str | None = None,
    temperature: float = 0.3,
) -> str:
    """Generate a market entry report from KG context."""
    if model is None:
        model = os.getenv("OPENAI_MODEL_DEV", "gpt-4o-mini")

    base_url = os.getenv("OPENAI_BASE_URL")
    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url=base_url if base_url else None,
    )

    user_prompt = REPORT_TEMPLATE.format(nace_4digit=nace_4digit, context=context)

    log.info(f"Generating report with {model} (temp={temperature})")
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
        f"Generated {len(report)} chars | "
        f"Tokens: {usage.prompt_tokens} in, {usage.completion_tokens} out"
    )
    return report


def save_report(report: str, nace_4digit: str, output_dir: str = "reports") -> str:
    """Save report to markdown file."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"AT_{nace_4digit}_{timestamp}.md"
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(report)
    log.info(f"Report saved to {path}")
    return path

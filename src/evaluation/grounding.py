"""
Grounding evaluator — checks whether numeric claims in LLM reports
are supported by the underlying data (KG or parquet).

Answers RQ2: Does structuring data via KG improve factual grounding?

Approach:
  1. Extract numeric claims from report text (regex-based).
  2. Build a lookup of "ground truth" values from KG context or parquet.
  3. For each extracted claim, check if there's a matching ground-truth value.
  4. Compute grounding rate = matched / total_claims.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


@dataclass
class Claim:
    """A numeric claim extracted from report text."""
    text: str          # surrounding sentence / phrase
    value: float       # parsed numeric value
    unit: str = ""     # %, B, M, K, or empty
    matched: bool = False
    match_source: str = ""   # which ground-truth field matched


@dataclass
class GroundingResult:
    """Result of grounding evaluation for a single report."""
    nace_4digit: str
    report_type: str        # "kg" or "baseline"
    total_claims: int = 0
    matched_claims: int = 0
    unmatched_claims: list = field(default_factory=list)
    claims: list = field(default_factory=list)

    @property
    def grounding_rate(self) -> float:
        if self.total_claims == 0:
            return 0.0
        return self.matched_claims / self.total_claims


# Regex patterns for numeric values in report text
_NUM_PATTERNS = [
    # "123.45B" or "123.45 billion"
    re.compile(r"([\d,]+\.?\d*)\s*(billion|B)\b", re.IGNORECASE),
    # "123.45M" or "123.45 million"
    re.compile(r"([\d,]+\.?\d*)\s*(million|M)\b", re.IGNORECASE),
    # "123.45K" or "123.45 thousand"
    re.compile(r"([\d,]+\.?\d*)\s*(thousand|K)\b", re.IGNORECASE),
    # Percentages: "12.3%"
    re.compile(r"([-]?[\d,]+\.?\d*)\s*%"),
    # Plain numbers in financial context (at least 4 digits, likely a financial figure)
    re.compile(r"(?:€|EUR|USD)\s*([\d,]+\.?\d*)"),
    # Firm count patterns: "123 firms"
    re.compile(r"([\d,]+)\s+firms?\b", re.IGNORECASE),
    # Employee counts: "1,234 employees"
    re.compile(r"([\d,]+)\s+employees?\b", re.IGNORECASE),
]

_MULTIPLIERS = {
    "billion": 1e9, "b": 1e9,
    "million": 1e6, "m": 1e6,
    "thousand": 1e3, "k": 1e3,
}


def _parse_number(raw: str, unit: str = "") -> float:
    """Parse a number string like '1,234.56' with optional unit multiplier."""
    cleaned = raw.replace(",", "")
    value = float(cleaned)
    mult = _MULTIPLIERS.get(unit.lower(), 1.0)
    return value * mult


def extract_claims(report_text: str) -> list[Claim]:
    """Extract numeric claims from a report."""
    claims = []
    seen_values = set()
    # Split into sentences for context (avoid splitting on decimal points like "1.5")
    sentences = re.split(r"(?<!\d)[.!?]\s+|\n", report_text)

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        for pattern in _NUM_PATTERNS:
            for match in pattern.finditer(sentence):
                groups = match.groups()
                raw_num = groups[0]
                unit = groups[1] if len(groups) > 1 else ""

                try:
                    value = _parse_number(raw_num, unit)
                except ValueError:
                    continue

                # Deduplicate same value in same sentence
                key = (sentence[:50], round(value, 2))
                if key in seen_values:
                    continue
                seen_values.add(key)

                # Determine unit category
                if "%" in match.group():
                    unit_cat = "%"
                elif unit.lower() in ("billion", "b"):
                    unit_cat = "B"
                elif unit.lower() in ("million", "m"):
                    unit_cat = "M"
                elif unit.lower() in ("thousand", "k"):
                    unit_cat = "K"
                else:
                    unit_cat = ""

                claims.append(Claim(
                    text=sentence[:120],
                    value=value,
                    unit=unit_cat,
                ))

    log.info(f"Extracted {len(claims)} numeric claims from report")
    return claims


def build_ground_truth(nace_4digit: str, data_dir: str = "data") -> dict[str, float]:
    """Build a flat dictionary of ground-truth numeric values from parquet data."""
    data = Path(data_dir)
    truth = {}

    # Industry aggregates
    agg = pd.read_parquet(data / "industry_aggregates.parquet")
    ind = agg[agg["nace_4digit"] == nace_4digit]
    if not ind.empty:
        row = ind.iloc[0]
        for col in agg.columns:
            val = row[col]
            if pd.notna(val) and isinstance(val, (int, float)):
                truth[f"industry_{col}"] = float(val)

    # Firm-level data
    firms = pd.read_parquet(data / "firms_cleaned.parquet")
    ind_firms = firms[firms["nace_4digit"] == nace_4digit]

    # Total firm count
    truth["firm_count"] = float(len(ind_firms))

    # Individual firm metrics (top firms by revenue)
    with_rev = ind_firms[ind_firms["revenue"].notna()].sort_values("revenue", ascending=False)
    for _, firm in with_rev.iterrows():
        name = firm.get("firm_name", "")
        for col in ["revenue", "total_assets", "net_income", "employees", "roe",
                     "roce", "profit_margin", "solvency_ratio", "current_ratio",
                     "market_cap_mil", "pe_ratio", "cash_flow", "shareholders_funds",
                     "debt_to_equity", "debt_to_assets", "asset_turnover",
                     "revenue_per_employee"]:
            val = firm.get(col)
            if pd.notna(val):
                truth[f"firm_{name}_{col}"] = float(val)
                # Also store without firm name for loose matching
                truth[f"any_firm_{col}_{val:.2f}"] = float(val)

    # Aggregate computed values
    for col in ["revenue", "total_assets", "employees", "roe", "roce",
                "profit_margin", "solvency_ratio"]:
        vals = ind_firms[col].dropna()
        if len(vals) > 0:
            truth[f"avg_{col}"] = float(vals.mean())
            truth[f"sum_{col}"] = float(vals.sum())
            truth[f"min_{col}"] = float(vals.min())
            truth[f"max_{col}"] = float(vals.max())
            truth[f"median_{col}"] = float(vals.median())

    log.info(f"Built ground truth with {len(truth)} values for NACE {nace_4digit}")
    return truth


def _values_match(claim_val: float, truth_val: float, tolerance: float = 0.05) -> bool:
    """Check if two values match within tolerance (relative or absolute)."""
    if truth_val == 0:
        return abs(claim_val) < 0.01
    relative_diff = abs(claim_val - truth_val) / abs(truth_val)
    return relative_diff <= tolerance


def verify_claims(claims: list[Claim], ground_truth: dict[str, float],
                  tolerance: float = 0.05) -> list[Claim]:
    """Verify each claim against ground truth. Returns claims with match status updated."""
    for claim in claims:
        for key, truth_val in ground_truth.items():
            if _values_match(claim.value, truth_val, tolerance):
                claim.matched = True
                claim.match_source = key
                break

    matched = sum(1 for c in claims if c.matched)
    log.info(f"Verified claims: {matched}/{len(claims)} matched ({matched/max(len(claims),1)*100:.1f}%)")
    return claims


def evaluate_report(
    report_text: str,
    nace_4digit: str,
    report_type: str = "kg",
    data_dir: str = "data",
    tolerance: float = 0.05,
) -> GroundingResult:
    """Full grounding evaluation pipeline for a single report."""
    claims = extract_claims(report_text)
    ground_truth = build_ground_truth(nace_4digit, data_dir)
    claims = verify_claims(claims, ground_truth, tolerance)

    result = GroundingResult(
        nace_4digit=nace_4digit,
        report_type=report_type,
        total_claims=len(claims),
        matched_claims=sum(1 for c in claims if c.matched),
        unmatched_claims=[c for c in claims if not c.matched],
        claims=claims,
    )

    log.info(
        f"Grounding result for {report_type} NACE {nace_4digit}: "
        f"{result.grounding_rate:.1%} ({result.matched_claims}/{result.total_claims})"
    )
    return result


def compare_grounding(
    kg_report: str,
    baseline_report: str,
    nace_4digit: str,
    data_dir: str = "data",
) -> dict:
    """Compare grounding rates between KG and baseline reports."""
    kg_result = evaluate_report(kg_report, nace_4digit, "kg", data_dir)
    bl_result = evaluate_report(baseline_report, nace_4digit, "baseline", data_dir)

    return {
        "nace_4digit": nace_4digit,
        "kg_grounding_rate": kg_result.grounding_rate,
        "kg_total_claims": kg_result.total_claims,
        "kg_matched_claims": kg_result.matched_claims,
        "baseline_grounding_rate": bl_result.grounding_rate,
        "baseline_total_claims": bl_result.total_claims,
        "baseline_matched_claims": bl_result.matched_claims,
        "grounding_delta": kg_result.grounding_rate - bl_result.grounding_rate,
    }

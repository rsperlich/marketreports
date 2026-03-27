"""
KG triplet accuracy checker — validates Neo4j graph against source data.

Answers RQ1: How accurately can corporate financial data be structured into a KG?

Approach:
  1. Sample N triplets from the Neo4j KG.
  2. For each triplet, look up the corresponding value in the source parquet.
  3. Compute precision (% of KG facts that are correct).
"""

import logging
import random
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.config import get_neo4j_driver

log = logging.getLogger(__name__)


@dataclass
class Triplet:
    """A triplet extracted from the KG for verification."""
    subject: str       # e.g., firm name or BvD ID
    predicate: str     # e.g., "revenue", "operates_in"
    object_val: str    # e.g., "1234567.89", "6419"
    verified: bool | None = None   # True=correct, False=wrong, None=unverifiable
    source_val: str = ""            # value found in source data
    note: str = ""


@dataclass
class TripletAccuracyResult:
    """Aggregated result of triplet verification."""
    total_sampled: int = 0
    correct: int = 0
    incorrect: int = 0
    unverifiable: int = 0
    triplets: list = field(default_factory=list)

    @property
    def precision(self) -> float:
        verifiable = self.correct + self.incorrect
        if verifiable == 0:
            return 0.0
        return self.correct / verifiable

    @property
    def verifiable_rate(self) -> float:
        if self.total_sampled == 0:
            return 0.0
        return (self.correct + self.incorrect) / self.total_sampled


def sample_firm_financial_triplets(driver, n: int = 50) -> list[Triplet]:
    """Sample firm → financial metric triplets from the KG."""
    with driver.session() as session:
        result = session.run("""
            MATCH (f:Firm)-[:HAS_FINANCIALS]->(fs:FinancialSnapshot)
            RETURN f.bvd_id AS bvd_id, f.name AS firm_name,
                   fs.revenue AS revenue, fs.total_assets AS total_assets,
                   fs.net_income AS net_income, fs.roe AS roe,
                   fs.roce AS roce, fs.profit_margin AS profit_margin,
                   fs.solvency_ratio AS solvency_ratio,
                   fs.current_ratio AS current_ratio,
                   fs.market_cap_mil AS market_cap_mil,
                   fs.cash_flow AS cash_flow,
                   fs.employees AS employees
        """)
        records = [dict(r) for r in result]

    if not records:
        log.warning("No firm-financial records found in KG")
        return []

    triplets = []
    metrics = ["revenue", "total_assets", "net_income", "roe", "roce",
               "profit_margin", "solvency_ratio", "current_ratio",
               "market_cap_mil", "cash_flow", "employees"]

    for record in records:
        bvd_id = record["bvd_id"]
        firm_name = record.get("firm_name", bvd_id)
        for metric in metrics:
            val = record.get(metric)
            if val is not None:
                triplets.append(Triplet(
                    subject=bvd_id,
                    predicate=metric,
                    object_val=str(val),
                ))

    # Random sample
    if len(triplets) > n:
        triplets = random.sample(triplets, n)

    log.info(f"Sampled {len(triplets)} firm-financial triplets from KG")
    return triplets


def sample_industry_triplets(driver, n: int = 20) -> list[Triplet]:
    """Sample firm → industry relationship triplets."""
    with driver.session() as session:
        result = session.run("""
            MATCH (f:Firm)-[r:OPERATES_IN]->(i:Industry)
            RETURN f.bvd_id AS bvd_id, i.nace_4digit AS nace_4digit,
                   r.role AS role
        """)
        records = [dict(r) for r in result]

    triplets = [
        Triplet(
            subject=r["bvd_id"],
            predicate="operates_in",
            object_val=r["nace_4digit"],
        )
        for r in records
    ]

    if len(triplets) > n:
        triplets = random.sample(triplets, n)

    log.info(f"Sampled {len(triplets)} industry relationship triplets from KG")
    return triplets


def sample_concentration_triplets(driver, n: int = 20) -> list[Triplet]:
    """Sample industry metrics triplets (HHI, CR4, etc.)."""
    with driver.session() as session:
        result = session.run("""
            MATCH (i:Industry)-[:HAS_METRICS]->(im:IndustryMetrics)
            RETURN i.nace_4digit AS nace_4digit,
                   im.hhi AS hhi, im.cr4 AS cr4, im.cr8 AS cr8,
                   im.firm_count AS firm_count,
                   im.total_industry_revenue AS total_revenue
        """)
        records = [dict(r) for r in result]

    triplets = []
    for r in records:
        nace = r["nace_4digit"]
        for metric in ["hhi", "cr4", "cr8", "firm_count", "total_revenue"]:
            val = r.get(metric)
            if val is not None:
                triplets.append(Triplet(
                    subject=nace,
                    predicate=f"industry_{metric}",
                    object_val=str(val),
                ))

    if len(triplets) > n:
        triplets = random.sample(triplets, n)

    log.info(f"Sampled {len(triplets)} concentration metric triplets from KG")
    return triplets


def verify_against_parquet(
    triplets: list[Triplet],
    data_dir: str = "data",
    tolerance: float = 0.001,
) -> list[Triplet]:
    """Verify KG triplets against source parquet files."""
    data = Path(data_dir)
    firms = pd.read_parquet(data / "firms_cleaned.parquet")
    agg = pd.read_parquet(data / "industry_aggregates.parquet")

    # Build lookup by bvd_id
    firm_lookup = {}
    for _, row in firms.iterrows():
        bvd_id = row.get("bvd_id", "")
        if bvd_id:
            firm_lookup[bvd_id] = row

    # Build industry lookup
    ind_lookup = {}
    for _, row in agg.iterrows():
        ind_lookup[row["nace_4digit"]] = row

    # Parquet column mapping for industry metrics
    ind_metric_map = {
        "industry_hhi": "hhi",
        "industry_cr4": "cr4",
        "industry_cr8": "cr8",
        "industry_firm_count": "firm_count",
        "industry_total_revenue": "total_industry_revenue",
    }

    for t in triplets:
        if t.predicate == "operates_in":
            # Check firm → industry relationship
            firm_row = firm_lookup.get(t.subject)
            if firm_row is not None:
                source_nace = firm_row.get("nace_4digit", "")
                nace_primary = firm_row.get("nace_primary", "")
                t.source_val = str(source_nace)
                if str(source_nace) == t.object_val or str(nace_primary).startswith(t.object_val):
                    t.verified = True
                else:
                    # Could also be a secondary NACE code
                    nace_sec = str(firm_row.get("nace_secondary", ""))
                    if t.object_val in nace_sec:
                        t.verified = True
                    else:
                        t.verified = False
                        t.note = f"Expected {t.object_val}, found primary={source_nace}"
            else:
                t.verified = None
                t.note = "BvD ID not found in source"

        elif t.predicate in ind_metric_map:
            # Check industry metric
            ind_row = ind_lookup.get(t.subject)
            if ind_row is not None:
                col = ind_metric_map[t.predicate]
                source_val = ind_row.get(col)
                if pd.notna(source_val):
                    t.source_val = str(source_val)
                    kg_val = float(t.object_val)
                    src_val = float(source_val)
                    if src_val == 0:
                        t.verified = abs(kg_val) < 0.01
                    else:
                        t.verified = abs(kg_val - src_val) / abs(src_val) <= tolerance
                    if not t.verified:
                        t.note = f"KG={kg_val}, source={src_val}"
                else:
                    t.verified = None
                    t.note = "Source value is NaN"
            else:
                t.verified = None
                t.note = "NACE code not found in aggregates"

        else:
            # Firm financial metric
            firm_row = firm_lookup.get(t.subject)
            if firm_row is not None:
                # Map metric name to parquet column
                col = t.predicate  # they should match since we used same names
                source_val = firm_row.get(col)
                if pd.notna(source_val):
                    t.source_val = str(source_val)
                    try:
                        kg_val = float(t.object_val)
                        src_val = float(source_val)
                        if src_val == 0:
                            t.verified = abs(kg_val) < 0.01
                        else:
                            t.verified = abs(kg_val - src_val) / abs(src_val) <= tolerance
                        if not t.verified:
                            t.note = f"KG={kg_val}, source={src_val}"
                    except ValueError:
                        t.verified = None
                        t.note = "Cannot parse numeric value"
                else:
                    t.verified = None
                    t.note = "Source value is NaN"
            else:
                t.verified = None
                t.note = "BvD ID not found in source"

    return triplets


def evaluate_triplet_accuracy(
    data_dir: str = "data",
    n_financial: int = 50,
    n_industry: int = 20,
    n_concentration: int = 20,
    seed: int = 42,
) -> TripletAccuracyResult:
    """Full triplet accuracy evaluation pipeline."""
    random.seed(seed)
    driver = get_neo4j_driver()

    try:
        # Sample triplets from different relationship types
        all_triplets = []
        all_triplets.extend(sample_firm_financial_triplets(driver, n_financial))
        all_triplets.extend(sample_industry_triplets(driver, n_industry))
        all_triplets.extend(sample_concentration_triplets(driver, n_concentration))

        # Verify against source
        all_triplets = verify_against_parquet(all_triplets, data_dir)

        # Aggregate results
        result = TripletAccuracyResult(
            total_sampled=len(all_triplets),
            correct=sum(1 for t in all_triplets if t.verified is True),
            incorrect=sum(1 for t in all_triplets if t.verified is False),
            unverifiable=sum(1 for t in all_triplets if t.verified is None),
            triplets=all_triplets,
        )

        log.info(
            f"Triplet accuracy: {result.precision:.1%} precision "
            f"({result.correct}/{result.correct + result.incorrect} verifiable), "
            f"{result.unverifiable} unverifiable out of {result.total_sampled}"
        )
        return result

    finally:
        driver.close()

"""
Neo4j Knowledge Graph builder.

Ontology:
  (:Firm {bvd_id, name, employees, firm_size, isin, lei})
  (:Country {iso_code})
  (:Industry {nace_4digit, nace_2digit, description})
  (:FinancialSnapshot {year, revenue, total_assets, cash_flow, shareholders_funds,
       pbt, net_income, profit_margin, current_ratio, roe, roce,
       solvency_ratio, market_cap_mil, pe_ratio,
       debt_to_equity, debt_to_assets, asset_turnover,
       revenue_per_employee, net_income_margin})
  (:IndustryMetrics {nace_4digit, firm_count, total_industry_revenue,
       avg_revenue, median_revenue, avg_roe, median_roe, avg_roce,
       avg_profit_margin, avg_solvency, avg_current_ratio,
       total_employees, avg_employees, hhi, cr4, cr8})

Relationships:
  (Firm)-[:HEADQUARTERED_IN]->(Country)
  (Firm)-[:OPERATES_IN {role: 'primary'}]->(Industry)
  (Firm)-[:OPERATES_IN {role: 'secondary'}]->(Industry)
  (Firm)-[:HAS_FINANCIALS]->(FinancialSnapshot)
  (Industry)-[:HAS_METRICS]->(IndustryMetrics)
"""

import logging
from pathlib import Path

import pandas as pd

from src.config import get_neo4j_driver

log = logging.getLogger(__name__)


def clear_database(driver):
    """Remove all nodes and relationships."""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    log.info("Database cleared")


def create_indexes(driver):
    """Create indexes for fast lookups."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (f:Firm) ON (f.bvd_id)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Country) ON (c.iso_code)",
        "CREATE INDEX IF NOT EXISTS FOR (i:Industry) ON (i.nace_4digit)",
        "CREATE INDEX IF NOT EXISTS FOR (i:Industry) ON (i.nace_2digit)",
        "CREATE INDEX IF NOT EXISTS FOR (fs:FinancialSnapshot) ON (fs.bvd_id)",
        "CREATE INDEX IF NOT EXISTS FOR (im:IndustryMetrics) ON (im.nace_4digit)",
    ]
    with driver.session() as session:
        for idx in indexes:
            session.run(idx)
    log.info(f"Created {len(indexes)} indexes")


def create_country_nodes(driver, df: pd.DataFrame):
    """Create Country nodes."""
    countries = df["country"].dropna().unique()
    with driver.session() as session:
        for iso in countries:
            session.run(
                "MERGE (c:Country {iso_code: $iso})",
                iso=str(iso),
            )
    log.info(f"Created {len(countries)} Country nodes")


def create_industry_nodes(driver, df: pd.DataFrame):
    """Create Industry nodes from NACE codes."""
    industries = df[["nace_4digit", "nace_2digit"]].dropna(subset=["nace_4digit"]).drop_duplicates()
    with driver.session() as session:
        for _, row in industries.iterrows():
            session.run(
                """
                MERGE (i:Industry {nace_4digit: $nace4})
                SET i.nace_2digit = $nace2
                """,
                nace4=str(row["nace_4digit"]),
                nace2=str(row["nace_2digit"]) if pd.notna(row.get("nace_2digit")) else None,
            )
    log.info(f"Created {len(industries)} Industry nodes")


def create_firm_nodes_and_relationships(driver, df: pd.DataFrame, batch_size: int = 500):
    """Create Firm nodes, FinancialSnapshot nodes, and relationships in batches."""
    total = 0
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start : start + batch_size]
        with driver.session() as session:
            for _, row in batch.iterrows():
                bvd = str(row["bvd_id"])
                # Create Firm node
                session.run(
                    """
                    MERGE (f:Firm {bvd_id: $bvd_id})
                    SET f.name = $name,
                        f.employees = $employees,
                        f.firm_size = $firm_size,
                        f.isin = $isin,
                        f.lei = $lei,
                        f.consolidation_code = $consol,
                        f.accounting_practice = $acct
                    """,
                    bvd_id=bvd,
                    name=_str_or_none(row.get("firm_name")),
                    employees=_float_or_none(row.get("employees")),
                    firm_size=_str_or_none(row.get("firm_size")),
                    isin=_str_or_none(row.get("isin")),
                    lei=_str_or_none(row.get("lei")),
                    consol=_str_or_none(row.get("consolidation_code")),
                    acct=_str_or_none(row.get("accounting_practice")),
                )

                # Firm -> Country
                country = _str_or_none(row.get("country"))
                if country:
                    session.run(
                        """
                        MATCH (f:Firm {bvd_id: $bvd_id})
                        MATCH (c:Country {iso_code: $country})
                        MERGE (f)-[:HEADQUARTERED_IN]->(c)
                        """,
                        bvd_id=bvd,
                        country=country,
                    )

                # Firm -> Industry (primary)
                nace_primary = _str_or_none(row.get("nace_4digit"))
                if nace_primary:
                    session.run(
                        """
                        MATCH (f:Firm {bvd_id: $bvd_id})
                        MATCH (i:Industry {nace_4digit: $nace})
                        MERGE (f)-[:OPERATES_IN {role: 'primary'}]->(i)
                        """,
                        bvd_id=bvd,
                        nace=nace_primary,
                    )

                # Firm -> Industry (secondary)
                nace_secondary = _str_or_none(row.get("nace_secondary"))
                if nace_secondary:
                    session.run(
                        """
                        MATCH (f:Firm {bvd_id: $bvd_id})
                        MERGE (i:Industry {nace_4digit: $nace})
                        MERGE (f)-[:OPERATES_IN {role: 'secondary'}]->(i)
                        """,
                        bvd_id=bvd,
                        nace=nace_secondary,
                    )

                # FinancialSnapshot (only if firm has financial data)
                if pd.notna(row.get("total_assets")) or pd.notna(row.get("revenue")):
                    session.run(
                        """
                        MATCH (f:Firm {bvd_id: $bvd_id})
                        CREATE (fs:FinancialSnapshot {
                            bvd_id: $bvd_id,
                            year: 2023,
                            revenue: $revenue,
                            total_assets: $total_assets,
                            cash_flow: $cash_flow,
                            shareholders_funds: $shareholders_funds,
                            pbt: $pbt,
                            net_income: $net_income,
                            profit_margin: $profit_margin,
                            current_ratio: $current_ratio,
                            roe: $roe,
                            roce: $roce,
                            solvency_ratio: $solvency_ratio,
                            market_cap_mil: $market_cap_mil,
                            pe_ratio: $pe_ratio,
                            debt_to_equity: $debt_to_equity,
                            debt_to_assets: $debt_to_assets,
                            asset_turnover: $asset_turnover,
                            revenue_per_employee: $revenue_per_employee,
                            net_income_margin: $net_income_margin
                        })
                        CREATE (f)-[:HAS_FINANCIALS]->(fs)
                        """,
                        bvd_id=bvd,
                        revenue=_float_or_none(row.get("revenue")),
                        total_assets=_float_or_none(row.get("total_assets")),
                        cash_flow=_float_or_none(row.get("cash_flow")),
                        shareholders_funds=_float_or_none(row.get("shareholders_funds")),
                        pbt=_float_or_none(row.get("pbt")),
                        net_income=_float_or_none(row.get("net_income")),
                        profit_margin=_float_or_none(row.get("profit_margin")),
                        current_ratio=_float_or_none(row.get("current_ratio")),
                        roe=_float_or_none(row.get("roe")),
                        roce=_float_or_none(row.get("roce")),
                        solvency_ratio=_float_or_none(row.get("solvency_ratio")),
                        market_cap_mil=_float_or_none(row.get("market_cap_mil")),
                        pe_ratio=_float_or_none(row.get("pe_ratio")),
                        debt_to_equity=_float_or_none(row.get("debt_to_equity")),
                        debt_to_assets=_float_or_none(row.get("debt_to_assets")),
                        asset_turnover=_float_or_none(row.get("asset_turnover")),
                        revenue_per_employee=_float_or_none(row.get("revenue_per_employee")),
                        net_income_margin=_float_or_none(row.get("net_income_margin")),
                    )

                total += 1
        log.info(f"Processed {min(start + batch_size, len(df))}/{len(df)} firms")

    log.info(f"Created {total} Firm nodes with relationships")


def create_industry_metrics(driver, industry_agg: pd.DataFrame):
    """Create IndustryMetrics nodes and link to Industry."""
    with driver.session() as session:
        for _, row in industry_agg.iterrows():
            session.run(
                """
                MATCH (i:Industry {nace_4digit: $nace})
                CREATE (im:IndustryMetrics {
                    nace_4digit: $nace,
                    firm_count: $firm_count,
                    total_industry_revenue: $total_revenue,
                    avg_revenue: $avg_revenue,
                    median_revenue: $median_revenue,
                    avg_roe: $avg_roe,
                    median_roe: $median_roe,
                    avg_roce: $avg_roce,
                    avg_profit_margin: $avg_profit_margin,
                    avg_solvency: $avg_solvency,
                    avg_current_ratio: $avg_current_ratio,
                    total_employees: $total_employees,
                    avg_employees: $avg_employees,
                    hhi: $hhi,
                    cr4: $cr4,
                    cr8: $cr8
                })
                CREATE (i)-[:HAS_METRICS]->(im)
                """,
                nace=str(row["nace_4digit"]),
                firm_count=int(row["firm_count"]),
                total_revenue=_float_or_none(row.get("total_industry_revenue")),
                avg_revenue=_float_or_none(row.get("avg_revenue")),
                median_revenue=_float_or_none(row.get("median_revenue")),
                avg_roe=_float_or_none(row.get("avg_roe")),
                median_roe=_float_or_none(row.get("median_roe")),
                avg_roce=_float_or_none(row.get("avg_roce")),
                avg_profit_margin=_float_or_none(row.get("avg_profit_margin")),
                avg_solvency=_float_or_none(row.get("avg_solvency")),
                avg_current_ratio=_float_or_none(row.get("avg_current_ratio")),
                total_employees=_float_or_none(row.get("total_employees")),
                avg_employees=_float_or_none(row.get("avg_employees")),
                hhi=_float_or_none(row.get("hhi")),
                cr4=_float_or_none(row.get("cr4")),
                cr8=_float_or_none(row.get("cr8")),
            )
    log.info(f"Created {len(industry_agg)} IndustryMetrics nodes")


def validate(driver):
    """Basic validation: count nodes and check sample paths."""
    with driver.session() as session:
        counts = {}
        for label in ["Firm", "Country", "Industry", "FinancialSnapshot", "IndustryMetrics"]:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) AS c")
            counts[label] = result.single()["c"]

        rels = {}
        for rel in ["HEADQUARTERED_IN", "OPERATES_IN", "HAS_FINANCIALS", "HAS_METRICS"]:
            result = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c")
            rels[rel] = result.single()["c"]

    log.info("=== Validation ===")
    for label, count in counts.items():
        log.info(f"  {label}: {count} nodes")
    for rel, count in rels.items():
        log.info(f"  {rel}: {count} relationships")


def _str_or_none(val):
    if pd.isna(val):
        return None
    return str(val)


def _float_or_none(val):
    if pd.isna(val):
        return None
    return float(val)


def run(data_dir: str = "data"):
    """Build the full knowledge graph from cleaned data."""
    data = Path(data_dir)
    firms = pd.read_parquet(data / "firms_cleaned.parquet")
    industry_agg = pd.read_parquet(data / "industry_aggregates.parquet")

    driver = get_neo4j_driver()
    try:
        clear_database(driver)
        create_indexes(driver)
        create_country_nodes(driver, firms)
        create_industry_nodes(driver, firms)
        create_firm_nodes_and_relationships(driver, firms)
        create_industry_metrics(driver, industry_agg)
        validate(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    run()

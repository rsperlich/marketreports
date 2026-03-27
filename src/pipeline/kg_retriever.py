"""
Knowledge Graph retrieval layer.

Parameterized Cypher queries to extract structured context for report generation.
Since the dataset is Austria-only, queries focus on industry-level analysis
with optional cross-industry comparison.
"""

import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def _run_query(driver, query: str, **params) -> list[dict]:
    with driver.session() as session:
        result = session.run(query, **params)
        return [dict(record) for record in result]


def get_industry_overview(driver, nace_4digit: str) -> dict:
    """Get industry-level overview: metrics, firm count, concentration."""
    rows = _run_query(
        driver,
        """
        MATCH (i:Industry {nace_4digit: $nace})-[:HAS_METRICS]->(im:IndustryMetrics)
        RETURN im {.*} AS metrics
        """,
        nace=nace_4digit,
    )
    if rows:
        return rows[0]["metrics"]

    # Fallback: compute from firm data directly
    rows = _run_query(
        driver,
        """
        MATCH (f:Firm)-[:OPERATES_IN]->(i:Industry {nace_4digit: $nace})
        OPTIONAL MATCH (f)-[:HAS_FINANCIALS]->(fs:FinancialSnapshot)
        RETURN count(DISTINCT f) AS firm_count,
               avg(fs.revenue) AS avg_revenue,
               avg(fs.roe) AS avg_roe,
               avg(fs.roce) AS avg_roce,
               sum(f.employees) AS total_employees
        """,
        nace=nace_4digit,
    )
    return rows[0] if rows else {}


def get_top_firms(driver, nace_4digit: str, top_n: int = 10, sort_by: str = "revenue") -> list[dict]:
    """Get top firms in an industry by revenue, assets, or employees."""
    valid_sorts = {"revenue": "fs.revenue", "assets": "fs.total_assets", "employees": "f.employees"}
    sort_field = valid_sorts.get(sort_by, "fs.revenue")

    return _run_query(
        driver,
        f"""
        MATCH (f:Firm)-[:OPERATES_IN {{role: 'primary'}}]->(i:Industry {{nace_4digit: $nace}})
        OPTIONAL MATCH (f)-[:HAS_FINANCIALS]->(fs:FinancialSnapshot)
        WHERE {sort_field} IS NOT NULL
        RETURN f.name AS firm_name,
               f.bvd_id AS bvd_id,
               f.employees AS employees,
               f.firm_size AS firm_size,
               fs.revenue AS revenue,
               fs.total_assets AS total_assets,
               fs.net_income AS net_income,
               fs.roe AS roe,
               fs.roce AS roce,
               fs.profit_margin AS profit_margin,
               fs.current_ratio AS current_ratio,
               fs.solvency_ratio AS solvency_ratio,
               fs.market_cap_mil AS market_cap_mil,
               fs.pe_ratio AS pe_ratio,
               fs.debt_to_equity AS debt_to_equity,
               fs.debt_to_assets AS debt_to_assets,
               fs.asset_turnover AS asset_turnover,
               fs.revenue_per_employee AS revenue_per_employee
        ORDER BY {sort_field} DESC
        LIMIT $top_n
        """,
        nace=nace_4digit,
        top_n=top_n,
    )


def get_all_firms_in_industry(driver, nace_4digit: str) -> list[dict]:
    """Get all firms in an industry (including those without financials)."""
    return _run_query(
        driver,
        """
        MATCH (f:Firm)-[:OPERATES_IN]->(i:Industry {nace_4digit: $nace})
        OPTIONAL MATCH (f)-[:HAS_FINANCIALS]->(fs:FinancialSnapshot)
        RETURN f.name AS firm_name,
               f.bvd_id AS bvd_id,
               f.employees AS employees,
               f.firm_size AS firm_size,
               fs.revenue AS revenue,
               fs.total_assets AS total_assets,
               fs.roe AS roe,
               fs.roce AS roce,
               fs.profit_margin AS profit_margin,
               fs.solvency_ratio AS solvency_ratio
        ORDER BY fs.revenue DESC NULLS LAST
        """,
        nace=nace_4digit,
    )


def get_industry_comparison(driver, nace_a: str, nace_b: str) -> list[dict]:
    """Side-by-side comparison of two industries."""
    return _run_query(
        driver,
        """
        MATCH (i:Industry)-[:HAS_METRICS]->(im:IndustryMetrics)
        WHERE i.nace_4digit IN [$nace_a, $nace_b]
        RETURN i.nace_4digit AS nace_4digit,
               im {.*} AS metrics
        ORDER BY i.nace_4digit
        """,
        nace_a=nace_a,
        nace_b=nace_b,
    )


def get_sector_overview(driver, nace_2digit: str) -> list[dict]:
    """Get overview of a 2-digit NACE sector (all sub-industries)."""
    return _run_query(
        driver,
        """
        MATCH (i:Industry {nace_2digit: $nace2})-[:HAS_METRICS]->(im:IndustryMetrics)
        RETURN i.nace_4digit AS nace_4digit,
               im.firm_count AS firm_count,
               im.total_industry_revenue AS total_revenue,
               im.avg_roe AS avg_roe,
               im.hhi AS hhi,
               im.cr4 AS cr4
        ORDER BY im.total_industry_revenue DESC
        """,
        nace2=nace_2digit,
    )


def get_market_concentration(driver, nace_4digit: str) -> dict:
    """Get detailed concentration metrics for an industry."""
    rows = _run_query(
        driver,
        """
        MATCH (i:Industry {nace_4digit: $nace})-[:HAS_METRICS]->(im:IndustryMetrics)
        RETURN im.hhi AS hhi, im.cr4 AS cr4, im.cr8 AS cr8,
               im.firm_count AS firm_count,
               im.total_industry_revenue AS total_revenue
        """,
        nace=nace_4digit,
    )
    if not rows:
        return {}

    result = rows[0]
    # Classify HHI
    hhi = result.get("hhi")
    if hhi is not None:
        if hhi < 1500:
            result["hhi_classification"] = "unconcentrated"
        elif hhi < 2500:
            result["hhi_classification"] = "moderately concentrated"
        else:
            result["hhi_classification"] = "highly concentrated"
    return result


def get_firm_size_distribution(driver, nace_4digit: str) -> list[dict]:
    """Get firm size distribution in an industry."""
    return _run_query(
        driver,
        """
        MATCH (f:Firm)-[:OPERATES_IN]->(i:Industry {nace_4digit: $nace})
        RETURN f.firm_size AS size_category, count(f) AS count
        ORDER BY count DESC
        """,
        nace=nace_4digit,
    )


def assemble_report_context(
    driver,
    nace_4digit: str,
    nace_2digit: str | None = None,
    comparison_nace: str | None = None,
    top_n: int = 10,
) -> str:
    """
    Assemble all KG data into a structured text context for LLM consumption.
    Kept under ~3000 tokens.
    """
    sections = []

    # 1. Industry overview
    overview = get_industry_overview(driver, nace_4digit)
    if overview:
        sections.append(f"## Industry Overview (NACE {nace_4digit})")
        sections.append(_format_dict(overview))

    # 2. Market concentration
    concentration = get_market_concentration(driver, nace_4digit)
    if concentration:
        sections.append("## Market Concentration")
        sections.append(_format_dict(concentration))

    # 3. Top firms
    top_firms = get_top_firms(driver, nace_4digit, top_n=top_n)
    if top_firms:
        sections.append(f"## Top {len(top_firms)} Firms by Revenue")
        sections.append(_format_firms_table(top_firms))

    # 4. Firm size distribution
    sizes = get_firm_size_distribution(driver, nace_4digit)
    if sizes:
        sections.append("## Firm Size Distribution")
        for s in sizes:
            sections.append(f"- {s.get('size_category', 'unknown')}: {s['count']} firms")

    # 5. Sector overview (2-digit)
    if nace_2digit:
        sector = get_sector_overview(driver, nace_2digit)
        if sector:
            sections.append(f"## Broader Sector Overview (NACE {nace_2digit})")
            for s in sector[:10]:
                sections.append(
                    f"- NACE {s['nace_4digit']}: {s['firm_count']} firms, "
                    f"revenue={_fmt_num(s.get('total_revenue'))}, "
                    f"HHI={_fmt_num(s.get('hhi'))}"
                )

    # 6. Cross-industry comparison
    if comparison_nace:
        comp = get_industry_comparison(driver, nace_4digit, comparison_nace)
        if comp:
            sections.append(f"## Industry Comparison: NACE {nace_4digit} vs {comparison_nace}")
            for c in comp:
                sections.append(f"### NACE {c['nace_4digit']}")
                sections.append(_format_dict(c["metrics"]))

    context = "\n\n".join(sections)
    log.info(f"Assembled context: {len(context)} chars (~{len(context)//4} tokens)")
    return context


def _format_dict(d: dict) -> str:
    lines = []
    for k, v in d.items():
        if v is not None:
            lines.append(f"- {k}: {_fmt_num(v)}")
    return "\n".join(lines)


def _format_firms_table(firms: list[dict]) -> str:
    lines = []
    for i, f in enumerate(firms, 1):
        name = f.get("firm_name", "Unknown")
        rev = _fmt_num(f.get("revenue"))
        roe = _fmt_num(f.get("roe"))
        roce = _fmt_num(f.get("roce"))
        emp = _fmt_num(f.get("employees"))
        margin = _fmt_num(f.get("profit_margin"))
        solv = _fmt_num(f.get("solvency_ratio"))
        lines.append(
            f"{i}. {name}: revenue={rev}, ROE={roe}%, ROCE={roce}%, "
            f"margin={margin}%, solvency={solv}%, employees={emp}"
        )
    return "\n".join(lines)


def _fmt_num(val) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, float):
        if abs(val) >= 1_000_000_000:
            return f"{val/1e9:.2f}B"
        if abs(val) >= 1_000_000:
            return f"{val/1e6:.2f}M"
        if abs(val) >= 1_000:
            return f"{val/1e3:.1f}K"
        return f"{val:.2f}"
    return str(val)


if __name__ == "__main__":
    driver = get_driver()
    try:
        # Demo: show context for a sample industry
        ctx = assemble_report_context(driver, nace_4digit="6419", nace_2digit="64")
        print(ctx)
    finally:
        driver.close()

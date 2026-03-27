"""
Data ingestion and cleaning pipeline for Moody's/BvD Austrian corporate financials.

Input:  firms_4.rds (BvD export, 7819 rows, heavy duplication)
Output: data/firms_cleaned.parquet (deduplicated, one row per firm, derived metrics)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pyreadr

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# BvD consolidation code priority: prefer consolidated (C1 > C2 > U1 > U2 > LF)
CONSOL_PRIORITY = {"C1": 1, "C2": 2, "U1": 3, "U2": 4, "LF": 5}

FINANCIAL_COLS = [
    "operating_revenue_turnover_",
    "total_assets",
    "cash_flow",
    "shareholders_funds",
    "p_l_before_tax",
    "p_l_for_period_net_income_",
    "number_of_employees",
    "profit_margin_",
    "current_ratio_x_",
    "roe_using_p_l_before_tax_",
    "roce_using_p_l_before_tax_",
    "solvency_ratio_asset_based_",
    "market_capitalisation_mil_",
    "price_earning_ratio_x_",
]

# Friendly column names for downstream use
COLUMN_RENAME = {
    "bvd_listed": "bvd_id",
    "name_internat": "firm_name",
    "country_iso_code": "country",
    "operating_revenue_turnover_": "revenue",
    "total_assets": "total_assets",
    "cash_flow": "cash_flow",
    "shareholders_funds": "shareholders_funds",
    "p_l_before_tax": "pbt",
    "p_l_for_period_net_income_": "net_income",
    "number_of_employees": "employees",
    "profit_margin_": "profit_margin",
    "current_ratio_x_": "current_ratio",
    "roe_using_p_l_before_tax_": "roe",
    "roce_using_p_l_before_tax_": "roce",
    "solvency_ratio_asset_based_": "solvency_ratio",
    "market_capitalisation_mil_": "market_cap_mil",
    "price_earning_ratio_x_": "pe_ratio",
    "nace_rev_2_core_code_4_digits_": "nace_4digit",
    "nace_rev_2_primary_code_s_": "nace_primary",
    "nace_rev_2_secondary_code_s_": "nace_secondary",
    "consolidation_code": "consolidation_code",
    "accounting_practice": "accounting_practice",
    "closing_date": "closing_date",
    "isin_number": "isin",
    "lei_legal_entity_identifier_": "lei",
}


def load_rds(path: str = "firms_4.rds") -> pd.DataFrame:
    """Load RDS file and return raw DataFrame."""
    log.info(f"Loading {path}")
    result = pyreadr.read_r(path)
    df = result[None]
    log.info(f"Raw shape: {df.shape} ({df['bvd_listed'].nunique()} unique firms)")
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate BvD export rows.

    The dataset has two groups:
    1. Rows WITH consolidation_code (3,348 rows) — contain financial data.
       Among these, C1/C2/U1/U2 rows have financials; LF rows do not.
    2. Rows WITHOUT consolidation_code (4,471 rows) — only name + NACE + employees.
       These are completely separate firms (zero overlap with group 1).

    Within each group, rows per firm are pure duplicates (BvD export artifact).
    Strategy:
    - Group 1: pick highest-priority consolidation code per firm, then deduplicate.
    - Group 2: deduplicate by bvd_id, keep first.
    - Union both.
    """
    has_consol = df[df["consolidation_code"].notna()].copy()
    no_consol = df[df["consolidation_code"].isna()].copy()

    # Group 1: rank by consolidation priority, keep best
    has_consol["_consol_rank"] = has_consol["consolidation_code"].map(CONSOL_PRIORITY)
    has_consol = has_consol.sort_values("_consol_rank")
    has_consol = has_consol.drop_duplicates(subset=["bvd_listed"], keep="first")
    has_consol = has_consol.drop(columns=["_consol_rank"])

    # Group 2: pure dedup
    no_consol = no_consol.drop_duplicates(subset=["bvd_listed"], keep="first")

    result = pd.concat([has_consol, no_consol], ignore_index=True)
    log.info(
        f"After dedup: {len(result)} rows "
        f"({len(has_consol)} with financials, {len(no_consol)} without)"
    )
    return result


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute additional metrics not already in the source data."""
    # Debt proxy: total_assets - shareholders_funds
    df["total_debt_proxy"] = df["total_assets"] - df["shareholders_funds"]

    # Debt-to-equity ratio
    mask = df["shareholders_funds"].abs() > 0
    df.loc[mask, "debt_to_equity"] = (
        df.loc[mask, "total_debt_proxy"] / df.loc[mask, "shareholders_funds"]
    )

    # Debt-to-assets ratio
    mask = df["total_assets"].abs() > 0
    df.loc[mask, "debt_to_assets"] = (
        df.loc[mask, "total_debt_proxy"] / df.loc[mask, "total_assets"]
    )

    # Asset turnover (revenue / total_assets)
    mask = df["total_assets"].abs() > 0
    df.loc[mask, "asset_turnover"] = (
        df.loc[mask, "operating_revenue_turnover_"] / df.loc[mask, "total_assets"]
    )

    # Revenue per employee
    mask = df["number_of_employees"] > 0
    df.loc[mask, "revenue_per_employee"] = (
        df.loc[mask, "operating_revenue_turnover_"] / df.loc[mask, "number_of_employees"]
    )

    # Net income margin (if not already captured by profit_margin_)
    mask = df["operating_revenue_turnover_"].abs() > 0
    df.loc[mask, "net_income_margin"] = (
        df.loc[mask, "p_l_for_period_net_income_"]
        / df.loc[mask, "operating_revenue_turnover_"]
        * 100
    )

    # NACE 2-digit sector code
    df["nace_2digit"] = df["nace_rev_2_core_code_4_digits_"].str[:2]

    # Firm size category by employees
    df["firm_size"] = pd.cut(
        df["number_of_employees"],
        bins=[0, 10, 50, 250, np.inf],
        labels=["micro", "small", "medium", "large"],
    )

    log.info("Derived metrics added")
    return df


def compute_industry_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-industry (NACE 4-digit) aggregate metrics including HHI."""
    firms_with_revenue = df.dropna(subset=["operating_revenue_turnover_"]).copy()

    if firms_with_revenue.empty:
        log.warning("No firms with revenue data — skipping industry aggregates")
        return pd.DataFrame()

    grouped = firms_with_revenue.groupby("nace_rev_2_core_code_4_digits_")

    agg = grouped.agg(
        firm_count=("bvd_listed", "nunique"),
        total_industry_revenue=("operating_revenue_turnover_", "sum"),
        avg_revenue=("operating_revenue_turnover_", "mean"),
        median_revenue=("operating_revenue_turnover_", "median"),
        avg_roe=("roe_using_p_l_before_tax_", "mean"),
        median_roe=("roe_using_p_l_before_tax_", "median"),
        avg_roce=("roce_using_p_l_before_tax_", "mean"),
        avg_profit_margin=("profit_margin_", "mean"),
        avg_solvency=("solvency_ratio_asset_based_", "mean"),
        avg_current_ratio=("current_ratio_x_", "mean"),
        total_employees=("number_of_employees", "sum"),
        avg_employees=("number_of_employees", "mean"),
    ).reset_index()

    # HHI: sum of squared market shares within each industry
    def calc_hhi(group):
        total = group["operating_revenue_turnover_"].sum()
        if total == 0:
            return np.nan
        shares = group["operating_revenue_turnover_"] / total
        return (shares**2).sum() * 10000  # scale to 0-10000

    hhi = grouped.apply(calc_hhi, include_groups=False).reset_index()
    hhi.columns = ["nace_rev_2_core_code_4_digits_", "hhi"]

    # CR4 / CR8: concentration ratios
    def calc_cr(group, n):
        total = group["operating_revenue_turnover_"].sum()
        if total == 0:
            return np.nan
        top_n = group["operating_revenue_turnover_"].nlargest(n).sum()
        return top_n / total * 100

    cr4 = grouped.apply(lambda g: calc_cr(g, 4), include_groups=False).reset_index()
    cr4.columns = ["nace_rev_2_core_code_4_digits_", "cr4"]

    cr8 = grouped.apply(lambda g: calc_cr(g, 8), include_groups=False).reset_index()
    cr8.columns = ["nace_rev_2_core_code_4_digits_", "cr8"]

    agg = agg.merge(hhi, on="nace_rev_2_core_code_4_digits_")
    agg = agg.merge(cr4, on="nace_rev_2_core_code_4_digits_")
    agg = agg.merge(cr8, on="nace_rev_2_core_code_4_digits_")

    agg = agg.rename(columns={"nace_rev_2_core_code_4_digits_": "nace_4digit"})
    log.info(f"Industry aggregates: {len(agg)} NACE codes with revenue data")
    return agg


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to friendly names, keeping only mapped + derived columns."""
    existing = {k: v for k, v in COLUMN_RENAME.items() if k in df.columns}
    df = df.rename(columns=existing)

    # Keep renamed + derived columns
    derived = [
        "total_debt_proxy", "debt_to_equity", "debt_to_assets",
        "asset_turnover", "revenue_per_employee", "net_income_margin",
        "nace_2digit", "firm_size",
    ]
    keep = list(existing.values()) + [c for c in derived if c in df.columns]
    df = df[[c for c in keep if c in df.columns]]
    return df


def run(rds_path: str = "firms_4.rds", output_dir: str = "data") -> None:
    """Full ingestion pipeline."""
    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    df = load_rds(rds_path)
    df = deduplicate(df)
    df = add_derived_metrics(df)

    # Industry aggregates (before renaming)
    industry_agg = compute_industry_aggregates(df)

    # Rename and save firm-level data
    df = rename_columns(df)
    firms_path = out / "firms_cleaned.parquet"
    df.to_parquet(firms_path, index=False)
    log.info(f"Saved {len(df)} firms to {firms_path}")

    # Save industry aggregates
    if not industry_agg.empty:
        ind_path = out / "industry_aggregates.parquet"
        industry_agg.to_parquet(ind_path, index=False)
        log.info(f"Saved {len(industry_agg)} industry aggregates to {ind_path}")

    # Summary stats
    log.info("--- Summary ---")
    log.info(f"Total firms: {len(df)}")
    log.info(f"Firms with financials: {df['total_assets'].notna().sum()}")
    log.info(f"Firms with revenue: {df['revenue'].notna().sum()}")
    log.info(f"NACE sectors (4-digit): {df['nace_4digit'].nunique()}")
    log.info(f"NACE sectors (2-digit): {df['nace_2digit'].nunique()}")


if __name__ == "__main__":
    run()

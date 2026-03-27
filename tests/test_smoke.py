"""
Smoke tests for the data ingestion pipeline.
Run with: python -m pytest tests/test_smoke.py -v
"""

from pathlib import Path

import pandas as pd
import pytest


DATA_DIR = Path("data")
FIRMS_PATH = DATA_DIR / "firms_cleaned.parquet"
INDUSTRY_PATH = DATA_DIR / "industry_aggregates.parquet"


@pytest.fixture
def firms():
    assert FIRMS_PATH.exists(), "Run 'python -m src.main ingest' first"
    return pd.read_parquet(FIRMS_PATH)


@pytest.fixture
def industry_agg():
    assert INDUSTRY_PATH.exists(), "Run 'python -m src.main ingest' first"
    return pd.read_parquet(INDUSTRY_PATH)


class TestDataIngestion:
    def test_firms_not_empty(self, firms):
        assert len(firms) > 0

    def test_firms_deduplicated(self, firms):
        assert firms["bvd_id"].is_unique

    def test_firms_expected_count(self, firms):
        # 3,855 unique firms after dedup
        assert 3800 <= len(firms) <= 3900

    def test_required_columns_exist(self, firms):
        required = [
            "bvd_id", "firm_name", "country", "nace_4digit", "nace_2digit",
            "revenue", "total_assets", "roe", "roce", "solvency_ratio",
            "profit_margin", "current_ratio", "employees", "firm_size",
        ]
        for col in required:
            assert col in firms.columns, f"Missing column: {col}"

    def test_derived_metrics_exist(self, firms):
        derived = [
            "debt_to_equity", "debt_to_assets", "asset_turnover",
            "revenue_per_employee", "net_income_margin",
        ]
        for col in derived:
            assert col in firms.columns, f"Missing derived column: {col}"

    def test_country_is_austria(self, firms):
        assert (firms["country"] == "AT").all()

    def test_no_duplicate_bvd_ids(self, firms):
        assert firms["bvd_id"].duplicated().sum() == 0

    def test_firms_with_financials(self, firms):
        # Should have at least 200 firms with total_assets
        assert firms["total_assets"].notna().sum() >= 200

    def test_nace_codes_present(self, firms):
        assert firms["nace_4digit"].notna().sum() > 3800

    def test_firm_size_categories(self, firms):
        valid_sizes = {"micro", "small", "medium", "large"}
        actual = set(firms["firm_size"].dropna().unique())
        assert actual.issubset(valid_sizes)


class TestIndustryAggregates:
    def test_not_empty(self, industry_agg):
        assert len(industry_agg) > 0

    def test_has_hhi(self, industry_agg):
        assert "hhi" in industry_agg.columns
        assert industry_agg["hhi"].notna().sum() > 0

    def test_hhi_in_range(self, industry_agg):
        hhi = industry_agg["hhi"].dropna()
        assert (hhi >= 0).all()
        assert (hhi <= 10000).all()

    def test_cr4_in_range(self, industry_agg):
        cr4 = industry_agg["cr4"].dropna()
        assert (cr4 >= 0).all()
        assert (cr4 <= 100).all()

    def test_has_expected_columns(self, industry_agg):
        expected = [
            "nace_4digit", "firm_count", "total_industry_revenue",
            "avg_roe", "hhi", "cr4", "cr8",
        ]
        for col in expected:
            assert col in industry_agg.columns, f"Missing: {col}"

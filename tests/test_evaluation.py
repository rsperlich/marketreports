"""
Tests for evaluation framework modules.
These tests are self-contained — they don't require Neo4j or the LLM API.
"""

import unittest
from src.evaluation.grounding import extract_claims, _values_match, _parse_number
from src.evaluation.consistency import (
    jaccard_similarity, cosine_similarity, numeric_overlap,
    structural_similarity, pairwise_similarity,
)


class TestClaimExtraction(unittest.TestCase):
    """Test numeric claim extraction from report text."""

    def test_extract_percentage(self):
        text = "The average ROE is 12.3% for this industry."
        claims = extract_claims(text)
        pct_claims = [c for c in claims if c.unit == "%"]
        self.assertTrue(any(abs(c.value - 12.3) < 0.01 for c in pct_claims))

    def test_extract_million(self):
        text = "Total revenue reached 450.2M in 2023."
        claims = extract_claims(text)
        self.assertTrue(any(abs(c.value - 450_200_000) < 1 for c in claims))

    def test_extract_billion(self):
        text = "Assets totalled 1.5 billion euros."
        claims = extract_claims(text)
        self.assertTrue(any(abs(c.value - 1_500_000_000) < 1 for c in claims))

    def test_extract_firm_count(self):
        text = "There are 42 firms operating in this sector."
        claims = extract_claims(text)
        self.assertTrue(any(abs(c.value - 42) < 0.01 for c in claims))

    def test_extract_employees(self):
        text = "The top firm employs 1,234 employees."
        claims = extract_claims(text)
        self.assertTrue(any(abs(c.value - 1234) < 0.01 for c in claims))

    def test_empty_text(self):
        claims = extract_claims("")
        self.assertEqual(len(claims), 0)

    def test_no_numbers(self):
        text = "This industry is moderately concentrated."
        claims = extract_claims(text)
        self.assertEqual(len(claims), 0)


class TestParseNumber(unittest.TestCase):

    def test_plain_number(self):
        self.assertAlmostEqual(_parse_number("1234"), 1234.0)

    def test_comma_number(self):
        self.assertAlmostEqual(_parse_number("1,234,567"), 1234567.0)

    def test_decimal(self):
        self.assertAlmostEqual(_parse_number("12.34"), 12.34)

    def test_with_multiplier(self):
        self.assertAlmostEqual(_parse_number("1.5", "billion"), 1.5e9)
        self.assertAlmostEqual(_parse_number("450", "M"), 450e6)
        self.assertAlmostEqual(_parse_number("100", "K"), 100e3)


class TestValuesMatch(unittest.TestCase):

    def test_exact_match(self):
        self.assertTrue(_values_match(100.0, 100.0))

    def test_within_tolerance(self):
        self.assertTrue(_values_match(100.0, 102.0, tolerance=0.05))

    def test_outside_tolerance(self):
        self.assertFalse(_values_match(100.0, 200.0, tolerance=0.05))

    def test_zero_truth(self):
        self.assertTrue(_values_match(0.005, 0.0))
        self.assertFalse(_values_match(1.0, 0.0))


class TestConsistencyMetrics(unittest.TestCase):

    def test_jaccard_identical(self):
        text = "the quick brown fox"
        self.assertAlmostEqual(jaccard_similarity(text, text), 1.0)

    def test_jaccard_different(self):
        a = "the quick brown fox"
        b = "lazy dog sleeps quietly"
        sim = jaccard_similarity(a, b)
        self.assertLess(sim, 0.5)

    def test_cosine_identical(self):
        text = "revenue is 100 million"
        self.assertAlmostEqual(cosine_similarity(text, text), 1.0)

    def test_cosine_different(self):
        a = "revenue is 100 million"
        b = "completely unrelated text about nothing"
        sim = cosine_similarity(a, b)
        self.assertLess(sim, 0.5)

    def test_numeric_overlap_identical(self):
        text = "revenue 12345.67 and ROE 15.2%"
        self.assertAlmostEqual(numeric_overlap(text, text), 1.0)

    def test_numeric_overlap_different(self):
        a = "revenue 12345 and ROE 15%"
        b = "revenue 99999 and ROE 88%"
        sim = numeric_overlap(a, b)
        self.assertLess(sim, 0.5)

    def test_structural_similarity_same_headings(self):
        a = "# Report\n## Section A\n## Section B"
        b = "# Report\n## Section A\n## Section B"
        self.assertAlmostEqual(structural_similarity(a, b), 1.0)

    def test_structural_similarity_different_headings(self):
        a = "# Report A\n## Market Size"
        b = "# Report B\n## Profitability"
        sim = structural_similarity(a, b)
        self.assertLess(sim, 1.0)

    def test_pairwise_three_identical(self):
        reports = ["same text here"] * 3
        result = pairwise_similarity(reports)
        self.assertAlmostEqual(result["jaccard_mean"], 1.0)
        self.assertAlmostEqual(result["cosine_mean"], 1.0)

    def test_empty_texts(self):
        self.assertAlmostEqual(jaccard_similarity("", ""), 1.0)
        self.assertAlmostEqual(cosine_similarity("", ""), 1.0)


if __name__ == "__main__":
    unittest.main()

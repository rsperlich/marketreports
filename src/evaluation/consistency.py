"""
Consistency evaluator — measures report stability across repeated generations.

Answers RQ2: Does KG-grounded generation produce more consistent outputs
than the baseline approach?

Approach:
  1. Generate the same report N times (default 3).
  2. Compute pairwise similarity using multiple metrics.
  3. Report mean similarity and variance.
"""

import logging
import re
from collections import Counter
from itertools import combinations

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def _tokenize(text: str) -> list[str]:
    """Simple word tokenization."""
    return re.findall(r"\b\w+\b", text.lower())


def jaccard_similarity(text_a: str, text_b: str) -> float:
    """Word-level Jaccard similarity."""
    tokens_a = set(_tokenize(text_a))
    tokens_b = set(_tokenize(text_b))
    if not tokens_a and not tokens_b:
        return 1.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def cosine_similarity(text_a: str, text_b: str) -> float:
    """Word-frequency cosine similarity."""
    freq_a = Counter(_tokenize(text_a))
    freq_b = Counter(_tokenize(text_b))
    all_words = set(freq_a) | set(freq_b)
    if not all_words:
        return 1.0

    dot = sum(freq_a.get(w, 0) * freq_b.get(w, 0) for w in all_words)
    norm_a = sum(v ** 2 for v in freq_a.values()) ** 0.5
    norm_b = sum(v ** 2 for v in freq_b.values()) ** 0.5

    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def numeric_overlap(text_a: str, text_b: str) -> float:
    """Fraction of numeric values that appear in both reports."""
    nums_a = set(re.findall(r"[\d,]+\.?\d*", text_a))
    nums_b = set(re.findall(r"[\d,]+\.?\d*", text_b))
    # Filter trivial numbers (single digits, years, section numbers)
    trivial = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
               "2023", "2024", "2025", "100"}
    nums_a -= trivial
    nums_b -= trivial
    if not nums_a and not nums_b:
        return 1.0
    if not nums_a or not nums_b:
        return 0.0
    intersection = nums_a & nums_b
    union = nums_a | nums_b
    return len(intersection) / len(union)


def structural_similarity(text_a: str, text_b: str) -> float:
    """Measures if the same section headings appear in both reports."""
    headings_a = set(re.findall(r"^#{1,3}\s+(.+)$", text_a, re.MULTILINE))
    headings_b = set(re.findall(r"^#{1,3}\s+(.+)$", text_b, re.MULTILINE))
    if not headings_a and not headings_b:
        return 1.0
    if not headings_a or not headings_b:
        return 0.0
    intersection = headings_a & headings_b
    union = headings_a | headings_b
    return len(intersection) / len(union)


def pairwise_similarity(reports: list[str]) -> dict:
    """Compute all pairwise similarities across a set of reports."""
    n = len(reports)
    metrics = {
        "jaccard": [],
        "cosine": [],
        "numeric_overlap": [],
        "structural": [],
    }

    for i, j in combinations(range(n), 2):
        metrics["jaccard"].append(jaccard_similarity(reports[i], reports[j]))
        metrics["cosine"].append(cosine_similarity(reports[i], reports[j]))
        metrics["numeric_overlap"].append(numeric_overlap(reports[i], reports[j]))
        metrics["structural"].append(structural_similarity(reports[i], reports[j]))

    result = {}
    for name, values in metrics.items():
        if values:
            mean_val = sum(values) / len(values)
            variance = sum((v - mean_val) ** 2 for v in values) / len(values)
            result[f"{name}_mean"] = mean_val
            result[f"{name}_variance"] = variance
        else:
            result[f"{name}_mean"] = 0.0
            result[f"{name}_variance"] = 0.0

    return result


def evaluate_consistency(
    generate_fn,
    nace_4digit: str,
    n_runs: int = 3,
    report_type: str = "kg",
) -> dict:
    """
    Generate a report N times and measure consistency.

    Args:
        generate_fn: Callable that takes nace_4digit and returns report text.
        nace_4digit: NACE industry code.
        n_runs: Number of generation runs.
        report_type: "kg" or "baseline" — for labelling.

    Returns:
        Dictionary with consistency metrics.
    """
    log.info(f"Generating {n_runs} {report_type} reports for NACE {nace_4digit}...")
    reports = []
    for i in range(n_runs):
        log.info(f"  Run {i+1}/{n_runs}")
        report = generate_fn(nace_4digit)
        reports.append(report)

    similarities = pairwise_similarity(reports)

    result = {
        "nace_4digit": nace_4digit,
        "report_type": report_type,
        "n_runs": n_runs,
        "avg_report_length": sum(len(r) for r in reports) / len(reports),
        "length_variance": (
            sum((len(r) - sum(len(r2) for r2 in reports) / len(reports)) ** 2
                for r in reports) / len(reports)
        ),
        **similarities,
    }

    log.info(
        f"Consistency ({report_type}): "
        f"jaccard={result['jaccard_mean']:.3f}, "
        f"cosine={result['cosine_mean']:.3f}, "
        f"numeric={result['numeric_overlap_mean']:.3f}"
    )
    return result


def compare_consistency(
    kg_generate_fn,
    baseline_generate_fn,
    nace_4digit: str,
    n_runs: int = 3,
) -> dict:
    """Compare consistency between KG and baseline report generation."""
    kg_result = evaluate_consistency(kg_generate_fn, nace_4digit, n_runs, "kg")
    bl_result = evaluate_consistency(baseline_generate_fn, nace_4digit, n_runs, "baseline")

    return {
        "nace_4digit": nace_4digit,
        "kg": kg_result,
        "baseline": bl_result,
        "cosine_delta": kg_result["cosine_mean"] - bl_result["cosine_mean"],
        "numeric_delta": kg_result["numeric_overlap_mean"] - bl_result["numeric_overlap_mean"],
    }

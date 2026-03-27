"""
Human evaluation scorer — reads evaluation CSV and computes statistics.

Reads scores from docs/EVALUATION_RUBRIC.md format CSV files,
computes aggregates, and runs significance tests.
"""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DIMENSIONS = ["fa", "ad", "ac", "dg", "sc"]
DIMENSION_NAMES = {
    "fa": "Factual Accuracy",
    "ad": "Analytical Depth",
    "ac": "Actionability",
    "dg": "Data Grounding",
    "sc": "Structural Clarity",
}


@dataclass
class EvalScores:
    """Parsed evaluation scores for one report."""
    evaluator_id: str
    nace_4digit: str
    report_type: str  # "kg" or "baseline"
    fa: int
    ad: int
    ac: int
    dg: int
    sc: int
    notes: str = ""

    @property
    def mean_score(self) -> float:
        return (self.fa + self.ad + self.ac + self.dg + self.sc) / 5


def load_scores(csv_path: str) -> list[EvalScores]:
    """Load evaluation scores from a CSV file."""
    scores = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            scores.append(EvalScores(
                evaluator_id=row["evaluator_id"],
                nace_4digit=row["nace_4digit"],
                report_type=row["report_type"],
                fa=int(row["fa"]),
                ad=int(row["ad"]),
                ac=int(row["ac"]),
                dg=int(row["dg"]),
                sc=int(row["sc"]),
                notes=row.get("notes", ""),
            ))
    log.info(f"Loaded {len(scores)} evaluation records from {csv_path}")
    return scores


def compute_summary(scores: list[EvalScores]) -> dict:
    """Compute summary statistics for KG vs baseline scores."""
    kg_scores = [s for s in scores if s.report_type == "kg"]
    bl_scores = [s for s in scores if s.report_type == "baseline"]

    summary = {"kg": {}, "baseline": {}, "deltas": {}}

    for dim in DIMENSIONS:
        kg_vals = [getattr(s, dim) for s in kg_scores]
        bl_vals = [getattr(s, dim) for s in bl_scores]

        if kg_vals:
            kg_mean = sum(kg_vals) / len(kg_vals)
            summary["kg"][dim] = {
                "mean": kg_mean,
                "n": len(kg_vals),
                "min": min(kg_vals),
                "max": max(kg_vals),
            }
        if bl_vals:
            bl_mean = sum(bl_vals) / len(bl_vals)
            summary["baseline"][dim] = {
                "mean": bl_mean,
                "n": len(bl_vals),
                "min": min(bl_vals),
                "max": max(bl_vals),
            }
        if kg_vals and bl_vals:
            summary["deltas"][dim] = kg_mean - bl_mean

    # Overall means
    if kg_scores:
        summary["kg"]["overall_mean"] = sum(s.mean_score for s in kg_scores) / len(kg_scores)
    if bl_scores:
        summary["baseline"]["overall_mean"] = sum(s.mean_score for s in bl_scores) / len(bl_scores)

    return summary


def cohens_d(group_a: list[float], group_b: list[float]) -> float:
    """Compute Cohen's d effect size."""
    n_a, n_b = len(group_a), len(group_b)
    if n_a < 2 or n_b < 2:
        return 0.0

    mean_a = sum(group_a) / n_a
    mean_b = sum(group_b) / n_b
    var_a = sum((x - mean_a) ** 2 for x in group_a) / (n_a - 1)
    var_b = sum((x - mean_b) ** 2 for x in group_b) / (n_b - 1)

    pooled_std = (((n_a - 1) * var_a + (n_b - 1) * var_b) / (n_a + n_b - 2)) ** 0.5
    if pooled_std == 0:
        return 0.0
    return (mean_a - mean_b) / pooled_std


def print_summary(scores: list[EvalScores]):
    """Print a formatted summary of evaluation results."""
    summary = compute_summary(scores)
    kg_scores = [s for s in scores if s.report_type == "kg"]
    bl_scores = [s for s in scores if s.report_type == "baseline"]

    print("\n" + "=" * 65)
    print("HUMAN EVALUATION RESULTS")
    print("=" * 65)
    print(f"{'Dimension':<22} {'KG Mean':>8} {'BL Mean':>8} {'Delta':>8} {'Cohen d':>8}")
    print("-" * 65)

    for dim in DIMENSIONS:
        kg_vals = [getattr(s, dim) for s in kg_scores]
        bl_vals = [getattr(s, dim) for s in bl_scores]

        kg_mean = sum(kg_vals) / len(kg_vals) if kg_vals else 0
        bl_mean = sum(bl_vals) / len(bl_vals) if bl_vals else 0
        delta = kg_mean - bl_mean
        d = cohens_d([float(v) for v in kg_vals], [float(v) for v in bl_vals])

        name = DIMENSION_NAMES[dim]
        print(f"{name:<22} {kg_mean:>8.2f} {bl_mean:>8.2f} {delta:>+8.2f} {d:>8.2f}")

    if kg_scores and bl_scores:
        kg_overall = sum(s.mean_score for s in kg_scores) / len(kg_scores)
        bl_overall = sum(s.mean_score for s in bl_scores) / len(bl_scores)
        print("-" * 65)
        print(f"{'Overall':<22} {kg_overall:>8.2f} {bl_overall:>8.2f} {kg_overall - bl_overall:>+8.2f}")

    print(f"\nKG reports: {len(kg_scores)}, Baseline reports: {len(bl_scores)}")
    print("=" * 65)

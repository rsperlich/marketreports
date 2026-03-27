"""
Batch report generator — generates matched KG and baseline report pairs,
then runs automated evaluation (grounding, consistency, triplet accuracy).

Usage:
    python -m src.eval_batch --nace 6419 2740 6512
    python -m src.eval_batch --all --max-industries 10
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def get_available_industries(data_dir: str = "data", min_firms: int = 3) -> list[str]:
    """Get NACE codes with enough firms for meaningful evaluation."""
    agg = pd.read_parquet(Path(data_dir) / "industry_aggregates.parquet")
    valid = agg[agg["firm_count"] >= min_firms]
    return sorted(valid["nace_4digit"].tolist())


def generate_report_pair(nace_4digit: str, data_dir: str = "data") -> dict:
    """Generate one KG-grounded and one baseline report for the same industry."""
    from src.pipeline.baseline_generator import build_baseline_context, generate_baseline_report
    from src.config import get_neo4j_driver
    from src.pipeline.kg_retriever import assemble_report_context
    from src.pipeline.report_generator import generate_report

    result = {"nace_4digit": nace_4digit, "timestamp": datetime.now().isoformat()}

    # KG-grounded report
    log.info(f"Generating KG report for NACE {nace_4digit}...")
    driver = get_neo4j_driver()
    try:
        kg_context = assemble_report_context(
            driver, nace_4digit=nace_4digit, nace_2digit=nace_4digit[:2]
        )
        kg_report = generate_report(kg_context, nace_4digit)
        result["kg_report"] = kg_report
        result["kg_context_len"] = len(kg_context)
    finally:
        driver.close()

    # Baseline report
    log.info(f"Generating baseline report for NACE {nace_4digit}...")
    bl_context = build_baseline_context(nace_4digit, data_dir)
    bl_report = generate_baseline_report(bl_context, nace_4digit)
    result["baseline_report"] = bl_report
    result["baseline_context_len"] = len(bl_context)

    return result


def run_grounding_eval(pair: dict, data_dir: str = "data") -> dict:
    """Run grounding evaluation on a report pair."""
    from src.evaluation.grounding import compare_grounding

    return compare_grounding(
        pair["kg_report"], pair["baseline_report"],
        pair["nace_4digit"], data_dir
    )


def run_batch(
    nace_codes: list[str],
    data_dir: str = "data",
    output_dir: str = "eval_results",
    run_grounding: bool = True,
    run_triplets: bool = True,
) -> dict:
    """Run batch evaluation across multiple industries."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = {
        "timestamp": timestamp,
        "n_industries": len(nace_codes),
        "nace_codes": nace_codes,
        "pairs": [],
        "grounding": [],
        "triplet_accuracy": None,
    }

    # Generate report pairs
    for nace in nace_codes:
        log.info(f"\n{'='*60}")
        log.info(f"Processing NACE {nace}")
        log.info(f"{'='*60}")

        try:
            pair = generate_report_pair(nace, data_dir)

            # Save individual reports
            for rtype in ["kg", "baseline"]:
                report_path = Path(output_dir) / f"{nace}_{rtype}_{timestamp}.md"
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(pair[f"{rtype}_report"])

            pair_summary = {
                "nace_4digit": nace,
                "kg_report_len": len(pair["kg_report"]),
                "baseline_report_len": len(pair["baseline_report"]),
                "kg_context_len": pair["kg_context_len"],
                "baseline_context_len": pair["baseline_context_len"],
            }
            results["pairs"].append(pair_summary)

            # Run grounding evaluation
            if run_grounding:
                grounding = run_grounding_eval(pair, data_dir)
                results["grounding"].append(grounding)
                log.info(
                    f"Grounding — KG: {grounding['kg_grounding_rate']:.1%}, "
                    f"Baseline: {grounding['baseline_grounding_rate']:.1%}, "
                    f"Delta: {grounding['grounding_delta']:+.1%}"
                )

        except Exception as e:
            log.error(f"Failed for NACE {nace}: {e}")
            results["pairs"].append({"nace_4digit": nace, "error": str(e)})

    # Run triplet accuracy (once, not per-industry)
    if run_triplets:
        log.info("\nRunning triplet accuracy evaluation...")
        try:
            from src.evaluation.triplets import evaluate_triplet_accuracy

            triplet_result = evaluate_triplet_accuracy(data_dir)
            results["triplet_accuracy"] = {
                "precision": triplet_result.precision,
                "total_sampled": triplet_result.total_sampled,
                "correct": triplet_result.correct,
                "incorrect": triplet_result.incorrect,
                "unverifiable": triplet_result.unverifiable,
            }
        except Exception as e:
            log.error(f"Triplet evaluation failed: {e}")
            results["triplet_accuracy"] = {"error": str(e)}

    # Aggregate grounding results
    if results["grounding"]:
        kg_rates = [g["kg_grounding_rate"] for g in results["grounding"]]
        bl_rates = [g["baseline_grounding_rate"] for g in results["grounding"]]
        results["grounding_summary"] = {
            "kg_mean_grounding": sum(kg_rates) / len(kg_rates),
            "baseline_mean_grounding": sum(bl_rates) / len(bl_rates),
            "mean_delta": sum(g["grounding_delta"] for g in results["grounding"]) / len(results["grounding"]),
        }

    # Save results
    results_path = Path(output_dir) / f"batch_results_{timestamp}.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    log.info(f"\nResults saved to {results_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("BATCH EVALUATION SUMMARY")
    print("=" * 60)
    if results.get("grounding_summary"):
        gs = results["grounding_summary"]
        print(f"Grounding rate (KG):       {gs['kg_mean_grounding']:.1%}")
        print(f"Grounding rate (Baseline): {gs['baseline_mean_grounding']:.1%}")
        print(f"Delta:                     {gs['mean_delta']:+.1%}")
    if results.get("triplet_accuracy") and "precision" in results["triplet_accuracy"]:
        ta = results["triplet_accuracy"]
        print(f"KG triplet precision:      {ta['precision']:.1%} ({ta['correct']}/{ta['correct']+ta['incorrect']})")
    print("=" * 60)

    return results


def main():
    parser = argparse.ArgumentParser(description="Batch report generation and evaluation")
    parser.add_argument("--nace", nargs="+", help="NACE codes to evaluate")
    parser.add_argument("--all", action="store_true", help="Evaluate all industries")
    parser.add_argument("--max-industries", type=int, default=10, help="Max industries (with --all)")
    parser.add_argument("--min-firms", type=int, default=3, help="Min firms per industry")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--output-dir", default="eval_results")
    parser.add_argument("--no-grounding", action="store_true")
    parser.add_argument("--no-triplets", action="store_true")

    args = parser.parse_args()

    if args.nace:
        nace_codes = args.nace
    elif args.all:
        nace_codes = get_available_industries(args.data_dir, args.min_firms)
        nace_codes = nace_codes[:args.max_industries]
        log.info(f"Selected {len(nace_codes)} industries: {nace_codes}")
    else:
        print("Specify --nace codes or --all")
        sys.exit(1)

    run_batch(
        nace_codes,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        run_grounding=not args.no_grounding,
        run_triplets=not args.no_triplets,
    )


if __name__ == "__main__":
    main()

"""
CLI entry point for the Cross-Border Market Report Pipeline.

Usage:
    # Step 1: Ingest and clean data
    python -m src.main ingest

    # Step 2: Build knowledge graph (requires Neo4j running)
    python -m src.main build-graph

    # Step 3: Generate a report for a specific NACE industry
    python -m src.main report --nace 6419

    # Generate with production model
    python -m src.main report --nace 6419 --model gpt-4o

    # Compare two industries
    python -m src.main report --nace 6419 --compare 6512

    # List available industries with data
    python -m src.main list-industries
"""

import argparse
import sys

import pandas as pd


def cmd_ingest(args):
    from src.pipeline.ingestion import run
    run(rds_path=args.rds, output_dir=args.data_dir)


def cmd_build_graph(args):
    from src.pipeline.graph_builder import run
    run(data_dir=args.data_dir)


def cmd_report(args):
    from src.config import get_neo4j_driver
    from src.pipeline.kg_retriever import assemble_report_context
    from src.pipeline.report_generator import generate_report, save_report

    nace_2digit = args.nace[:2] if args.nace else None

    driver = get_neo4j_driver()
    try:
        context = assemble_report_context(
            driver,
            nace_4digit=args.nace,
            nace_2digit=nace_2digit,
            comparison_nace=args.compare,
            top_n=args.top_n,
        )

        if not context.strip():
            print(f"No data found for NACE {args.nace}. Use 'list-industries' to see available codes.")
            sys.exit(1)

        if args.context_only:
            print(context)
            return

        report = generate_report(
            context=context,
            nace_4digit=args.nace,
            model=args.model,
            temperature=args.temperature,
        )

        path = save_report(report, args.nace, output_dir=args.output_dir)
        print(f"\nReport saved to: {path}")

        if args.print:
            print("\n" + "=" * 80)
            print(report)
    finally:
        driver.close()


def cmd_list_industries(args):
    try:
        agg = pd.read_parquet(f"{args.data_dir}/industry_aggregates.parquet")
    except FileNotFoundError:
        print("No industry aggregates found. Run 'ingest' first.")
        sys.exit(1)

    agg = agg.sort_values("total_industry_revenue", ascending=False)
    print(f"{'NACE':<8} {'Firms':>6} {'Total Revenue':>18} {'Avg ROE':>10} {'HHI':>10}")
    print("-" * 56)
    for _, row in agg.iterrows():
        rev = row["total_industry_revenue"]
        rev_str = f"{rev/1e9:.2f}B" if rev >= 1e9 else f"{rev/1e6:.0f}M"
        roe = f"{row['avg_roe']:.1f}%" if pd.notna(row["avg_roe"]) else "N/A"
        hhi = f"{row['hhi']:.0f}" if pd.notna(row["hhi"]) else "N/A"
        print(f"{row['nace_4digit']:<8} {row['firm_count']:>6} {rev_str:>18} {roe:>10} {hhi:>10}")


def cmd_baseline_report(args):
    from src.pipeline.baseline_generator import build_baseline_context, generate_baseline_report
    from src.pipeline.report_generator import save_report

    context = build_baseline_context(args.nace, args.data_dir)
    if not context.strip():
        print(f"No data found for NACE {args.nace}.")
        sys.exit(1)

    report = generate_baseline_report(
        context=context,
        nace_4digit=args.nace,
        model=args.model,
        temperature=args.temperature,
    )

    path = save_report(report, f"{args.nace}_baseline", output_dir=args.output_dir)
    print(f"\nBaseline report saved to: {path}")
    if args.print:
        print("\n" + "=" * 80)
        print(report)


def cmd_evaluate(args):
    from src.evaluation.batch import get_available_industries, run_batch

    if args.nace:
        nace_codes = args.nace
    elif args.all:
        nace_codes = get_available_industries(args.data_dir, args.min_firms)
        nace_codes = nace_codes[:args.max_industries]
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


def cmd_eval_triplets(args):
    from src.evaluation.triplets import evaluate_triplet_accuracy

    result = evaluate_triplet_accuracy(
        data_dir=args.data_dir,
        n_financial=args.n_financial,
        n_industry=args.n_industry,
        n_concentration=args.n_concentration,
    )

    print(f"\nKG Triplet Accuracy: {result.precision:.1%}")
    print(f"  Correct:      {result.correct}")
    print(f"  Incorrect:    {result.incorrect}")
    print(f"  Unverifiable: {result.unverifiable}")
    print(f"  Total:        {result.total_sampled}")

    if result.incorrect > 0:
        print("\nIncorrect triplets:")
        for t in result.triplets:
            if t.verified is False:
                print(f"  {t.subject} —[{t.predicate}]→ {t.object_val}  (source: {t.source_val}, {t.note})")


def cmd_eval_human(args):
    from src.evaluation.human import load_scores, print_summary

    scores = load_scores(args.csv)
    print_summary(scores)


def main():
    parser = argparse.ArgumentParser(
        description="Cross-Border Market Entry Report Pipeline"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ingest
    p_ingest = sub.add_parser("ingest", help="Ingest and clean RDS data")
    p_ingest.add_argument("--rds", default="firms_4.rds", help="Path to RDS file")
    p_ingest.add_argument("--data-dir", default="data", help="Output directory")
    p_ingest.set_defaults(func=cmd_ingest)

    # build-graph
    p_graph = sub.add_parser("build-graph", help="Populate Neo4j knowledge graph")
    p_graph.add_argument("--data-dir", default="data", help="Data directory")
    p_graph.set_defaults(func=cmd_build_graph)

    # report
    p_report = sub.add_parser("report", help="Generate a market entry report")
    p_report.add_argument("--nace", required=True, help="NACE 4-digit industry code")
    p_report.add_argument("--compare", help="NACE code for cross-industry comparison")
    p_report.add_argument("--model", help="OpenAI model override")
    p_report.add_argument("--temperature", type=float, default=0.3)
    p_report.add_argument("--top-n", type=int, default=10, help="Top N firms to include")
    p_report.add_argument("--output-dir", default="reports")
    p_report.add_argument("--print", action="store_true", help="Also print report to stdout")
    p_report.add_argument("--context-only", action="store_true", help="Print KG context only (no LLM)")
    p_report.add_argument("--data-dir", default="data")
    p_report.set_defaults(func=cmd_report)

    # list-industries
    p_list = sub.add_parser("list-industries", help="List industries with data")
    p_list.add_argument("--data-dir", default="data")
    p_list.set_defaults(func=cmd_list_industries)

    # baseline-report
    p_baseline = sub.add_parser("baseline-report", help="Generate a baseline (no-KG) report")
    p_baseline.add_argument("--nace", required=True, help="NACE 4-digit industry code")
    p_baseline.add_argument("--model", help="OpenAI model override")
    p_baseline.add_argument("--temperature", type=float, default=0.3)
    p_baseline.add_argument("--output-dir", default="reports")
    p_baseline.add_argument("--print", action="store_true")
    p_baseline.add_argument("--data-dir", default="data")
    p_baseline.set_defaults(func=cmd_baseline_report)

    # evaluate
    p_eval = sub.add_parser("evaluate", help="Run batch evaluation (grounding, triplets)")
    p_eval.add_argument("--nace", nargs="+", help="NACE codes to evaluate")
    p_eval.add_argument("--all", action="store_true", help="Evaluate all industries")
    p_eval.add_argument("--max-industries", type=int, default=10)
    p_eval.add_argument("--min-firms", type=int, default=3)
    p_eval.add_argument("--data-dir", default="data")
    p_eval.add_argument("--output-dir", default="eval_results")
    p_eval.add_argument("--no-grounding", action="store_true")
    p_eval.add_argument("--no-triplets", action="store_true")
    p_eval.set_defaults(func=cmd_evaluate)

    # eval-triplets (standalone)
    p_triplets = sub.add_parser("eval-triplets", help="Run KG triplet accuracy check")
    p_triplets.add_argument("--data-dir", default="data")
    p_triplets.add_argument("--n-financial", type=int, default=50)
    p_triplets.add_argument("--n-industry", type=int, default=20)
    p_triplets.add_argument("--n-concentration", type=int, default=20)
    p_triplets.set_defaults(func=cmd_eval_triplets)

    # eval-human (summarize human evaluation scores)
    p_human = sub.add_parser("eval-human", help="Summarize human evaluation scores")
    p_human.add_argument("--csv", required=True, help="Path to evaluation CSV file")
    p_human.set_defaults(func=cmd_eval_human)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

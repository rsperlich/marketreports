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
    from src.data_ingestion import run
    run(rds_path=args.rds, output_dir=args.data_dir)


def cmd_build_graph(args):
    from src.graph_builder import run
    run(data_dir=args.data_dir)


def cmd_report(args):
    from src.kg_retriever import assemble_report_context, get_driver
    from src.report_generator import generate_report, save_report

    nace_2digit = args.nace[:2] if args.nace else None

    driver = get_driver()
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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

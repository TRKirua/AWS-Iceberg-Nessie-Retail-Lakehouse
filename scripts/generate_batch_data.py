#!/usr/bin/env python3
"""
Generate batch CSV files with controlled data quality issues.

This script generates multiple CSV batch files from the source superstore_sales.csv
with intentional data quality issues to demonstrate the Silver layer filtering.

Issues introduced (approximately 5% of rows):
- Null sales values
- Null product_id values
- Duplicate order_id + product_id combinations
- Discount values > 1
- Negative or zero quantity values

Usage:
    python scripts/generate_batch_data.py [--num-batches <n>] [--output-dir <dir>]

Examples:
    # Generate 3 batch files (default)
    python scripts/generate_batch_data.py

    # Generate 5 batch files
    python scripts/generate_batch_data.py --num-batches 5
"""

import argparse
import random
from pathlib import Path

import pandas as pd
import numpy as np


def introduce_quality_issues(df: pd.DataFrame, issue_rate: float = 0.05) -> pd.DataFrame:
    """
    Introduce controlled data quality issues into a DataFrame.

    Args:
        df: Clean input DataFrame
        issue_rate: Proportion of rows to affect with issues (default: 5%)

    Returns:
        DataFrame with quality issues
    """
    df_issues = df.copy()
    n_rows = len(df_issues)
    n_issues = int(n_rows * issue_rate)

    # Random indices to affect
    issue_indices = random.sample(range(n_rows), min(n_issues, n_rows))

    for idx in issue_indices:
        issue_type = random.choice([
            "null_sales",
            "null_product_id",
            "duplicate",
            "invalid_discount",
            "invalid_quantity",
        ])

        if issue_type == "null_sales":
            df_issues.loc[idx, "Sales"] = np.nan

        elif issue_type == "null_product_id":
            df_issues.loc[idx, "Product ID"] = None

        elif issue_type == "invalid_discount":
            # Set discount to invalid value (> 1 or < 0)
            df_issues.loc[idx, "Discount"] = random.choice([1.5, -0.1, 2.0])

        elif issue_type == "invalid_quantity":
            # Set quantity to 0 or negative
            df_issues.loc[idx, "Quantity"] = random.choice([0, -1, -2])

        elif issue_type == "duplicate":
            # This will be handled by duplicating rows after
            pass

    # Add some duplicate rows (same order_id + product_id)
    # We'll duplicate about 1% of rows
    n_dupes = int(n_rows * 0.01)
    if n_dupes > 0:
        dupe_indices = random.sample(range(n_rows), min(n_dupes, n_rows))
        duplicates = df_issues.iloc[dupe_indices].copy()
        df_issues = pd.concat([df_issues, duplicates], ignore_index=True)

    return df_issues


def generate_batches(
    source_file: Path,
    output_dir: Path,
    num_batches: int = 3,
    issue_rate: float = 0.05,
) -> list[Path]:
    """
    Generate multiple batch CSV files with quality issues.

    Args:
        source_file: Path to source CSV
        output_dir: Directory to write batch files
        num_batches: Number of batch files to generate
        issue_rate: Data quality issue rate (0.0 to 1.0)

    Returns:
        List of generated file paths
    """
    # Read source data
    print(f"Reading source data from {source_file}...")
    df_source = pd.read_csv(source_file)
    n_total = len(df_source)
    print(f"Total source records: {n_total:,}")

    # Calculate records per batch
    batch_size = n_total // num_batches
    print(f"Generating {num_batches} batches with ~{batch_size:,} records each...")

    generated_files = []

    for i in range(num_batches):
        # Determine start and end indices
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size if i < num_batches - 1 else n_total

        # Extract batch
        batch_df = df_source.iloc[start_idx:end_idx].copy()

        # Introduce quality issues
        batch_df = introduce_quality_issues(batch_df, issue_rate)

        # Write batch file
        batch_filename = f"sales_batch_{i+1:03d}.csv"
        batch_path = output_dir / batch_filename
        batch_df.to_csv(batch_path, index=False, quoting=1)

        print(f"  Generated {batch_filename}: {len(batch_df):,} records")
        generated_files.append(batch_path)

    return generated_files


def generate_summary_report(batches: list[Path], output_dir: Path) -> Path:
    """
    Generate a summary report of the batch files.

    Args:
        batches: List of batch file paths
        output_dir: Output directory

    Returns:
        Path to summary report
    """
    report_path = output_dir / "batch_summary.md"

    with open(report_path, "w") as f:
        f.write("# Batch Data Summary\n\n")
        f.write("This directory contains batch CSV files generated from `superstore_sales.csv`\n")
        f.write("with controlled data quality issues for testing the Silver layer.\n\n")

        f.write("## Quality Issues Introduced (~5% of rows)\n\n")
        f.write("- **Null Sales**: Missing sales values\n")
        f.write("- **Null Product ID**: Missing product identifiers\n")
        f.write("- **Invalid Discount**: Discount values > 1 or < 0\n")
        f.write("- **Invalid Quantity**: Quantity <= 0\n")
        f.write("- **Duplicates**: Duplicate order_id + product_id combinations (~1%)\n\n")

        f.write("## Expected Filtering Behavior\n\n")
        f.write("| Layer | Expected Action |\n")
        f.write("|-------|-----------------|\n")
        f.write("| Bronze | Keep all records (append-only) |\n")
        f.write("| Silver | Filter nulls, duplicates, invalid values |\n")
        f.write("| Gold | Aggregate clean Silver data |\n\n")

        f.write("## Batch Files\n\n")
        for batch_path in batches:
            batch_df = pd.read_csv(batch_path)
            f.write(f"### {batch_path.name}\n")
            f.write(f"- Records: {len(batch_df):,}\n")
            f.write(f"- Null sales: {batch_df['Sales'].isna().sum()}\n")
            f.write(f"- Null Product ID: {batch_df['Product ID'].isna().sum()}\n")
            f.write(f"- Invalid discount: {((batch_df['Discount'] < 0) | (batch_df['Discount'] > 1)).sum()}\n")
            f.write(f"- Invalid quantity: {(batch_df['Quantity'] <= 0).sum()}\n")
            f.write("\n")

    print(f"\nSummary report written to {report_path}")
    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate batch CSV files with data quality issues"
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=3,
        help="Number of batch files to generate (default: 3)",
    )
    parser.add_argument(
        "--source-file",
        type=str,
        default="data/raw/superstore_sales.csv",
        help="Path to source CSV file",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw_batches",
        help="Output directory for batch files",
    )
    parser.add_argument(
        "--issue-rate",
        type=float,
        default=0.05,
        help="Proportion of rows with quality issues (default: 0.05)",
    )

    args = parser.parse_args()

    # Setup paths
    source_file = Path(args.source_file)
    output_dir = Path(args.output_dir)

    if not source_file.exists():
        print(f"Error: Source file not found: {source_file}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate batches
    random.seed(42)  # For reproducibility
    np.random.seed(42)

    batches = generate_batches(
        source_file=source_file,
        output_dir=output_dir,
        num_batches=args.num_batches,
        issue_rate=args.issue_rate,
    )

    # Generate summary
    generate_summary_report(batches, output_dir)

    print(f"\nCompleted! {len(batches)} batch files generated in {output_dir}")
    return 0


if __name__ == "__main__":
    exit(main())

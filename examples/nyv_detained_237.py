#!/usr/bin/env python3
"""
NYV Detained S.237 Removal Analysis

Wrapper around court_analysis.py with NYV defaults.

Usage:
    python examples/nyv_detained_237.py
    python examples/nyv_detained_237.py --db /path/to/eoir.duckdb
    python examples/nyv_detained_237.py --pdf --output-dir ./figures
"""

import argparse
from pathlib import Path

from court_analysis import run_analysis


def main():
    parser = argparse.ArgumentParser(description="NYV Detained S.237 Analysis")
    parser.add_argument("--db", type=Path, default=Path("eoir.duckdb"))
    parser.add_argument("--output-dir", type=Path, default=Path("figures"))
    parser.add_argument("--pdf", action="store_true")
    args = parser.parse_args()

    run_analysis(
        court="NYV",
        custody="D",
        charge="237",
        case_type="RMV",
        db=args.db,
        output_dir=args.output_dir,
        pdf=args.pdf,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
NYV Detained S.237 Removal Analysis

Comprehensive analysis of detained S.237 (deportability) removal proceedings
at Varick Street Immigration Court (NYV), using the EOIR DuckDB database.

Usage:
    python examples/nyv_detained_237.py
    python examples/nyv_detained_237.py --db /path/to/eoir.duckdb
    python examples/nyv_detained_237.py --output-dir ./figures
    python examples/nyv_detained_237.py --pdf
"""

import argparse
import sys
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

# Base filter: NYV detained removal cases with a S.237 charge
BASE_FILTER = """
    p.BASE_CITY_CODE = 'NYV'
    AND p.CUSTODY = 'D'
    AND p.CASE_TYPE = 'RMV'
    AND EXISTS (
        SELECT 1 FROM charges ch
        WHERE ch.IDNPROCEEDING = p.IDNPROCEEDING
          AND ch.CHARGE LIKE '%237%'
    )
"""

LAST_12M = "p.OSC_DATE >= CURRENT_DATE - INTERVAL '12 months'"
SINCE_2015 = "p.OSC_DATE >= '2015-01-01'"


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Part 1: Timeline Analysis
# ---------------------------------------------------------------------------

def timeline_analysis(con: duckdb.DuckDBPyConnection, out: Path, figures: list):
    section("Part 1: Timeline Analysis")

    # --- Cumulative % table (last 12 months) ---
    print("\n  OSC -> First Hearing (last 12 months):")
    df = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT
            COUNT(*) AS total,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 5)  / COUNT(*), 1) AS by_day_5,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 10) / COUNT(*), 1) AS by_day_10,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 15) / COUNT(*), 1) AS by_day_15,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 25) / COUNT(*), 1) AS by_day_25,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 50) / COUNT(*), 1) AS by_day_50,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 75) / COUNT(*), 1) AS by_day_75,
            MEDIAN(days_wait) AS median,
            ROUND(AVG(days_wait), 1) AS mean,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY days_wait) AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_wait) AS p75
        FROM base
    """).fetchdf()
    print(f"    N = {int(df['total'].iloc[0]):,}")
    for col in ['by_day_5', 'by_day_10', 'by_day_15', 'by_day_25', 'by_day_50', 'by_day_75']:
        print(f"    {col:>12s}: {df[col].iloc[0]}%")
    print(f"    Median: {df['median'].iloc[0]} days | Mean: {df['mean'].iloc[0]} days | IQR: {df['p25'].iloc[0]}-{df['p75'].iloc[0]}")

    # --- Detention -> First Hearing ---
    print("\n  Detention -> First Hearing (last 12 months):")
    df2 = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE,
                   DATEDIFF('day', p.DATE_DETAINED, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.DATE_DETAINED IS NOT NULL
              AND p.HEARING_DATE >= p.DATE_DETAINED
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT
            COUNT(*) AS total,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 5)  / COUNT(*), 1) AS by_day_5,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 10) / COUNT(*), 1) AS by_day_10,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 15) / COUNT(*), 1) AS by_day_15,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 25) / COUNT(*), 1) AS by_day_25,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 50) / COUNT(*), 1) AS by_day_50,
            ROUND(100.0 * COUNT(*) FILTER (WHERE days_wait <= 75) / COUNT(*), 1) AS by_day_75,
            MEDIAN(days_wait) AS median,
            ROUND(AVG(days_wait), 1) AS mean,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY days_wait) AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_wait) AS p75
        FROM base
    """).fetchdf()
    if len(df2) and df2['total'].iloc[0] > 0:
        print(f"    N = {int(df2['total'].iloc[0]):,}")
        for col in ['by_day_5', 'by_day_10', 'by_day_15', 'by_day_25', 'by_day_50', 'by_day_75']:
            print(f"    {col:>12s}: {df2[col].iloc[0]}%")
        print(f"    Median: {df2['median'].iloc[0]} days | Mean: {df2['mean'].iloc[0]} days | IQR: {df2['p25'].iloc[0]}-{df2['p75'].iloc[0]}")
    else:
        print("    (insufficient data with DATE_DETAINED)")

    # --- Distribution histogram (last 12 months) ---
    hist_df = con.execute(f"""
        WITH base AS (
            SELECT DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT days_wait FROM base WHERE days_wait <= 200
    """).fetchdf()

    if len(hist_df) > 10:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(hist_df['days_wait'], bins=50, edgecolor='white', color='steelblue')
        ax.set_xlabel('Days from NTA to First Hearing')
        ax.set_ylabel('Cases')
        ax.set_title('NYV Detained S.237: Days to First Hearing (Last 12 Months)')
        ax.axvline(hist_df['days_wait'].median(), color='red', ls='--', label=f"Median: {hist_df['days_wait'].median():.0f} days")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out / 'timeline_histogram.png', dpi=150)
        figures.append(fig)
        print(f"\n    Saved: {out / 'timeline_histogram.png'}")

    # --- Quarterly trend since 2015 ---
    trend_df = con.execute(f"""
        WITH base AS (
            SELECT
                DATE_TRUNC('quarter', p.OSC_DATE) AS qtr,
                DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {SINCE_2015}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT qtr, COUNT(*) AS n, MEDIAN(days_wait) AS median_days
        FROM base
        GROUP BY 1
        HAVING COUNT(*) >= 5
        ORDER BY 1
    """).fetchdf()

    if len(trend_df) > 4:
        fig, ax1 = plt.subplots(figsize=(12, 5))
        ax1.bar(trend_df['qtr'], trend_df['n'], width=80, alpha=0.3, color='steelblue', label='Case count')
        ax1.set_ylabel('Cases per quarter', color='steelblue')
        ax2 = ax1.twinx()
        ax2.plot(trend_df['qtr'], trend_df['median_days'], color='red', marker='o', ms=3, label='Median days')
        ax2.set_ylabel('Median days to hearing', color='red')
        ax1.set_title('NYV Detained S.237: Quarterly Trend (2015-Present)')
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        fig.tight_layout()
        fig.savefig(out / 'timeline_trend.png', dpi=150)
        figures.append(fig)
        print(f"    Saved: {out / 'timeline_trend.png'}")


# ---------------------------------------------------------------------------
# Part 2: Demographics
# ---------------------------------------------------------------------------

def demographics(con: duckdb.DuckDBPyConnection, out: Path, figures: list):
    section("Part 2: Demographics")

    # --- Top 10 nationalities ---
    print("\n  Top 10 nationalities (last 12 months):")
    df = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE, p.NAT,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT n.NAT_COUNTRY_NAME AS country, COUNT(*) AS cases,
               MEDIAN(b.days_wait) AS median_days
        FROM base b
        JOIN lu_nationality n ON b.NAT = n.NAT_CODE
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """).fetchdf()
    for _, r in df.iterrows():
        print(f"    {r['country']:<25s} {int(r['cases']):>6,} cases  median {r['median_days']:>3.0f} days")

    # --- Criminal indicator ---
    print("\n  Criminal indicator breakdown (last 12 months):")
    df2 = con.execute(f"""
        SELECT
            CASE WHEN p.CRIM_IND = '1' OR p.CRIM_IND = 'Y' THEN 'Criminal'
                 WHEN p.AGGRAVATE_FELON THEN 'Aggravated felon'
                 ELSE 'Non-criminal' END AS category,
            COUNT(DISTINCT p.IDNCASE) AS cases
        FROM proceedings p
        WHERE {BASE_FILTER} AND {LAST_12M}
        GROUP BY 1
        ORDER BY 2 DESC
    """).fetchdf()
    for _, r in df2.iterrows():
        print(f"    {r['category']:<25s} {int(r['cases']):>6,}")

    # --- Judge variation ---
    print("\n  Judge variation (last 12 months, >=10 cases):")
    df3 = con.execute(f"""
        WITH base AS (
            SELECT p.IJ_CODE,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT j.JUDGE_NAME AS judge, COUNT(*) AS cases, MEDIAN(b.days_wait) AS median_days
        FROM base b
        JOIN lu_judge j ON b.IJ_CODE = j.JUDGE_CODE
        GROUP BY 1
        HAVING COUNT(*) >= 10
        ORDER BY 3
    """).fetchdf()
    for _, r in df3.iterrows():
        print(f"    {r['judge']:<35s} {int(r['cases']):>5,} cases  median {r['median_days']:>3.0f} days")

    if len(df3) > 2:
        fig, ax = plt.subplots(figsize=(10, max(4, len(df3) * 0.4)))
        ax.barh(df3['judge'], df3['median_days'], color='steelblue')
        ax.set_xlabel('Median Days to First Hearing')
        ax.set_title('NYV Detained S.237: Judge Variation (Last 12 Months)')
        fig.tight_layout()
        fig.savefig(out / 'judge_variation.png', dpi=150)
        figures.append(fig)
        print(f"\n    Saved: {out / 'judge_variation.png'}")


# ---------------------------------------------------------------------------
# Part 3: Representation
# ---------------------------------------------------------------------------

def representation(con: duckdb.DuckDBPyConnection, out: Path, figures: list):
    section("Part 3: Representation")

    # --- % with counsel ---
    print("\n  Counsel rates (last 12 months):")
    df = con.execute(f"""
        WITH base AS (
            SELECT DISTINCT p.IDNCASE
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
        ),
        rep AS (
            SELECT b.IDNCASE,
                   MAX(CASE WHEN r.STRATTYTYPE = 'ALIEN' THEN 1 ELSE 0 END) AS has_counsel
            FROM base b
            LEFT JOIN representatives r ON b.IDNCASE = r.IDNCASE
            GROUP BY 1
        )
        SELECT COUNT(*) AS total,
               SUM(has_counsel) AS with_counsel,
               ROUND(100.0 * SUM(has_counsel) / COUNT(*), 1) AS pct
        FROM rep
    """).fetchdf()
    print(f"    Total cases: {int(df['total'].iloc[0]):,}")
    print(f"    With counsel: {int(df['with_counsel'].iloc[0]):,} ({df['pct'].iloc[0]}%)")

    # --- Median days from detention to attorney appearance ---
    print("\n  Time to attorney (E_28_DATE - DATE_DETAINED):")
    df2 = con.execute(f"""
        WITH base AS (
            SELECT DISTINCT p.IDNCASE, p.DATE_DETAINED
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.DATE_DETAINED IS NOT NULL
        )
        SELECT MEDIAN(DATEDIFF('day', b.DATE_DETAINED, r.E_28_DATE)) AS median_days,
               COUNT(*) AS n
        FROM base b
        JOIN representatives r ON b.IDNCASE = r.IDNCASE
        WHERE r.STRATTYTYPE = 'ALIEN'
          AND r.E_28_DATE IS NOT NULL
          AND r.E_28_DATE >= b.DATE_DETAINED
    """).fetchdf()
    if len(df2) and df2['n'].iloc[0] > 0:
        print(f"    N = {int(df2['n'].iloc[0]):,}")
        print(f"    Median days from detention to attorney: {df2['median_days'].iloc[0]}")

    # --- Counsel timing vs first hearing ---
    print("\n  Counsel timing relative to first hearing:")
    df3 = con.execute(f"""
        WITH first_hear AS (
            SELECT p.IDNCASE,
                   MIN(p.HEARING_DATE) AS first_hearing
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL
            GROUP BY 1
        ),
        atty AS (
            SELECT r.IDNCASE, MIN(r.E_28_DATE) AS first_atty
            FROM representatives r
            WHERE r.STRATTYTYPE = 'ALIEN' AND r.E_28_DATE IS NOT NULL
            GROUP BY 1
        )
        SELECT
            CASE WHEN a.first_atty <= fh.first_hearing THEN 'Before first hearing'
                 WHEN a.first_atty > fh.first_hearing THEN 'After first hearing'
                 END AS timing,
            COUNT(*) AS n
        FROM first_hear fh
        JOIN atty a ON fh.IDNCASE = a.IDNCASE
        WHERE a.first_atty IS NOT NULL AND fh.first_hearing IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    for _, r in df3.iterrows():
        print(f"    {r['timing']:<30s} {int(r['n']):>6,}")


# ---------------------------------------------------------------------------
# Part 4: Case Outcomes
# ---------------------------------------------------------------------------

def outcomes(con: duckdb.DuckDBPyConnection, out: Path, figures: list):
    section("Part 4: Case Outcomes")

    # --- Decision breakdown ---
    print("\n  Decision breakdown (completed cases, all time):")
    df = con.execute(f"""
        SELECT d.strDecDescription AS decision,
               COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM proceedings p
        JOIN lu_court_decision d
            ON p.DEC_CODE = d.strDecCode AND p.CASE_TYPE = d.strCaseType
        WHERE {BASE_FILTER}
          AND p.COMP_DATE IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """).fetchdf()
    for _, r in df.iterrows():
        print(f"    {r['decision']:<40s} {int(r['n']):>6,}  ({r['pct']}%)")

    # --- Outcomes by wait time group ---
    print("\n  Outcomes by wait time group (last 12 months):")
    df2 = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE, p.DEC_CODE, p.CASE_TYPE,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.COMP_DATE IS NOT NULL AND p.DEC_CODE IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT
            CASE WHEN days_wait <= 25 THEN '1. Fast (<=25d)'
                 WHEN days_wait <= 75 THEN '2. Medium (26-75d)'
                 ELSE '3. Slow (>75d)' END AS speed_group,
            d.strDecDescription AS decision,
            COUNT(*) AS n
        FROM base b
        JOIN lu_court_decision d ON b.DEC_CODE = d.strDecCode AND b.CASE_TYPE = d.strCaseType
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """).fetchdf()
    if len(df2):
        for group in df2['speed_group'].unique():
            subset = df2[df2['speed_group'] == group]
            print(f"\n    {group}:")
            for _, r in subset.head(5).iterrows():
                print(f"      {r['decision']:<40s} {int(r['n']):>5,}")

    # --- Outcome bar chart ---
    if len(df) > 2:
        top = df.head(8)
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(top['decision'][::-1], top['n'][::-1], color='steelblue')
        ax.set_xlabel('Cases')
        ax.set_title('NYV Detained S.237: Case Outcomes')
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
        fig.tight_layout()
        fig.savefig(out / 'outcomes.png', dpi=150)
        figures.append(fig)
        print(f"\n    Saved: {out / 'outcomes.png'}")


# ---------------------------------------------------------------------------
# Part 5: Applications for Relief
# ---------------------------------------------------------------------------

def applications_analysis(con: duckdb.DuckDBPyConnection, out: Path, figures: list):
    section("Part 5: Applications for Relief")

    df = con.execute(f"""
        WITH base_cases AS (
            SELECT DISTINCT p.IDNCASE, p.IDNPROCEEDING
            FROM proceedings p
            WHERE {BASE_FILTER}
        )
        SELECT
            la.strdescription AS application_type,
            COUNT(*) AS filed,
            SUM(CASE WHEN a.APPL_DEC IN ('G', 'F', 'I', 'P', 'C', 'L') THEN 1 ELSE 0 END) AS granted,
            SUM(CASE WHEN a.APPL_DEC = 'D' THEN 1 ELSE 0 END) AS denied,
            SUM(CASE WHEN a.APPL_DEC = 'W' THEN 1 ELSE 0 END) AS withdrawn,
            ROUND(100.0 * SUM(CASE WHEN a.APPL_DEC IN ('G', 'F', 'I', 'P', 'C', 'L') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN a.APPL_DEC IN ('G', 'F', 'I', 'P', 'C', 'L', 'D') THEN 1 ELSE 0 END), 0), 1)
                AS grant_rate_pct
        FROM base_cases bc
        JOIN applications a ON bc.IDNCASE = a.IDNCASE
        JOIN lu_application la ON a.APPL_CODE = la.strcode
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """).fetchdf()

    print("\n  Applications filed (all time):")
    for _, r in df.iterrows():
        gr = f"{r['grant_rate_pct']}%" if pd.notna(r['grant_rate_pct']) else "N/A"
        print(f"    {r['application_type']:<30s} filed:{int(r['filed']):>7,}  grant:{int(r['granted']):>6,}  deny:{int(r['denied']):>6,}  rate:{gr:>6s}")


# ---------------------------------------------------------------------------
# Part 6: Bond
# ---------------------------------------------------------------------------

def bond_analysis(con: duckdb.DuckDBPyConnection, out: Path, figures: list):
    section("Part 6: Bond")

    # --- Bond decisions ---
    print("\n  Bond hearing decisions:")
    df = con.execute(f"""
        WITH base_cases AS (
            SELECT DISTINCT p.IDNCASE
            FROM proceedings p
            WHERE {BASE_FILTER}
        )
        SELECT
            bo.DEC AS decision_code,
            COUNT(*) AS n,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM base_cases bc
        JOIN bonds bo ON bc.IDNCASE = bo.IDNCASE
        WHERE bo.DEC IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
    """).fetchdf()
    for _, r in df.iterrows():
        print(f"    {r['decision_code']:<10s} {int(r['n']):>7,}  ({r['pct']}%)")

    # --- Bond amounts ---
    print("\n  Bond amounts (where bond was set):")
    df2 = con.execute(f"""
        WITH base_cases AS (
            SELECT DISTINCT p.IDNCASE
            FROM proceedings p
            WHERE {BASE_FILTER}
        )
        SELECT
            COUNT(*) AS n,
            MEDIAN(bo.NEW_BOND) AS median_bond,
            ROUND(AVG(bo.NEW_BOND)) AS mean_bond,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bo.NEW_BOND) AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bo.NEW_BOND) AS p75
        FROM base_cases bc
        JOIN bonds bo ON bc.IDNCASE = bo.IDNCASE
        WHERE bo.NEW_BOND IS NOT NULL AND bo.NEW_BOND > 0
    """).fetchdf()
    if len(df2) and df2['n'].iloc[0] > 0:
        print(f"    N = {int(df2['n'].iloc[0]):,}")
        print(f"    Median: ${df2['median_bond'].iloc[0]:,.0f}")
        print(f"    Mean:   ${df2['mean_bond'].iloc[0]:,.0f}")
        print(f"    IQR:    ${df2['p25'].iloc[0]:,.0f} - ${df2['p75'].iloc[0]:,.0f}")


# ---------------------------------------------------------------------------
# Part 7: Key Findings Summary
# ---------------------------------------------------------------------------

def summary(con: duckdb.DuckDBPyConnection):
    section("Part 7: Key Findings Summary")

    total = con.execute(f"""
        SELECT COUNT(DISTINCT p.IDNCASE) FROM proceedings p WHERE {BASE_FILTER}
    """).fetchone()[0]

    recent = con.execute(f"""
        SELECT COUNT(DISTINCT p.IDNCASE) FROM proceedings p WHERE {BASE_FILTER} AND {LAST_12M}
    """).fetchone()[0]

    median_days = con.execute(f"""
        WITH base AS (
            SELECT DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS dw
            FROM proceedings p
            WHERE {BASE_FILTER} AND {LAST_12M}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT MEDIAN(dw) FROM base
    """).fetchone()[0]

    print(f"""
    NYV Detained S.237 Removal Proceedings
    --------------------------------------
    Total cases (all time):     {total:>10,}
    Cases (last 12 months):     {recent:>10,}
    Median days to hearing:     {median_days if median_days else 'N/A':>10}
    """)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="NYV Detained S.237 Analysis")
    parser.add_argument("--db", type=Path, default=Path("eoir.duckdb"), help="Path to eoir.duckdb")
    parser.add_argument("--output-dir", type=Path, default=Path("figures"), help="Directory for output figures")
    parser.add_argument("--pdf", action="store_true", help="Generate a combined PDF report")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: Database not found at {args.db}")
        print("Run build_database.py first, or use --db /path/to/eoir.duckdb")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db), read_only=True)
    figures: list[plt.Figure] = []

    print("=" * 60)
    print("  NYV Detained S.237 Removal Analysis")
    print("=" * 60)

    timeline_analysis(con, args.output_dir, figures)
    demographics(con, args.output_dir, figures)
    representation(con, args.output_dir, figures)
    outcomes(con, args.output_dir, figures)
    applications_analysis(con, args.output_dir, figures)
    bond_analysis(con, args.output_dir, figures)
    summary(con)

    if args.pdf and figures:
        pdf_path = args.output_dir / "nyv_detained_237_report.pdf"
        with PdfPages(pdf_path) as pdf:
            for fig in figures:
                pdf.savefig(fig)
        print(f"\n  PDF report saved to {pdf_path}")

    con.close()
    plt.close('all')
    print("\nDone.")


if __name__ == "__main__":
    main()

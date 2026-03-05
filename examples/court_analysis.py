#!/usr/bin/env python3
"""
Immigration Court Analysis

Parameterized analysis of immigration court proceedings using the EOIR DuckDB
database. Covers timelines, demographics, representation, outcomes,
applications for relief, and bond.

Usage:
    python examples/court_analysis.py
    python examples/court_analysis.py --court NYV --custody D --charge 237
    python examples/court_analysis.py --court MIA --charge % --pdf
    python examples/court_analysis.py --court NYC,NYV,NYB --custody N --charge 208
    python examples/court_analysis.py --court SFR --months 24 --trend-start 2018 --pdf
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
# Analysis configuration
# ---------------------------------------------------------------------------

class AnalysisConfig:
    """Holds all analysis parameters and builds SQL filter clauses."""

    def __init__(
        self,
        court: str = "NYV",
        custody: str = "D",
        case_type: str = "RMV",
        charge: str = "237",
        months: int = 12,
        trend_start: int = 2015,
        db: Path = Path("eoir.duckdb"),
        output_dir: Path = Path("output"),
        pdf: bool = False,
    ):
        self.courts = [c.strip() for c in court.split(",")]
        self.custody = custody.upper()
        self.case_type = case_type.upper()
        self.charge = charge
        self.months = months
        self.trend_start = trend_start
        self.db = db
        self.output_dir = output_dir
        self.pdf = pdf

    @property
    def base_filter(self) -> str:
        """SQL WHERE clause for the core filter (against proceedings alias p)."""
        parts = []

        if len(self.courts) == 1:
            parts.append(f"p.BASE_CITY_CODE = '{self.courts[0]}'")
        else:
            codes = ", ".join(f"'{c}'" for c in self.courts)
            parts.append(f"p.BASE_CITY_CODE IN ({codes})")

        if self.custody != "ALL":
            parts.append(f"p.CUSTODY = '{self.custody}'")

        parts.append(f"p.CASE_TYPE = '{self.case_type}'")

        if self.charge != "%":
            parts.append(
                f"EXISTS (SELECT 1 FROM charges ch "
                f"WHERE ch.IDNPROCEEDING = p.IDNPROCEEDING "
                f"AND ch.CHARGE LIKE '%{self.charge}%')"
            )

        return " AND ".join(parts)

    @property
    def recent_filter(self) -> str:
        return f"p.OSC_DATE >= CURRENT_DATE - INTERVAL '{self.months} months'"

    @property
    def trend_filter(self) -> str:
        return f"p.OSC_DATE >= '{self.trend_start}-01-01'"

    def resolve_title(self, con: duckdb.DuckDBPyConnection) -> str:
        """Build a human-readable title from the parameters."""
        # Resolve court names
        if len(self.courts) == 1:
            row = con.execute(
                f"SELECT BASE_CITY_NAME FROM lu_base_city WHERE BASE_CITY_CODE = '{self.courts[0]}'"
            ).fetchone()
            court_label = f"{row[0]} ({self.courts[0]})" if row else self.courts[0]
        else:
            court_label = ", ".join(self.courts)

        custody_label = {
            "D": "Detained", "N": "Non-Detained", "R": "Released", "ALL": "All Custody"
        }.get(self.custody, self.custody)

        charge_label = f"S.{self.charge}" if self.charge != "%" else "All Charges"

        return f"{custody_label} {charge_label} {self.case_type} Proceedings -- {court_label}"


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Part 1: Timeline Analysis
# ---------------------------------------------------------------------------

def timeline_analysis(
    con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, out: Path, figures: list
):
    section("Part 1: Timeline Analysis")
    bf = cfg.base_filter
    rf = cfg.recent_filter

    print(f"\n  OSC -> First Hearing (last {cfg.months} months):")
    df = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {bf} AND {rf}
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

    if len(df) and df["total"].iloc[0] > 0:
        print(f"    N = {int(df['total'].iloc[0]):,}")
        for col in ["by_day_5", "by_day_10", "by_day_15", "by_day_25", "by_day_50", "by_day_75"]:
            print(f"    {col:>12s}: {df[col].iloc[0]}%")
        print(f"    Median: {df['median'].iloc[0]} days | Mean: {df['mean'].iloc[0]} days | IQR: {df['p25'].iloc[0]}-{df['p75'].iloc[0]}")
    else:
        print("    (no data)")
        return

    # Detention -> First Hearing
    if cfg.custody == "D":
        print(f"\n  Detention -> First Hearing (last {cfg.months} months):")
        df2 = con.execute(f"""
            WITH base AS (
                SELECT p.IDNCASE,
                       DATEDIFF('day', p.DATE_DETAINED, p.HEARING_DATE) AS days_wait
                FROM proceedings p
                WHERE {bf} AND {rf}
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
        if len(df2) and df2["total"].iloc[0] > 0:
            print(f"    N = {int(df2['total'].iloc[0]):,}")
            for col in ["by_day_5", "by_day_10", "by_day_15", "by_day_25", "by_day_50", "by_day_75"]:
                print(f"    {col:>12s}: {df2[col].iloc[0]}%")
            print(f"    Median: {df2['median'].iloc[0]} days | Mean: {df2['mean'].iloc[0]} days | IQR: {df2['p25'].iloc[0]}-{df2['p75'].iloc[0]}")
        else:
            print("    (insufficient data with DATE_DETAINED)")

    # Distribution histogram
    hist_df = con.execute(f"""
        WITH base AS (
            SELECT DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {bf} AND {rf}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT days_wait FROM base WHERE days_wait <= 200
    """).fetchdf()

    if len(hist_df) > 10:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(hist_df["days_wait"], bins=50, edgecolor="white", color="steelblue")
        ax.set_xlabel("Days from NTA to First Hearing")
        ax.set_ylabel("Cases")
        ax.set_title(f"Days to First Hearing (Last {cfg.months} Months)")
        med = hist_df["days_wait"].median()
        ax.axvline(med, color="red", ls="--", label=f"Median: {med:.0f} days")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out / "timeline_histogram.png", dpi=150)
        figures.append(fig)
        print(f"\n    Saved: {out / 'timeline_histogram.png'}")

    # Quarterly trend
    tf = cfg.trend_filter
    trend_df = con.execute(f"""
        WITH base AS (
            SELECT DATE_TRUNC('quarter', p.OSC_DATE) AS qtr,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {bf} AND {tf}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT qtr, COUNT(*) AS n, MEDIAN(days_wait) AS median_days
        FROM base GROUP BY 1 HAVING COUNT(*) >= 5 ORDER BY 1
    """).fetchdf()

    if len(trend_df) > 4:
        fig, ax1 = plt.subplots(figsize=(12, 5))
        ax1.bar(trend_df["qtr"], trend_df["n"], width=80, alpha=0.3, color="steelblue", label="Case count")
        ax1.set_ylabel("Cases per quarter", color="steelblue")
        ax2 = ax1.twinx()
        ax2.plot(trend_df["qtr"], trend_df["median_days"], color="red", marker="o", ms=3, label="Median days")
        ax2.set_ylabel("Median days to hearing", color="red")
        ax1.set_title(f"Quarterly Trend ({cfg.trend_start}-Present)")
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
        fig.tight_layout()
        fig.savefig(out / "timeline_trend.png", dpi=150)
        figures.append(fig)
        print(f"    Saved: {out / 'timeline_trend.png'}")


# ---------------------------------------------------------------------------
# Part 2: Demographics
# ---------------------------------------------------------------------------

def demographics(
    con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, out: Path, figures: list
):
    section("Part 2: Demographics")
    bf = cfg.base_filter
    rf = cfg.recent_filter

    print(f"\n  Top 10 nationalities (last {cfg.months} months):")
    df = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE, p.NAT,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {bf} AND {rf}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT n.NAT_COUNTRY_NAME AS country, COUNT(*) AS cases,
               MEDIAN(b.days_wait) AS median_days
        FROM base b
        JOIN lu_nationality n ON b.NAT = n.NAT_CODE
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchdf()
    for _, r in df.iterrows():
        print(f"    {r['country']:<25s} {int(r['cases']):>6,} cases  median {r['median_days']:>3.0f} days")

    # Criminal indicator
    print(f"\n  Criminal indicator breakdown (last {cfg.months} months):")
    df2 = con.execute(f"""
        SELECT
            CASE WHEN p.CRIM_IND = '1' OR p.CRIM_IND = 'Y' THEN 'Criminal'
                 WHEN p.AGGRAVATE_FELON THEN 'Aggravated felon'
                 ELSE 'Non-criminal' END AS category,
            COUNT(DISTINCT p.IDNCASE) AS cases
        FROM proceedings p
        WHERE {bf} AND {rf}
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    for _, r in df2.iterrows():
        print(f"    {r['category']:<25s} {int(r['cases']):>6,}")

    # Judge variation
    print(f"\n  Judge variation (last {cfg.months} months, >=10 cases):")
    df3 = con.execute(f"""
        WITH base AS (
            SELECT p.IJ_CODE,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {bf} AND {rf}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT j.JUDGE_NAME AS judge, COUNT(*) AS cases, MEDIAN(b.days_wait) AS median_days
        FROM base b
        JOIN lu_judge j ON b.IJ_CODE = j.JUDGE_CODE
        GROUP BY 1 HAVING COUNT(*) >= 10 ORDER BY 3
    """).fetchdf()
    for _, r in df3.iterrows():
        print(f"    {r['judge']:<35s} {int(r['cases']):>5,} cases  median {r['median_days']:>3.0f} days")

    if len(df3) > 2:
        fig, ax = plt.subplots(figsize=(10, max(4, len(df3) * 0.4)))
        ax.barh(df3["judge"], df3["median_days"], color="steelblue")
        ax.set_xlabel("Median Days to First Hearing")
        ax.set_title(f"Judge Variation (Last {cfg.months} Months)")
        fig.tight_layout()
        fig.savefig(out / "judge_variation.png", dpi=150)
        figures.append(fig)
        print(f"\n    Saved: {out / 'judge_variation.png'}")


# ---------------------------------------------------------------------------
# Part 3: Representation
# ---------------------------------------------------------------------------

def representation(
    con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, out: Path, figures: list
):
    section("Part 3: Representation")
    bf = cfg.base_filter
    rf = cfg.recent_filter

    print(f"\n  Counsel rates (last {cfg.months} months):")
    df = con.execute(f"""
        WITH base AS (
            SELECT DISTINCT p.IDNCASE FROM proceedings p WHERE {bf} AND {rf}
        ),
        rep AS (
            SELECT b.IDNCASE,
                   MAX(CASE WHEN r.STRATTYTYPE = 'ALIEN' THEN 1 ELSE 0 END) AS has_counsel
            FROM base b LEFT JOIN representatives r ON b.IDNCASE = r.IDNCASE
            GROUP BY 1
        )
        SELECT COUNT(*) AS total,
               SUM(has_counsel) AS with_counsel,
               ROUND(100.0 * SUM(has_counsel) / COUNT(*), 1) AS pct
        FROM rep
    """).fetchdf()
    print(f"    Total cases: {int(df['total'].iloc[0]):,}")
    print(f"    With counsel: {int(df['with_counsel'].iloc[0]):,} ({df['pct'].iloc[0]}%)")

    # Time to attorney (only for detained)
    if cfg.custody == "D":
        print("\n  Time to attorney (E_28_DATE - DATE_DETAINED):")
        df2 = con.execute(f"""
            WITH base AS (
                SELECT DISTINCT p.IDNCASE, p.DATE_DETAINED
                FROM proceedings p
                WHERE {bf} AND {rf} AND p.DATE_DETAINED IS NOT NULL
            )
            SELECT MEDIAN(DATEDIFF('day', b.DATE_DETAINED, r.E_28_DATE)) AS median_days,
                   COUNT(*) AS n
            FROM base b
            JOIN representatives r ON b.IDNCASE = r.IDNCASE
            WHERE r.STRATTYTYPE = 'ALIEN'
              AND r.E_28_DATE IS NOT NULL
              AND r.E_28_DATE >= b.DATE_DETAINED
        """).fetchdf()
        if len(df2) and df2["n"].iloc[0] > 0:
            print(f"    N = {int(df2['n'].iloc[0]):,}")
            print(f"    Median days from detention to attorney: {df2['median_days'].iloc[0]}")

    # Counsel timing vs first hearing
    print("\n  Counsel timing relative to first hearing:")
    df3 = con.execute(f"""
        WITH first_hear AS (
            SELECT p.IDNCASE, MIN(p.HEARING_DATE) AS first_hearing
            FROM proceedings p
            WHERE {bf} AND {rf} AND p.HEARING_DATE IS NOT NULL
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
        GROUP BY 1 ORDER BY 1
    """).fetchdf()
    for _, r in df3.iterrows():
        print(f"    {r['timing']:<30s} {int(r['n']):>6,}")


# ---------------------------------------------------------------------------
# Part 4: Case Outcomes
# ---------------------------------------------------------------------------

def outcomes(
    con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, out: Path, figures: list
):
    section("Part 4: Case Outcomes")
    bf = cfg.base_filter

    print("\n  Decision breakdown (completed cases, all time):")
    df = con.execute(f"""
        SELECT d.strDecDescription AS decision,
               COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM proceedings p
        JOIN lu_court_decision d
            ON p.DEC_CODE = d.strDecCode AND p.CASE_TYPE = d.strCaseType
        WHERE {bf} AND p.COMP_DATE IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """).fetchdf()
    for _, r in df.iterrows():
        print(f"    {r['decision']:<40s} {int(r['n']):>6,}  ({r['pct']}%)")

    # Outcomes by wait time group
    rf = cfg.recent_filter
    print(f"\n  Outcomes by wait time group (last {cfg.months} months):")
    df2 = con.execute(f"""
        WITH base AS (
            SELECT p.IDNCASE, p.DEC_CODE, p.CASE_TYPE,
                   DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS days_wait
            FROM proceedings p
            WHERE {bf} AND {rf}
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
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """).fetchdf()
    if len(df2):
        for group in sorted(df2["speed_group"].unique()):
            subset = df2[df2["speed_group"] == group]
            print(f"\n    {group}:")
            for _, r in subset.head(5).iterrows():
                print(f"      {r['decision']:<40s} {int(r['n']):>5,}")

    if len(df) > 2:
        top = df.head(8)
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(top["decision"][::-1], top["n"][::-1], color="steelblue")
        ax.set_xlabel("Cases")
        ax.set_title("Case Outcomes")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        fig.tight_layout()
        fig.savefig(out / "outcomes.png", dpi=150)
        figures.append(fig)
        print(f"\n    Saved: {out / 'outcomes.png'}")


# ---------------------------------------------------------------------------
# Part 5: Applications for Relief
# ---------------------------------------------------------------------------

def applications_analysis(
    con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, out: Path, figures: list
):
    section("Part 5: Applications for Relief")
    bf = cfg.base_filter

    df = con.execute(f"""
        WITH base_cases AS (
            SELECT DISTINCT p.IDNCASE, p.IDNPROCEEDING FROM proceedings p WHERE {bf}
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
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """).fetchdf()

    print("\n  Applications filed (all time):")
    for _, r in df.iterrows():
        gr = f"{r['grant_rate_pct']}%" if pd.notna(r["grant_rate_pct"]) else "N/A"
        print(
            f"    {r['application_type']:<30s} "
            f"filed:{int(r['filed']):>7,}  "
            f"grant:{int(r['granted']):>6,}  "
            f"deny:{int(r['denied']):>6,}  "
            f"rate:{gr:>6s}"
        )


# ---------------------------------------------------------------------------
# Part 6: Bond
# ---------------------------------------------------------------------------

def bond_analysis(
    con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, out: Path, figures: list
):
    section("Part 6: Bond")
    bf = cfg.base_filter

    print("\n  Bond hearing decisions:")
    df = con.execute(f"""
        WITH base_cases AS (
            SELECT DISTINCT p.IDNCASE FROM proceedings p WHERE {bf}
        )
        SELECT bo.DEC AS decision_code,
               COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM base_cases bc
        JOIN bonds bo ON bc.IDNCASE = bo.IDNCASE
        WHERE bo.DEC IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    for _, r in df.iterrows():
        print(f"    {r['decision_code']:<10s} {int(r['n']):>7,}  ({r['pct']}%)")

    print("\n  Bond amounts (where bond was set):")
    df2 = con.execute(f"""
        WITH base_cases AS (
            SELECT DISTINCT p.IDNCASE FROM proceedings p WHERE {bf}
        )
        SELECT COUNT(*) AS n,
               MEDIAN(bo.NEW_BOND) AS median_bond,
               ROUND(AVG(bo.NEW_BOND)) AS mean_bond,
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bo.NEW_BOND) AS p25,
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bo.NEW_BOND) AS p75
        FROM base_cases bc
        JOIN bonds bo ON bc.IDNCASE = bo.IDNCASE
        WHERE bo.NEW_BOND IS NOT NULL AND bo.NEW_BOND > 0
    """).fetchdf()
    if len(df2) and df2["n"].iloc[0] > 0:
        print(f"    N = {int(df2['n'].iloc[0]):,}")
        print(f"    Median: ${df2['median_bond'].iloc[0]:,.0f}")
        print(f"    Mean:   ${df2['mean_bond'].iloc[0]:,.0f}")
        print(f"    IQR:    ${df2['p25'].iloc[0]:,.0f} - ${df2['p75'].iloc[0]:,.0f}")


# ---------------------------------------------------------------------------
# Part 7: Summary
# ---------------------------------------------------------------------------

def summary(con: duckdb.DuckDBPyConnection, cfg: AnalysisConfig, title: str):
    section("Part 7: Key Findings Summary")
    bf = cfg.base_filter
    rf = cfg.recent_filter

    total = con.execute(
        f"SELECT COUNT(DISTINCT p.IDNCASE) FROM proceedings p WHERE {bf}"
    ).fetchone()[0]

    recent = con.execute(
        f"SELECT COUNT(DISTINCT p.IDNCASE) FROM proceedings p WHERE {bf} AND {rf}"
    ).fetchone()[0]

    median_days = con.execute(f"""
        WITH base AS (
            SELECT DATEDIFF('day', p.OSC_DATE, p.HEARING_DATE) AS dw
            FROM proceedings p
            WHERE {bf} AND {rf}
              AND p.HEARING_DATE IS NOT NULL AND p.OSC_DATE IS NOT NULL
              AND p.HEARING_DATE >= p.OSC_DATE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.IDNCASE ORDER BY p.HEARING_DATE) = 1
        )
        SELECT MEDIAN(dw) FROM base
    """).fetchone()[0]

    print(f"""
    {title}
    {'-' * len(title)}
    Total cases (all time):     {total:>10,}
    Cases (last {cfg.months} months):{'':>{8-len(str(cfg.months))}}{recent:>10,}
    Median days to hearing:     {median_days if median_days else 'N/A':>10}
    """)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_analysis(
    court: str = "NYV",
    custody: str = "D",
    case_type: str = "RMV",
    charge: str = "237",
    months: int = 12,
    trend_start: int = 2015,
    db: Path = Path("eoir.duckdb"),
    output_dir: Path = Path("output"),
    pdf: bool = False,
):
    """Run the full analysis programmatically."""
    cfg = AnalysisConfig(
        court=court, custody=custody, case_type=case_type, charge=charge,
        months=months, trend_start=trend_start, db=db, output_dir=output_dir, pdf=pdf,
    )
    _run(cfg)


def _run(cfg: AnalysisConfig):
    if not cfg.db.exists():
        print(f"Error: Database not found at {cfg.db}")
        print("Run build_database.py first, or use --db /path/to/eoir.duckdb")
        sys.exit(1)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.db), read_only=True)
    title = cfg.resolve_title(con)
    figures: list[plt.Figure] = []

    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)

    timeline_analysis(con, cfg, cfg.output_dir, figures)
    demographics(con, cfg, cfg.output_dir, figures)
    representation(con, cfg, cfg.output_dir, figures)
    outcomes(con, cfg, cfg.output_dir, figures)
    applications_analysis(con, cfg, cfg.output_dir, figures)
    bond_analysis(con, cfg, cfg.output_dir, figures)
    summary(con, cfg, title)

    if cfg.pdf and figures:
        pdf_path = cfg.output_dir / "report.pdf"
        with PdfPages(pdf_path) as pdf_file:
            for fig in figures:
                pdf_file.savefig(fig)
        print(f"\n  PDF report saved to {pdf_path}")

    con.close()
    plt.close("all")
    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(description="Immigration Court Analysis")
    parser.add_argument("--db", type=Path, default=Path("eoir.duckdb"))
    parser.add_argument("--court", default="NYV", help="BASE_CITY_CODE(s), comma-separated")
    parser.add_argument("--custody", default="D", help="D, N, R, or ALL")
    parser.add_argument("--case-type", default="RMV", help="Case type (default: RMV)")
    parser.add_argument("--charge", default="237", help="Charge pattern for LIKE filter (use %% for all)")
    parser.add_argument("--months", type=int, default=12, help="Lookback months for recent analysis")
    parser.add_argument("--trend-start", type=int, default=2015, help="Start year for trend charts")
    parser.add_argument("--pdf", action="store_true", help="Generate PDF report")
    parser.add_argument("--output-dir", type=Path, default=Path("output"), help="Output directory")
    args = parser.parse_args()

    cfg = AnalysisConfig(
        court=args.court, custody=args.custody, case_type=args.case_type,
        charge=args.charge, months=args.months, trend_start=args.trend_start,
        db=args.db, output_dir=args.output_dir, pdf=args.pdf,
    )
    _run(cfg)


if __name__ == "__main__":
    main()

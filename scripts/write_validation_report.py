#!/usr/bin/env python3
"""Regenerate VALIDATION.md from the built eoir.duckdb (read-only queries).

Usage: uv run python scripts/write_validation_report.py [--db eoir.duckdb] [--data-version "August 2026 (2026-0801)"]
"""
import argparse, datetime
from pathlib import Path
import duckdb

CORE = ["schedule", "representatives", "charges", "proceedings", "applications", "cases",
        "custody_history", "motions", "juvenile_history", "lead_rider", "case_identifiers",
        "bonds", "appeals", "appeal2", "attorneys", "appeal_fed_courts", "case_priority_history",
        "three_mbr_referrals", "pro_bono"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="eoir.duckdb"); ap.add_argument("--data-version", required=True)
    ap.add_argument("--out", default="VALIDATION.md")
    a = ap.parse_args()
    con = duckdb.connect(a.db, read_only=True); con.execute("SET memory_limit='2GB'"); con.execute("SET threads=2")
    q = lambda sql: con.execute(sql).fetchone()
    tables = [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_type='BASE TABLE' AND table_name NOT LIKE '\\_%' ESCAPE '\\'").fetchall()]
    lookups = [t for t in tables if t.startswith("lu_")]; core = [t for t in tables if not t.startswith("lu_")]
    total = q("SELECT SUM(row_count) FROM _metadata")[0]
    size_gb = Path(a.db).stat().st_size / 1024**3
    L = []
    L += ["# Data Validation Report", "",
          f"**Build date:** {datetime.date.today().isoformat()}",
          f"**EOIR data version:** {a.data_version}",
          f"**Total tables:** {len(tables)} ({len(core)} core + {len(lookups)} lookup) + `_metadata`/`_columns`",
          f"**Total rows:** {total:,}", f"**Database size:** {size_gb:.1f} GB", "",
          "## Row counts (core tables)", "", "| Table | Rows |", "|-------|------|"]
    counts = {t: q(f'SELECT COUNT(*) FROM "{t}"')[0] for t in CORE if t in tables}
    for t, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        L.append(f"| `{t}` | {n:,} |")
    zero = [t for t in tables if q(f'SELECT COUNT(*) FROM "{t}"')[0] == 0]
    L += ["", f"Zero-row tables: {', '.join(zero) if zero else 'none'}.", ""]
    L += ["## Date parsing rates (proceedings)", "", "| Column | Non-null | Null % |", "|--------|----------|--------|"]
    n_p = counts["proceedings"]
    for c in ("OSC_DATE", "HEARING_DATE", "COMP_DATE"):
        nn = q(f"SELECT COUNT({c}) FROM proceedings")[0]
        L.append(f"| `{c}` | {nn:,} | {100*(1-nn/n_p):.1f}% |")
    L += ["", "## Referential integrity", "", "| Check | Orphans | % of child |", "|-------|---------|------------|"]
    for child, parent, key in [("proceedings", "cases", "IDNCASE"), ("charges", "proceedings", "IDNPROCEEDING"),
                               ("representatives", "cases", "IDNCASE"), ("applications", "cases", "IDNCASE"), ("bonds", "cases", "IDNCASE")]:
        try:
            orph = q(f'SELECT COUNT(*) FROM {child} c LEFT JOIN {parent} p USING ({key}) WHERE p.{key} IS NULL AND c.{key} IS NOT NULL')[0]
            L.append(f"| {child} -> {parent} | {orph:,} | {100*orph/counts[child]:.3f}% |")
        except Exception as e:
            L.append(f"| {child} -> {parent} | n/a | {str(e)[:40]} |")
    L += ["", "## Lookup coverage", "", "| Join | Matched | Total (non-null) | Coverage |", "|------|---------|------------------|----------|"]
    for child, col, lu, lcol in [("proceedings", "NAT", "lu_nationality", "NAT_CODE"), ("proceedings", "BASE_CITY_CODE", "lu_base_city", "BASE_CITY_CODE"),
                                 ("proceedings", "IJ_CODE", "lu_judge", "JUDGE_CODE"), ("charges", "CHARGE", "lu_charges", "strCode"), ("cases", "LANG", "lu_language", "strCode")]:
        try:
            m, t = q(f'SELECT COUNT(*) FILTER (WHERE l.{lcol} IS NOT NULL), COUNT(*) FROM {child} c LEFT JOIN {lu} l ON c.{col} = l.{lcol} WHERE c.{col} IS NOT NULL')
            L.append(f"| {child}.{col} -> {lu} | {m:,} | {t:,} | {100*m/t:.1f}% |")
        except Exception as e:
            L.append(f"| {child}.{col} -> {lu} | n/a | n/a | {str(e)[:40]} |")
    L += ["", "## Views", "", "| View | Rows | Reference | Status |", "|------|------|-----------|--------|"]
    vp = q("SELECT COUNT(*) FROM v_proceedings_full")[0]; vc = q("SELECT COUNT(*) FROM v_case_summary")[0]; vf = q("SELECT COUNT(*) FROM v_first_hearing")[0]
    L.append(f"| `v_proceedings_full` | {vp:,} | proceedings {n_p:,} | {'OK (1:1)' if vp == n_p else 'MISMATCH'} |")
    L.append(f"| `v_case_summary` | {vc:,} | cases {counts['cases']:,} | {'OK (1:1)' if vc == counts['cases'] else 'MISMATCH'} |")
    L.append(f"| `v_first_hearing` | {vf:,} | <= cases {counts['cases']:,} | {'OK' if vf <= counts['cases'] else 'MISMATCH'} |")
    try:
        mn, mx, med, neg = q("SELECT MIN(days_to_first_hearing), MAX(days_to_first_hearing), MEDIAN(days_to_first_hearing), COUNT(*) FILTER (WHERE days_to_first_hearing < 0) FROM v_first_hearing")
        L += ["", f"`v_first_hearing` days to hearing: min {mn}, median {med}, max {mx:,}, negative values {neg}."]
    except Exception:
        pass
    L += ["", "## Normalization checks (July 2026 audit guards)", ""]
    nul = q("SELECT COUNT(*) FROM proceedings WHERE strpos(DEC_CODE, chr(0)) > 0")[0]
    pad = q("SELECT COUNT(*) FROM charges WHERE CHARGE <> TRIM(CHARGE)")[0]
    L += [f"- NUL bytes in `proceedings.DEC_CODE`: {nul}", f"- Padded values in `charges.CHARGE`: {pad}", "",
          "## Known caveats", "",
          "- A handful of upstream dates are in the future or implausibly old; cap dates to 1950-2035 in analysis.",
          "- `cases.C_BIRTHDATE` is 100% NULL in the FOIA release (redacted).",
          "- DuckDB's CSV reader runs with `ignore_errors`, so a very small number of malformed rows may be dropped.", ""]
    Path(a.out).write_text("\n".join(L)); print(f"wrote {a.out}: {len(tables)} tables, {total:,} rows")

if __name__ == "__main__":
    main()

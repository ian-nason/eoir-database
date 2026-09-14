"""Normalize VARCHAR values in the built eoir.duckdb (July 2026 audit).

The raw EOIR CSVs pad codes with trailing spaces and store literal NUL bytes
('\\x00') and blank strings as values. This silently breaks lookup joins
(charge codes: 23% unmatched; Spanish language rows: 18% unmatched) and
inflates category counts. For every user table, any VARCHAR column containing
artifacts is rewritten with NULLIF(TRIM(REPLACE(col, chr(0), '')), '').

Only affected columns are updated (one detection scan + at most one UPDATE
per table), then _columns/DICTIONARY/validation are regenerated.
"""
import time
from pathlib import Path

import duckdb

from build_database import build_columns_table, export_dictionary, run_sanity_checks

NORM = "NULLIF(TRIM(REPLACE({q}, chr(0), '')), '')"

db_path = Path("eoir.duckdb")
con = duckdb.connect(str(db_path))
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{db_path.resolve()}.tmp'")
con.execute("SET memory_limit = '8GB'")

tables = [
    r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND table_name NOT LIKE '\\_%' ESCAPE '\\' "
        "ORDER BY table_name"
    ).fetchall()
]

t0 = time.time()
total_cols_fixed = 0
for tbl in tables:
    vcols = [
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? AND data_type = 'VARCHAR' "
            "ORDER BY ordinal_position",
            [tbl],
        ).fetchall()
    ]
    if not vcols:
        continue

    # One scan: count artifact values per column.
    probes = ", ".join(
        f'COUNT(*) FILTER (WHERE "{c}" IS NOT NULL AND '
        f'("{c}" = \'\' OR "{c}" <> TRIM("{c}") OR strpos("{c}", chr(0)) > 0))'
        for c in vcols
    )
    counts = con.execute(f'SELECT {probes} FROM "{tbl}"').fetchone()
    dirty = [c for c, n in zip(vcols, counts) if n > 0]
    if not dirty:
        continue

    sets = ", ".join(f'"{c}" = ' + NORM.format(q=f'"{c}"') for c in dirty)
    con.execute(f'UPDATE "{tbl}" SET {sets}')
    total_cols_fixed += len(dirty)
    print(f"  {tbl}: normalized {len(dirty)} column(s): "
          f"{', '.join(dirty[:8])}{'...' if len(dirty) > 8 else ''}", flush=True)

print(f"\nNormalized {total_cols_fixed} columns across {len(tables)} tables "
      f"in {(time.time() - t0) / 60:.1f} min")

print("\nVerifying the audit's headline joins now work:")
charge_cov = con.execute("""
    SELECT COUNT(*) FILTER (WHERE l.strCode IS NOT NULL) * 100.0 / COUNT(*)
    FROM charges c LEFT JOIN lu_charges l ON c.CHARGE = l.strCode
    WHERE c.CHARGE IS NOT NULL
""").fetchone()[0]
print(f"  charge lookup coverage: {charge_cov:.2f}% (was 76.97%)")
lang_cov = con.execute("""
    SELECT COUNT(*) FILTER (WHERE l.strCode IS NOT NULL) * 100.0 / COUNT(*)
    FROM cases c LEFT JOIN lu_language l ON c.LANG = l.strCode
    WHERE c.LANG IS NOT NULL
""").fetchone()[0]
print(f"  cases.LANG lookup coverage: {lang_cov:.2f}%")
nul_left = con.execute(
    "SELECT COUNT(*) FROM proceedings WHERE strpos(DEC_CODE, chr(0)) > 0"
).fetchone()[0]
print(f"  NUL bytes left in proceedings.DEC_CODE: {nul_left} (must be 0)")
assert nul_left == 0

print("\nRegenerating _columns, DICTIONARY.md, validation")
build_columns_table(con)
export_dictionary(con, Path("DICTIONARY.md"))
run_sanity_checks(con, db_path)
con.execute("CHECKPOINT")
con.close()
print("NORMALIZE REPAIR DONE")

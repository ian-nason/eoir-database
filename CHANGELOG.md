# Changelog

## 2026-09-14 — Data refresh (DOJ August 2026 file)

- Rebuilt from the EOIR FOIA case-data file dated 2026-09-01 (the "August
  2026" release). Total rows 169,184,982 → 173,278,686 across 97 tables
  (+3 views); `cases` 12,821,375, `proceedings` 16,817,359, `charges`
  19,176,677, `schedule` 48,049,351. Proceedings filed in 2026 to date:
  426,758.
- Value normalization (trim, strip NUL bytes, blank → NULL) is applied at
  load, so charge-code and language lookups join at 100.00% without the
  July in-place repair. The builder now **fails** instead of silently
  loading a table untyped (`--allow-untyped-fallback` to override), always
  starts from an empty file, and exits non-zero if any audit check fails
  (lookup coverage, NUL bytes, padding, core tables non-empty).
- DuckDB memory capped at 6 GB with 4 threads for the 10 GB build VM
  (overridable via DATAPOND_MEMORY_LIMIT / DATAPOND_THREADS);
  `publish_to_hf.py --card-only`.
- Known caveats unchanged: a handful of upstream dates are in the future
  (one proceeding dated 2029); cap dates to 1950-2035 in analysis.

## 2026-07-06 — Full refresh + data-quality audit

Rebuilt from the latest DOJ EOIR FOIA release (proceedings through 2026) and
repaired after an independent SQL-verified audit.

**Data changes**
- 169,184,982 rows across 97 tables (data through 2026; 248k proceedings
  filed in 2026).
- All VARCHAR values normalized: trailing-space padding trimmed, literal NUL
  bytes stripped, blank strings converted to NULL (214 columns affected).

**Fixes**
- Charge-code lookup joins went from 76.97% to **100.00%** coverage — the
  gap was whitespace padding in the source export, not missing lookup codes
  as previously documented.
- Language lookups no longer silently drop 1.7M trailing-space `'SP '`
  (Spanish) rows.
- README quickstart query corrected (filing dates live on `proceedings`,
  not `cases`).

**Known caveats** (see README for the full list)
- `cases.C_BIRTHDATE` is 100% NULL (redacted at source) — no age analyses.
- ~45% of proceedings have no usable decision code once blanks are counted.
- Proceedings (16.5M) vs cases (12.7M): counting proceedings overcounts
  people ~1.3x. Cap dates to 1950-2035 before computing durations.

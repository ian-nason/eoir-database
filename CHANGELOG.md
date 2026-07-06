# Changelog

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

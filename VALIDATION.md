# Data Validation Report

**Build date:** 2026-09-14
**EOIR data version:** August 2026 (file dated 2026-09-01)
**Total tables:** 97 (19 core + 78 lookup) + `_metadata`/`_columns`
**Total rows:** 173,278,686
**Database size:** 4.4 GB

## Row counts (core tables)

| Table | Rows |
|-------|------|
| `schedule` | 48,049,351 |
| `representatives` | 26,890,405 |
| `charges` | 19,176,677 |
| `proceedings` | 16,817,359 |
| `applications` | 16,370,148 |
| `cases` | 12,821,375 |
| `custody_history` | 10,219,825 |
| `motions` | 9,025,261 |
| `juvenile_history` | 3,176,275 |
| `lead_rider` | 2,667,982 |
| `case_identifiers` | 2,633,378 |
| `bonds` | 1,643,732 |
| `appeals` | 1,569,505 |
| `appeal2` | 1,272,470 |
| `attorneys` | 410,696 |
| `appeal_fed_courts` | 183,070 |
| `case_priority_history` | 151,124 |
| `three_mbr_referrals` | 84,810 |
| `pro_bono` | 67,909 |

Zero-row tables: none.

## Date parsing rates (proceedings)

| Column | Non-null | Null % |
|--------|----------|--------|
| `OSC_DATE` | 16,723,277 | 0.6% |
| `HEARING_DATE` | 16,271,819 | 3.2% |
| `COMP_DATE` | 13,723,647 | 18.4% |

## Referential integrity

| Check | Orphans | % of child |
|-------|---------|------------|
| proceedings -> cases | 247 | 0.001% |
| charges -> proceedings | 12,147 | 0.063% |
| representatives -> cases | 3,717 | 0.014% |
| applications -> cases | 331 | 0.002% |
| bonds -> cases | 96 | 0.006% |

## Lookup coverage

| Join | Matched | Total (non-null) | Coverage |
|------|---------|------------------|----------|
| proceedings.NAT -> lu_nationality | 16,761,318 | 16,761,594 | 100.0% |
| proceedings.BASE_CITY_CODE -> lu_base_city | 16,817,314 | 16,817,332 | 100.0% |
| proceedings.IJ_CODE -> lu_judge | 16,644,026 | 16,674,905 | 99.8% |
| charges.CHARGE -> lu_charges | 19,176,101 | 19,176,675 | 100.0% |
| cases.LANG -> lu_language | 12,671,961 | 12,672,558 | 100.0% |

## Views

| View | Rows | Reference | Status |
|------|------|-----------|--------|
| `v_proceedings_full` | 16,817,359 | proceedings 16,817,359 | OK (1:1) |
| `v_case_summary` | 12,821,375 | cases 12,821,375 | OK (1:1) |
| `v_first_hearing` | 12,508,200 | <= cases 12,821,375 | OK |

## Normalization checks (July 2026 audit guards)

- NUL bytes in `proceedings.DEC_CODE`: 0
- Padded values in `charges.CHARGE`: 0

## Known caveats

- A handful of upstream dates are in the future or implausibly old; cap dates to 1950-2035 in analysis.
- `cases.C_BIRTHDATE` is 100% NULL in the FOIA release (redacted).
- DuckDB's CSV reader runs with `ignore_errors`, so a very small number of malformed rows may be dropped.

# Data Validation Report

**Build date:** 2026-03-05
**EOIR data version:** February 2026 (2026-0201)
**Total tables:** 98 (19 core + 78 lookup + 1 metadata)
**Total rows:** 164,633,807
**Database size:** 6.6 GB

## Row count validation

Core tables compared to approximate expected sizes from the EOIR data dump:

| Table | Actual rows | Expected (approx) | Difference | Status |
|-------|------------|-------------------|------------|--------|
| `schedule` | 45,183,976 | ~45M | ~0% | OK |
| `representatives` | 25,639,802 | ~25M | ~3% | OK |
| `charges` | 18,484,804 | ~18M | ~3% | OK |
| `proceedings` | 16,216,773 | ~16M | ~1% | OK |
| `applications` | 15,756,896 | ~15M | ~5% | OK |
| `cases` | 12,461,924 | ~8M* | ~56% | See note |
| `custody_history` | 9,777,557 | — | — | OK |
| `motions` | 8,030,259 | — | — | OK |
| `juvenile_history` | 2,953,170 | — | — | OK |
| `lead_rider` | 2,582,001 | — | — | OK |
| `case_identifiers` | 2,392,405 | — | — | OK |
| `bonds` | 1,586,265 | ~1.5M | ~6% | OK |
| `appeals` | 1,457,288 | — | — | OK |
| `appeal2` | 1,195,783 | — | — | OK |
| `attorneys` | 403,612 | — | — | OK |
| `appeal_fed_courts` | 179,707 | — | — | OK |
| `case_priority_history` | 138,004 | — | — | OK |
| `three_mbr_referrals` | 83,464 | — | — | OK |
| `pro_bono` | 65,944 | — | — | OK |

*`cases` expected count of ~8M was from older documentation. The Feb 2026 dump contains 12.5M cases, reflecting continued growth in the immigration court system. No zero-row tables.

## Type casting validation

All type casts applied correctly:

| Column type | Example columns | DuckDB type | Status |
|------------|----------------|-------------|--------|
| Dates | `OSC_DATE`, `HEARING_DATE`, `COMP_DATE`, `DATE_DETAINED`, `C_BIRTHDATE`, `DATE_OF_ENTRY` | `DATE` | OK |
| IDs | `IDNCASE`, `IDNPROCEEDING` | `INTEGER` | OK |
| Booleans | `LPR` (cases), `AGGRAVATE_FELON` (proceedings) | `BOOLEAN` | OK |

### Date parsing rates (proceedings table)

| Column | Parsed (non-null) | Null % | Notes |
|--------|-------------------|--------|-------|
| `OSC_DATE` | 16,132,288 | 0.5% | Expected — some old records lack dates |
| `HEARING_DATE` | 15,690,314 | 3.3% | Expected — pending/incomplete cases |
| `COMP_DATE` | 12,867,168 | 20.7% | Expected — many cases not yet completed |

## Referential integrity

| Check | Orphan count | % of child table | Notes |
|-------|-------------|-----------------|-------|
| proceedings -> cases | 271 | 0.002% | Negligible |
| charges -> proceedings | 12,213 | 0.07% | Minor — likely from partial data loads |
| representatives -> cases | 3,687 | 0.01% | Negligible |
| applications -> cases | 338 | 0.002% | Negligible |
| bonds -> cases | 107 | 0.007% | Negligible |

All orphan rates are well under 0.1%. These are consistent with known EOIR data quality issues (some records reference cases that were purged from later data dumps).

## Lookup coverage

| Join | Matched | Total (non-null) | Coverage |
|------|---------|-----------------|----------|
| proceedings.NAT -> lu_nationality | 16,161,585 | 16,216,594 | 99.7% |
| proceedings.BASE_CITY_CODE -> lu_base_city | 16,216,728 | 16,216,757 | 100.0% |
| proceedings.IJ_CODE -> lu_judge | 16,043,127 | 16,077,039 | 99.8% |
| charges.CHARGE -> lu_charges | 18,484,229 | 18,484,804 | 100.0% |

All lookups have >99.7% coverage.

## View validation

| View | Row count | Expected | Status |
|------|----------|----------|--------|
| `v_proceedings_full` | 16,216,773 | = proceedings (16,216,773) | OK (1:1 ratio) |
| `v_case_summary` | 12,461,924 | = cases (12,461,924) | OK (1:1 ratio) |
| `v_first_hearing` | 12,159,330 | <= distinct cases (12,328,082) | OK |

### v_first_hearing timeline sanity

| Metric | Value |
|--------|-------|
| Min days to hearing | 0 |
| Max days to hearing | 65,861 |
| Median days to hearing | 106 |
| Negative values | 0 |

The max of 65,861 days (~180 years) is likely a data entry error in the source data but is preserved as-is. The view filters `HEARING_DATE >= OSC_DATE`, so negative values are excluded by design.

## Known caveats

- **`ignore_errors` mode:** DuckDB's CSV reader silently drops malformed rows. The actual row counts may be slightly lower than the raw file line counts. This affects <0.01% of rows based on spot checks.
- **Null bytes:** The source CSVs contain null bytes (`\x00`) which are handled by the reader but may appear as empty strings in some VARCHAR fields.
- **`lu_court_decision` duplicates:** The raw lookup has duplicate `(strDecCode, strCaseType)` keys (active + inactive entries). The `v_proceedings_full` view deduplicates by preferring the active entry.

---
license: other
license_name: public-domain
license_link: https://www.justice.gov/eoir/foia-library
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - immigration
  - eoir
  - immigration-court
  - foia
  - duckdb
  - legal
  - policy
  - government-data
pretty_name: EOIR Immigration Court Database
size_categories:
  - 100M<n<1B
---

# EOIR Immigration Court Database

Explore U.S. immigration court cases, proceedings, hearings, charges, applications for relief, representation and appeals from the Executive Office for Immigration Review (EOIR) public Freedom of Information Act (FOIA) records. The hosted DuckDB database supports analysis of court workloads, case timelines and recorded outcomes without first rebuilding the source files.

The hosted file contains **174,560,256 rows across 98 tables**. Tables describe different units: a case can have several proceedings, hearings and applications. Choose that unit before counting records. The documented plausible proceeding filing-date range is 1990–2026, with incomplete early coverage; it is not a claim of a complete historical census.

Start with the Python or R examples below. For outcome analysis, distinguish completed from pending proceedings and inspect missing decision codes. Birth dates are redacted, and some dates require validity checks. The table guide explains keys and prepared views; source-cleaning and build instructions follow the analyst guide.

<a id="quick-start"></a>

## Start querying

Preview removal proceeding records. A case can have several proceedings; this is not a count of people.

`IDNPROCEEDING` identifies a proceeding, `OSC_DATE` is the documented filing-date field, and `RMV` is the code for removal proceedings.

Choose R or Python. Install datapond once: use `install.packages("datapond")` in R, or run `python -m pip install datapond` in a terminal.

### R

```r
library(datapond)
con <- dp_connect("eoir")
tryCatch({
  DBI::dbExecute(con, "SET threads=1")
  DBI::dbExecute(con, "SET memory_limit='128MiB'")
  DBI::dbExecute(con, "SET max_temp_directory_size='0B'")
  print(DBI::dbGetQuery(con, "SELECT IDNPROCEEDING, OSC_DATE, CASE_TYPE FROM proceedings WHERE CASE_TYPE='RMV' LIMIT 5"))
}, finally = dp_disconnect(con))
```

### Python

```python
import datapond
con = datapond.connect('eoir')
try:
    con.execute("SET threads=1")
    con.execute("SET memory_limit='128MiB'")
    con.execute("SET max_temp_directory_size='0B'")
    print(con.execute("""SELECT IDNPROCEEDING, OSC_DATE, CASE_TYPE FROM proceedings WHERE CASE_TYPE='RMV' LIMIT 5""").fetchall())
finally:
    con.close()
```

Queries run on your computer and read the byte ranges they need. A small result can still require a large scan. These examples limit DuckDB to one thread and a 128 MiB buffer pool with disk spill disabled; that is not a limit on the entire R or Python process. Larger analyses may need more memory or a local download with enough disk space.

<a id="additional-dataset-notes"></a>

<a id="tables"></a>

## Tables and coverage

| Table | Description | Rows |
|-------|-------------|------|
| `proceedings` | Proceedings: hearings, decisions, judges, charges | 16,899,722 |
| `cases` | Case-level demographics, custody, dates, attorney info | 12,869,593 |
| `schedule` | Hearing schedule entries with calendar type and adjournments | 48,492,125 |
| `representatives` | Attorney representation records | 27,105,444 |
| `charges` | Individual charges per proceeding | 19,270,794 |
| `applications` | Applications for relief (asylum, cancellation, etc.) | 16,460,394 |
| `custody_history` | Custody status change records | 10,279,713 |
| `motions` | Motions filed in proceedings | 9,178,220 |
| `juvenile_history` | Juvenile designation records | 3,183,371 |
| `lead_rider` | Lead/rider case relationships | 2,678,697 |
| `case_identifiers` | Case ID cross-references | 2,669,114 |
| `bonds` | Bond hearing records and amounts | 1,654,849 |
| `appeals` | Appeals to Board of Immigration Appeals | 1,585,540 |
| `appeal2` | Meaning not documented here; source file: tblAppeal2.csv | 1,283,643 |
| `attorneys` | Attorney registry | 411,877 |
| `appeal_fed_courts` | Meaning not documented here; source file: tblAppealFedCourts.csv | 183,628 |
| `case_priority_history` | Priority code changes | 151,250 |
| `three_mbr_referrals` | Meaning not documented here; source file: tblThreeMbrReferrals.csv | 85,059 |
| `pro_bono` | Pro bono screening records | 68,485 |
| `lu_judge_base_city` | Meaning not documented here; source file: tblLookupJudgeBaseCity.csv | 12,777 |
| `lu_insta` | Meaning not documented here; source file: tblLookupINSTA.csv | 9,671 |
| `lu_inmate` | Meaning not documented here; source file: tblLookupInmate.csv | 4,850 |
| `lu_inmate_housing` | Meaning not documented here; source file: tblInmateHousing.csv | 4,752 |
| `lu_responsible_party` | Meaning not documented here; source file: tblResponsibleParty.csv | 3,381 |
| `lu_judge` | Immigration judge codes | 1,840 |
| `lu_insaddress` | Meaning not documented here; source file: tblLookUpINSAddress.csv | 1,469 |
| `lu_hearing_location` | Hearing location codes | 1,381 |
| `lu_insloc` | Meaning not documented here; source file: tblLookupINSLOC.csv | 1,020 |
| `lu_bia` | Meaning not documented here; source file: tblLookupBIA.csv | 895 |
| `lu_language` | Language codes | 743 |
| `lu_holidays` | Meaning not documented here; source file: tblLookupHolidays.csv | 613 |
| `lu_notice` | Meaning not documented here; source file: tblLookupNOTICE.csv | 598 |
| `lu_district_location` | Meaning not documented here; source file: tblLookupDistrictLocation.csv | 289 |
| `lu_office_location` | Meaning not documented here; source file: tblLookupOfficeLocation.csv | 265 |
| `lu_alien_nat` | Meaning not documented here; source file: tblLookupAlienNat.csv | 256 |
| `lu_country` | Meaning not documented here; source file: tblLookupCountry.csv | 254 |
| `lu_charges` | Charge codes and descriptions | 252 |
| `lu_nationality` | Nationality/country codes | 251 |
| `lu_appeal_issues` | Meaning not documented here; source file: tblLookupAppealIssues.csv | 248 |
| `lu_biacluster` | Meaning not documented here; source file: tblLookup_BIACluster.csv | 244 |
| `lu_court_decision` | Court decision codes (by case type) | 217 |
| `lu_bar_code_type` | Meaning not documented here; source file: tblBarCodeType.csv | 141 |
| `lu_adjournment` | Adjournment reason codes | 124 |
| `lu_case_identifier` | Meaning not documented here; source file: tblLookUpCaseIdentifier.csv | 122 |
| `lu_base_city` | Immigration court locations | 97 |
| `lu_in_tr_pr` | Meaning not documented here; source file: tblLookupInTrPr.csv | 94 |
| `lu_insoffice` | Meaning not documented here; source file: tblINSoffice.csv | 74 |
| `lu_motion_type` | Motion type codes | 72 |
| `lu_state` | U.S. state codes | 59 |
| `lu_bia_decision` | BIA decision codes | 55 |
| `lu_special_issue` | Meaning not documented here; source file: tblLookupSpecialIssue.csv | 43 |
| `lu_application` | Application type codes | 40 |
| `lu_teams` | Meaning not documented here; source file: tblTeams.csv | 37 |
| `lu_hold_reason` | Meaning not documented here; source file: tblLookupHoldReason.csv | 37 |
| `lu_schedule_type` | Schedule type codes | 36 |
| `lu_callup_reasons` | Meaning not documented here; source file: tblLookupCallup_Reasons.csv | 30 |
| `lu_cal_type` | Calendar type codes | 29 |
| `lu_dddecision_types` | Meaning not documented here; source file: tblLookUpDDDecisionTypes.csv | 23 |
| `lu_panels` | Meaning not documented here; source file: tblPanels.csv | 20 |
| `lu_appeal_type` | Meaning not documented here; source file: tbllookupAppealType.csv | 19 |
| `lu_board_motions` | Meaning not documented here; source file: tblLookupBoardMotions.csv | 18 |
| `lu_priority` | Meaning not documented here; source file: tblLookupPriority.csv | 16 |
| `lu_br_deduct_codes` | Meaning not documented here; source file: tbl_br_DeductCodes.csv | 15 |
| `lu_app_decision` | Application decision codes | 14 |
| `lu_intr_pr_lang` | Meaning not documented here; source file: tblLookupIntrPrLang.csv | 14 |
| `lu_transcribers` | Meaning not documented here; source file: tblLookupTranscribers.csv | 14 |
| `lu_case_type` | Case type codes | 13 |
| `lu_dec_code` | Meaning not documented here; source file: tblDecCode.csv | 12 |
| `lu_filing_method_party` | Meaning not documented here; source file: tblLookupFiling_Method_Party.csv | 10 |
| `lu_filed_by` | Meaning not documented here; source file: tblLookupFiledBy.csv | 9 |
| `lu_comment_type_board` | Meaning not documented here; source file: tblLookupCommentTypeBoard.csv | 9 |
| `lu_contact_change` | Meaning not documented here; source file: tblLookupContactChange.csv | 9 |
| `lu_torture_conv_id` | Meaning not documented here; source file: tblTortureConvID.csv | 8 |
| `lu_ins_clock_entry_status` | Meaning not documented here; source file: tblLookup_INS_Clock_Entry_Status.csv | 8 |
| `lu_3_mbr_referred_reason` | Meaning not documented here; source file: tblLookup3MbrReferredReason.csv | 7 |
| `lu_category` | Meaning not documented here; source file: tbllookup_Category.csv | 7 |
| `lu_vote` | Meaning not documented here; source file: tblLookupVote.csv | 7 |
| `lu_juvenile` | Meaning not documented here; source file: tblLookup_Juvenile.csv | 6 |
| `lu_service_method` | Meaning not documented here; source file: tblLookupService_Method.csv | 6 |
| `lu_case_priority` | Meaning not documented here; source file: tblLookup_CasePriority.csv | 6 |
| `lu_fee_status` | Meaning not documented here; source file: tblLookupFeeStatus.csv | 5 |
| `lu_consulted_with` | Meaning not documented here; source file: tblLookupConsultedWith.csv | 5 |
| `lu_biadecision_type` | Meaning not documented here; source file: tblLookupBIADecisionType.csv | 5 |
| `lu_in_tr_pr_agenda` | Meaning not documented here; source file: tblLookupInTrPrAgenda.csv | 5 |
| `lu_fed_court_decision` | Meaning not documented here; source file: tblLookupFedCourtDecision.csv | 4 |
| `lu_oaresponse` | Meaning not documented here; source file: tblLookupOAResponse.csv | 4 |
| `lu_components` | Meaning not documented here; source file: tblLookupComponents.csv | 4 |
| `lu_filing_method` | Meaning not documented here; source file: tblLookupFiling_Method.csv | 4 |
| `lu_final_disposition` | Meaning not documented here; source file: tblLookupFinalDisposition.csv | 4 |
| `lu_claimed_issue` | Meaning not documented here; source file: tblLookupClaimedIssue.csv | 4 |
| `lu_custody_status` | Custody status codes | 3 |
| `lu_fee_type` | Meaning not documented here; source file: tblLookupFeeType.csv | 3 |
| `lu_allegation_stmt` | Meaning not documented here; source file: tblLookUpAllegationStmt.csv | 3 |
| `lu_region` | Meaning not documented here; source file: tblLookupRegion.csv | 2 |
| `lu_sex` | Meaning not documented here; source file: tblLookupSex.csv | 2 |
| `lu_in_tr_pr_lang_group` | Meaning not documented here; source file: tblLookupInTrPrLangGroup.csv | 2 |
| `lu_fed_court_remand_to` | Meaning not documented here; source file: tblLookupFedCourtRemandTo.csv | 2 |

<a id="coverage-fields-and-sources"></a>

See the [data dictionary](https://github.com/ian-nason/eoir-database/blob/main/DICTIONARY.md) for column definitions.

## Keys and joins

The most important columns for joining tables:

- **`IDNCASE`** — Primary case identifier. Links `cases`, `proceedings`, `representatives`, `schedule`, `bonds`, etc.
- **`IDNPROCEEDING`** — Proceeding identifier within a case. Links `proceedings` to `charges`, `applications`, `schedule`.
- **`BASE_CITY_CODE`** — Immigration court code. Join to `lu_base_city`.
- **`IJ_CODE`** / **`JUDGE_CODE`** — Judge code. Join to `lu_judge`.
- **`NAT`** / **`NAT_CODE`** — Nationality code. Join to `lu_nationality`.
- **`DEC_CODE`** x **`CASE_TYPE`** — Decision code (compound key). Join to `lu_court_decision`.
- **`CHARGE`** — Charge code. Join to `lu_charges` on `strCode`.
- **`LANG`** — Language code. Join to `lu_language` on `strCode`.

## Prepared views

| View | Description |
|------|-------------|
| `v_proceedings_full` | Proceedings with decoded court, judge, nationality, and decision |
| `v_first_hearing` | One row per case with first hearing date and timeline metrics |
| `v_case_summary` | One row per case with demographics, representation, and outcome |

## Interpret records and dates

- `cases.C_BIRTHDATE` is 100% NULL (redacted), so this data cannot support age analyses.
- About 45% of proceedings have no usable decision code once blanks are counted.
- Historical audit counts of 16.5M proceedings and 12.7M cases imply about 1.3 proceedings per case, not a verified unique-person ratio.
- Some upstream dates are invalid or in the future, including one proceeding dated 2029. Cap dates to 1950–2035 before computing durations; scheduled future hearings can be legitimate.
- Normalized charge and language codes join without the earlier whitespace repair. Use the prepared proceeding view to avoid duplicate decision-lookup keys.

<a id="duckdb-cli"></a>

## More example queries

Count recorded removal proceedings by court. This counts proceeding records, not people.

```sql
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb' AS eoir (READ_ONLY);

-- Query immediately
SELECT court_name, COUNT(*) as proceeding_records
FROM eoir.v_proceedings_full
WHERE CASE_TYPE = 'RMV'
GROUP BY court_name
ORDER BY proceeding_records DESC
LIMIT 10;
```

<a id="advanced-access-quick-start"></a>

## Other ways to connect

### Python metadata inspection

This alternative inspects table metadata rather than answering an analyst question.

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("""
    ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb'
    AS eoir (READ_ONLY)
""")
con.sql("SELECT * FROM eoir._metadata").show()
```

DuckDB uses HTTP range requests, so only the pages needed for your query are downloaded.

## Sources and maintenance

### Source and license reference

See the [data dictionary](https://github.com/ian-nason/eoir-database/blob/main/DICTIONARY.md) for column definitions and the [DOJ Executive Office for Immigration Review](https://www.justice.gov/eoir/foia-library-0) for the source. License: Public domain.

### Data Source

[EOIR FOIA Library](https://www.justice.gov/eoir/foia-library-0) -- updated monthly by the Executive Office for Immigration Review (U.S. Department of Justice). This is public domain U.S. government data.

### Build source

Built with [eoir-database](https://github.com/ian-nason/eoir-database). The database is assembled from the [EOIR FOIA data dump](https://www.justice.gov/eoir/foia-library-0).

### License

Database build code: MIT. Underlying data: public domain (U.S. government work).

### GitHub

Full source code, build instructions, and example analyses: [github.com/ian-nason/eoir-database](https://github.com/ian-nason/eoir-database)


### Changelog

#### 2026-09-14 — Data refresh (DOJ August 2026 file)

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

#### 2026-07-06 — Full refresh + data-quality audit

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
- Proceedings (16.5M) vs cases (12.7M): about 1.3 proceedings per case,
  not a verified unique-person ratio. Cap dates to 1950-2035 before computing durations.


### R with DBI and duckdb

Install the dependencies once with `install.packages(c("DBI", "duckdb"))`.

```r
library(DBI)
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "INSTALL httpfs")
dbExecute(con, "LOAD httpfs")
dbExecute(con, "ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb' AS eoir (READ_ONLY)")
dbGetQuery(con, "SELECT court_name, CASE_TYPE FROM eoir.v_proceedings_full LIMIT 5")
dbDisconnect(con, shutdown = TRUE)
```

DuckDB uses HTTP range requests; queries run on your machine and may need substantial memory or download data. A `LIMIT` does not guarantee a small scan.


### November 16, 2026 — September 2026 source refresh

This release contains **174,560,256 stored rows across 98 source tables**, plus three views and internal metadata tables. It uses the September 2026 EOIR archive dated October 1. Relative to the prior release, all 97 existing source tables remain and the publisher’s 1,051-row `eoirdb_schema` reference is also retained. The database is 4,725,420,032 bytes, SHA-256 `b86bfdbca2867d5859ba625f76bb51e90218b47a78676712168c4c0fea856634`.

The independent streaming audit reconciled all 98 CSV members. Ninety-seven tables exactly match their nonempty raw record counts. The case source contains **159 records with invalid UTF-8**, which the established importer skips; 12,869,593 case rows are stored from 12,869,752 raw records. The source also has malformed tab-separated records: permissive parsing, padding and typed coercion follow the existing pipeline and do not establish raw-cell equality. No untyped fallback was used. All builder checks passed. These limits matter when interpreting small count differences; the original ZIP is archived with this release so the excluded source records remain available for investigation.

Source hashes, table counts, the source-record reconciliation and baseline comparison are in `releases/2026-11-16` on GitHub and accompanying JSON files on Hugging Face. The complete original ZIP is [source-archive-2026-10-01.zip](https://huggingface.co/datasets/Nason/eoir-database/resolve/main/source-archive-2026-10-01.zip), SHA-256 `a1d0394d31a6b810b81ae91a676adef5d6839ead7ca33c8e3ee38653687a7d2f`. Use the immutable release commit when citing or downloading it.

To replay with Python 3.11+ and the pinned project dependencies in a fresh checkout:

```sh
python refresh_20261116.py --archive source-archive-2026-10-01.zip
```

The release script verifies the entire ZIP before parsing, extracts one CSV at a time, preserves the ZIP and refuses to overwrite an existing partial build. It requires 25 GiB free at startup and keeps a 5 GiB extraction reserve, using one DuckDB thread, a 1 GiB buffer pool and at most 1 GiB spill. Engine limits do not cap total process memory. The general `build_database.py` workflow remains available for future source editions.

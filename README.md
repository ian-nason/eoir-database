<a id="eoir-database"></a>

# EOIR Immigration Court Database

Explore U.S. immigration court cases, proceedings, hearings, charges, applications for relief, representation and appeals from the Executive Office for Immigration Review (EOIR) public Freedom of Information Act (FOIA) records. The hosted DuckDB database supports analysis of court workloads, case timelines and recorded outcomes without first rebuilding the source files.

Tables describe different units: a case can have several proceedings, hearings and applications. Choose that unit before counting records. The documented plausible proceeding filing-date range is 1990–2026, with incomplete early coverage; it is not a claim of a complete historical census.

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

The row counts below describe the November 16 build from EOIR’s September 2026 release. The 98 stored source tables include the publisher’s 1,051-row `eoirdb_schema` reference table; totals mix units and do not count unique cases or people.

- **Coverage.** Proceedings carry `OSC_DATE`s from 1990 (early years are incomplete)
  and a handful of sentinel and future dates; the registry's `1990-2026` is the range of
  plausible `OSC_DATE` values, not a guarantee that early years are complete.

### Core tables

| Table | Source file | Description | ~Rows |
|-------|-----------|-------------|------:|
| `proceedings` | `B_TblProceeding.csv` | Proceedings: hearings, decisions, judges | 16,899,722 |
| `cases` | `A_TblCase.csv` | Case-level demographics, custody, dates, attorney | 12,869,593 |
| `charges` | `B_TblProceedCharges.csv` | Individual charges per proceeding | 19,270,794 |
| `schedule` | `tbl_schedule.csv` | Hearing schedule, calendar type, adjournments | 48,492,125 |
| `applications` | `tbl_Court_Appln.csv` | Applications for relief (asylum, etc.) | 16,460,394 |
| `bonds` | `D_TblAssociatedBond.csv` | Bond hearing records and amounts | 1,654,849 |
| `representatives` | `tbl_RepsAssigned.csv` | Attorney representation records | 27,105,444 |
| `motions` | `tbl_Court_Motions.csv` | Motions filed in proceedings | 9,178,220 |
| `appeals` | `tblAppeal.csv` | Appeals to Board of Immigration Appeals | 1,585,540 |
| `custody_history` | `tbl_CustodyHistory.csv` | Custody status changes | 10,279,713 |
| `case_identifiers` | `A_TblCaseIdentifier.csv` | Case ID cross-references | 2,669,114 |
| `juvenile_history` | `tbl_JuvenileHistory.csv` | Juvenile designation records | 3,183,371 |
| `lead_rider` | `tbl_Lead_Rider.csv` | Lead/rider case relationships | 2,678,697 |
| `case_priority_history` | `tbl_CasePriorityHistory.csv` | Priority code changes | 151,250 |
| `attorneys` | `tbl_EOIR_Attorney.csv` | Attorney registry | 411,877 |
| `pro_bono` | `tblProBono.csv` | Pro bono screening records | 68,485 |

### Lookup tables

All files in the `Lookup/` directory are loaded with the `lu_` prefix (78 tables total). Key ones:

| Table | Description | Key columns |
|-------|-------------|-------------|
| `lu_nationality` | Country/nationality codes | 251 |
| `lu_language` | Language codes | 743 |
| `lu_base_city` | Immigration court locations | 97 |
| `lu_hearing_location` | Hearing locations & detention type | 1,381 |
| `lu_judge` | Immigration judge codes | 1,840 |
| `lu_charges` | Charge codes | 252 |
| `lu_court_decision` | Court decision codes (by case type) | 217 |
| `lu_app_decision` | Application decision codes | 14 |
| `lu_cal_type` | Calendar type codes | 29 |
| `lu_adjournment` | Adjournment reason codes | 124 |
| `lu_application` | Application type codes | 40 |
| `lu_custody_status` | Custody status codes | 3 |
| `lu_case_type` | Case type codes | 13 |
| `lu_motion_type` | Motion type codes | 72 |
| `lu_bia_decision` | BIA decision codes | 55 |
| `lu_schedule_type` | Schedule type codes | 36 |
| `lu_state` | U.S. state codes | 59 |

Any additional lookup files found are also loaded automatically.

### Pre-built views

| View | Description |
|------|-------------|
| `v_proceedings_full` | Proceedings with decoded court, judge, nationality, and decision |
| `v_first_hearing` | One row per case with first hearing date and timeline metrics |
| `v_case_summary` | One row per case with demographics, representation, and outcome |

### Metadata

The `_metadata` table has one row per table with source file, description, row count, column count, and build timestamp.

<a id="coverage-fields-and-sources"></a>

See the [data dictionary](https://github.com/ian-nason/eoir-database/blob/main/DICTIONARY.md) for column definitions.

## Key join columns

The most important columns for joining tables:

- **`IDNCASE`** — Primary case identifier. Links `cases`, `proceedings`, `representatives`, `schedule`, `bonds`, etc.
- **`IDNPROCEEDING`** — Proceeding identifier within a case. Links `proceedings` to `charges`, `applications`, `schedule`.
- **`BASE_CITY_CODE`** — Immigration court code. Join to `lu_base_city`.
- **`IJ_CODE`** / **`JUDGE_CODE`** — Judge code. Join to `lu_judge`.
- **`NAT`** / **`NAT_CODE`** — Nationality code. Join to `lu_nationality`.
- **`DEC_CODE`** x **`CASE_TYPE`** — Decision code (compound key). Join to `lu_court_decision`.
- **`CHARGE`** — Charge code. Join to `lu_charges` on `strCode`.
- **`LANG`** — Language code. Join to `lu_language` on `strCode`.

## Researcher caveats

Values that cannot be converted to their documented type become NULL; type conversion itself does not remove their rows. Source parsing has separate limitations below.

- **`cases.C_BIRTHDATE` is 100% NULL** (redacted at source). No age-based
  analysis is possible from this database.
- **Code values are normalized, so lookup joins do not need `TRIM()`.** About
  45% of proceedings have no usable decision code once blanks are counted,
  rather than the roughly 32% raw NULL rate.
- **Proceedings vs cases granularity:** 16.5M proceedings belong to 12.7M
  cases, or about 1.3 proceedings per case. Neither count establishes unique
  people. About 20% of
  proceedings have no completion date (pending) — condition outcome rates on
  completion.
- **Dates:** scheduled-future hearing dates are legitimate; a small number of
  impossible dates exist (a hearing in 2925, sentinels like 1900-01-01). Cap
  to 1950-2035 before computing durations. Trend analyses are trustworthy
  from roughly 1992 onward.
- **`lu_court_decision` has duplicate (code, case_type) keys** — use the
  shipped `v_proceedings_full` view, which dedupes, rather than a naive join.

## Additional interpretation limits

- **Data quality:** The raw CSVs contain null bytes, inconsistent quoting, and malformed rows. The `ignore_errors` and `null_padding` options in DuckDB's CSV reader handle most of these, but a small number of rows may be silently dropped.
- **Column names:** Column names are preserved as-is from the source files (mixed case, inconsistent naming conventions).

## More example queries

```sql
-- How many proceedings per year?
SELECT EXTRACT(YEAR FROM OSC_DATE) AS year, COUNT(*) AS n
FROM proceedings
WHERE OSC_DATE IS NOT NULL
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;

-- Top nationalities in proceedings filed since 2020
-- (filing dates live on proceedings, not cases)
SELECT n.NAT_COUNTRY_NAME, COUNT(*) AS proceedings
FROM proceedings p
JOIN cases c ON p.IDNCASE = c.IDNCASE
JOIN lu_nationality n ON c.NAT = n.NAT_CODE
WHERE p.OSC_DATE >= '2020-01-01'
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

### Example queries

See [`examples/query_examples.sql`](examples/query_examples.sql) for 15 ready-to-run queries covering:

- Proceedings volume by fiscal year
- Top courts by caseload
- Nationalities over time
- Asylum grant rates
- Wait times (NTA to first hearing)
- Bond amounts
- Attorney representation rates
- Common charges
- Adjournment reasons
- Judge caseloads
- Detained vs. non-detained timelines
- Appeals volume
- Languages spoken

### Example analyses

[`examples/court_analysis.py`](examples/court_analysis.py) is a parameterized analysis script covering timelines, demographics, representation, outcomes, applications, and bond. It works for any court, custody status, or charge section.

```bash
# Detained S.237 at Varick Street (default)
uv run python examples/court_analysis.py --db eoir.duckdb

# All detained cases in Miami
uv run python examples/court_analysis.py --db eoir.duckdb --court MIA --charge % --pdf

# Non-detained asylum cases in San Francisco
uv run python examples/court_analysis.py --db eoir.duckdb --court SFR --custody N --charge 208 --pdf

# Compare all New York courts
uv run python examples/court_analysis.py --db eoir.duckdb --court NYC,NYV,NYB,NYD --pdf

# Custom lookback window
uv run python examples/court_analysis.py --db eoir.duckdb --court NYV --months 24 --trend-start 2018
```

[`examples/nyv_detained_237.py`](examples/nyv_detained_237.py) is a convenience wrapper with NYV defaults.

<a id="advanced-access-remote-access-no-download-required"></a>

## Other ways to connect

The pre-built database is hosted on Hugging Face. Query it directly without downloading:

### From DuckDB CLI

```sql
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb' AS eoir (READ_ONLY);
SELECT * FROM eoir._metadata;
```

### From Python

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb' AS eoir (READ_ONLY)")
con.sql("SELECT * FROM eoir._metadata").show()
```

### From Google Colab

```python
!pip install duckdb -q  # or: uv pip install duckdb
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb' AS eoir (READ_ONLY)")
con.sql("SELECT * FROM eoir.v_proceedings_full LIMIT 10").show()
```

DuckDB uses HTTP range requests, so only the pages needed for your query are downloaded -- you're not pulling the entire database.

<a id="advanced-access-colab-quickstart"></a>

### Colab notebook option

Want to query the data without installing anything locally? Open the [Colab notebook](examples/colab_quickstart.ipynb):

1. Upload `eoir.duckdb` to your Google Drive (or build it in Colab from the zip)
2. Open the notebook in Colab
3. Start querying — the notebook includes guided examples covering case volumes, wait times, representation rates, and outcomes

## Sources and maintenance

### Source and license reference

See the [data dictionary](https://github.com/ian-nason/eoir-database/blob/main/DICTIONARY.md) for column definitions and the [DOJ Executive Office for Immigration Review](https://www.justice.gov/eoir/foia-library-0) for the source. License: Public domain.

### Data source

EOIR publishes a monthly data dump at their [FOIA Library](https://www.justice.gov/eoir/foia-library-0):

- **Download URL:** `https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip`
- ~4 GB zip, extracts to ~15-20 GB of tab-delimited CSV files
- Updated monthly
- Contains two directories: `EOIR Case Data/` (core tables) and `Lookup/` (reference tables)

### Build overview

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The raw tab-delimited exports require normalization and type handling. One command downloads, cleans, type-casts and builds an indexed [DuckDB](https://duckdb.org/) database with lookup tables and pre-built views. Modeled on [paulgp/ipeds-database](https://github.com/paulgp/ipeds-database).

<a id="advanced-access-quick-start"></a>

### Build a local database

```bash
# Install dependencies (using uv)
uv sync

# Build the database (downloads ~4 GB, takes ~5 min)
uv run python build_database.py

# Query it
duckdb eoir.duckdb
```

<a id="usage"></a>

### Build options

```bash
# Default: download from EOIR, extract, build everything
uv run python build_database.py

# Use a local zip file (skip download)
uv run python build_database.py --zip /path/to/EOIR_Case_Data.zip

# Use already-extracted directory (skip download + extraction)
uv run python build_database.py --data-dir /path/to/extracted/

# Build only specific tables (faster iteration during development)
uv run python build_database.py --tables cases proceedings charges

# Custom output path
uv run python build_database.py --output my_eoir.duckdb
```

### Type casting

All columns are read as VARCHAR first, then cast using DuckDB's `TRY_CAST`:

- **Date columns** (`*_DATE`, `DAT*`, etc.) -> `DATE`
- **ID columns** (`IDN*`) -> `INTEGER`
- **Money columns** (`INITIAL_BOND`, `NEW_BOND`) -> `DOUBLE`
- **Boolean columns** (`bln*`, `LPR`, `AGGRAVATE_FELON`) -> `BOOLEAN`
- **Everything else** -> `VARCHAR`

`TRY_CAST` returns `NULL` for unparseable values rather than failing, so no rows are lost.

### Normalization history

As of the July 2026 build, trailing-space padding and literal NUL bytes are stripped from FOIA values, and blank strings become NULL. Earlier builds required `TRIM()` around lookup joins.

<a id="known-issues"></a>

### Build issues and refresh behavior

- **Zip extraction:** The EOIR zip file does not extract properly with some archive utilities. The build script uses Python's `zipfile` module and falls back to the `unzip` command if that fails.
- **Monthly updates:** EOIR updates the dump monthly at a stable URL. Re-running the build re-downloads the zip whenever its ETag / Last-Modified / size differs from the copy on disk (recorded in `<zip>.meta.json`), discards the previous extraction, and rebuilds from scratch; `DATAPOND_REFRESH=skip` reuses the cached zip offline.
- **Large tables:** The `schedule` table (~45M rows) and `representatives` table (~26M rows) take the longest. Build time depends on local CPU, memory and disk performance; the bounded refresh can take substantially longer.

### Data validation

Every build runs the checks in `build_database.py` (row counts, type casting, referential
integrity, lookup coverage, view correctness, data freshness, end-to-end queries) and
fails if any of them does; the numbers for the current file are in `VALIDATION.md`.

- **Charge lookup coverage** must be at least 99.9%; the current release passed this check. See `VALIDATION.md` for the release audit.

### Coming soon

- More example analyses

### License

MIT


### November 16, 2026 — September 2026 source refresh

This release contains **174,560,256 stored rows across 98 source tables**, plus three views and internal metadata tables. It uses the September 2026 EOIR archive dated October 1. Relative to the prior release, all 97 existing source tables remain and the publisher’s 1,051-row `eoirdb_schema` reference is also retained. The database is 4,725,420,032 bytes, SHA-256 `b86bfdbca2867d5859ba625f76bb51e90218b47a78676712168c4c0fea856634`.

The independent streaming audit reconciled all 98 CSV members. Ninety-seven tables exactly match their nonempty raw record counts. The case source contains **159 records with invalid UTF-8**, which the established importer skips; 12,869,593 case rows are stored from 12,869,752 raw records. The source also has malformed tab-separated records: permissive parsing, padding and typed coercion follow the existing pipeline and do not establish raw-cell equality. No untyped fallback was used. All builder checks passed. These limits matter when interpreting small count differences; the original ZIP is archived with this release so the excluded source records remain available for investigation.

Source hashes, table counts, the source-record reconciliation and baseline comparison are in `releases/2026-11-16` on GitHub and accompanying JSON files on Hugging Face. The complete original ZIP is [source-archive-2026-10-01.zip](https://huggingface.co/datasets/Nason/eoir-database/resolve/main/source-archive-2026-10-01.zip), SHA-256 `a1d0394d31a6b810b81ae91a676adef5d6839ead7ca33c8e3ee38653687a7d2f`. Use the immutable release commit when citing or downloading it.

To replay with Python 3.11+ and the pinned project dependencies in a fresh checkout:

```sh
python refresh_20261116.py --archive source-archive-2026-10-01.zip
```

The release script verifies the entire ZIP before parsing, extracts one CSV at a time, preserves the ZIP and refuses to overwrite an existing partial build. It requires 25 GiB free at startup and keeps a 5 GiB extraction reserve, using one DuckDB thread, a 1 GiB buffer pool and at most 1 GiB spill. Engine limits do not cap total process memory. The general `build_database.py` workflow remains available for future source editions.

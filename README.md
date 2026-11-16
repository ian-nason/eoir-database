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

The row counts below are approximate figures retained from the repository guide; use the hosted database's `_metadata` for its current counts.

- **Coverage.** Proceedings carry `OSC_DATE`s from 1990 (925,750 rows dated 1990-1995)
  and a handful of sentinel and future dates; the registry's `1990-2026` is the range of
  plausible `OSC_DATE` values, not a guarantee that early years are complete.

### Core tables

| Table | Source file | Description | ~Rows |
|-------|-----------|-------------|------:|
| `proceedings` | `B_TblProceeding.csv` | Proceedings: hearings, decisions, judges | ~16M |
| `cases` | `A_TblCase.csv` | Case-level demographics, custody, dates, attorney | ~12M |
| `charges` | `B_TblProceedCharges.csv` | Individual charges per proceeding | ~18M |
| `schedule` | `tbl_schedule.csv` | Hearing schedule, calendar type, adjournments | ~45M |
| `applications` | `tbl_Court_Appln.csv` | Applications for relief (asylum, etc.) | ~16M |
| `bonds` | `D_TblAssociatedBond.csv` | Bond hearing records and amounts | ~1.6M |
| `representatives` | `tbl_RepsAssigned.csv` | Attorney representation records | ~26M |
| `motions` | `tbl_Court_Motions.csv` | Motions filed in proceedings | ~8M |
| `appeals` | `tblAppeal.csv` | Appeals to Board of Immigration Appeals | ~1.5M |
| `custody_history` | `tbl_CustodyHistory.csv` | Custody status changes | ~10M |
| `case_identifiers` | `A_TblCaseIdentifier.csv` | Case ID cross-references | ~2.4M |
| `juvenile_history` | `tbl_JuvenileHistory.csv` | Juvenile designation records | ~3M |
| `lead_rider` | `tbl_Lead_Rider.csv` | Lead/rider case relationships | ~2.6M |
| `case_priority_history` | `tbl_CasePriorityHistory.csv` | Priority code changes | ~138K |
| `attorneys` | `tbl_EOIR_Attorney.csv` | Attorney registry | ~404K |
| `pro_bono` | `tblProBono.csv` | Pro bono screening records | ~66K |

### Lookup tables

All files in the `Lookup/` directory are loaded with the `lu_` prefix (78 tables total). Key ones:

| Table | Description | Key columns |
|-------|-------------|-------------|
| `lu_nationality` | Country/nationality codes | `NAT_CODE` -> `NAT_NAME`, `NAT_COUNTRY_NAME` |
| `lu_language` | Language codes | `strCode` -> `strDescription` |
| `lu_base_city` | Immigration court locations | `BASE_CITY_CODE` -> `BASE_CITY_NAME` |
| `lu_hearing_location` | Hearing locations & detention type | `HEARING_LOC_CODE` -> name, address |
| `lu_judge` | Immigration judge codes | `JUDGE_CODE` -> `JUDGE_NAME` |
| `lu_charges` | Charge codes | `strCode` -> `strCodeDescription` |
| `lu_court_decision` | Court decision codes (by case type) | `strDecCode` x `strCaseType` -> description |
| `lu_app_decision` | Application decision codes | `strCourtApplnDecCode` -> description |
| `lu_cal_type` | Calendar type codes | `strCalTypeCode` -> description |
| `lu_adjournment` | Adjournment reason codes | `strcode` -> `strDesciption` |
| `lu_application` | Application type codes | `strcode` -> `strdescription` |
| `lu_custody_status` | Custody status codes | `strCode` -> description |
| `lu_case_type` | Case type codes | `strCode` -> description |
| `lu_motion_type` | Motion type codes | `strMotionCode` -> description |
| `lu_bia_decision` | BIA decision codes | `strCode` -> description |
| `lu_schedule_type` | Schedule type codes | `strCode` -> description |
| `lu_state` | U.S. state codes | `state_code` -> `state_name` |

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
- **Large tables:** The `schedule` table (~45M rows) and `representatives` table (~26M rows) take the longest. Total build time is typically ~5 minutes.

### Data validation

Every build runs the checks in `build_database.py` (row counts, type casting, referential
integrity, lookup coverage, view correctness, data freshness, end-to-end queries) and
fails if any of them does; the numbers for the current file are in `VALIDATION.md`.

- **Charge lookup coverage** is 99.997% (19,176,101 of 19,176,675 non-NULL charge rows
  join to `lu_charges`) after the July 2026 normalisation of charge codes; the build
  requires at least 99.9%.

### Coming soon

- More example analyses

### License

MIT

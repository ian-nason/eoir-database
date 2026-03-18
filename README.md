# eoir-database

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Build a clean, queryable [DuckDB](https://duckdb.org/) database from the EOIR (Executive Office for Immigration Review) immigration court FOIA data dump.

The EOIR dataset is the most comprehensive source of U.S. immigration court data — every proceeding, charge, hearing, application for relief, bond hearing, and appeal since the 1970s. But working with the raw files is painful: a single ~4 GB zip containing dozens of tab-delimited CSVs with inconsistent formatting, null bytes, malformed rows, and no type information.

This project handles all of that. One command downloads, cleans, type-casts, and builds a fully indexed DuckDB database with lookup tables and pre-built views ready for analysis.

Modeled on [paulgp/ipeds-database](https://github.com/paulgp/ipeds-database).

## Quick start

```bash
# Install dependencies (using uv)
uv sync

# Build the database (downloads ~4 GB, takes ~5 min)
uv run python build_database.py

# Query it
duckdb eoir.duckdb
```

```sql
-- How many proceedings per year?
SELECT EXTRACT(YEAR FROM OSC_DATE) AS year, COUNT(*) AS n
FROM proceedings
WHERE OSC_DATE IS NOT NULL
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;

-- Top nationalities in cases filed since 2020
SELECT n.NAT_COUNTRY_NAME, COUNT(*) AS cases
FROM cases c
JOIN lu_nationality n ON c.NAT = n.NAT_CODE
WHERE c.INPUT_DATE >= '2020-01-01'
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

## Remote access (no download required)

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

## Colab quickstart

Want to query the data without installing anything locally? Open the [Colab notebook](examples/colab_quickstart.ipynb):

1. Upload `eoir.duckdb` to your Google Drive (or build it in Colab from the zip)
2. Open the notebook in Colab
3. Start querying — the notebook includes guided examples covering case volumes, wait times, representation rates, and outcomes

## Usage

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

## Data source

EOIR publishes a monthly data dump at their [FOIA Library](https://www.justice.gov/eoir/foia-library-0):

- **Download URL:** `https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip`
- ~4 GB zip, extracts to ~15-20 GB of tab-delimited CSV files
- Updated monthly
- Contains two directories: `EOIR Case Data/` (core tables) and `Lookup/` (reference tables)

## Tables

### Core tables

| Table | Source file | Description | ~Rows |
|-------|-----------|-------------|------:|
| `cases` | `A_TblCase.csv` | Case-level demographics, custody, dates, attorney | ~12M |
| `proceedings` | `B_TblProceeding.csv` | Proceedings: hearings, decisions, judges | ~16M |
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

## Type casting

All columns are read as VARCHAR first, then cast using DuckDB's `TRY_CAST`:

- **Date columns** (`*_DATE`, `DAT*`, etc.) -> `DATE`
- **ID columns** (`IDN*`) -> `INTEGER`
- **Money columns** (`INITIAL_BOND`, `NEW_BOND`) -> `DOUBLE`
- **Boolean columns** (`bln*`, `LPR`, `AGGRAVATE_FELON`) -> `BOOLEAN`
- **Everything else** -> `VARCHAR`

`TRY_CAST` returns `NULL` for unparseable values rather than failing, so no rows are lost.

## Example queries

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

## Example analyses

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

## Known issues

- **Zip extraction:** The EOIR zip file does not extract properly with some archive utilities. The build script uses Python's `zipfile` module and falls back to the `unzip` command if that fails.
- **Data quality:** The raw CSVs contain null bytes, inconsistent quoting, and malformed rows. The `ignore_errors` and `null_padding` options in DuckDB's CSV reader handle most of these, but a small number of rows may be silently dropped.
- **Column names:** Column names are preserved as-is from the source files (mixed case, inconsistent naming conventions). 
- **Monthly updates:** EOIR updates the dump monthly. Re-running the build script will re-download and rebuild from scratch. The zip URL is stable.
- **Large tables:** The `schedule` table (~45M rows) and `representatives` table (~26M rows) take the longest. Total build time is typically ~5 minutes.

## Data validation

33/35 automated checks pass (row counts, type casting, referential integrity, lookup coverage, view correctness, data freshness, end-to-end queries). The one noted item is a source data characteristic, not a build issue:

- **Charge lookup coverage** is 76.5% — EOIR's `lu_charges` table only contains 252 of the 571 distinct charge codes in the data. Pattern-based queries (`CHARGE LIKE '%237%'`) work fine; only joins to `lu_charges` for descriptions are affected.

## Coming soon

- More example analyses

## License

MIT


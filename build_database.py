#!/usr/bin/env python3
"""
EOIR Database Builder

Downloads the EOIR (Executive Office for Immigration Review) FOIA data dump,
cleans and type-casts the raw tab-delimited CSVs, and produces a queryable
DuckDB database with lookup tables and pre-built views.

Usage:
    uv run python build_database.py
    uv run python build_database.py --zip /path/to/EOIR_Case_Data.zip
    uv run python build_database.py --data-dir /path/to/extracted/
    uv run python build_database.py --tables cases proceedings charges
    uv run python build_database.py --output my_eoir.duckdb
"""

import argparse
import os
import re
import subprocess
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EOIR_ZIP_URL = "https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip"
DEFAULT_OUTPUT = "eoir.duckdb"
DEFAULT_RAW_DIR = Path("data/raw")
ZIP_FILENAME = "EOIR_Case_Data.zip"

# DuckDB read_csv options — the ONLY reliable way to ingest these files.
# They are tab-delimited despite .csv extension, contain null bytes,
# malformed rows, and inconsistent quoting.
READ_CSV_OPTS = {
    "delim": "'\\t'",
    "header": "true",
    "null_padding": "true",
    "ignore_errors": "true",
    "quote": "''",  # empty string disables quote parsing
    "all_varchar": "true",
    "strict_mode": "false",
}

# ---------------------------------------------------------------------------
# File → table name mappings
# ---------------------------------------------------------------------------

# Core tables (Data/ directory)
CORE_FILE_MAP: dict[str, dict] = {
    "A_TblCase.csv": {
        "table": "cases",
        "description": "Case-level demographics, custody, dates, attorney info",
    },
    "B_TblProceeding.csv": {
        "table": "proceedings",
        "description": "Proceedings: hearings, decisions, judges, charges",
    },
    "B_TblProceedCharges.csv": {
        "table": "charges",
        "description": "Individual charges per proceeding",
    },
    "tbl_schedule.csv": {
        "table": "schedule",
        "description": "Hearing schedule entries with calendar type and adjournments",
    },
    "tbl_Court_Appln.csv": {
        "table": "applications",
        "description": "Applications for relief (asylum, cancellation, etc.)",
    },
    "D_TblAssociatedBond.csv": {
        "table": "bonds",
        "description": "Bond hearing records and amounts",
    },
    "tbl_RepsAssigned.csv": {
        "table": "representatives",
        "description": "Attorney representation records",
    },
    "tbl_Court_Motions.csv": {
        "table": "motions",
        "description": "Motions filed in proceedings",
    },
    "tblAppeal.csv": {
        "table": "appeals",
        "description": "Appeals to Board of Immigration Appeals",
    },
    "tbl_CustodyHistory.csv": {
        "table": "custody_history",
        "description": "Custody status change records",
    },
    "A_TblCaseIdentifier.csv": {
        "table": "case_identifiers",
        "description": "Case ID cross-references",
    },
    "tbl_JuvenileHistory.csv": {
        "table": "juvenile_history",
        "description": "Juvenile designation records",
    },
    "tbl_Lead_Rider.csv": {
        "table": "lead_rider",
        "description": "Lead/rider case relationships",
    },
    "tbl_CasePriorityHistory.csv": {
        "table": "case_priority_history",
        "description": "Priority code changes",
    },
    "tbl_EOIR_Attorney.csv": {
        "table": "attorneys",
        "description": "Attorney registry",
    },
    "tblAction.csv": {
        "table": "actions",
        "description": "Docket actions/events",
    },
    "tblProBono.csv": {
        "table": "pro_bono",
        "description": "Pro bono screening records",
    },
}

# Lookup tables (Lookup/ directory) — explicit name overrides
LOOKUP_FILE_MAP: dict[str, dict] = {
    "tblLookupNationality.csv": {
        "table": "lu_nationality",
        "description": "Nationality/country codes",
    },
    "tblLanguage.csv": {
        "table": "lu_language",
        "description": "Language codes",
    },
    "tblLookupBaseCity.csv": {
        "table": "lu_base_city",
        "description": "Immigration court locations",
    },
    "tblLookupHloc.csv": {
        "table": "lu_hearing_location",
        "description": "Hearing location codes",
    },
    "tblLookupJudge.csv": {
        "table": "lu_judge",
        "description": "Immigration judge codes",
    },
    "tbllookupCharges.csv": {
        "table": "lu_charges",
        "description": "Charge codes and descriptions",
    },
    "tblLookupCourtDecision.csv": {
        "table": "lu_court_decision",
        "description": "Court decision codes (by case type)",
    },
    "tblLookupCourtAppDecisions.csv": {
        "table": "lu_app_decision",
        "description": "Application decision codes",
    },
    "tbllookupCal_Type.csv": {
        "table": "lu_cal_type",
        "description": "Calendar type codes",
    },
    "tblAdjournmentcodes.csv": {
        "table": "lu_adjournment",
        "description": "Adjournment reason codes",
    },
    "tblLookUp_Appln.csv": {
        "table": "lu_application",
        "description": "Application type codes",
    },
    "tblLookupCustodyStatus.csv": {
        "table": "lu_custody_status",
        "description": "Custody status codes",
    },
    "tblLookupCaseType.csv": {
        "table": "lu_case_type",
        "description": "Case type codes",
    },
    "tblLookupMotionType.csv": {
        "table": "lu_motion_type",
        "description": "Motion type codes",
    },
    "tblLookupBIADecision.csv": {
        "table": "lu_bia_decision",
        "description": "BIA decision codes",
    },
    "tbllookupSchedule_Type.csv": {
        "table": "lu_schedule_type",
        "description": "Schedule type codes",
    },
    "tblLookupState.csv": {
        "table": "lu_state",
        "description": "U.S. state codes",
    },
}

# Build order for core tables: smallest → largest
CORE_BUILD_ORDER = [
    "case_identifiers",
    "juvenile_history",
    "lead_rider",
    "case_priority_history",
    "attorneys",
    "custody_history",
    "appeals",
    "pro_bono",
    "actions",
    "cases",
    "applications",
    "bonds",
    "motions",
    "charges",
    "proceedings",
    "representatives",
    "schedule",
]


# ---------------------------------------------------------------------------
# Type casting helpers
# ---------------------------------------------------------------------------

# Patterns for date columns
DATE_PATTERNS = [
    re.compile(r"_DATE$", re.IGNORECASE),
    re.compile(r"^DAT[A-Z]", re.IGNORECASE),
    re.compile(r"^DATE_", re.IGNORECASE),
]
DATE_EXACT = {
    "OSC_DATE", "INPUT_DATE", "HEARING_DATE", "COMP_DATE", "TRANS_IN_DATE",
    "VENUE_CHG_GRANTED", "DATE_APPEAL_DUE_STATUS", "DATE_DETAINED",
    "DATE_RELEASED", "C_BIRTHDATE", "DATE_OF_ENTRY", "E_28_DATE",
    "LATEST_HEARING", "UP_BOND_DATE", "C_RELEASE_DATE", "ADDRESS_CHANGEDON",
    "DETENTION_DATE", "APPL_RECD_DATE", "ADJ_DATE", "BOND_HEAR_REQ_DATE",
    "MOTION_RECD_DATE",
}

# Patterns for integer ID columns
INT_PATTERNS = [
    re.compile(r"^IDN", re.IGNORECASE),
]

# Exact integer columns
INT_EXACT: set[str] = set()

# Numeric / money columns
NUMERIC_EXACT = {"INITIAL_BOND", "NEW_BOND"}

# Boolean columns (stored as '0'/'1' in source)
BOOL_PATTERNS = [
    re.compile(r"^bln", re.IGNORECASE),
]
BOOL_EXACT = {
    "LPR", "AGGRAVATE_FELON", "CRIMINAL_FLAG",
}


def is_date_col(col: str) -> bool:
    col_upper = col.upper()
    if col_upper in DATE_EXACT:
        return True
    return any(p.search(col) for p in DATE_PATTERNS)


def is_int_col(col: str) -> bool:
    col_upper = col.upper()
    if col_upper in INT_EXACT:
        return True
    return any(p.search(col) for p in INT_PATTERNS)


def is_numeric_col(col: str) -> bool:
    return col.upper() in NUMERIC_EXACT


def is_bool_col(col: str) -> bool:
    col_upper = col.upper()
    if col_upper in BOOL_EXACT:
        return True
    return any(p.search(col) for p in BOOL_PATTERNS)


def cast_expression(col: str) -> str:
    """Return a SQL expression that casts a VARCHAR column to its proper type."""
    quoted = f'"{col}"'
    if is_int_col(col):
        return f'TRY_CAST({quoted} AS INTEGER) AS {quoted}'
    if is_date_col(col):
        return f'TRY_CAST({quoted} AS DATE) AS {quoted}'
    if is_numeric_col(col):
        return f'TRY_CAST({quoted} AS DOUBLE) AS {quoted}'
    if is_bool_col(col):
        return (
            f'CASE WHEN {quoted} = \'1\' THEN TRUE '
            f'WHEN {quoted} = \'0\' THEN FALSE '
            f'ELSE NULL END AS {quoted}'
        )
    return quoted


# ---------------------------------------------------------------------------
# CSV reading helper
# ---------------------------------------------------------------------------

def read_csv_sql(filepath: str | Path) -> str:
    """Build a DuckDB read_csv() expression with the standard options."""
    # Use forward slashes — DuckDB handles them on all platforms and it avoids
    # the Windows backslash being mis-parsed in the SQL string.
    filepath = str(filepath).replace("\\", "/").replace("'", "''")
    opts = ", ".join(f"{k}={v}" for k, v in READ_CSV_OPTS.items())
    return f"read_csv('{filepath}', {opts})"


def get_columns(con: duckdb.DuckDBPyConnection, filepath: str | Path) -> list[str]:
    """Get column names from a CSV file without loading all data."""
    sql = f"SELECT * FROM {read_csv_sql(filepath)} LIMIT 0"
    return [desc[0] for desc in con.execute(sql).description]


def build_typed_select(columns: list[str]) -> str:
    """Build a SELECT clause that applies type casts to all columns."""
    exprs = [cast_expression(col) for col in columns]
    return ", ".join(exprs)


# ---------------------------------------------------------------------------
# Download & extraction
# ---------------------------------------------------------------------------

def download_zip(dest_dir: Path) -> Path:
    """Download the EOIR zip file with progress bar."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / ZIP_FILENAME

    if dest.exists():
        size_gb = dest.stat().st_size / (1024**3)
        print(f"  Zip already exists: {dest} ({size_gb:.1f} GB), skipping download")
        return dest

    print(f"  Downloading from {EOIR_ZIP_URL}")
    print("  (This is ~4 GB, may take a while)")

    resp = requests.get(EOIR_ZIP_URL, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc="  Downloading"
    ) as pbar:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            pbar.update(len(chunk))

    size_gb = dest.stat().st_size / (1024**3)
    print(f"  Downloaded {size_gb:.1f} GB to {dest}")
    return dest


def extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    """
    Extract the EOIR zip file.

    The EOIR zip doesn't extract properly with some tools. Try Python's
    zipfile first, fall back to subprocess unzip.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Check if already extracted — look for Data/ or Lookup/ dirs
    data_dir = find_subdir(dest_dir, "Data")
    lookup_dir = find_subdir(dest_dir, "Lookup")
    if data_dir and lookup_dir:
        print(f"  Already extracted in {dest_dir}")
        return dest_dir

    print(f"  Extracting {zip_path.name} to {dest_dir}")
    print("  (This may take several minutes for ~15-20 GB of data)")

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            for member in tqdm(members, desc="  Extracting", unit="files"):
                zf.extract(member, dest_dir)
        print(f"  Extracted {len(members)} files via zipfile")
    except (zipfile.BadZipFile, Exception) as e:
        print(f"  Python zipfile failed ({e}), falling back to unzip command")
        result = subprocess.run(
            ["unzip", "-o", str(zip_path), "-d", str(dest_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"  unzip stderr: {result.stderr[:500]}")
            raise RuntimeError(f"Failed to extract zip: {result.stderr[:200]}")
        print("  Extracted via unzip command")

    return dest_dir


def find_subdir(base: Path, name: str) -> Optional[Path]:
    """Find a subdirectory by name (case-insensitive), searching up to two levels deep.

    The EOIR zip sometimes nests files under "EOIR Case Data" instead of "Data",
    so we also accept directories whose name ends with the target (after splitting
    on spaces), e.g. "EOIR Case Data" matches a search for "Data".
    """
    name_lower = name.lower()

    def _matches(dirname: str) -> bool:
        dl = dirname.lower()
        if dl == name_lower:
            return True
        # Accept "EOIR Case Data" when searching for "Data"
        parts = dl.split()
        return len(parts) > 1 and parts[-1] == name_lower

    # Exact matches first, then suffix matches; prefer deeper nesting
    for exact in (True, False):
        for item in base.iterdir():
            if not item.is_dir():
                continue
            if exact and item.name.lower() == name_lower:
                return item
            # Search one level deeper
            for child in item.iterdir():
                if not child.is_dir():
                    continue
                if exact and child.name.lower() == name_lower:
                    return child
                if not exact and _matches(child.name):
                    return child
        # Check top-level suffix matches only after exhausting deeper exact+suffix
        if not exact:
            for item in base.iterdir():
                if item.is_dir() and _matches(item.name):
                    return item

    return None


# ---------------------------------------------------------------------------
# Table building
# ---------------------------------------------------------------------------

def discover_files(base_dir: Path) -> tuple[dict[str, Path], dict[str, Path]]:
    """
    Discover CSV files in Data/ and Lookup/ directories.

    Returns:
        (core_files, lookup_files) — dicts of table_name → file_path
    """
    data_dir = find_subdir(base_dir, "Data")
    lookup_dir = find_subdir(base_dir, "Lookup")

    if not data_dir:
        raise FileNotFoundError(
            f"Could not find 'Data' directory under {base_dir}. "
            "Check that the zip extracted correctly."
        )
    if not lookup_dir:
        raise FileNotFoundError(
            f"Could not find 'Lookup' directory under {base_dir}. "
            "Check that the zip extracted correctly."
        )

    # Build reverse map: lowercase filename → info
    core_by_filename = {k.lower(): v for k, v in CORE_FILE_MAP.items()}
    lookup_by_filename = {k.lower(): v for k, v in LOOKUP_FILE_MAP.items()}

    core_files: dict[str, Path] = {}
    for f in sorted(data_dir.iterdir()):
        if not f.is_file() or not f.name.lower().endswith(".csv"):
            continue
        info = core_by_filename.get(f.name.lower())
        if info:
            core_files[info["table"]] = f
        else:
            # Derive table name from filename
            table_name = derive_table_name(f.name)
            core_files[table_name] = f

    lookup_files: dict[str, Path] = {}
    for f in sorted(lookup_dir.iterdir()):
        if not f.is_file() or not f.name.lower().endswith(".csv"):
            continue
        info = lookup_by_filename.get(f.name.lower())
        if info:
            lookup_files[info["table"]] = f
        else:
            table_name = derive_table_name(f.name, prefix="lu_")
            lookup_files[table_name] = f

    return core_files, lookup_files


def derive_table_name(filename: str, prefix: str = "") -> str:
    """Derive a clean table name from a filename."""
    name = Path(filename).stem
    # Strip common prefixes
    for strip in ["tblLookup", "tblLookUp", "tbllookup", "tbl_", "tbl", "A_Tbl", "B_Tbl", "D_Tbl"]:
        if name.startswith(strip):
            name = name[len(strip):]
            break
    # Convert CamelCase to snake_case
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    if prefix and not name.startswith(prefix):
        name = prefix + name
    return name


def get_table_description(table_name: str, source_file: str) -> str:
    """Get a human description for a table."""
    # Check core map
    for info in CORE_FILE_MAP.values():
        if info["table"] == table_name:
            return info["description"]
    # Check lookup map
    for info in LOOKUP_FILE_MAP.values():
        if info["table"] == table_name:
            return info["description"]
    return f"Loaded from {source_file}"


def build_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    filepath: Path,
) -> int:
    """
    Load a CSV file into a DuckDB table with type casting.

    Returns the row count.
    """
    try:
        columns = get_columns(con, filepath)
    except Exception as e:
        print(f"    WARNING: Could not read columns from {filepath.name}: {e}")
        return 0

    typed_select = build_typed_select(columns)

    sql = (
        f"CREATE OR REPLACE TABLE {table_name} AS "
        f"SELECT {typed_select} FROM {read_csv_sql(filepath)}"
    )

    try:
        con.execute(sql)
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        return row_count
    except Exception as e:
        print(f"    WARNING: Failed to build table {table_name}: {e}")
        # Try without type casting as a fallback
        try:
            fallback_sql = (
                f"CREATE OR REPLACE TABLE {table_name} AS "
                f"SELECT * FROM {read_csv_sql(filepath)}"
            )
            con.execute(fallback_sql)
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"    Loaded {table_name} without type casting (fallback)")
            return row_count
        except Exception as e2:
            print(f"    ERROR: Could not load {table_name} at all: {e2}")
            return 0


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

VIEWS = {
    "v_proceedings_full": """
        CREATE OR REPLACE VIEW v_proceedings_full AS
        SELECT
            p.*,
            bc.BASE_CITY_NAME AS court_name,
            j.JUDGE_NAME AS judge_name,
            n.NAT_NAME AS nationality_name,
            n.NAT_COUNTRY_NAME AS country_name,
            d.strDecDescription AS decision_description
        FROM proceedings p
        LEFT JOIN lu_base_city bc ON p.BASE_CITY_CODE = bc.BASE_CITY_CODE
        LEFT JOIN lu_judge j ON p.IJ_CODE = j.JUDGE_CODE
        LEFT JOIN lu_nationality n ON p.NAT = n.NAT_CODE
        LEFT JOIN (
            SELECT DISTINCT ON (strDecCode, strCaseType)
                strDecCode, strCaseType, strDecDescription
            FROM lu_court_decision
            ORDER BY strDecCode, strCaseType, blnActive DESC NULLS LAST
        ) d ON p.DEC_CODE = d.strDecCode AND p.CASE_TYPE = d.strCaseType
    """,
    "v_first_hearing": """
        CREATE OR REPLACE VIEW v_first_hearing AS
        WITH ranked AS (
            SELECT
                IDNCASE, IDNPROCEEDING, BASE_CITY_CODE, CUSTODY, CASE_TYPE,
                OSC_DATE, HEARING_DATE, DATE_DETAINED,
                ROW_NUMBER() OVER (
                    PARTITION BY IDNCASE ORDER BY HEARING_DATE
                ) AS rn
            FROM proceedings
            WHERE HEARING_DATE IS NOT NULL
              AND OSC_DATE IS NOT NULL
              AND HEARING_DATE >= OSC_DATE
        )
        SELECT
            IDNCASE, IDNPROCEEDING, BASE_CITY_CODE, CUSTODY, CASE_TYPE,
            OSC_DATE, HEARING_DATE AS first_hearing_date, DATE_DETAINED,
            DATEDIFF('day', OSC_DATE, HEARING_DATE) AS days_osc_to_hearing,
            DATEDIFF('day', DATE_DETAINED, HEARING_DATE)
                AS days_detained_to_hearing,
            DATEDIFF('day', DATE_DETAINED, OSC_DATE) AS days_detained_to_osc
        FROM ranked
        WHERE rn = 1
    """,
    "v_case_summary": """
        CREATE OR REPLACE VIEW v_case_summary AS
        SELECT
            c.IDNCASE,
            c.NAT,
            n.NAT_COUNTRY_NAME AS country,
            c.LANG,
            c.SEX,
            c.CUSTODY,
            c.CASE_TYPE,
            c.LPR,
            c.DATE_OF_ENTRY,
            c.C_BIRTHDATE,
            c.DATE_DETAINED,
            c.DATE_RELEASED,
            c.DETENTION_FACILITY_TYPE,
            fh.first_hearing_date,
            fh.OSC_DATE,
            fh.days_osc_to_hearing,
            fh.days_detained_to_hearing,
            fh.BASE_CITY_CODE
        FROM cases c
        LEFT JOIN lu_nationality n ON c.NAT = n.NAT_CODE
        LEFT JOIN v_first_hearing fh ON c.IDNCASE = fh.IDNCASE
    """,
}

# Which tables each view depends on
VIEW_DEPS = {
    "v_proceedings_full": {"proceedings", "lu_base_city", "lu_judge", "lu_nationality", "lu_court_decision"},
    "v_first_hearing": {"proceedings"},
    "v_case_summary": {"cases", "lu_nationality", "v_first_hearing"},
}


def create_views(con: duckdb.DuckDBPyConnection, built_tables: set[str]) -> list[str]:
    """Create views, skipping any whose dependencies are missing."""
    created = []
    # v_first_hearing must be created before v_case_summary
    view_order = ["v_proceedings_full", "v_first_hearing", "v_case_summary"]

    for view_name in view_order:
        deps = VIEW_DEPS[view_name]
        # Views can depend on other views we just created
        available = built_tables | set(created)
        missing = deps - available
        if missing:
            print(f"  Skipping {view_name} — missing tables: {missing}")
            continue

        try:
            con.execute(VIEWS[view_name])
            created.append(view_name)
            print(f"  Created view: {view_name}")
        except Exception as e:
            print(f"  WARNING: Could not create {view_name}: {e}")

    return created


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def build_metadata(
    con: duckdb.DuckDBPyConnection,
    table_sources: dict[str, str],
    lookup_tables: set[str],
):
    """Create and populate the _metadata table."""
    con.execute("""
        CREATE OR REPLACE TABLE _metadata (
            table_name VARCHAR,
            source_file VARCHAR,
            description VARCHAR,
            row_count BIGINT,
            column_count INTEGER,
            is_lookup BOOLEAN,
            built_at TIMESTAMP
        )
    """)

    now = datetime.now().isoformat()

    for table_name, source_file in sorted(table_sources.items()):
        try:
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            col_count = len(con.execute(
                f"SELECT * FROM {table_name} LIMIT 0"
            ).description)
        except Exception:
            continue

        desc = get_table_description(table_name, source_file)
        is_lookup = table_name in lookup_tables

        con.execute(
            "INSERT INTO _metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
            [table_name, source_file, desc, row_count, col_count, is_lookup, now],
        )


# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------

def run_sanity_checks(con: duckdb.DuckDBPyConnection, db_path: Path):
    """Run and print sanity checks after building."""
    print("\n" + "=" * 60)
    print("SANITY CHECKS")
    print("=" * 60)

    # Total tables
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
    ).fetchall()
    print(f"\n  Tables created: {len(tables)}")

    # Total views
    views = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'VIEW'"
    ).fetchall()
    print(f"  Views created:  {len(views)}")

    # Total rows from metadata
    try:
        total_rows = con.execute(
            "SELECT SUM(row_count) FROM _metadata"
        ).fetchone()[0]
        print(f"  Total rows:     {total_rows:,}")
    except Exception:
        pass

    # Proceedings by year (last 5 years)
    try:
        print("\n  Proceedings by year (last 5):")
        rows = con.execute("""
            SELECT EXTRACT(YEAR FROM OSC_DATE) AS year, COUNT(*) AS cnt
            FROM proceedings
            WHERE OSC_DATE IS NOT NULL
              AND EXTRACT(YEAR FROM OSC_DATE) >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 5
        """).fetchall()
        for year, cnt in rows:
            print(f"    {int(year)}: {cnt:>10,}")
    except Exception as e:
        print(f"    (skipped — {e})")

    # Top 5 courts
    try:
        print("\n  Top 5 courts by case volume:")
        rows = con.execute("""
            SELECT
                p.BASE_CITY_CODE,
                COALESCE(bc.BASE_CITY_NAME, p.BASE_CITY_CODE) AS court,
                COUNT(*) AS cnt
            FROM proceedings p
            LEFT JOIN lu_base_city bc ON p.BASE_CITY_CODE = bc.BASE_CITY_CODE
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT 5
        """).fetchall()
        for _, court, cnt in rows:
            print(f"    {court:<30s} {cnt:>10,}")
    except Exception as e:
        print(f"    (skipped — {e})")

    # Database file size
    size_mb = db_path.stat().st_size / (1024**2)
    print(f"\n  Database size:  {size_mb:,.0f} MB ({size_mb / 1024:.1f} GB)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build a DuckDB database from the EOIR FOIA data dump."
    )
    parser.add_argument(
        "--zip",
        type=Path,
        help="Path to an already-downloaded EOIR zip file",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Path to already-extracted EOIR data directory",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Build only specific tables (by table name)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(DEFAULT_OUTPUT),
        help=f"Output database path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    t_start = time.time()

    print("=" * 60)
    print("EOIR Database Builder")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # Step 1: Resolve data directory
    # -----------------------------------------------------------------------
    if args.data_dir:
        base_dir = args.data_dir
        print(f"\n[1/6] Using existing data directory: {base_dir}")
    else:
        raw_dir = DEFAULT_RAW_DIR
        print(f"\n[1/6] Download & extract")

        if args.zip:
            zip_path = args.zip
            print(f"  Using local zip: {zip_path}")
        else:
            zip_path = download_zip(raw_dir)

        base_dir = raw_dir
        extract_zip(zip_path, base_dir)

    # -----------------------------------------------------------------------
    # Step 2: Discover files
    # -----------------------------------------------------------------------
    print(f"\n[2/6] Discovering files")
    core_files, lookup_files = discover_files(base_dir)
    print(f"  Found {len(core_files)} core tables, {len(lookup_files)} lookup tables")

    # Filter to requested tables if --tables specified
    if args.tables:
        requested = set(args.tables)
        core_files = {k: v for k, v in core_files.items() if k in requested}
        lookup_files = {k: v for k, v in lookup_files.items() if k in requested}
        print(f"  Filtered to {len(core_files)} core + {len(lookup_files)} lookup")

    # -----------------------------------------------------------------------
    # Step 3: Open database and load lookup tables
    # -----------------------------------------------------------------------
    print(f"\n[3/6] Loading lookup tables")
    db_path = args.output
    con = duckdb.connect(str(db_path))

    built_tables: set[str] = set()
    table_sources: dict[str, str] = {}

    for table_name, filepath in sorted(lookup_files.items()):
        t0 = time.time()
        row_count = build_table(con, table_name, filepath)
        elapsed = time.time() - t0

        if row_count > 0:
            built_tables.add(table_name)
            table_sources[table_name] = filepath.name
            print(f"  {table_name:<30s} {row_count:>10,} rows  ({elapsed:.1f}s)")
        else:
            print(f"  {table_name:<30s} SKIPPED")

    # -----------------------------------------------------------------------
    # Step 4: Load core tables (in build order)
    # -----------------------------------------------------------------------
    print(f"\n[4/6] Loading core tables")

    # Build ordered list: known order first, then any discovered extras
    ordered_core: list[tuple[str, Path]] = []
    remaining = dict(core_files)

    for table_name in CORE_BUILD_ORDER:
        if table_name in remaining:
            ordered_core.append((table_name, remaining.pop(table_name)))

    # Append any files not in the explicit build order
    for table_name, filepath in sorted(remaining.items()):
        ordered_core.append((table_name, filepath))

    for table_name, filepath in ordered_core:
        t0 = time.time()
        print(f"  Building {table_name} from {filepath.name} ...", end=" ", flush=True)
        row_count = build_table(con, table_name, filepath)
        elapsed = time.time() - t0

        if row_count > 0:
            built_tables.add(table_name)
            table_sources[table_name] = filepath.name
            print(f"{row_count:>12,} rows  ({elapsed:.1f}s)")
        else:
            print("SKIPPED")

    # -----------------------------------------------------------------------
    # Step 5: Create views
    # -----------------------------------------------------------------------
    print(f"\n[5/6] Creating views")
    created_views = create_views(con, built_tables)

    # -----------------------------------------------------------------------
    # Step 6: Metadata
    # -----------------------------------------------------------------------
    print(f"\n[6/6] Building metadata")
    build_metadata(con, table_sources, set(lookup_files.keys()))
    meta_count = con.execute("SELECT COUNT(*) FROM _metadata").fetchone()[0]
    print(f"  {meta_count} tables cataloged in _metadata")

    # -----------------------------------------------------------------------
    # Done — sanity checks
    # -----------------------------------------------------------------------
    run_sanity_checks(con, db_path)

    con.close()

    elapsed_total = time.time() - t_start
    minutes = int(elapsed_total // 60)
    seconds = int(elapsed_total % 60)
    print(f"\nDone in {minutes}m {seconds}s")
    print(f"Database: {db_path.resolve()}")


if __name__ == "__main__":
    main()

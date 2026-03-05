#!/usr/bin/env python3
"""Upload eoir.duckdb to Hugging Face for remote access.

Usage:
    uv run python publish_to_hf.py
    uv run python publish_to_hf.py --db eoir.duckdb --repo Nason/eoir-database
    HF_TOKEN=hf_xxx uv run python publish_to_hf.py
"""

import argparse
import sys
from pathlib import Path

import duckdb
from huggingface_hub import HfApi, create_repo


def generate_dataset_card(db_path: str) -> str:
    """Generate a HF-compatible README with YAML frontmatter."""
    con = duckdb.connect(db_path, read_only=True)
    metadata = con.sql(
        "SELECT table_name, description, row_count FROM _metadata ORDER BY row_count DESC"
    ).fetchdf()
    con.close()

    table_rows = "\n".join(
        f"| `{row['table_name']}` | {row['description']} | {row['row_count']:,} |"
        for _, row in metadata.iterrows()
    )
    total_rows = int(metadata["row_count"].sum())
    n_tables = len(metadata)

    return f"""---
license: mit
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

A clean, queryable DuckDB database built from the [EOIR FOIA data dump](https://www.justice.gov/eoir/foia-library-0) -- the most comprehensive public dataset on U.S. immigration court proceedings.

**{total_rows:,} rows** across **{n_tables} tables** covering every immigration court case since the 1970s.

Built with [eoir-database](https://github.com/ian-nason/eoir-database).

## Quick Start

### DuckDB CLI

```sql
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb' AS eoir (READ_ONLY);

-- Query immediately
SELECT court_name, COUNT(*) as cases
FROM eoir.v_proceedings_full
WHERE CASE_TYPE = 'RMV'
GROUP BY court_name
ORDER BY cases DESC
LIMIT 10;
```

### Python

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql(\"\"\"
    ATTACH 'https://huggingface.co/datasets/Nason/eoir-database/resolve/main/eoir.duckdb'
    AS eoir (READ_ONLY)
\"\"\")
con.sql("SELECT * FROM eoir._metadata").show()
```

DuckDB uses HTTP range requests, so only the pages needed for your query are downloaded.

## Tables

| Table | Description | Rows |
|-------|-------------|------|
{table_rows}

## Data Source

[EOIR FOIA Library](https://www.justice.gov/eoir/foia-library-0) -- updated monthly by the Executive Office for Immigration Review (U.S. Department of Justice). This is public domain U.S. government data.

## License

Database build code: MIT. Underlying data: public domain (U.S. government work).

## GitHub

Full source code, build instructions, and example analyses: [github.com/ian-nason/eoir-database](https://github.com/ian-nason/eoir-database)
"""


def main():
    parser = argparse.ArgumentParser(
        description="Upload eoir.duckdb to Hugging Face"
    )
    parser.add_argument("--db", type=Path, default=Path("eoir.duckdb"))
    parser.add_argument("--repo", default="Nason/eoir-database")
    parser.add_argument("--token", help="HF token (or set HF_TOKEN env var)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: Database not found at {args.db}")
        sys.exit(1)

    api = HfApi(token=args.token)

    print(f"Creating dataset repo: {args.repo}")
    create_repo(args.repo, repo_type="dataset", exist_ok=True, token=args.token)

    print(f"Generating dataset card from {args.db}")
    card = generate_dataset_card(str(args.db))

    print("Uploading dataset card...")
    api.upload_file(
        path_or_fileobj=card.encode(),
        path_in_repo="README.md",
        repo_id=args.repo,
        repo_type="dataset",
    )

    size_gb = args.db.stat().st_size / (1024**3)
    print(f"Uploading {args.db} ({size_gb:.1f} GB)...")
    api.upload_file(
        path_or_fileobj=str(args.db),
        path_in_repo="eoir.duckdb",
        repo_id=args.repo,
        repo_type="dataset",
    )

    print(f"\nUploaded to https://huggingface.co/datasets/{args.repo}")
    print(f"\nUsers can now query remotely:")
    print(f"  ATTACH 'https://huggingface.co/datasets/{args.repo}/resolve/main/eoir.duckdb' AS eoir (READ_ONLY);")


if __name__ == "__main__":
    main()

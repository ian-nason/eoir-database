#!/bin/bash
# auto_rebuild.sh — Monthly EOIR database rebuild and publish
# Run via cron: 0 4 5 * * /home/ubuntu/eoir-database/scripts/auto_rebuild.sh >> /home/ubuntu/eoir-database/logs/cron.log 2>&1

set -e

REPO_DIR="/home/ubuntu/eoir-database"
LOG_DIR="$REPO_DIR/logs"
TIMESTAMP=$(date +%Y-%m-%d_%H%M)

mkdir -p "$LOG_DIR"
echo "=========================================="
echo "EOIR Database Rebuild — $TIMESTAMP"
echo "=========================================="

cd "$REPO_DIR"

# Pull latest code
git pull origin main

# Check if EOIR has a new data dump by comparing Last-Modified header
EOIR_URL="https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip"
LAST_MODIFIED=$(curl -sI "$EOIR_URL" | grep -i "Last-Modified" | head -1 | tr -d '\r')
echo "EOIR zip Last-Modified: $LAST_MODIFIED"

LAST_KNOWN_FILE="$REPO_DIR/data/.last_eoir_modified"
if [ -f "$LAST_KNOWN_FILE" ]; then
    LAST_KNOWN=$(cat "$LAST_KNOWN_FILE")
    if [ "$LAST_MODIFIED" = "$LAST_KNOWN" ]; then
        echo "No new data detected. Skipping rebuild."
        exit 0
    fi
fi

echo "New data detected (or first run). Starting rebuild..."

# Clean previous raw data to avoid disk exhaustion
rm -rf "$REPO_DIR/data/raw/"

# Build the database
uv run python build_database.py --output eoir.duckdb 2>&1 | tee "$LOG_DIR/build_$TIMESTAMP.log"

# Quick validation
uv run python -c "
import duckdb
con = duckdb.connect('eoir.duckdb', read_only=True)
meta = con.sql('SELECT COUNT(*) as tables, SUM(row_count) as rows FROM _metadata').fetchone()
print(f'Tables: {meta[0]}, Rows: {meta[1]:,}')
assert meta[0] > 50, f'Too few tables: {meta[0]}'
assert meta[1] > 100_000_000, f'Too few rows: {meta[1]}'
print('Validation passed!')
con.close()
"

# Upload to Hugging Face
uv run python publish_to_hf.py --db eoir.duckdb 2>&1 | tee "$LOG_DIR/publish_$TIMESTAMP.log"

# Save the modification date so we skip next month if unchanged
echo "$LAST_MODIFIED" > "$LAST_KNOWN_FILE"

# Clean up raw data after successful build (saves ~20 GB disk)
rm -rf "$REPO_DIR/data/raw/"

echo "Rebuild complete at $(date)"

# Optional: send notification
# curl -X POST "https://hooks.slack.com/services/YOUR/WEBHOOK/URL" \
#   -H 'Content-type: application/json' \
#   --data "{\"text\":\"EOIR database rebuilt and published to HuggingFace ($TIMESTAMP)\"}"

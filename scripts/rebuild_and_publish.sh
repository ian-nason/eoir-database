#!/usr/bin/env bash
set -euo pipefail

# rebuild_and_publish.sh — EOIR Immigration Court Database
# Usage: cd ~/eoir-database && bash scripts/rebuild_and_publish.sh

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

SOURCE_URL="https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip"
HF_REPO="Nason/eoir-database"
LAST_MOD_FILE="data/.last_modified"
LOG_DIR="logs"

# --- Preflight ---
if [[ -z "${HF_TOKEN:-}" ]]; then
    echo "ERROR: HF_TOKEN environment variable is required."
    exit 1
fi

mkdir -p "$LOG_DIR" data
LOGFILE="$LOG_DIR/rebuild_$(date +%Y-%m-%d).log"
exec > >(tee -a "$LOGFILE") 2>&1
echo "=== EOIR rebuild started at $(date) ==="

# --- Check for updates ---
echo "Checking for updates..."
REMOTE_MOD=$(curl -sI "$SOURCE_URL" | grep -i "^last-modified:" | sed 's/^[Ll]ast-[Mm]odified: //' | tr -d '\r')

if [[ -z "$REMOTE_MOD" ]]; then
    echo "WARNING: Could not get Last-Modified header. Proceeding with rebuild."
elif [[ -f "$LAST_MOD_FILE" ]]; then
    LOCAL_MOD=$(cat "$LAST_MOD_FILE")
    if [[ "$REMOTE_MOD" == "$LOCAL_MOD" ]]; then
        echo "No update available (Last-Modified: $REMOTE_MOD)."
        echo "=== EOIR rebuild finished at $(date) ==="
        exit 0
    fi
    echo "New data found (remote: $REMOTE_MOD, local: $LOCAL_MOD)."
else
    echo "No previous build timestamp found. Proceeding with rebuild."
fi

# --- Rebuild ---
echo "New data found, rebuilding..."

echo "[1/3] Building database..."
uv run python build_database.py

echo "[2/3] Validating database..."
if [[ -f validate_database.py ]]; then
    uv run python validate_database.py
else
    echo "  No validate_database.py found, skipping validation."
fi

echo "[3/3] Uploading to Hugging Face..."
uv run python publish_to_hf.py --repo "$HF_REPO" --token "$HF_TOKEN"

# --- Save timestamp ---
if [[ -n "$REMOTE_MOD" ]]; then
    echo "$REMOTE_MOD" > "$LAST_MOD_FILE"
fi

echo "Upload complete."
echo "=== EOIR rebuild finished at $(date) ==="

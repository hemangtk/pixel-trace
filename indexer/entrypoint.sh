#!/bin/bash
set -euo pipefail

echo "ENTRYPOINT: Starting indexer..."

# If SERVICE_ACCOUNT_URL provided, download it to /app/serviceAcc.json
if [ -n "${SERVICE_ACCOUNT_URL:-}" ]; then
  echo "Downloading service account from SERVICE_ACCOUNT_URL..."
  # attempt download (supports basic https url; if private, use signed URL)
  curl -fsSL "$SERVICE_ACCOUNT_URL" -o /app/serviceAcc.json || { echo "Failed to download service account file"; exit 2; }
  export SERVICE_ACCOUNT_FILE="/app/serviceAcc.json"
fi

# debug print (only in dev)
echo "ENV: DRIVE_FOLDER_ID=${DRIVE_FOLDER_ID:-"<not-set>"} EVENT_NAME=${EVENT_NAME:-"<not-set>"} OWNER_ID=${OWNER_ID:-public}"

# run the indexer (allow additional CLI args)
python3 indexer.py "$@" 2>&1 | tee /tmp/indexer.log
EXIT_CODE=${PIPESTATUS[0]}

# post manifest if exists
OWNER_ID=${OWNER_ID:-public}
EVENT_NAME=${EVENT_NAME:-event}
MANIFEST_FILE="./index_manifest_${OWNER_ID}_${EVENT_NAME}.json"

if [ -f "$MANIFEST_FILE" ]; then
  if [ -n "${CALLBACK_URL:-}" ]; then
    echo "Posting manifest to $CALLBACK_URL"
    curl -s -X POST -H "Content-Type: application/json" -H "x-runpod-secret: ${WEBHOOK_SECRET:-}" -d @"$MANIFEST_FILE" "${CALLBACK_URL}" || true
  else
    echo "CALLBACK_URL not provided; manifest saved to $MANIFEST_FILE"
  fi
else
  echo "Manifest file not found: $MANIFEST_FILE"
fi

exit $EXIT_CODE

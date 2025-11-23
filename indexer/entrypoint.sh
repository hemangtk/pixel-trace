#!/bin/bash
set -euo pipefail

echo "ENTRYPOINT: Starting indexer..."

# Correct way: Service account JSON file from the secret
export SERVICE_ACCOUNT_FILE="/runpod_secrets/GCP_SERVICE_ACCOUNT"

echo "Service account file present: $SERVICE_ACCOUNT_FILE"

# Run indexer
python3 indexer.py "$@" 2>&1 | tee /tmp/indexer.log
EXIT_CODE=${PIPESTATUS[0]}

OWNER_ID=${OWNER_ID:-public}
EVENT_NAME=${EVENT_NAME:-event}
MANIFEST_FILE="./index_manifest_${OWNER_ID}_${EVENT_NAME}.json"

if [ -f "$MANIFEST_FILE" ]; then
  echo "Manifest written."
else
  echo "Manifest NOT found."
fi

exit $EXIT_CODE

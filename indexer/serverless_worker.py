import runpod
import os
import subprocess
import json

def handler(event):
    input_data = event["input"]

    drive_folder = input_data["drive_folder_id"]
    owner_id = input_data.get("owner_id", "public")
    event_name = input_data.get("event_name", "event")

    # Environment for indexer
    os.environ["DRIVE_FOLDER_ID"] = drive_folder
    os.environ["OWNER_ID"] = owner_id
    os.environ["EVENT_NAME"] = event_name

    # Qdrant secrets
    os.environ["QDRANT_URL"] = os.environ.get("QDRANT_URL", "")
    os.environ["QDRANT_API_KEY"] = os.environ.get("QDRANT_API_KEY", "")

    # Service account file
    os.environ["SERVICE_ACCOUNT_FILE"] = os.environ.get("GCP_SERVICE_ACCOUNT", "")

    process = subprocess.Popen(
        ["/bin/bash", "/app/entrypoint.sh"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    logs = []
    for line in process.stdout:
        logs.append(line.strip())
    process.wait()

    manifest_path = f"/app/index_manifest_{owner_id}_{event_name}.json"
    manifest = None

    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
    else:
        manifest = {"status": "manifest not found"}

    return {
        "exit_code": process.returncode,
        "logs": logs[-60:], 
        "manifest": manifest
    }

runpod.serverless.start({"handler": handler})

import runpod
import os
import subprocess
import json

def handler(event):
    try:
        print("Handler started")
        input_data = event["input"]

        print(f"Received input: {json.dumps(input_data)}")

        drive_folder = input_data["drive_folder_id"]
        owner_id = input_data.get("owner_id", "public")
        event_name = input_data.get("event_name", "event")

        print(f"Processing: drive_folder={drive_folder}, owner={owner_id}, event={event_name}")

        # Environment for indexer
        os.environ["DRIVE_FOLDER_ID"] = drive_folder
        os.environ["OWNER_ID"] = owner_id
        os.environ["EVENT_NAME"] = event_name

        # Qdrant secrets
        os.environ["QDRANT_URL"] = os.environ.get("QDRANT_URL", "")
        os.environ["QDRANT_API_KEY"] = os.environ.get("QDRANT_API_KEY", "")

        # Service account file
        os.environ["SERVICE_ACCOUNT_FILE"] = os.environ.get("GCP_SERVICE_ACCOUNT", "")

        print("Starting subprocess...")
        print(f"DEBUG: Set DRIVE_FOLDER_ID={os.environ.get('DRIVE_FOLDER_ID')}")
        print(f"DEBUG: Set OWNER_ID={os.environ.get('OWNER_ID')}")
        print(f"DEBUG: Set EVENT_NAME={os.environ.get('EVENT_NAME')}")

        # Explicitly pass environment to subprocess
        env = os.environ.copy()
        print(f"DEBUG: Env copy has DRIVE_FOLDER_ID={env.get('DRIVE_FOLDER_ID')}")

        process = subprocess.Popen(
            ["/bin/bash", "/app/entrypoint.sh"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env
        )

        logs = []
        for line in process.stdout:
            print(f"[subprocess] {line.strip()}")
            logs.append(line.strip())
        process.wait()

        print(f"Subprocess finished with exit code: {process.returncode}")

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
    except Exception as e:
        print(f"ERROR in handler: {e}")
        import traceback
        traceback.print_exc()
        return {
            "exit_code": -1,
            "logs": [f"Handler error: {str(e)}"],
            "manifest": {"error": str(e)}
        }

runpod.serverless.start({"handler": handler})

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

        # Build environment dictionary explicitly
        env = os.environ.copy()

        # Override with our specific values
        env["DRIVE_FOLDER_ID"] = drive_folder
        env["OWNER_ID"] = owner_id
        env["EVENT_NAME"] = event_name
        env["SERVICE_ACCOUNT_FILE"] = "/runpod_secrets/GCP_SERVICE_ACCOUNT"

        print("Starting subprocess...")
        print(f"DEBUG: Env has DRIVE_FOLDER_ID={env.get('DRIVE_FOLDER_ID')}")
        print(f"DEBUG: Env has OWNER_ID={env.get('OWNER_ID')}")
        print(f"DEBUG: Env has EVENT_NAME={env.get('EVENT_NAME')}")

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

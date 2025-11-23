# indexer/check_drive_perms.py
import json, os, sys
from google.oauth2 import service_account
from googleapiclient.discovery import build

SA_PATH = "indexer/serviceAcc.json"
FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or "1Zw0C4nt1o5x3hfweSwSsH6TnVRc4A0rj"

creds = service_account.Credentials.from_service_account_file(
    SA_PATH, scopes=["https://www.googleapis.com/auth/drive.metadata.readonly"]
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

# list files
resp = drive.files().list(q=f"'{FOLDER_ID}' in parents and trashed=false",
                          fields="files(id,name,owners,shared,permissions)").execute()
files = resp.get("files", [])
print(f"Found {len(files)} files in folder {FOLDER_ID}")
for f in files:
    owners = [o.get("emailAddress") for o in f.get("owners", [])]
    perms = f.get("permissions", [])
    print("----")
    print("id:", f["id"])
    print("name:", f["name"])
    print("owners:", owners)
    print("shared:", f.get("shared"))
    if perms:
        for p in perms:
            print(" perm:", p.get("role"), p.get("type"), p.get("emailAddress", p.get("domain", p.get("id"))))
    else:
        print(" perm: <none listed>")

# If folder has subfolders, recursively list them (optional)

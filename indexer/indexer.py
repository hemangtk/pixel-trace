#!/usr/bin/env python3
"""
indexer.py

- Supports:
  * --local-folder : read images recursively from local path (useful for local tests)
  * --folder-id    : Google Drive folder ID (production flow)
  * env vars for credentials, qdrant, etc.

Expect these env vars for cloud run:
  SERVICE_ACCOUNT_FILE (path to serviceAcc.json) or place file in same dir
  QDRANT_URL
  QDRANT_API_KEY
  COLLECTION_NAME (optional, default: pixeltrace_faces)
  MODEL_DIR (optional)
  MAX_DIM (optional)
  PROVIDER_CTX (optional) - set to -1 for CPU, 0 for first GPU
"""

import os
import sys
import time
import io
import json
import argparse
import traceback
import uuid
import threading
import concurrent.futures
from pathlib import Path

import numpy as np
from tqdm import tqdm

# image libs
import cv2
from PIL import Image
import rawpy
import pyheif
import requests

# google drive
from google.oauth2 import service_account
from googleapiclient.discovery import build

# insightface & onnx/ort
FaceAnalysis = None

# qdrant
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance

os.system("ls -R /app/.insightface/models")

# ---------------- configuration defaults ----------------
MAX_DIM = int(os.getenv("MAX_DIM", "800"))
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "serviceAcc.json")
QDRANT_URL = os.getenv("QDRANT_URL", None)
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", None)
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "pixeltrace_faces")
MODEL_DIR = os.getenv("MODEL_DIR", "models/antelope")
CTX_ID = int(os.getenv("PROVIDER_CTX", "0"))  # use -1 for CPU
_MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

# threading lock for collection creation
_collection_lock = threading.Lock()
_collection_created = False
_collection_dim = None

# ---------------- clients (initialized later) ----------------
drive_service = None
drive_creds = None
app = None
qdrant = None

# ---------------- helpers ----------------
def ensure_qdrant_client():
    global qdrant
    if qdrant is not None:
        return
    if not QDRANT_URL:
        raise SystemExit("QDRANT_URL env var must be set")
    qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

# add near other helpers (ensure_qdrant_client etc.)
def ensure_collection(dim):
    """
    Ensure Qdrant collection exists with specified dim.
    Thread-safe.
    """
    global _collection_created, _collection_dim, qdrant
    ensure_qdrant_client()
    if _collection_created and _collection_dim == dim:
        return
    with _collection_lock:
        if _collection_created and _collection_dim == dim:
            return
        try:
            qdrant.get_collection(collection_name=COLLECTION_NAME)
            _collection_created = True
            _collection_dim = dim
            return
        except Exception:
            qdrant.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )
            _collection_created = True
            _collection_dim = dim
            return


def ensure_insightface():
    global app, FaceAnalysis, MODEL_DIR
    if app is not None:
        return

    print("Initializing InsightFace FaceAnalysis...")

    # ROOT for insightface
    ROOT_DIR = "/app/.insightface"
    INSIGHT_ROOT = os.path.join(ROOT_DIR, "models")

    # Correct MODEL_DIR
    os.makedirs(INSIGHT_ROOT, exist_ok=True)
    MODEL_DIR = os.path.join(INSIGHT_ROOT, "antelopev2")
    os.makedirs(MODEL_DIR, exist_ok=True)

    detection_dir = os.path.join(MODEL_DIR, "detection")

    # 1) Download model into INSIGHT_ROOT (important!)
    if not os.path.exists(detection_dir):
        print("Downloading antelopev2 model manually...")
        import zipfile, requests

        zip_path = os.path.join(INSIGHT_ROOT, "antelopev2.zip")

        with open(zip_path, "wb") as f:
            f.write(requests.get(
                "https://github.com/deepinsight/insightface/releases/download/v0.7/antelopev2.zip"
            ).content)

        print("Extracting antelopev2 model...")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(INSIGHT_ROOT)   # extract *into* INSIGHT_ROOT
        print("Extraction complete.")

    # 2) Import AFTER extraction
    if FaceAnalysis is None:
        from insightface.app import FaceAnalysis as _FA
        FaceAnalysis = _FA

    providers = [
        "CUDAExecutionProvider",
        "CPUExecutionProvider"
    ]
  

    app = FaceAnalysis(
        name="antelopev2",
        root=ROOT_DIR,        # CRITICAL: must point to .insightface
        providers=providers
    )
    import onnxruntime as ort
    print("ORT Providers:", ort.get_available_providers())

    app.prepare(ctx_id=0 if "CUDAExecutionProvider" in providers else -1,det_size=(640, 640))


    print("Loaded models:", list(app.models.keys()))
    print("Antelopev2 initialized successfully!")

def ensure_drive_service():
    global drive_service, drive_creds

    if drive_service is not None:
        return

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise RuntimeError(f"Service account file not found at {SERVICE_ACCOUNT_FILE}")

    drive_creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )

    drive_service = build("drive", "v3", credentials=drive_creds)

    print("Initialized Google Drive service")


def decode_image_from_bytes(content_bytes, file_name):
    lower = file_name.lower()
    try:
        if lower.endswith(".heic") or lower.endswith(".heif"):
            heif_file = pyheif.read_heif(content_bytes)
            image = Image.frombytes(
                heif_file.mode,
                heif_file.size,
                heif_file.data,
                "raw",
                heif_file.mode,
                heif_file.stride,
            )
            return cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)

        if lower.endswith(('.nef', '.cr2', '.arw', '.raf')):
            with rawpy.imread(io.BytesIO(content_bytes)) as raw:
                rgb = raw.postprocess()
                return cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)

        arr = np.asarray(bytearray(content_bytes), dtype=np.uint8)
        return cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception as e:
        print(f"decode error for {file_name}: {e}")
        return None

# ---------------- Drive helpers ----------------
def get_all_images_recursive(parent_id):
    ensure_drive_service()
    all_files = []
    page_token = None
    while True:
        response = drive_service.files().list(
            q=f"'{parent_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType, webViewLink)",
            pageSize=500,
            pageToken=page_token
        ).execute()

        items = response.get('files', [])
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                all_files.extend(get_all_images_recursive(item['id']))
            elif item['mimeType'].startswith('image/'):
                all_files.append(item)

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return all_files

# ---------------- processing ----------------
def process_file_upsert(file, owner_id, event_id):
    file_id = file.get('id')
    file_name = file.get('name', f"{file_id}.jpg")

    try:
        # -----------------------------
        # 1) AUTHENTICATED DOWNLOAD
        # -----------------------------
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        fh = io.BytesIO()

        try:
            # Try primary authenticated method: MediaIoBaseDownload
            from googleapiclient.http import MediaIoBaseDownload
            request = drive_service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0)
            content_bytes = fh.getvalue()

        except Exception as e:
            # Primary method failed — fallback to authenticated HTTP GET using SA token
            print(f"Authenticated MediaIoBaseDownload failed for {file_name}: {e}")
            try:
                from google.auth.transport.requests import Request as GoogleRequest
                global drive_creds
                if drive_creds is None:
                    raise RuntimeError("drive_creds not initialized")

                # refresh service account token
                drive_creds.refresh(GoogleRequest())
                token = drive_creds.token
                if not token:
                    raise RuntimeError("Could not obtain access token for service account")

                headers = {"Authorization": f"Bearer {token}"}
                resp = requests.get(url, headers=headers, timeout=60)
                resp.raise_for_status()
                content_bytes = resp.content

            except Exception as e2:
                print(f"Authenticated fallback download also failed for {file_name}: {e2}")
                raise

        # -----------------------------
        # 2) DECODE IMAGE
        # -----------------------------
        img = decode_image_from_bytes(content_bytes, file_name)
        if img is None:
            print(f"❌ Could not read image: {file_name}")
            return 0

        # Resize for speed
        h, w = img.shape[:2]
        if max(h, w) > MAX_DIM:
            scale = MAX_DIM / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))

        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        # -----------------------------
        # 3) FACE DETECTION
        # -----------------------------
        faces = app.get(img_rgb)
        if not faces:
            return 0

        # -----------------------------
        # 4) ENSURE QDRANT COLLECTION EXISTS
        # -----------------------------
        first_dim = len(faces[0].normed_embedding)
        ensure_collection(first_dim)

        # -----------------------------
        # 5) UPSERT FACES
        # -----------------------------
        points = []
        for i, face in enumerate(faces):
            emb = face.normed_embedding.astype(np.float32)
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{file_id}_{i}"))
            payload = {
                "owner_id": owner_id,
                "event_id": event_id,
                "file_id": file_id,
                "file_name": file_name,
                "link": file.get('webViewLink', ''),
                "bbox": face.bbox.tolist() if hasattr(face, "bbox") else None
            }
            points.append({
                "id": point_id,
                "vector": emb.tolist(),
                "payload": payload
            })

        if points:
            qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
            return len(points)

        return 0

    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        return 0

# ---------------- local-folder helpers ----------------
def get_images_from_local_folder(path):
    exts = ('.jpg', '.jpeg', '.png', '.heic', '.heif', '.nef', '.cr2', '.arw', '.raf')
    files = []
    for p in Path(path).rglob('*'):
        if p.is_file() and p.suffix.lower() in exts:
            files.append({
                "id": str(p.absolute()),
                "name": p.name,
                "mimeType": "image/" + p.suffix.lstrip('.'),
                "webViewLink": str(p.absolute())
            })
    return files

def process_local_file_upsert(file, owner_id, event_id):
    # file['id'] is path in local mode
    try:
        path = file['id']
        with open(path, 'rb') as f:
            content_bytes = f.read()
        # reuse process logic by creating a minimal file dict and using decode_image
        fake_file = {"id": path, "name": file['name'], "webViewLink": file['webViewLink']}
        # reuse decode + embedding logic (we call decode_image_from_bytes then process like above)
        img = decode_image_from_bytes(content_bytes, file['name'])
        if img is None:
            print(f"❌ Cannot decode local file {path}")
            return 0

        # resize
        h, w = img.shape[:2]
        if max(h, w) > MAX_DIM:
            scale = MAX_DIM / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))

        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        faces = app.get(img_rgb)
        if not faces:
            return 0

        first_dim = len(faces[0].normed_embedding)
        ensure_collection(first_dim)

        points = []
        for i, face in enumerate(faces):
            emb = face.normed_embedding.astype(np.float32)
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{path}_{i}"))
            payload = {
                "owner_id": owner_id,
                "event_id": event_id,
                "file_id": path,
                "file_name": file['name'],
                "link": file['webViewLink'],
                "bbox": face.bbox.tolist() if hasattr(face, "bbox") else None
            }
            points.append({"id": point_id, "vector": emb.tolist(), "payload": payload})

        if points:
            qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
            return len(points)
        return 0

    except Exception as e:
        print("Error processing local file", file.get('name'), e)
        return 0

# ---------------- main run ----------------
def run_indexing(owner_id, folder_id, event_name, local_folder=None):
    global qdrant
    # init services
    ensure_insightface()
    ensure_qdrant_client()

    start_time = time.time()

    if local_folder:
        files = get_images_from_local_folder(local_folder)
    else:
        ensure_drive_service()
        files = get_all_images_recursive(folder_id)

    print(f"Found {len(files)} images to process")

    total_upserted = 0
    worker_fn = process_local_file_upsert if local_folder else process_file_upsert

    with concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [executor.submit(worker_fn, f, owner_id, event_name) for f in files]
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            try:
                upserted = fut.result()
                total_upserted += upserted
            except Exception as e:
                print("Worker exception:", e)

    duration_min = (time.time() - start_time) / 60.0
    print(f"Indexing complete. Points upserted: {total_upserted}. Time: {duration_min:.2f} minutes.")

    manifest = {
        "owner_id": owner_id,
        "event_id": event_name,
        "files_indexed": len(files),
        "points_upserted": total_upserted,
        "timestamp": time.time()
    }
    manifest_path = f"index_manifest_{owner_id}_{event_name}.json"
    with open(manifest_path, "w") as mf:
        json.dump(manifest, mf, indent=2)

    return manifest_path

if __name__ == "__main__":
    os.system("cat /app/indexer.py | sed -n '1,200p'")
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder-id", dest="folder_id", default=os.getenv("DRIVE_FOLDER_ID"))
    parser.add_argument("--event-name", dest="event_name", default=os.getenv("EVENT_NAME", "event"))
    parser.add_argument("--owner-id", dest="owner_id", default=os.getenv("OWNER_ID", "public"))
    parser.add_argument("--local-folder", dest="local_folder", default=None, help="Local folder for testing")
    args = parser.parse_args()

    if not args.folder_id and not args.local_folder:
        print("Error: must provide --folder-id (Drive) or --local-folder (local test). Exiting.")
        sys.exit(2)

    try:
        manifest_path = run_indexing(owner_id=args.owner_id, folder_id=args.folder_id, event_name=args.event_name, local_folder=args.local_folder)
        print("Wrote manifest:", manifest_path)
    except Exception as e:
        print("Fatal error:", e)
        traceback.print_exc()
        sys.exit(1)

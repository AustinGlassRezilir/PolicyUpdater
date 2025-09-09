"""
sharepoint_uploader.py

Uploads the freshly generated .ctxt bundle (and optional .meta.json files)
into a timestamped run folder under the configured SharePoint parent path.

- Creates parent path segments if missing (Clients/Saul/policies/PolicyBackup)
- Creates a run folder named YYYY-MM-DD_HHMMET (America/New_York)
  * If a name collision occurs, suffixes -1, -2, ... until free
- Uploads files using Microsoft Graph
- Writes a run-level manifest.json (hashes, sizes, IDs, URLs)
- Verifies the upload (counts & sizes)

Environment (.env):
  # Azure AD application credentials for Graph (app-only)
  TENANT_ID=...
  CLIENT_ID=...
  CLIENT_SECRET=...
  SCOPE=https://graph.microsoft.com/.default

  # SharePoint target
  SP_TENANT_HOSTNAME=rezilirhealth.sharepoint.com
  SP_SITE_PATH=sites/RezAI
  SP_PARENT_PATH=Clients/Saul/policies/PolicyBackup

  # Local export directory from the CTXT generator
  EXPORT_DIR=sharepoint_exports

  # Behavior toggles
  SP_UPLOAD_INCLUDE_META=true
  SP_UPLOAD_CREATE_MANIFEST=true
  SP_UPLOAD_VERIFY_SIZE=true
  SP_UPLOAD_MAX_RETRIES=5
  SP_DRY_RUN=false

Usage:
    from sharepoint_uploader import upload_backup_to_sharepoint
    upload_backup_to_sharepoint()

"""
from __future__ import annotations

import os
import re
import json
import time
import math
import hashlib
from dataclasses import dataclass
from typing import Iterable, List, Dict, Tuple
from datetime import datetime

import requests
from dotenv import load_dotenv

# Reuse your token retriever
from token_retriever import get_access_token

load_dotenv()

# ----- Config -----
EXPORT_DIR = os.getenv("EXPORT_DIR", "sharepoint_exports")
HOSTNAME   = os.getenv("SP_TENANT_HOSTNAME", "")
SITE_PATH  = os.getenv("SP_SITE_PATH", "")      # e.g., sites/RezAI
PARENT_PATH= os.getenv("SP_PARENT_PATH", "Clients/Saul/policies/PolicyBackup")

# Optional fast-path IDs (skip discovery). If provided, these override HOSTNAME/SITE_PATH/PARENT_PATH
SP_UPLOAD_SITE_ID = os.getenv("SP_UPLOAD_SITE_ID", "")
SP_UPLOAD_DRIVE_ID = os.getenv("SP_UPLOAD_DRIVE_ID", "")
SP_UPLOAD_PARENT_ITEM_ID = os.getenv("SP_UPLOAD_PARENT_ITEM_ID", "")

# Upload-side credentials (use these if uploading to a different tenant/app); fallback to default token_retriever envs
UPLOAD_TENANT_ID = os.getenv("UPLOAD_TENANT_ID", "")
UPLOAD_CLIENT_ID = os.getenv("UPLOAD_CLIENT_ID", "")
UPLOAD_CLIENT_SECRET = os.getenv("UPLOAD_CLIENT_SECRET", "")
UPLOAD_SCOPE = os.getenv("UPLOAD_SCOPE", os.getenv("SCOPE", "https://graph.microsoft.com/.default"))

INCLUDE_META   = os.getenv("SP_UPLOAD_INCLUDE_META", "true").lower() == "true"
CREATE_MANIFEST= os.getenv("SP_UPLOAD_CREATE_MANIFEST", "true").lower() == "true"
VERIFY_SIZE    = os.getenv("SP_UPLOAD_VERIFY_SIZE", "true").lower() == "true"
MAX_RETRIES    = int(os.getenv("SP_UPLOAD_MAX_RETRIES", "5"))
DRY_RUN        = os.getenv("SP_DRY_RUN", "false").lower() == "true"

GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

INVALID_CHARS = r'[:*?"<>|#%&{}\\\n\r\t]'

@dataclass
class GraphIds:
    site_id: str
    drive_id: str
    parent_item_id: str  # driveItem.id for the configured parent path

@dataclass
class RunFolder:
    name: str
    item_id: str
    web_url: str


def _get_upload_access_token() -> str:
    """Get an access token for the UPLOAD tenant/app if configured; otherwise fall back to default token_retriever envs."""
    if UPLOAD_TENANT_ID and UPLOAD_CLIENT_ID and UPLOAD_CLIENT_SECRET:
        token_url = f"https://login.microsoftonline.com/{UPLOAD_TENANT_ID}/oauth2/v2.0/token"
        payload = {
            "client_id": UPLOAD_CLIENT_ID,
            "scope": UPLOAD_SCOPE,
            "client_secret": UPLOAD_CLIENT_SECRET,
            "grant_type": "client_credentials",
        }
        resp = requests.post(token_url, data=payload)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("Failed to retrieve upload access token")
        return token
    # Fallback: use the project's default token
    return get_access_token()


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_upload_access_token()}",
    }


def _req(method: str, url: str, **kwargs) -> requests.Response:
    """HTTP wrapper with simple retry/backoff on 429/5xx."""
    headers = kwargs.pop("headers", {})
    headers.update(_headers())
    attempt = 0
    backoff = 1.0
    while True:
        attempt += 1
        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt > MAX_RETRIES:
                break
            retry_after = resp.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else backoff
            time.sleep(min(30.0, sleep_for))
            backoff = min(30.0, backoff * 2.0)  # exponential with cap
            continue
        return resp
    resp.raise_for_status()
    return resp


def _resolve_site_and_drive() -> Tuple[str, str]:
    """Resolve (or accept) the upload site & drive.
    Precedence:
      1) SP_UPLOAD_SITE_ID + SP_UPLOAD_DRIVE_ID (use as-is)
      2) SP_UPLOAD_SITE_ID only (look up drive)
      3) HOSTNAME + SITE_PATH (discover site then drive)
    """
    if SP_UPLOAD_SITE_ID and SP_UPLOAD_DRIVE_ID:
        return SP_UPLOAD_SITE_ID, SP_UPLOAD_DRIVE_ID

    if SP_UPLOAD_SITE_ID:
        drive_url = f"{GRAPH_ROOT}/sites/{SP_UPLOAD_SITE_ID}/drive"
        r = _req("GET", drive_url)
        r.raise_for_status()
        return SP_UPLOAD_SITE_ID, r.json()["id"]

    if not HOSTNAME or not SITE_PATH:
        raise RuntimeError("Provide either SP_UPLOAD_SITE_ID (+ optional SP_UPLOAD_DRIVE_ID) or SP_TENANT_HOSTNAME + SP_SITE_PATH in .env")

    site_url = f"{GRAPH_ROOT}/sites/{HOSTNAME}:/{SITE_PATH}"
    r = _req("GET", site_url)
    r.raise_for_status()
    site_id = r.json()["id"]

    drive_url = f"{GRAPH_ROOT}/sites/{site_id}/drive"
    r = _req("GET", drive_url)
    r.raise_for_status()
    drive_id = r.json()["id"]
    return site_id, drive_id


def _ensure_parent_folder(drive_id: str, parent_path: str) -> str:
    """Ensure each segment exists under the drive root; return item id of the deepest folder.
    If SP_UPLOAD_PARENT_ITEM_ID is provided, skip creation and return it directly.
    """
    if SP_UPLOAD_PARENT_ITEM_ID:
        return SP_UPLOAD_PARENT_ITEM_ID

    # Normalize slashes and trim
    parts = [p for p in parent_path.strip("/ ").split("/") if p]
    current_path = ""
    parent_id = None

    # Quick path existence check
    path_url = f"{GRAPH_ROOT}/drives/{drive_id}/root:/{'/'.join(parts)}"
    r = _req("GET", path_url)
    if r.status_code == 200:
        return r.json()["id"]

    # Walk & create as needed
    for i, part in enumerate(parts):
        current_path = "/".join(parts[: i + 1])
        url = f"{GRAPH_ROOT}/drives/{drive_id}/root:/{current_path}"
        r = _req("GET", url)
        if r.status_code == 200:
            parent_id = r.json()["id"]
            continue

        if DRY_RUN:
            print(f"[DRY-RUN] Would create folder: {current_path}")
            continue

        # Create under the immediate parent (path of previous segment)
        if i == 0:
            parent_api = f"{GRAPH_ROOT}/drives/{drive_id}/root/children"
        else:
            prev_path = "/".join(parts[:i])
            parent_api = f"{GRAPH_ROOT}/drives/{drive_id}/root:/{prev_path}:/children"

        payload = {"name": part, "folder": {}}
        r = _req("POST", parent_api, json=payload)
        r.raise_for_status()
        parent_id = r.json()["id"]

    # Final fetch to return the id for the deepest path
    final = _req("GET", f"{GRAPH_ROOT}/drives/{drive_id}/root:/{'/'.join(parts)}")
    final.raise_for_status()
    return final.json()["id"]


def _new_run_folder_name() -> str:
    # Use UTC for run folder naming; no local-time conversions
    from datetime import datetime
    now = datetime.utcnow()
    return now.strftime("%Y-%m-%d_%H%MUTC")


def _create_run_folder(drive_id: str, parent_item_id: str) -> RunFolder:
    base = _new_run_folder_name()

    # Helper to try a specific name
    def try_create(name: str) -> Tuple[bool, dict | None]:
        # Check existence under parent
        exists_url = f"{GRAPH_ROOT}/drives/{drive_id}/items/{parent_item_id}/children"
        r = _req("GET", exists_url)
        r.raise_for_status()
        items = r.json().get("value", [])
        for it in items:
            if it.get("name") == name and it.get("folder") is not None:
                return True, it

        if DRY_RUN:
            print(f"[DRY-RUN] Would create run folder: {name}")
            # Simulate return structure
            return True, {"id": "dry-run-id", "webUrl": "https://contoso/dry-run"}

        payload = {"name": name, "folder": {}}
        create_url = f"{GRAPH_ROOT}/drives/{drive_id}/items/{parent_item_id}/children"
        r = _req("POST", create_url, json=payload)
        if r.status_code in (200, 201):
            return True, r.json()
        return False, None

    # Try base, then -1, -2, ... up to 100
    name = base
    for n in range(0, 101):
        candidate = name if n == 0 else f"{base}-{n}"
        ok, data = try_create(candidate)
        if ok and data:
            return RunFolder(candidate, data.get("id", ""), data.get("webUrl", ""))
    raise RuntimeError("Failed to create a unique run folder name after 100 attempts")


def _sanitize_filename(name: str) -> str:
    name = re.sub(INVALID_CHARS, "_", name)
    name = name.strip().strip(".")  # no trailing spaces or dots
    name = re.sub(r"\s+", " ", name)
    return name or "file"


def _iter_local_files() -> List[Tuple[str, str]]:
    """Return list of (absolute_path, uploaded_name) for files to upload."""
    if not os.path.isdir(EXPORT_DIR):
        raise FileNotFoundError(f"EXPORT_DIR not found: {EXPORT_DIR}")

    chosen: List[Tuple[str, str]] = []
    for fname in os.listdir(EXPORT_DIR):
        if not fname.lower().endswith(".ctxt") and not (INCLUDE_META and fname.lower().endswith(".meta.json")):
            continue
        abs_path = os.path.join(EXPORT_DIR, fname)
        if os.path.isfile(abs_path):
            chosen.append((abs_path, _sanitize_filename(fname)))
    chosen.sort(key=lambda t: t[1].lower())
    return chosen


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _put_small_file(drive_id: str, parent_item_id: str, dest_name: str, local_path: str) -> dict:
    url = f"{GRAPH_ROOT}/drives/{drive_id}/items/{parent_item_id}:/{requests.utils.quote(dest_name) }:/content"
    with open(local_path, "rb") as f:
        data = f.read()
    r = _req("PUT", url, data=data)
    r.raise_for_status()
    return r.json()


def _list_children(drive_id: str, item_id: str) -> list[dict]:
    # Pulls *all* children (handles paging); trims payload to name/size
    url = f"{GRAPH_ROOT}/drives/{drive_id}/items/{item_id}/children?$select=name,size&$top=200"
    items = []
    while True:
        r = _req("GET", url)
        r.raise_for_status()
        data = r.json()
        items.extend(data.get("value", []))
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link
    return items



def upload_backup_to_sharepoint() -> None:
    print("\n=== SharePoint Backup Upload ===")
    print(f"Target site: {HOSTNAME}/{SITE_PATH}")
    print(f"Parent path: {PARENT_PATH}")
    print(f"Export dir : {EXPORT_DIR}")
    if DRY_RUN:
        print("Mode       : DRY-RUN (no changes will be made)")

    # 1) Resolve site & drive
    site_id, drive_id = _resolve_site_and_drive()
    print(f"Using IDs     : site={site_id} | drive={drive_id}")
    parent_item_id = _ensure_parent_folder(drive_id, PARENT_PATH)
    print(f"Parent itemID : {parent_item_id}")

    # 2) Ensure parent folder exists
    parent_item_id = _ensure_parent_folder(drive_id, PARENT_PATH)

    # 3) Create run folder (with auto-suffix if needed)
    run = _create_run_folder(drive_id, parent_item_id)
    print(f"Run folder : {run.name}")
    if run.web_url:
        print(f"Run URL    : {run.web_url}")

    # 4) Enumerate local files
    files = _iter_local_files()
    if not files:
        raise RuntimeError("No files to upload (.ctxt and/or .meta.json)")
    print(f"Uploading  : {len(files)} files")

    # 5) Upload sequentially with retries
    manifest_files = []
    total_bytes = 0

    for local_path, dest_name in files:
        size = os.path.getsize(local_path)
        total_bytes += size
        ftype = "meta" if dest_name.lower().endswith(".meta.json") else "ctxt"
        sha_local = _sha256_file(local_path)

        print(f" - {dest_name} ({size} bytes)")
        if DRY_RUN:
            item = {"id": "dry-run", "name": dest_name, "size": size, "webUrl": "https://contoso/dry"}
        else:
            # per-file retry
            attempt = 0
            while True:
                attempt += 1
                try:
                    item = _put_small_file(drive_id, run.item_id, dest_name, local_path)
                    break
                except requests.HTTPError as e:
                    if attempt > MAX_RETRIES:
                        raise
                    print(f"   retry {attempt}/{MAX_RETRIES} after error: {e}")
                    time.sleep(min(30, 2 ** attempt))

        manifest_files.append({
            "name": dest_name,
            "type": ftype,
            "size_bytes": size,
            "sha256_local": sha_local,
            "sharepoint_item_id": item.get("id", ""),
            "web_url": item.get("webUrl", ""),
        })

    # 6) Create and upload manifest.json last
    started_utc = datetime.utcnow().isoformat() + "Z"
    finished_utc = datetime.utcnow().isoformat() + "Z"
    manifest = {
        "run_folder_name": run.name,
        "run_started_at_utc": started_utc,
        "run_finished_at_utc": finished_utc,
        "timezone": "UTC",
        "site_hostname": HOSTNAME,
        "site_path": SITE_PATH,
        "drive_id": drive_id,
        "parent_item_id": parent_item_id,
        "run_folder_item_id": run.item_id,
        "run_folder_web_url": run.web_url,
        "config": {
            "include_meta": INCLUDE_META,
            "create_manifest": CREATE_MANIFEST,
            "verify_size": VERIFY_SIZE,
            "max_retries": MAX_RETRIES,
            "dry_run": DRY_RUN,
        },
        "counts": {
            "total": len(manifest_files),
            "ctxt": sum(1 for f in manifest_files if f["type"] == "ctxt"),
            "meta": sum(1 for f in manifest_files if f["type"] == "meta"),
            "bytes_total": total_bytes,
        },
        "files": manifest_files,
    }

    if CREATE_MANIFEST:
        manifest_name = "__manifest.json"
        tmp_path = os.path.join(EXPORT_DIR, manifest_name)
        with open(tmp_path, "w", encoding="utf-8") as mf:
            json.dump(manifest, mf, ensure_ascii=False, indent=2)

        print(f" - {manifest_name} ({os.path.getsize(tmp_path)} bytes)")
        if not DRY_RUN:
            _put_small_file(drive_id, run.item_id, manifest_name, tmp_path)

    # 7) Final verification (counts & sizes)
    if not DRY_RUN:
        children = _list_children(drive_id, run.item_id)
        uploaded = {c.get("name"): c for c in children}
        expected = {f["name"]: f for f in manifest_files}
        if CREATE_MANIFEST:
            expected[manifest_name] = {"size_bytes": os.path.getsize(tmp_path)}

        missing = [n for n in expected.keys() if n not in uploaded]
        size_mismatch = []
        if VERIFY_SIZE:
            for n, meta in expected.items():
                item = uploaded.get(n)
                if item and int(item.get("size", -1)) != int(meta.get("size_bytes", -1)):
                    size_mismatch.append((n, meta.get("size_bytes"), item.get("size")))

        if missing or size_mismatch:
            print("\n! Verification failed")
            if missing:
                print("  Missing:", ", ".join(missing))
            if size_mismatch:
                for n, exp, got in size_mismatch:
                    print(f"  Size mismatch: {n} expected {exp} got {got}")
            raise RuntimeError("SharePoint upload verification failed")

    print("\n✅ SharePoint backup complete.")


if __name__ == "__main__":
    upload_backup_to_sharepoint()

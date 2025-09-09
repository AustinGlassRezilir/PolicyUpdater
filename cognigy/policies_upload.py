from __future__ import annotations
import os, argparse, requests
from typing import List, Tuple
from .cognigy_client import CognigyClient, find_latest_run_dir

DEF_EXPORT_DIR = os.getenv("EXPORT_DIR", "sharepoint_exports")

def env_truthy(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def collect_ctxt_files(run_dir: str) -> List[str]:
    files = []
    for name in os.listdir(run_dir):
        if not name.lower().endswith(".ctxt"):
            continue
        if name.startswith("__manifest"):  # safety: skip any manifest-named files
            continue
        files.append(os.path.join(run_dir, name))
    files.sort()
    return files

def upload_ctxt_set(export_root: str, dry_run_env: bool | None = None) -> Tuple[int, List[str], List[Tuple[str, str]], List[dict]]:
    """
    Returns:
      uploaded_count: int
      uploaded_names: [str]          # stems from local .ctxt
      failed: [(filename, error)]
      uploaded_sources: [            # NEW — per file handle
        {"id": "<sourceId>", "name": "<title>"}              # when API returned a source
        or {"job_id": "<jobId>", "name": "<fileName>"}       # when API returned an ingest job
      ]
    """
    export_root_abs = os.path.abspath(export_root)
    run_dir = find_latest_run_dir(export_root_abs)
    print(f"Run folder (resolved): {run_dir}")

    ctxt_paths = collect_ctxt_files(run_dir)
    if not ctxt_paths:
        print(f"⚠️  No .ctxt files found in {run_dir}")
        return (0, [], [], [])

    names = [os.path.basename(p)[:-5] for p in ctxt_paths]  # strip .ctxt
    print(f"Found {len(ctxt_paths)} CTXT files.")
    for n in names[:10]:
        print(f" - {n}")
    if len(names) > 10:
        print(f" ... and {len(names)-10} more")

    dry_run = env_truthy("COGNIGY_DRY_RUN") if dry_run_env is None else dry_run_env
    if dry_run:
        print("[DRY-RUN] No uploads performed (COGNIGY_DRY_RUN=yes).")
        return (0, names, [], [])

    client = CognigyClient()
    uploaded = 0
    failed: List[Tuple[str, str]] = []
    uploaded_sources: List[dict] = []

    for p in ctxt_paths:
        file_name = os.path.basename(p)            # keep ".ctxt" in what we send
        try:
            resp = client.create_source_from_ctxt(p, name=file_name)

            # Two common shapes:
            # 1) Ingest job queued immediately
            if isinstance(resp, dict) and resp.get("type") == "ingestKnowledgeSource":
                job_id = resp.get("_id") or resp.get("id")
                reported = (resp.get("parameters") or {}).get("fileName") or file_name
                print(f"📥 Queued ingest: {reported} (job {job_id})")
                uploaded_sources.append({"job_id": job_id, "name": reported})

            # 2) Source created/returned directly (rare but possible)
            else:
                title = resp.get("name") or resp.get("fileName") or file_name
                # Prefer canonical id helper if present
                sid = resp.get("_id") or resp.get("id") or resp.get("sourceId")
                print(f"✅ Uploaded: {title}")
                uploaded_sources.append({"id": sid, "name": title})

            uploaded += 1

        except requests.exceptions.HTTPError as e:
            try:
                detail = e.response.json()
            except Exception:
                detail = getattr(e.response, "text", str(e))
            failed.append((file_name, f"{e}. Details: {detail}"))
            print(f"❌ Upload failed: {file_name} :: {e}. Details: {detail}")

        except Exception as e:
            failed.append((file_name, str(e)))
            print(f"❌ Upload failed: {file_name} :: {e}")

    print(f"Done. Uploaded {uploaded}/{len(ctxt_paths)}.")
    if failed:
        print("Failures:")
        for n, err in failed:
            print(f" - {n}: {err}")

    return (uploaded, names, failed, uploaded_sources)

def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Upload CTXT files to Cognigy Knowledge Store")
    ap.add_argument("--dir", default=DEF_EXPORT_DIR)
    args = ap.parse_args(argv)
    upload_ctxt_set(args.dir, None)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

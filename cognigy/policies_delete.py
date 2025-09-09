from __future__ import annotations
import argparse
from typing import List, Dict, Tuple, Optional
from .cognigy_client import CognigyClient, get_source_id

DEF_TAG = "policies"

def env_truthy(name: str, default: bool = False) -> bool:
    import os
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in ("1", "true", "yes", "y", "on")

def _has_tag_policies(src: Dict, tag: str) -> bool:
    # Match your working script's shape: metaData.tags (fallback: metadata.tags)
    md = src.get("metaData") or src.get("metadata") or {}
    tags = md.get("tags") or []
    return tag.lower() in [str(t).lower() for t in tags]

def delete_by_tag(tag: str, dry_run_env: bool | None = None) -> Tuple[int, List[str]]:
    client = CognigyClient()
    dry_run = env_truthy("COGNIGY_DRY_RUN") if dry_run_env is None else dry_run_env

    to_delete: List[Dict] = []
    total = 0
    for s in client.list_sources():
        total += 1
        if _has_tag_policies(s, tag):
            sid = get_source_id(s)
            if sid:
                to_delete.append({"id": sid, "name": s.get("name")})

    names = sorted(x["name"] or x["id"] for x in to_delete)
    print(f"Store total: {total}")
    print(f"Matched by tag '{tag}': {len(to_delete)}")
    for n in names:
        print(f" - {n}")

    if dry_run:
        print("[DRY-RUN] No deletions performed (COGNIGY_DRY_RUN=yes).")
        return (0, names)

    deleted = 0
    for x in to_delete:
        sid = x["id"]
        nm = x["name"]
        try:
            client.delete_source(sid)
            deleted += 1
            print(f"✅ Deleted: {nm} ({sid})")
        except Exception as e:
            print(f"❌ Delete failed for {nm} ({sid}): {e}")

    print(f"Done. Deleted {deleted}/{len(to_delete)} by tag '{tag}'.")
    return (deleted, names)

def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Delete Cognigy Knowledge Sources by tag")
    ap.add_argument("--tag", default=DEF_TAG)
    args = ap.parse_args(argv)
    delete_by_tag(args.tag, None)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

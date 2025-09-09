# cognigy/ingestion_verify.py
"""
Run-tag ingestion verification for Cognigy Knowledge Store.

Usage:
  from cognigy.ingestion_verify import wait_for_ingestion_by_tag
  ready, pending = wait_for_ingestion_by_tag(expected_count=137, tag=os.environ["RUN_TAG"])

Success criteria:
  - At least `expected_count` sources exist for the tag, AND
  - Every source is ingested (status 'ready' or chunkCount > 0).

Env (optional):
  INGESTION_TIMEOUT_SEC   default 600  (10m)
  INGESTION_POLL_INTERVAL default 5s
"""

import os
import time
from typing import Dict, List, Tuple

from .cognigy_client import CognigyClient, get_source_id

TIMEOUT  = int(os.getenv("INGESTION_TIMEOUT_SEC", "600"))
INTERVAL = int(os.getenv("INGESTION_POLL_INTERVAL", "5"))

# ---------- helpers ----------

def _has_tag(src: Dict, tag: str) -> bool:
    """True if source carries the tag (checks metaData.tags and metadata.tags)."""
    md = src.get("metaData") or src.get("metadata") or {}
    tags = md.get("tags") or []
    return tag.lower() in [str(t).lower() for t in tags]

def _label(src: Dict) -> str:
    """Human-friendly label for logs: prefer name → fileName → id."""
    md = src.get("metaData") or src.get("metadata") or {}
    return (src.get("name")
            or md.get("fileName")
            or get_source_id(src)
            or "?")

def _needs_detail(src: Dict) -> bool:
    """If summary lacks status/chunk info, we should fetch details."""
    has_status = "status" in src and isinstance(src.get("status"), str)
    has_chunks = ("chunks" in src) or ("chunkCount" in src)
    return not (has_status and has_chunks)

def _is_ingested(src: Dict) -> bool:
    """Treat 'ready' or non-zero chunk count as success; be schema-tolerant."""
    status = (src.get("status") or "").lower()
    if status in ("ready", "completed", "complete", "done"):
        return True
    chunks = src.get("chunks") or {}
    c = chunks.get("count", src.get("chunkCount"))
    try:
        return int(c or 0) > 0
    except Exception:
        return False

def _get_detail(client: CognigyClient, src: Dict) -> Dict:
    """Fetch full object if needed; otherwise return input."""
    if not _needs_detail(src):
        return src
    sid = get_source_id(src)
    if not sid:
        return src  # nothing more we can do
    r = client._request("GET", f"/v2.0/knowledgestores/{client.store_id}/sources/{sid}")
    return r.json()

# ---------- public API ----------

def wait_for_ingestion_by_tag(expected_count: int, tag: str) -> Tuple[List[str], List[str]]:
    """
    Poll the knowledge store for sources carrying `tag` until:
      - at least `expected_count` items exist, and
      - all of them are ingested (ready or chunkCount > 0).

    Returns:
      (ready_labels, pending_labels_at_timeout)

    Notes:
      - Labels are source names (fallback: metaData.fileName / id) for easy logging.
      - If fewer than `expected_count` items are visible yet, the shortfall is logged
        and reflected as '(awaiting creation) xN' placeholders in the pending list.
    """
    client = CognigyClient()
    deadline = time.time() + TIMEOUT
    last_snapshot: Tuple[int, Tuple[str, ...]] = (-1, ())

    while True:
        # 1) List all sources and filter by tag (client handles paging)
        tagged: List[Dict] = []
        for s in client.list_sources():
            if _has_tag(s, tag):
                tagged.append(s)

        # 2) Ensure we have status/chunk info; fetch details only when needed
        ready_labels: List[str] = []
        pending_labels: List[str] = []
        for s in tagged:
            detail = _get_detail(client, s)
            (ready_labels if _is_ingested(detail) else pending_labels).append(_label(detail))

        # 3) Reflect creation shortfall (not all items visible yet)
        shortfall = max(0, expected_count - len(tagged))
        gap_markers = [f"(awaiting creation) +{shortfall}"] if shortfall else []
        snapshot = (len(ready_labels), tuple(sorted(pending_labels + gap_markers)))

        if snapshot != last_snapshot:
            print(
                f"[Ingestion tag={tag}] ready={len(ready_labels)}/{expected_count} | "
                f"seen={len(tagged)}/{expected_count} | "
                f"pending: {', '.join(pending_labels) if pending_labels else ('-' if not shortfall else gap_markers[0])}"
            )
            last_snapshot = snapshot

        # 4) Success criteria
        if len(tagged) >= expected_count and not pending_labels:
            return ready_labels, []

        # 5) Timeout?
        if time.time() >= deadline:
            pending_out = pending_labels + gap_markers
            return ready_labels, pending_out

        time.sleep(INTERVAL)

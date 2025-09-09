from __future__ import annotations
import os, time, re
from typing import Dict, Iterable, List, Optional, Tuple
import requests

DEFAULT_TIMEOUT = 60
MAX_RETRIES = 5
BACKOFF_BASE = 0.8  # seconds base (exponential)

class CognigyClient:
    def __init__(self,
                 base_url: Optional[str] = None,
                 api_key: Optional[str] = None,
                 store_id: Optional[str] = None,
                 project_id: Optional[str] = None):
        self.base_url = (base_url or os.getenv("COGNIGY_API_URL", "")).rstrip("/")
        self.api_key = api_key or os.getenv("COGNIGY_API_KEY", "")
        self.store_id = store_id or os.getenv("COGNIGY_STORE_ID", "")
        self.project_id = project_id or os.getenv("COGNIGY_PROJECT_ID", "")
        if not (self.base_url and self.api_key and self.store_id):
            raise RuntimeError("Missing COGNIGY_API_URL / COGNIGY_API_KEY / COGNIGY_STORE_ID")

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers = kwargs.pop("headers", {})
        headers.update({"X-API-Key": self.api_key, "Accept": "application/json"})
        timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)

        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} transient", response=r)
                r.raise_for_status()
                if r.status_code >= 400:
                    try:
                        detail = r.json()
                    except Exception:
                        detail = r.text
                    raise requests.HTTPError(f"{r.status_code} {r.reason}: {detail}", response=r)

                return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(min(8.0, BACKOFF_BASE * (2 ** (attempt - 1))))
        if last_exc:
            raise last_exc
        raise RuntimeError("Request failed without exception")

    # -------- pagination aligned with your working script --------
    @staticmethod
    def _extract_items_and_next(payload: Dict) -> Tuple[List[Dict], Optional[str], Optional[int]]:
        items = payload.get("items") or payload.get("value") or []
        nxt = payload.get("nextCursor") or payload.get("next") or payload.get("nextLink")
        total = payload.get("total")
        return items, nxt, total

    def list_sources(self, limit: int = 100) -> Iterable[Dict]:
        """Yield sources in the current knowledge store (paged via items/nextCursor)."""
        cursor = None
        while True:
            params = {"limit": str(limit)}
            if cursor:
                params["next"] = cursor  # <— important: 'next', not 'cursor'
            r = self._request("GET", f"/v2.0/knowledgestores/{self.store_id}/sources", params=params)
            data = r.json()
            items, cursor, _ = self._extract_items_and_next(data)
            for it in items:
                yield it
            if not cursor:
                break

    def delete_source(self, source_id: str) -> None:
        self._request("DELETE", f"/v2.0/knowledgestores/{self.store_id}/sources/{source_id}")

    def get_job(self, job_id: str) -> Dict:
        r = self._request("GET", f"/v2.0/jobs/{job_id}")
        return r.json()

    def create_source_from_ctxt(self, file_path: str, name: str | None = None):
        url = f"{self.base_url}/v2.0/knowledgestores/{self.store_id}/sources/upload"
        headers = {
            "X-API-Key": self.api_key,
            "Accept": "application/json",  # ← no Content-Type here
        }
        fname = name or os.path.basename(file_path)
        with open(file_path, "rb") as fh:
            data = {"type": "ctxt"}
            files = {"file": (fname, fh, "application/octet-stream")}  # ← the key change
            r = requests.post(url, headers=headers, data=data, files=files, timeout=120)
        r.raise_for_status()
        return r.json()


# -------- util: latest run folder (unchanged) --------
RUN_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{4}UTC(?:-\d+)?$")

def find_latest_run_dir(root: str) -> str:
    root_abs = os.path.abspath(root)
    if not os.path.isdir(root_abs):
        raise RuntimeError(f"Not a directory: {root_abs}")

    candidates = [d for d in os.listdir(root_abs)
                  if RUN_DIR_PATTERN.match(d) and os.path.isdir(os.path.join(root_abs, d))]
    if not candidates:
        # fallback: allow CTXT files directly in root
        if any(n.lower().endswith(".ctxt") for n in os.listdir(root_abs)):
            return root_abs
        raise RuntimeError(f"No run folders found in {root_abs} and no *.ctxt in root")

    def sort_key(name: str):
        base, _, suffix = name.partition("-")
        return (base, int(suffix) if suffix.isdigit() else -1)

    candidates.sort(key=sort_key)
    return os.path.join(root_abs, candidates[-1])

# -------- util: source id helper (id or _id) --------
def get_source_id(src: Dict) -> Optional[str]:
    return src.get("_id") or src.get("id")

from __future__ import annotations
import os, json, argparse
from typing import List, Set, Dict
from .cognigy_client import CognigyClient, find_latest_run_dir, get_source_id

DEF_TAG = "policies"
DEF_EXPORT_DIR = os.getenv("EXPORT_DIR", "sharepoint_exports")

def collect_local_names(run_dir: str) -> Set[str]:
    names: Set[str] = set()
    for name in os.listdir(run_dir):
        if name.lower().endswith(".ctxt") and not name.startswith("__manifest"):
            names.add(name[:-5])  # strip .ctxt
    return names

def _has_tag_policies(src: Dict, tag: str) -> bool:
    md = src.get("metaData") or src.get("metadata") or {}
    tags = md.get("tags") or []
    return tag.lower() in [str(t).lower() for t in tags]

def verify_against_local(export_root: str, tag: str = DEF_TAG, write_report: bool = True) -> Dict:
    export_root_abs = os.path.abspath(export_root)
    run_dir = find_latest_run_dir(export_root_abs)
    expected = collect_local_names(run_dir)

    client = CognigyClient()
    actual = set()
    for s in client.list_sources():
        if _has_tag_policies(s, tag):
            nm = s.get("name") or get_source_id(s) or ""
            if nm:
                # If you uploaded with full filename, keep as-is; if you upload with stem, align here.
                actual.add(nm if not nm.lower().endswith(".ctxt") else nm[:-5])

    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)

    report = {
        "run_dir": run_dir,
        "tag": tag,
        "expected_count": len(expected),
        "actual_count": len(actual),
        "missing": missing,
        "unexpected": unexpected,
    }

    print(f"Run folder: {run_dir}")
    print(f"Local CTXT (expected) count: {len(expected)}")
    print(f"Store (actual) count for tag='{tag}': {len(actual)}")
    if missing:
        print("\nMissing after upload (present locally, not in store):")
        for n in missing:
            print(f" - {n}")
    if unexpected:
        print("\nUnexpected residual (present in store, not expected locally):")
        for n in unexpected:
            print(f" - {n}")
    print("\nSummary:")
    print(f" expected={len(expected)} actual={len(actual)} missing={len(missing)} unexpected={len(unexpected)}")

    if write_report:
        out = os.path.join(run_dir, "__cognigy_sync_report.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\nReport written: {out}")

    return report

def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Verify Cognigy store matches local CTXT set by tag")
    ap.add_argument("--dir", default=DEF_EXPORT_DIR)
    ap.add_argument("--tag", default=DEF_TAG)
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args(argv)
    verify_against_local(args.dir, args.tag, args.report)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

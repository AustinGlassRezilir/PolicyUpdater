# Main.py
import os, sys, glob
from datetime import datetime, timezone
from dotenv import load_dotenv

# Ensure local imports work when run from anywhere
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

# --- Project modules ---
from page_puller import pull_all_pages
from ctxt_generator import process_files
from sharepoint_uploader import upload_backup_to_sharepoint

from cognigy.policies_delete import delete_by_tag
from cognigy.policies_upload import upload_ctxt_set
from cognigy.policies_verify import verify_against_local

# Run-tag waiter
from cognigy.ingestion_verify import wait_for_ingestion_by_tag

# Email notifier (import as module to avoid symbol import issues)
import notify_after_run as notifier


def env_truthy(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _make_run_tag() -> str:
    existing = os.getenv("RUN_TAG", "").strip()
    if existing:
        return existing
    return "run-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%MUTC")


def _ctxts_include_run_tag(export_dir: str, run_tag: str) -> bool:
    """Peek at 1–2 CTXT files to see if RUN_TAG appears in their tags header."""
    try:
        candidates = sorted(glob.glob(os.path.join(export_dir, "*.ctxt")))[:2]
        for p in candidates:
            with open(p, "r", encoding="utf-8") as f:
                head = f.read(1024)
            if run_tag and run_tag in head:
                return True
    except Exception:
        pass
    return False


def main():
    EXPORT_DIR = os.getenv("EXPORT_DIR", "sharepoint_exports")

    # Granular "dry run / skip" flags
    SKIP_PULL     = env_truthy("SKIP_PULL", False)
    SKIP_GENERATE = env_truthy("SKIP_GENERATE", False)
    SKIP_BACKUP   = env_truthy("SKIP_BACKUP", False)
    SKIP_DELETE   = env_truthy("SKIP_DELETE", False)
    SKIP_UPLOAD   = env_truthy("SKIP_UPLOAD", False)
    SKIP_WAIT     = env_truthy("SKIP_WAIT", False) or SKIP_UPLOAD  # waiting makes no sense if we don't upload
    SKIP_VERIFY   = env_truthy("SKIP_VERIFY", False)
    SKIP_EMAIL    = env_truthy("SKIP_EMAIL", False)

    # 0) Establish a RUN_TAG for this run (and expose to children)
    if not os.getenv("RUN_TAG"):
        os.environ["RUN_TAG"] = _make_run_tag()
    RUN_TAG = os.environ["RUN_TAG"]
    print(f"🏷️  Using RUN_TAG={RUN_TAG}")

    # 1) Pull SharePoint pages → HTML
    if SKIP_PULL:
        print("⏭️  SKIP_PULL=true → skipping page pull; using existing local HTML.")
    else:
        print("🌐 Pulling pages…")
        pull_all_pages()

    # 2) Generate .ctxt from HTML
    if SKIP_GENERATE:
        print("⏭️  SKIP_GENERATE=true → skipping CTXT generation; using existing CTXT files.")
    else:
        print("📝 Generating .ctxt documents…")
        process_files()

    # 3) Upload a backup bundle to SharePoint
    if SKIP_BACKUP:
        print("⏭️  SKIP_BACKUP=true → skipping SharePoint backup of CTXT bundle.")
    else:
        print("📤 Uploading CTXT bundle to SharePoint backup…")
        upload_backup_to_sharepoint()

    # 4) Cognigy: delete existing by 'policies' tag
    if SKIP_DELETE:
        print("⏭️  SKIP_DELETE=true → skipping Cognigy delete step.")
        deleted_count = 0
    else:
        print(f"🔧 COGNIGY_DRY_RUN={env_truthy('COGNIGY_DRY_RUN', False)}")
        print("🧹 Cognigy: deleting sources tagged 'policies'…")
        deleted_count, _ = delete_by_tag(tag="policies")

    # 5) Cognigy: upload the new CTXT set
    if SKIP_UPLOAD:
        print("⏭️  SKIP_UPLOAD=true → skipping Cognigy upload.")
        uploaded_count, uploaded_names, failed = 0, [], []
        uploaded_sources = []
    else:
        print("⬆️  Cognigy: uploading new CTXT set…")
        _upload_result = upload_ctxt_set(export_root=EXPORT_DIR)
        # Accept (count, names, failed) or (count, names, failed, uploaded_sources)
        if isinstance(_upload_result, tuple) and len(_upload_result) == 4:
            uploaded_count, uploaded_names, failed, uploaded_sources = _upload_result
        else:
            uploaded_count, uploaded_names, failed = _upload_result
            uploaded_sources = []  # not needed for run-tag wait
        if failed:
            print("⚠️  Some uploads failed:")
            for name, err in failed:
                print(f" - {name}: {err}")

    # 6) Wait for ingestion by RUN_TAG (fallback to 'policies' if CTXT didn't get the tag)
    if SKIP_WAIT:
        print("⏭️  SKIP_WAIT=true → skipping ingestion wait.")
        ready, pending = [], []
    else:
        wait_tag = RUN_TAG if _ctxts_include_run_tag(EXPORT_DIR, RUN_TAG) else "policies"
        if wait_tag != RUN_TAG:
            print("⚠️  RUN_TAG not found in CTXT headers; falling back to tag='policies' for ingestion wait.")
        print(f"⏳ Waiting for Cognigy ingestion to complete (tag='{wait_tag}')…")
        ready, pending = wait_for_ingestion_by_tag(expected_count=uploaded_count, tag=wait_tag)
        if pending:
            raise RuntimeError(
                f"Ingestion timeout for tag='{wait_tag}': {len(pending)} source(s) still pending\n"
                f"Pending: {', '.join(pending[:10])}{' …' if len(pending) > 10 else ''}"
            )
        print("✅ All sources ingested.")

    # 7) Verify store vs local (by 'policies' tag)
    if SKIP_VERIFY:
        print("⏭️  SKIP_VERIFY=true → skipping verify step.")
        report = {"actual_count": 0, "missing": [], "unexpected": []}
    else:
        print("🔎 Cognigy: verifying results…")
        report = verify_against_local(export_root=EXPORT_DIR, tag="policies", write_report=True)

    # 8) Summary
    print("\n==== COGNIGY SYNC SUMMARY ====")
    print(f"Deleted (tag=policies): {deleted_count} {'(skipped)' if SKIP_DELETE else ''}")
    print(f"Uploaded CTXT         : {uploaded_count} {'(skipped)' if SKIP_UPLOAD else ''}")
    print(f"Store actual (tag)    : {report.get('actual_count', 0)} {'(skipped)' if SKIP_VERIFY else ''}")
    if not SKIP_VERIFY:
        if report["missing"] or report["unexpected"]:
            if report["missing"]:
                print(" - Missing after upload:")
                for n in report["missing"]:
                    print(f"   • {n}")
            if report["unexpected"]:
                print(" - Unexpected residual in store:")
                for n in report["unexpected"]:
                    print(f"   • {n}")
        else:
            print("No differences — store matches local CTXT set.")
    print("\n✅ Pipeline complete.")

    # 9) Send success email
    if SKIP_EMAIL:
        print("⏭️  SKIP_EMAIL=true → not sending success email.")
    else:
        notifier.notify_success(
            run_tag=RUN_TAG,
            uploaded_count=uploaded_count,
            deleted_count=deleted_count,
            report=report,
            ingestion_ready=len(ready),
            expected_count=uploaded_count,
            export_dir=EXPORT_DIR,
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Best-effort failure email
        try:
            if not env_truthy("SKIP_EMAIL", False):
                notifier.notify_failure(
                    run_tag=os.getenv("RUN_TAG", "run-unknown"),
                    error_message=str(e),
                    export_dir=os.getenv("EXPORT_DIR", "sharepoint_exports")
                )
        except Exception as _mail_err:
            print("⚠️  Failed to send failure email:", _mail_err)
        raise

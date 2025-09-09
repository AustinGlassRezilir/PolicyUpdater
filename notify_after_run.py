# notify_after_run.py
import os
from datetime import datetime
from typing import Iterable, Optional, Dict, List, Union

from mail_client import MailClient


# ------------------ helpers ------------------

def _as_list(csv_or_list: Optional[Union[str, Iterable[str]]]) -> List[str]:
    if not csv_or_list:
        return []
    if isinstance(csv_or_list, str):
        return [x.strip() for x in csv_or_list.split(",") if x.strip()]
    return [str(x).strip() for x in csv_or_list if str(x).strip()]

def _attachments_existing(paths: Iterable[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        if p and os.path.exists(p):
            out.append(p)
    return out

def _build_success_bodies(
        run_tag: str,
        uploaded_count: int,
        deleted_count: int,
        report: Dict,
        ingestion_ready: int,
        expected_count: int,
) -> tuple:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    actual = report.get("actual_count", 0)
    missing = report.get("missing", []) or []
    unexpected = report.get("unexpected", []) or []

    plain_lines = [
        "Policies Sync — SUCCESS",
        "Timestamp: {}".format(ts),
        "Run tag : {}".format(run_tag),
        "",
        "Uploaded CTXT : {}".format(uploaded_count),
        "Deleted (policies): {}".format(deleted_count),
        "Ingested (ready)  : {}/{}".format(ingestion_ready, expected_count),
        "Store actual (policies): {}".format(actual),
    ]
    if missing or unexpected:
        plain_lines.append("")
        if missing:
            plain_lines.append("Missing after upload:")
            for n in missing[:50]:
                plain_lines.append("  • {}".format(n))
            if len(missing) > 50:
                plain_lines.append("  …and {} more".format(len(missing)-50))
        if unexpected:
            plain_lines.append("Unexpected residual in store:")
            for n in unexpected[:50]:
                plain_lines.append("  • {}".format(n))
            if len(unexpected) > 50:
                plain_lines.append("  …and {} more".format(len(unexpected)-50))
    else:
        plain_lines.append("")
        plain_lines.append("Store matches local CTXT set (no differences).")

    html_missing = "".join("<li>{}</li>".format(n) for n in missing[:50])
    html_unexp   = "".join("<li>{}</li>".format(n) for n in unexpected[:50])

    html = """
    <html><body>
      <h2>Policies Sync — SUCCESS</h2>
      <p><b>Timestamp:</b> {ts}<br/>
         <b>Run tag:</b> {run_tag}</p>
      <ul>
        <li><b>Uploaded CTXT:</b> {uploaded_count}</li>
        <li><b>Deleted (policies):</b> {deleted_count}</li>
        <li><b>Ingested (ready):</b> {ingestion_ready}/{expected_count}</li>
        <li><b>Store actual (policies):</b> {actual}</li>
      </ul>
      {missing_block}
      {unexpected_block}
    </body></html>
    """.format(
        ts=ts,
        run_tag=run_tag,
        uploaded_count=uploaded_count,
        deleted_count=deleted_count,
        ingestion_ready=ingestion_ready,
        expected_count=expected_count,
        actual=actual,
        missing_block=("<h3>Missing after upload</h3><ul>"+html_missing+"</ul>") if missing else "",
        unexpected_block=("<h3>Unexpected residual in store</h3><ul>"+html_unexp+"</ul>") if unexpected else "<p>No differences — store matches local CTXT set.</p>",
    )

    return "\n".join(plain_lines), html


def _build_failure_bodies(run_tag: str, error_message: str) -> tuple:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    plain = "Policies Sync — FAILED\nTimestamp: {}\nRun tag: {}\n\nDetails:\n{}".format(ts, run_tag, error_message)
    html  = "<html><body><h2>Policies Sync — FAILED</h2><p><b>Timestamp:</b> {}<br/><b>Run tag:</b> {}</p><pre>{}</pre></body></html>".format(ts, run_tag, error_message)
    return plain, html

def _find_manifest_paths(export_dir: str) -> List[str]:
    # Try root-level first
    root_manifest = os.path.join(export_dir, "__manifest.json")
    if os.path.exists(root_manifest):
        return [root_manifest]

    # Otherwise, look for the most recent subfolder that contains __manifest.json
    try:
        subdirs = [
            os.path.join(export_dir, d)
            for d in os.listdir(export_dir)
            if os.path.isdir(os.path.join(export_dir, d))
        ]
        # sort newest first by modified time
        subdirs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for d in subdirs:
            cand = os.path.join(d, "__manifest.json")
            if os.path.exists(cand):
                return [cand]
    except Exception:
        pass
    return []



# ------------------ public API ------------------

def notify_success(
        *,
        run_tag: str,
        uploaded_count: int,
        deleted_count: int,
        report: Dict,
        ingestion_ready: int,
        expected_count: int,
        export_dir: str,
        extra_attachments: Optional[Iterable[str]] = None,
) -> None:
    """
    Send a SUCCESS email. Tries to include HTML; falls back to plain text
    if your MailClient doesn't accept an html_body kwarg.
    """
    SMTP_USER = os.getenv("SMTP_USER", "")
    SMTP_PASS = os.getenv("SMTP_PASS", "")
    MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)
    MAIL_TO   = _as_list(os.getenv("MAIL_TO", ""))

    if not (SMTP_USER and SMTP_PASS and MAIL_FROM and MAIL_TO):
        print("⚠️  Email not sent: SMTP_USER/SMTP_PASS/MAIL_FROM/MAIL_TO not fully configured.")
        return

    subject = "[Policies Sync] SUCCESS — {}".format(run_tag)
    plain, html = _build_success_bodies(
        run_tag=run_tag,
        uploaded_count=uploaded_count,
        deleted_count=deleted_count,
        report=report,
        ingestion_ready=ingestion_ready,
        expected_count=expected_count,
    )

    default_attach = [
                         os.path.join(export_dir, "__cognigy_sync_report.json"),
                     ] + _find_manifest_paths(export_dir)

    attachments = _attachments_existing(list(default_attach) + list(extra_attachments or []))

    client = MailClient(SMTP_USER, SMTP_PASS)
    try:
        client.send_email_with_attachments(
            subject=subject,
            body=plain,
            to_emails=MAIL_TO,
            from_email=MAIL_FROM,
            attachments=attachments,
            html_body=html,  # will be ignored if your MailClient doesn't accept it
        )
    except TypeError:
        client.send_email_with_attachments(
            subject=subject,
            body=plain,
            to_emails=MAIL_TO,
            from_email=MAIL_FROM,
            attachments=attachments,
        )


def notify_failure(
        *,
        run_tag: str,
        error_message: str,
        export_dir: Optional[str] = None,
        extra_attachments: Optional[Iterable[str]] = None,
) -> None:
    """
    Send a FAILURE email. Tries to include HTML; falls back to plain text
    if your MailClient doesn't accept an html_body kwarg.
    """
    SMTP_USER = os.getenv("SMTP_USER", "")
    SMTP_PASS = os.getenv("SMTP_PASS", "")
    MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)
    MAIL_TO   = _as_list(os.getenv("MAIL_TO", ""))

    if not (SMTP_USER and SMTP_PASS and MAIL_FROM and MAIL_TO):
        print("⚠️  Failure email not sent: SMTP_USER/SMTP_PASS/MAIL_FROM/MAIL_TO not fully configured.")
        return

    subject = "[Policies Sync] FAILED — {}".format(run_tag)
    plain, html = _build_failure_bodies(run_tag, error_message)

    attachments: List[str] = []
    if export_dir:
        attachments = _attachments_existing(
            [os.path.join(export_dir, "__cognigy_sync_report.json")]
            + _find_manifest_paths(export_dir)
            + list(extra_attachments or [])
        )

    client = MailClient(SMTP_USER, SMTP_PASS)
    try:
        client.send_email_with_attachments(
            subject=subject,
            body=plain,
            to_emails=MAIL_TO,
            from_email=MAIL_FROM,
            attachments=attachments,
            html_body=html,
        )
    except TypeError:
        client.send_email_with_attachments(
            subject=subject,
            body=plain,
            to_emails=MAIL_TO,
            from_email=MAIL_FROM,
            attachments=attachments,
        )

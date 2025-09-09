# mail_client.py
import smtplib
from email.message import EmailMessage
import mimetypes
import os
from typing import Iterable, Optional

class MailClient:
    """
    Simple SMTP client for sending audit notifications with optional attachments.
    Compatible with Office365 (default host/port).
    """
    def __init__(self, smtp_user: str, smtp_password: str,
                 smtp_server: str = "smtp.office365.com", smtp_port: int = 587):
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def send_email_with_attachments(
            self,
            subject: str,
            body: str,
            to_emails: list,
            from_email: str,
            attachments: Optional[Iterable[str]] = None,
            html_body: Optional[str] = None
    ):
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = ", ".join([t for t in to_emails if t])

        # Plain + (optional) HTML
        msg.set_content(body or "")
        if html_body:
            msg.add_alternative(html_body, subtype="html")

        # Attach files (skip missing)
        for file_path in attachments or []:
            if not file_path or not os.path.exists(file_path):
                continue
            with open(file_path, "rb") as f:
                data = f.read()
            mime_type, _ = mimetypes.guess_type(file_path)
            maintype, subtype = mime_type.split("/") if mime_type else ("application", "octet-stream")
            msg.add_attachment(data, maintype=maintype, subtype=subtype,
                               filename=os.path.basename(file_path))

        # Send
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as smtp:
                smtp.starttls()
                smtp.login(self.smtp_user, self.smtp_password)
                smtp.send_message(msg)
        except Exception as e:
            # Bubble up to caller so the pipeline can notify failure upstream
            raise RuntimeError(f"SMTP send failed: {e}") from e

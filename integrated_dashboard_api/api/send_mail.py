from email.message import EmailMessage
import smtplib
from pydantic import BaseModel
from typing import Optional, List


class Usermodel(BaseModel):
    to: List[str] = [""]
    sender: str = "dfs@walmart.com"
    subject: str = ""
    message: str = ""
    cc: Optional[List[str]] = [""]
    bcc: Optional[List[str]] = [""]


class Mail:
    """Sends an alert mail with the HTML payload passed from UI"""

    def __init__(self):
        """Establish SMTP connection"""
        self.server = smtplib.SMTP("vsitemta.walmart.com", 25)
        self.msg = EmailMessage()

    def send_mail(self, payload: Usermodel):
        try:
            self.msg["Subject"] = payload.subject
            self.msg["From"] = payload.sender
            self.msg["To"] = ",".join(payload.to)
            if payload.bcc:
                self.msg["bcc"] = ",".join(payload.bcc)
            if payload.cc:
                self.msg["Cc"] = ",".join(payload.cc)
            self.msg.add_alternative(payload.message, subtype="html")
            self.server.send_message(msg=self.msg)
            self.server.quit()
        except Exception as err:
            print(f"Mail delivery failed. {err} ocurred...")
            self.server.quit()
        return {"msg": "Mail sent successfully"}


if __name__ == "__main__":
    mail_entries_obj = Mail()
    mail_entries_obj.send_mail(
        Usermodel(
            **{
                "to": ["Vijay.Bhatkar.Bhatkar@walmart.com"],
                "cc": ["Parthasarathi.Sukla@walmart.com"],
                "subject": "This is alert testing email",
                "message": """<h2>HTML Content Goes here!<h2>""",
            }
        )
    )

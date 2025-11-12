from email.message import EmailMessage
from dotenv import load_dotenv
import aiosmtplib
from app.core.config import Config

load_dotenv()

SMTP_SERVER = Config.BREVO_SMTP_SERVER
SMTP_PORT = Config.BREVO_SMTP_PORT
SMTP_USER = Config.BREVO_SMTP_USERNAME
SMTP_PASS = Config.BREVO_SMTP_PASSWORD
SENDER_EMAIL = Config.SENDER_EMAIL


async def send_email(to: str, subject: str, body: str):
    """Send email using Brevo SMTP (async)"""
    
    message = EmailMessage()
    message["From"] = SENDER_EMAIL
    message["To"] = to
    message["Subject"] = subject
    message.set_content(body)

    try:
        await aiosmtplib.send(
            message,
            hostname=SMTP_SERVER,
            port=SMTP_PORT,
            start_tls=True,
            username=SMTP_USER,
            password=SMTP_PASS
        )
        print(f"✅ Email sent to {to}")

    except Exception as ex:
        print(f"❌ Email send failed: {ex}")

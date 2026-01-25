import os
from twilio.rest import Client

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "").strip()

_twilio = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    _twilio = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

def send_whatsapp(to_number: str, body: str) -> str:
    """
    Sends WhatsApp message via Twilio.
    Returns message SID if successful.
    Raises on error.
    """
    if not _twilio:
        raise RuntimeError("Twilio client not configured (missing SID/AUTH).")
    if not TWILIO_WHATSAPP_NUMBER:
        raise RuntimeError("TWILIO_WHATSAPP_NUMBER not set.")

    to_number = (to_number or "").strip()
    if not to_number.startswith("whatsapp:"):
        to_number = "whatsapp:" + to_number

    msg = _twilio.messages.create(
        from_=TWILIO_WHATSAPP_NUMBER,
        to=to_number,
        body=(body or "").strip()
    )
    return msg.sid

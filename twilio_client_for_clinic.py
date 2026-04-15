from twilio.rest import Client
from clinic_twilio import get_twilio_profile


def get_twilio_client_for_clinic(clinic_id: str):
    profile = get_twilio_profile(clinic_id)

    sid = (profile.get("subaccount_sid") or "").strip()
    token = (profile.get("subaccount_auth_token") or "").strip()

    if not sid or not token:
        raise RuntimeError(f"Clinic {clinic_id} missing Twilio subaccount credentials")

    return Client(sid, token)


def get_clinic_sender(clinic_id: str) -> str:
    profile = get_twilio_profile(clinic_id)
    sender = (profile.get("whatsapp_sender") or "").strip()

    if not sender:
        raise RuntimeError(f"Clinic {clinic_id} missing WhatsApp sender")

    return sender
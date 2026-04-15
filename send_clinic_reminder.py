import json

from twilio_client_for_clinic import get_twilio_client_for_clinic, get_clinic_sender
from clinic_twilio import get_twilio_profile
from clinic_readiness import clinic_can_send_reminders


def _normalize_whatsapp_number(number: str) -> str:
    n = (number or "").strip()
    if not n:
        return n
    if n.startswith("whatsapp:"):
        return n
    return f"whatsapp:{n}"


def send_appointment_reminder(
    clinic_id: str,
    to_number: str,
    patient_name: str,
    clinic_name: str,
    appt_date: str,
    appt_time: str
):
    ok, reason = clinic_can_send_reminders(clinic_id)
    if not ok:
        raise RuntimeError(f"Reminder blocked: {reason}")

    client = get_twilio_client_for_clinic(clinic_id)
    sender = _normalize_whatsapp_number(get_clinic_sender(clinic_id))
    to_number = _normalize_whatsapp_number(to_number)

    profile = get_twilio_profile(clinic_id)
    template = profile.get("templates", {}).get("appointment_reminder", {})
    content_sid = (template.get("content_sid") or "").strip()

    if not content_sid:
        raise RuntimeError("Missing appointment reminder content_sid")

    variables = {
        "1": patient_name or "Patient",
        "2": clinic_name or "Our Clinic",
        "3": appt_date or "",
        "4": appt_time or ""
    }

    print(
        f"[REMINDER_SEND] Sending template reminder "
        f"clinic_id={clinic_id} to={to_number} from={sender} content_sid={content_sid}"
    )

    message = client.messages.create(
        from_=sender,
        to=to_number,
        content_sid=content_sid,
        content_variables=json.dumps(variables)
    )

    print(
        f"[REMINDER_SEND] Twilio accepted reminder "
        f"clinic_id={clinic_id} sid={message.sid} status={getattr(message, 'status', None)}"
    )

    return {
        "sid": message.sid,
        "status": getattr(message, "status", None)
    }
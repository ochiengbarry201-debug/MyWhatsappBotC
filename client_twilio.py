import json
from db import db_conn


def get_clinic_settings(clinic_id: str) -> dict:
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        "SELECT settings FROM clinic_settings WHERE clinic_id = %s",
        (clinic_id,)
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return {}

    settings = row[0] or {}
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except Exception:
            settings = {}

    return settings


def save_clinic_settings(clinic_id: str, settings: dict):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO clinic_settings (clinic_id, settings)
        VALUES (%s, %s)
        ON CONFLICT (clinic_id)
        DO UPDATE SET settings = EXCLUDED.settings, updated_at = NOW()
        """,
        (clinic_id, json.dumps(settings))
    )
    conn.commit()
    conn.close()


def ensure_twilio_settings(clinic_id: str) -> dict:
    settings = get_clinic_settings(clinic_id)

    if "twilio" not in settings:
        settings["twilio"] = {
            "parent_account_sid": "",
            "subaccount_sid": "",
            "subaccount_auth_token": "",
            "whatsapp_sender": "",
            "waba_id": "",
            "business_name": "",
            "onboarding_status": "draft",
            "template_language": "en",
            "templates": {
                "appointment_reminder": {
                    "friendly_name": "appointment_reminder",
                    "content_sid": "",
                    "status": "not_created"
                }
            }
        }
        save_clinic_settings(clinic_id, settings)

    return settings


def update_twilio_fields(clinic_id: str, updates: dict):
    settings = ensure_twilio_settings(clinic_id)
    twilio = settings.get("twilio", {})

    for k, v in updates.items():
        twilio[k] = v

    settings["twilio"] = twilio
    save_clinic_settings(clinic_id, settings)


def update_template_info(
    clinic_id: str,
    template_key: str,
    friendly_name: str = None,
    content_sid: str = None,
    status: str = None
):
    settings = ensure_twilio_settings(clinic_id)
    twilio = settings.get("twilio", {})
    templates = twilio.get("templates", {})

    if template_key not in templates:
        templates[template_key] = {
            "friendly_name": friendly_name or template_key,
            "content_sid": "",
            "status": "not_created"
        }

    if friendly_name is not None:
        templates[template_key]["friendly_name"] = friendly_name
    if content_sid is not None:
        templates[template_key]["content_sid"] = content_sid
    if status is not None:
        templates[template_key]["status"] = status

    twilio["templates"] = templates
    settings["twilio"] = twilio
    save_clinic_settings(clinic_id, settings)


def get_twilio_profile(clinic_id: str) -> dict:
    settings = ensure_twilio_settings(clinic_id)
    return settings.get("twilio", {})
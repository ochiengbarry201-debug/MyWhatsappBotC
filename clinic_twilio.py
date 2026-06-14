import json
import psycopg2.extras

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

    if not isinstance(settings, dict):
        settings = {}

    return settings


def save_clinic_settings(clinic_id: str, settings: dict):
    if not isinstance(settings, dict):
        raise ValueError("settings must be a dict")

    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO clinic_settings (clinic_id, settings)
        VALUES (%s, %s)
        ON CONFLICT (clinic_id)
        DO UPDATE SET settings = EXCLUDED.settings, updated_at = NOW()
        """,
        (clinic_id, psycopg2.extras.Json(settings))
    )
    conn.commit()
    conn.close()


def _default_twilio_settings() -> dict:
    return {
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


def ensure_twilio_settings(clinic_id: str) -> dict:
    settings = get_clinic_settings(clinic_id)

    twilio = settings.get("twilio")
    if not isinstance(twilio, dict):
        twilio = _default_twilio_settings()
    else:
        defaults = _default_twilio_settings()
        merged = defaults.copy()
        merged.update({k: v for k, v in twilio.items() if k != "templates"})

        templates = twilio.get("templates", {})
        if not isinstance(templates, dict):
            templates = {}

        default_templates = defaults.get("templates", {})
        merged_templates = default_templates.copy()
        for key, value in templates.items():
            if isinstance(value, dict):
                merged_templates[key] = {
                    "friendly_name": value.get("friendly_name", key),
                    "content_sid": value.get("content_sid", ""),
                    "status": value.get("status", "not_created"),
                }

        merged["templates"] = merged_templates
        twilio = merged

    settings["twilio"] = twilio
    save_clinic_settings(clinic_id, settings)
    return settings


def update_twilio_fields(clinic_id: str, updates: dict):
    if not isinstance(updates, dict):
        raise ValueError("updates must be a dict")

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
    if not template_key:
        raise ValueError("template_key is required")

    settings = ensure_twilio_settings(clinic_id)
    twilio = settings.get("twilio", {})
    templates = twilio.get("templates", {})

    if not isinstance(templates, dict):
        templates = {}

    if template_key not in templates or not isinstance(templates.get(template_key), dict):
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


def mask_twilio_profile(profile: dict) -> dict:
    if not isinstance(profile, dict):
        return {}

    out = dict(profile)
    token = str(out.get("subaccount_auth_token") or "")
    if token:
        if len(token) <= 8:
            out["subaccount_auth_token"] = "***"
        else:
            out["subaccount_auth_token"] = token[:4] + "***" + token[-4:]

    templates = out.get("templates", {})
    if isinstance(templates, dict):
        clean_templates = {}
        for key, value in templates.items():
            if isinstance(value, dict):
                clean_templates[key] = {
                    "friendly_name": value.get("friendly_name", key),
                    "content_sid": value.get("content_sid", ""),
                    "status": value.get("status", ""),
                }
        out["templates"] = clean_templates

    return out
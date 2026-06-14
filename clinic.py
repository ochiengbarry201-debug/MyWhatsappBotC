from zoneinfo import ZoneInfo

from config import GOOGLE_SHEETS_ID, SHEET_TAB, DEFAULT_SHEET_ID, DEFAULT_SHEET_TAB
from db import db_conn


def resolve_clinic_id(to_number: str):
    try:
        conn = db_conn()
        c = conn.cursor()
        c.execute(
            """
            select clinic_id
            from channels
            where provider='twilio' and to_number=%s and is_active=true
            limit 1
            """,
            (to_number,)
        )
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        print("resolve_clinic_id FAILED:", repr(e))
        return None


def _default_twilio_settings():
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


def validate_clinic_settings(clinic_settings: dict):
    settings = clinic_settings if isinstance(clinic_settings, dict) else {}

    errors = []
    warnings = []

    # -----------------------------
    # Clinic name
    # -----------------------------
    clinic_name = (
        str(
            settings.get("name")
            or settings.get("clinic_name")
            or "PrimeCare Dental Clinic"
        ).strip()
    )
    if not clinic_name:
        clinic_name = "PrimeCare Dental Clinic"
        warnings.append("Missing clinic name; defaulted to PrimeCare Dental Clinic")

    # -----------------------------
    # Admins
    # -----------------------------
    admins = settings.get("admins", [])
    if admins is None:
        admins = []
    elif isinstance(admins, str):
        admins = [admins.strip()] if admins.strip() else []
        warnings.append("admins was a string; converted to a one-item list")
    elif not isinstance(admins, list):
        admins = []
        warnings.append("admins must be a list; defaulted to []")

    admins = [str(x).strip() for x in admins if str(x).strip()]

    # -----------------------------
    # Sheet config
    # -----------------------------
    raw_sheet = settings.get("sheet", {})
    if raw_sheet is None:
        raw_sheet = {}
    if not isinstance(raw_sheet, dict):
        errors.append("sheet config must be an object/dict")
        raw_sheet = {}

    spreadsheet_id = str(
        raw_sheet.get("spreadsheet_id")
        or GOOGLE_SHEETS_ID
        or DEFAULT_SHEET_ID
        or ""
    ).strip()

    tab = str(
        raw_sheet.get("tab")
        or SHEET_TAB
        or DEFAULT_SHEET_TAB
        or "Sheet1"
    ).strip()

    if not tab:
        tab = "Sheet1"
        warnings.append("sheet.tab missing; defaulted to Sheet1")

    # -----------------------------
    # Hours config
    # -----------------------------
    raw_hours = settings.get("hours", {})
    if raw_hours is None:
        raw_hours = {}
    if not isinstance(raw_hours, dict):
        errors.append("hours config must be an object/dict")
        raw_hours = {}

    tz_name = str(
        raw_hours.get("timezone")
        or settings.get("timezone")
        or "Africa/Nairobi"
    ).strip()

    try:
        ZoneInfo(tz_name)
    except Exception:
        errors.append(f"Invalid timezone: {tz_name}")
        tz_name = "Africa/Nairobi"

    slot_minutes = raw_hours.get("slot_minutes", settings.get("slot_minutes", 30))
    try:
        slot_minutes = int(slot_minutes)
    except Exception:
        slot_minutes = 30
        warnings.append("slot_minutes invalid; defaulted to 30")

    if slot_minutes not in (15, 20, 30, 45, 60):
        warnings.append(f"Unusual slot_minutes={slot_minutes}; defaulted to 30")
        slot_minutes = 30

    weekly = raw_hours.get("weekly", {})
    if weekly is None:
        weekly = {}
    if not isinstance(weekly, dict):
        errors.append("hours.weekly must be an object/dict")
        weekly = {}

    # Validate each weekday block shape
    for day, blocks in list(weekly.items()):
        if blocks is None:
            weekly[day] = []
            continue

        if not isinstance(blocks, list):
            errors.append(f"hours.weekly.{day} must be a list")
            continue

        for i, block in enumerate(blocks):
            if not isinstance(block, dict):
                errors.append(f"hours.weekly.{day}[{i}] must be an object/dict")
                continue

            start = str(block.get("start", "")).strip()
            end = str(block.get("end", "")).strip()
            if not start or not end:
                errors.append(f"hours.weekly.{day}[{i}] must contain start and end")

    # -----------------------------
    # Twilio config
    # -----------------------------
    raw_twilio = settings.get("twilio", {})
    if raw_twilio is None:
        raw_twilio = {}
    if not isinstance(raw_twilio, dict):
        errors.append("twilio config must be an object/dict")
        raw_twilio = {}

    default_twilio = _default_twilio_settings()

    twilio = {
        "parent_account_sid": str(raw_twilio.get("parent_account_sid", default_twilio["parent_account_sid"]) or "").strip(),
        "subaccount_sid": str(raw_twilio.get("subaccount_sid", default_twilio["subaccount_sid"]) or "").strip(),
        "subaccount_auth_token": str(raw_twilio.get("subaccount_auth_token", default_twilio["subaccount_auth_token"]) or "").strip(),
        "whatsapp_sender": str(raw_twilio.get("whatsapp_sender", default_twilio["whatsapp_sender"]) or "").strip(),
        "waba_id": str(raw_twilio.get("waba_id", default_twilio["waba_id"]) or "").strip(),
        "business_name": str(raw_twilio.get("business_name", default_twilio["business_name"]) or "").strip(),
        "onboarding_status": str(raw_twilio.get("onboarding_status", default_twilio["onboarding_status"]) or "draft").strip(),
        "template_language": str(raw_twilio.get("template_language", default_twilio["template_language"]) or "en").strip(),
        "templates": {}
    }

    if not twilio["template_language"]:
        twilio["template_language"] = "en"
        warnings.append("twilio.template_language missing; defaulted to en")

    raw_templates = raw_twilio.get("templates", {})
    if raw_templates is None:
        raw_templates = {}
    if not isinstance(raw_templates, dict):
        errors.append("twilio.templates must be an object/dict")
        raw_templates = {}

    merged_templates = {}
    default_templates = default_twilio.get("templates", {})

    # start with defaults
    for key, value in default_templates.items():
        merged_templates[key] = {
            "friendly_name": str(value.get("friendly_name", key) or key).strip(),
            "content_sid": str(value.get("content_sid", "") or "").strip(),
            "status": str(value.get("status", "not_created") or "not_created").strip(),
        }

    # merge provided templates
    for key, value in raw_templates.items():
        if not isinstance(value, dict):
            errors.append(f"twilio.templates.{key} must be an object/dict")
            continue

        merged_templates[key] = {
            "friendly_name": str(value.get("friendly_name", key) or key).strip(),
            "content_sid": str(value.get("content_sid", "") or "").strip(),
            "status": str(value.get("status", "not_created") or "not_created").strip(),
        }

    twilio["templates"] = merged_templates

    cleaned = {
        "name": clinic_name,
        "admins": admins,
        "sheet": {
            "spreadsheet_id": spreadsheet_id,
            "tab": tab,
        },
        "hours": {
            "timezone": tz_name,
            "slot_minutes": slot_minutes,
            "weekly": weekly,
        },
        "twilio": twilio,
    }

    return cleaned, errors, warnings


def get_clinic_sheet_config(clinic_settings: dict):
    validated, _, _ = validate_clinic_settings(clinic_settings)
    sheet = validated.get("sheet", {})
    sid = (sheet.get("spreadsheet_id") or "").strip()
    tab = (sheet.get("tab") or "Sheet1").strip()
    return sid, tab
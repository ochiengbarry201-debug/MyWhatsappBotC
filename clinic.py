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
    for day, blocks in weekly.items():
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
    }

    return cleaned, errors, warnings


def get_clinic_sheet_config(clinic_settings: dict):
    validated, _, _ = validate_clinic_settings(clinic_settings)
    sheet = validated.get("sheet", {})
    sid = (sheet.get("spreadsheet_id") or "").strip()
    tab = (sheet.get("tab") or "Sheet1").strip()
    return sid, tab
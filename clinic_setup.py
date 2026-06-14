import re

import psycopg2
import psycopg2.extras

from db import db_conn
from clinic import validate_clinic_settings


def _normalize_whatsapp_number(number: str) -> str:
    s = str(number or "").strip()
    s = s.replace("whatsapp:", "").strip()
    s = re.sub(r"\s+", "", s)

    if not s:
        raise ValueError("WhatsApp number is required")

    if not s.startswith("+"):
        raise ValueError("WhatsApp number must start with +, e.g. +2547XXXXXXXX")

    return f"whatsapp:{s}"


def _normalize_admins(admins):
    if admins is None:
        admins = []
    if isinstance(admins, str):
        admins = [admins]
    if not isinstance(admins, list):
        raise ValueError("admins must be a list of phone numbers")

    return [str(x).strip() for x in admins if str(x).strip()]


def _default_weekly():
    return {
        "mon": [{"start": "09:00", "end": "17:00"}],
        "tue": [{"start": "09:00", "end": "17:00"}],
        "wed": [{"start": "09:00", "end": "17:00"}],
        "thu": [{"start": "09:00", "end": "17:00"}],
        "fri": [{"start": "09:00", "end": "17:00"}],
        "sat": [{"start": "09:00", "end": "13:00"}],
        "sun": [],
    }


def _build_raw_settings(
    clinic_name: str,
    admins,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
    twilio: dict = None,
):
    return {
        "name": str(clinic_name or "").strip(),
        "admins": _normalize_admins(admins),
        "sheet": {
            "spreadsheet_id": str(spreadsheet_id or "").strip(),
            "tab": str(sheet_tab or "Sheet1").strip(),
        },
        "hours": {
            "timezone": str(timezone or "Africa/Nairobi").strip(),
            "slot_minutes": int(slot_minutes or 30),
            "weekly": weekly or _default_weekly(),
        },
        "twilio": twilio if isinstance(twilio, dict) else {},
    }


def _validate_and_clean_settings(
    clinic_name: str,
    admins,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
    twilio: dict = None,
):
    raw_settings = _build_raw_settings(
        clinic_name=clinic_name,
        admins=admins,
        spreadsheet_id=spreadsheet_id,
        sheet_tab=sheet_tab,
        timezone=timezone,
        slot_minutes=slot_minutes,
        weekly=weekly,
        twilio=twilio,
    )

    cleaned_settings, errors, warnings = validate_clinic_settings(raw_settings)
    if errors:
        raise ValueError(f"Clinic settings validation failed: {errors}")

    return cleaned_settings, warnings


def create_clinic_only(
    clinic_name: str,
    admins: list,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
    twilio: dict = None,
):
    """
    Creates:
    1. clinics
    2. clinic_settings

    No channel mapping yet.
    """
    clinic_name = str(clinic_name or "").strip()
    if not clinic_name:
        raise ValueError("clinic_name is required")

    cleaned_settings, warnings = _validate_and_clean_settings(
        clinic_name=clinic_name,
        admins=admins,
        spreadsheet_id=spreadsheet_id,
        sheet_tab=sheet_tab,
        timezone=timezone,
        slot_minutes=slot_minutes,
        weekly=weekly,
        twilio=twilio,
    )

    conn = db_conn()
    try:
        c = conn.cursor()

        c.execute(
            """
            INSERT INTO clinics (name)
            VALUES (%s)
            RETURNING id
            """,
            (clinic_name,)
        )
        clinic_id = c.fetchone()[0]

        c.execute(
            """
            INSERT INTO clinic_settings (clinic_id, settings)
            VALUES (%s, %s)
            ON CONFLICT (clinic_id)
            DO UPDATE SET settings = EXCLUDED.settings,
                          updated_at = now()
            """,
            (clinic_id, psycopg2.extras.Json(cleaned_settings))
        )

        conn.commit()

        return {
            "clinic_id": str(clinic_id),
            "clinic_name": clinic_name,
            "to_number": None,
            "channel_attached": False,
            "settings": cleaned_settings,
            "warnings": warnings,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def attach_channel_to_clinic(clinic_id: str, to_number: str):
    """
    Attaches or updates the clinic's WhatsApp/Twilio receiving number.
    """
    normalized_to = _normalize_whatsapp_number(to_number)

    conn = db_conn()
    try:
        c = conn.cursor()

        c.execute(
            "SELECT id FROM clinics WHERE id=%s LIMIT 1",
            (clinic_id,)
        )
        row = c.fetchone()
        if not row:
            raise ValueError(f"Clinic not found: {clinic_id}")

        c.execute(
            """
            INSERT INTO channels (clinic_id, provider, to_number, is_active)
            VALUES (%s, 'twilio', %s, true)
            ON CONFLICT (provider, to_number)
            DO UPDATE SET clinic_id = EXCLUDED.clinic_id,
                          is_active = true
            """,
            (clinic_id, normalized_to)
        )

        conn.commit()

        return {
            "clinic_id": str(clinic_id),
            "to_number": normalized_to,
            "channel_attached": True,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def onboard_clinic(
    clinic_name: str,
    to_number: str = None,
    admins: list = None,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
    twilio: dict = None,
):
    """
    Creates/updates:
    1. clinics
    2. channels (if to_number is provided)
    3. clinic_settings

    Returns:
        {
            "clinic_id": "...",
            "clinic_name": "...",
            "to_number": "whatsapp:+2547..." or None,
            "channel_attached": bool,
            "settings": {...}
        }
    """
    result = create_clinic_only(
        clinic_name=clinic_name,
        admins=admins or [],
        spreadsheet_id=spreadsheet_id,
        sheet_tab=sheet_tab,
        timezone=timezone,
        slot_minutes=slot_minutes,
        weekly=weekly,
        twilio=twilio,
    )

    if to_number:
        channel_result = attach_channel_to_clinic(result["clinic_id"], to_number)
        result["to_number"] = channel_result["to_number"]
        result["channel_attached"] = True

    return result


def update_existing_clinic_setup(
    clinic_id: str,
    clinic_name: str,
    to_number: str = None,
    admins: list = None,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
    twilio: dict = None,
):
    clinic_name = str(clinic_name or "").strip()
    if not clinic_name:
        raise ValueError("clinic_name is required")

    cleaned_settings, warnings = _validate_and_clean_settings(
        clinic_name=clinic_name,
        admins=admins or [],
        spreadsheet_id=spreadsheet_id,
        sheet_tab=sheet_tab,
        timezone=timezone,
        slot_minutes=slot_minutes,
        weekly=weekly,
        twilio=twilio,
    )

    normalized_to = _normalize_whatsapp_number(to_number) if to_number else None

    conn = db_conn()
    try:
        c = conn.cursor()

        c.execute(
            """
            UPDATE clinics
            SET name=%s
            WHERE id=%s
            """,
            (clinic_name, clinic_id)
        )

        if normalized_to:
            c.execute(
                """
                INSERT INTO channels (clinic_id, provider, to_number, is_active)
                VALUES (%s, 'twilio', %s, true)
                ON CONFLICT (provider, to_number)
                DO UPDATE SET clinic_id = EXCLUDED.clinic_id,
                              is_active = true
                """,
                (clinic_id, normalized_to)
            )

        c.execute(
            """
            INSERT INTO clinic_settings (clinic_id, settings)
            VALUES (%s, %s)
            ON CONFLICT (clinic_id)
            DO UPDATE SET settings = EXCLUDED.settings,
                          updated_at = now()
            """,
            (clinic_id, psycopg2.extras.Json(cleaned_settings))
        )

        conn.commit()

        return {
            "clinic_id": str(clinic_id),
            "clinic_name": clinic_name,
            "to_number": normalized_to,
            "channel_attached": bool(normalized_to),
            "settings": cleaned_settings,
            "warnings": warnings,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
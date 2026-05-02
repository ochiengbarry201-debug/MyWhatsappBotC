import json
import re
import uuid

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


def onboard_clinic(
    clinic_name: str,
    to_number: str,
    admins: list,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
):
    """
    Creates/updates:
    1. clinics
    2. channels
    3. clinic_settings

    Returns:
        {
            "clinic_id": "...",
            "clinic_name": "...",
            "to_number": "whatsapp:+2547...",
            "settings": {...}
        }
    """
    clinic_name = str(clinic_name or "").strip()
    if not clinic_name:
        raise ValueError("clinic_name is required")

    normalized_to = _normalize_whatsapp_number(to_number)

    if admins is None:
        admins = []
    if isinstance(admins, str):
        admins = [admins]
    if not isinstance(admins, list):
        raise ValueError("admins must be a list of phone numbers")

    admins = [str(x).strip() for x in admins if str(x).strip()]

    weekly = weekly or {
        "mon": [{"start": "09:00", "end": "17:00"}],
        "tue": [{"start": "09:00", "end": "17:00"}],
        "wed": [{"start": "09:00", "end": "17:00"}],
        "thu": [{"start": "09:00", "end": "17:00"}],
        "fri": [{"start": "09:00", "end": "17:00"}],
        "sat": [{"start": "09:00", "end": "13:00"}],
        "sun": [],
    }

    raw_settings = {
        "name": clinic_name,
        "admins": admins,
        "sheet": {
            "spreadsheet_id": str(spreadsheet_id or "").strip(),
            "tab": str(sheet_tab or "Sheet1").strip(),
        },
        "hours": {
            "timezone": str(timezone or "Africa/Nairobi").strip(),
            "slot_minutes": int(slot_minutes or 30),
            "weekly": weekly,
        },
    }

    cleaned_settings, errors, warnings = validate_clinic_settings(raw_settings)
    if errors:
        raise ValueError(f"Clinic settings validation failed: {errors}")

    conn = db_conn()
    try:
        c = conn.cursor()

        # -------------------------------------------------
        # 1. Create clinic
        # -------------------------------------------------
        c.execute(
            """
            INSERT INTO clinics (name)
            VALUES (%s)
            RETURNING id
            """,
            (clinic_name,)
        )
        clinic_id = c.fetchone()[0]

        # -------------------------------------------------
        # 2. Map WhatsApp number to clinic
        # -------------------------------------------------
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

        # -------------------------------------------------
        # 3. Save clinic settings
        # -------------------------------------------------
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
            "settings": cleaned_settings,
            "warnings": warnings,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_existing_clinic_setup(
    clinic_id: str,
    clinic_name: str,
    to_number: str,
    admins: list,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,
):
    clinic_name = str(clinic_name or "").strip()
    if not clinic_name:
        raise ValueError("clinic_name is required")

    normalized_to = _normalize_whatsapp_number(to_number)

    if admins is None:
        admins = []
    if isinstance(admins, str):
        admins = [admins]
    if not isinstance(admins, list):
        raise ValueError("admins must be a list of phone numbers")

    admins = [str(x).strip() for x in admins if str(x).strip()]

    weekly = weekly or {
        "mon": [{"start": "09:00", "end": "17:00"}],
        "tue": [{"start": "09:00", "end": "17:00"}],
        "wed": [{"start": "09:00", "end": "17:00"}],
        "thu": [{"start": "09:00", "end": "17:00"}],
        "fri": [{"start": "09:00", "end": "17:00"}],
        "sat": [{"start": "09:00", "end": "13:00"}],
        "sun": [],
    }

    raw_settings = {
        "name": clinic_name,
        "admins": admins,
        "sheet": {
            "spreadsheet_id": str(spreadsheet_id or "").strip(),
            "tab": str(sheet_tab or "Sheet1").strip(),
        },
        "hours": {
            "timezone": str(timezone or "Africa/Nairobi").strip(),
            "slot_minutes": int(slot_minutes or 30),
            "weekly": weekly,
        },
    }

    cleaned_settings, errors, warnings = validate_clinic_settings(raw_settings)
    if errors:
        raise ValueError(f"Clinic settings validation failed: {errors}")

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
            "settings": cleaned_settings,
            "warnings": warnings,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
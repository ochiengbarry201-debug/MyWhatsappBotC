from config import GOOGLE_SHEETS_ID, SHEET_TAB, DEFAULT_SHEET_ID, DEFAULT_SHEET_TAB

from db import db_conn

def resolve_clinic_id(to_number: str):
    try:
        conn = db_conn()
        c = conn.cursor()
        c.execute("""
            select clinic_id
            from channels
            where provider='twilio' and to_number=%s and is_active=true
            limit 1
        """, (to_number,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        print("resolve_clinic_id FAILED:", repr(e))
        return None

def get_clinic_sheet_config(clinic_settings: dict):
    sheet = clinic_settings.get("sheet", {}) if isinstance(clinic_settings, dict) else {}
    sid = (sheet.get("spreadsheet_id") or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet.get("tab") or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    return sid, tab

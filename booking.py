import secrets
import string
import datetime

import psycopg2

from sheets import sheets_api, a1, get_sheet_header_map, _col_to_idx
from config import GOOGLE_SHEETS_ID, SHEET_TAB, DEFAULT_SHEET_ID, DEFAULT_SHEET_TAB
from db import db_conn

# -------------------------------------------------
# Double booking check (DB + Google Sheets)
# -------------------------------------------------
def check_double_booking(clinic_id, date, time, sheet_id=None, sheet_tab=None):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        "SELECT id FROM appointments WHERE clinic_id=%s AND date=%s AND time=%s AND status='Booked'",
        (clinic_id, date, time)
    )
    exists = c.fetchone()
    conn.close()
    if exists:
        return True

    if not sheets_api:
        return False

    sid = (sheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return False

    try:
        header_map = get_sheet_header_map(sid, tab)

        if header_map and "date" in header_map and "time" in header_map:
            date_i = _col_to_idx(header_map["date"])
            time_i = _col_to_idx(header_map["time"])

            res = sheets_api.values().get(
                spreadsheetId=sid,
                range=a1(tab, "A2:Z")
            ).execute()

            for row in res.get("values", []):
                d = row[date_i] if len(row) > date_i else ""
                t = row[time_i] if len(row) > time_i else ""
                if d == date and t == time:
                    return True

            return False

        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A2:F")
        ).execute()

        for row in res.get("values", []):
            if len(row) >= 2 and row[0] == date and row[1] == time:
                return True

    except Exception as e:
        print("Sheets check error:", repr(e))

    return False

# -------------------------------------------------
# Appointment reference code
# -------------------------------------------------
def generate_ref_code():
    alphabet = string.ascii_uppercase + string.digits
    return "AP-" + "".join(secrets.choice(alphabet) for _ in range(6))

def save_appointment_local(clinic_id, user, name, date, time):
    """
    Saves and returns (appt_id, ref_code).
    Retries ref_code on rare collision.
    """
    for _ in range(5):
        ref_code = generate_ref_code()
        try:
            conn = db_conn()
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO appointments
                (clinic_id, user_number, name, date, time, status, source, created_at, sheet_sync_status, ref_code)
                VALUES (%s,%s,%s,%s,%s,'Booked','WhatsApp',%s,'pending',%s)
                RETURNING id
                """,
                (clinic_id, user, name, date, time, datetime.datetime.utcnow(), ref_code)
            )
            appt_id = c.fetchone()[0]
            conn.commit()
            conn.close()
            return appt_id, ref_code
        except psycopg2.Error as e:
            if getattr(e, "pgcode", None) == "23505":
                try:
                    conn.close()
                except:
                    pass
                continue
            raise
    raise RuntimeError("Failed to generate a unique appointment reference. Try again.")

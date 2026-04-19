import secrets
import string
import datetime

import psycopg2

from sheets import sheets_api, a1, get_sheet_header_map, _col_to_idx
from config import GOOGLE_SHEETS_ID, SHEET_TAB, DEFAULT_SHEET_ID, DEFAULT_SHEET_TAB
from db import db_conn
from hours import normalize_time_to_24h


def _normalize_sheet_date(value):
    if value is None:
        return ""
    s = str(value).strip()

    try:
        dt = datetime.datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y", "%Y/%m/%d"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return s


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

            status_i = None
            if "status" in header_map:
                status_i = _col_to_idx(header_map["status"])

            res = sheets_api.values().get(
                spreadsheetId=sid,
                range=a1(tab, "A2:Z")
            ).execute()

            for row in res.get("values", []):
                d = row[date_i] if len(row) > date_i else ""
                t = row[time_i] if len(row) > time_i else ""
                status = row[status_i] if status_i is not None and len(row) > status_i else ""

                d_norm = _normalize_sheet_date(d)
                t_norm = normalize_time_to_24h(str(t).strip()) if t else ""

                if status_i is not None and str(status).strip().lower() in {"cancelled", "rescheduled"}:
                    continue

                if d_norm == date and t_norm == time:
                    return True

            return False

        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A2:Z")
        ).execute()

        for row in res.get("values", []):
            d = row[0] if len(row) > 0 else ""
            t = row[1] if len(row) > 1 else ""

            d_norm = _normalize_sheet_date(d)
            t_norm = normalize_time_to_24h(str(t).strip()) if t else ""

            if d_norm == date and t_norm == time:
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
                except Exception:
                    pass
                continue
            raise
    raise RuntimeError("Failed to generate a unique appointment reference. Try again.")
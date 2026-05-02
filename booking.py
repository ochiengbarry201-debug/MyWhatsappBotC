import secrets
import string
import datetime

import psycopg2

from sheets import sheets_api, a1, get_sheet_header_map, _col_to_idx
from config import GOOGLE_SHEETS_ID, SHEET_TAB, DEFAULT_SHEET_ID, DEFAULT_SHEET_TAB
from db import db_conn
from hours import normalize_time_to_24h


def log_booking(tag, **kwargs):
    try:
        parts = [f"[{tag}]"]
        for k, v in kwargs.items():
            v = str(v)
            if len(v) > 300:
                v = v[:300] + "..."
            parts.append(f"{k}={v}")
        print(" | ".join(parts))
    except Exception as e:
        print(f"[BOOKING_LOG_FAILED] tag={tag} error={repr(e)}")


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
    log_booking(
        "DOUBLE_BOOKING_START",
        clinic_id=clinic_id,
        date=date,
        time=time,
        sheet_id_provided=bool(sheet_id),
        sheet_tab=sheet_tab or ""
    )

    conn = db_conn()
    c = conn.cursor()
    c.execute(
        "SELECT id FROM appointments WHERE clinic_id=%s AND date=%s AND time=%s AND status='Booked'",
        (clinic_id, date, time)
    )
    exists = c.fetchone()
    conn.close()

    if exists:
        log_booking(
            "DOUBLE_BOOKING_DB_HIT",
            clinic_id=clinic_id,
            date=date,
            time=time,
            appointment_id=exists[0]
        )
        return True

    if not sheets_api:
        log_booking(
            "DOUBLE_BOOKING_SHEETS_SKIPPED",
            clinic_id=clinic_id,
            date=date,
            time=time,
            reason="sheets_api_not_initialized"
        )
        return False

    sid = (sheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()

    if not sid:
        log_booking(
            "DOUBLE_BOOKING_SHEETS_SKIPPED",
            clinic_id=clinic_id,
            date=date,
            time=time,
            reason="missing_spreadsheet_id"
        )
        return False

    try:
        header_map = get_sheet_header_map(sid, tab)
        log_booking(
            "DOUBLE_BOOKING_SHEETS_HEADER_MAP",
            clinic_id=clinic_id,
            date=date,
            time=time,
            spreadsheet_id_present=bool(sid),
            tab=tab,
            has_header_map=bool(header_map),
            header_keys=list(header_map.keys()) if header_map else []
        )

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

            rows = res.get("values", [])
            log_booking(
                "DOUBLE_BOOKING_SHEETS_ROWS_FETCHED",
                clinic_id=clinic_id,
                date=date,
                time=time,
                tab=tab,
                rows_count=len(rows),
                mode="header_map"
            )

            for idx, row in enumerate(rows):
                d = row[date_i] if len(row) > date_i else ""
                t = row[time_i] if len(row) > time_i else ""
                status = row[status_i] if status_i is not None and len(row) > status_i else ""

                d_norm = _normalize_sheet_date(d)
                t_norm = normalize_time_to_24h(str(t).strip()) if t else ""

                if status_i is not None and str(status).strip().lower() in {"cancelled", "rescheduled"}:
                    continue

                if d_norm == date and t_norm == time:
                    log_booking(
                        "DOUBLE_BOOKING_SHEETS_HIT",
                        clinic_id=clinic_id,
                        date=date,
                        time=time,
                        tab=tab,
                        row_index=idx + 2,
                        matched_date=d_norm,
                        matched_time=t_norm,
                        status=status
                    )
                    return True

            log_booking(
                "DOUBLE_BOOKING_NOT_FOUND",
                clinic_id=clinic_id,
                date=date,
                time=time,
                checked_db=True,
                checked_sheets=True
            )
            return False

        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A2:Z")
        ).execute()

        rows = res.get("values", [])
        log_booking(
            "DOUBLE_BOOKING_SHEETS_ROWS_FETCHED",
            clinic_id=clinic_id,
            date=date,
            time=time,
            tab=tab,
            rows_count=len(rows),
            mode="fallback_columns"
        )

        for idx, row in enumerate(rows):
            d = row[0] if len(row) > 0 else ""
            t = row[1] if len(row) > 1 else ""

            d_norm = _normalize_sheet_date(d)
            t_norm = normalize_time_to_24h(str(t).strip()) if t else ""

            if d_norm == date and t_norm == time:
                log_booking(
                    "DOUBLE_BOOKING_SHEETS_HIT",
                    clinic_id=clinic_id,
                    date=date,
                    time=time,
                    tab=tab,
                    row_index=idx + 2,
                    matched_date=d_norm,
                    matched_time=t_norm,
                    mode="fallback_columns"
                )
                return True

    except Exception as e:
        log_booking(
            "DOUBLE_BOOKING_SHEETS_ERROR",
            clinic_id=clinic_id,
            date=date,
            time=time,
            spreadsheet_id_present=bool(sid),
            tab=tab,
            error=repr(e)
        )

    log_booking(
        "DOUBLE_BOOKING_NOT_FOUND",
        clinic_id=clinic_id,
        date=date,
        time=time,
        checked_db=True,
        checked_sheets=True
    )
    return False


# -------------------------------------------------
# Appointment reference code
# -------------------------------------------------
def generate_ref_code():
    alphabet = string.ascii_uppercase + string.digits
    ref_code = "AP-" + "".join(secrets.choice(alphabet) for _ in range(6))
    log_booking("REF_CODE_GENERATED", ref_code=ref_code)
    return ref_code


def save_appointment_local(clinic_id, user, name, date, time, source_message_sid=None):
    """
    Saves and returns (appt_id, ref_code).

    Retries only on rare ref_code collision.
    Prevents duplicate appointment creation from the same inbound confirmation
    when source_message_sid is provided.
    """
    log_booking(
        "SAVE_APPOINTMENT_START",
        clinic_id=clinic_id,
        user=user,
        name=name,
        date=date,
        time=time,
        source_message_sid=source_message_sid or ""
    )

    for attempt in range(1, 6):
        ref_code = generate_ref_code()
        conn = None
        try:
            conn = db_conn()
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO appointments
                (
                    clinic_id, user_number, name, date, time,
                    status, source, created_at, sheet_sync_status,
                    ref_code, source_message_sid
                )
                VALUES (%s,%s,%s,%s,%s,'Booked','WhatsApp',%s,'pending',%s,%s)
                RETURNING id
                """,
                (
                    clinic_id,
                    user,
                    name,
                    date,
                    time,
                    datetime.datetime.utcnow(),
                    ref_code,
                    source_message_sid,
                )
            )
            appt_id = c.fetchone()[0]
            conn.commit()
            conn.close()

            log_booking(
                "SAVE_APPOINTMENT_SUCCESS",
                clinic_id=clinic_id,
                user=user,
                appointment_id=appt_id,
                ref_code=ref_code,
                attempt=attempt,
                source_message_sid=source_message_sid or ""
            )
            return appt_id, ref_code

        except psycopg2.Error as e:
            pgcode = getattr(e, "pgcode", None)
            constraint_name = getattr(getattr(e, "diag", None), "constraint_name", "") or ""

            log_booking(
                "SAVE_APPOINTMENT_DB_ERROR",
                clinic_id=clinic_id,
                user=user,
                attempt=attempt,
                ref_code=ref_code,
                pgcode=pgcode,
                constraint_name=constraint_name,
                error=repr(e),
                source_message_sid=source_message_sid or ""
            )

            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception as close_err:
                log_booking(
                    "SAVE_APPOINTMENT_CONN_CLOSE_ERROR",
                    clinic_id=clinic_id,
                    user=user,
                    attempt=attempt,
                    error=repr(close_err)
                )

            # Retry only if the generated reference code collided
            if pgcode == "23505" and constraint_name == "uq_appointments_ref_code":
                log_booking(
                    "SAVE_APPOINTMENT_REF_COLLISION",
                    clinic_id=clinic_id,
                    user=user,
                    attempt=attempt,
                    ref_code=ref_code
                )
                continue

            # If the same confirmation message tries to create another appointment,
            # return the already-created appointment instead of making a new one.
            if pgcode == "23505" and constraint_name == "uq_appointments_source_message_sid" and source_message_sid:
                log_booking(
                    "SAVE_APPOINTMENT_DUPLICATE_SOURCE_SID",
                    clinic_id=clinic_id,
                    user=user,
                    source_message_sid=source_message_sid
                )

                conn2 = db_conn()
                c2 = conn2.cursor()
                c2.execute(
                    """
                    SELECT id, ref_code
                    FROM appointments
                    WHERE source_message_sid=%s
                    LIMIT 1
                    """,
                    (source_message_sid,)
                )
                row = c2.fetchone()
                conn2.close()

                if row:
                    log_booking(
                        "SAVE_APPOINTMENT_EXISTING_RETURNED",
                        clinic_id=clinic_id,
                        user=user,
                        appointment_id=row[0],
                        ref_code=row[1],
                        source_message_sid=source_message_sid
                    )
                    return row[0], row[1]

            raise

    log_booking(
        "SAVE_APPOINTMENT_FAILED",
        clinic_id=clinic_id,
        user=user,
        reason="exhausted_ref_code_attempts",
        source_message_sid=source_message_sid or ""
    )
    raise RuntimeError("Failed to generate a unique appointment reference. Try again.")
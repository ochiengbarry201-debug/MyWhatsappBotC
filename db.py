import datetime
import json

import psycopg2
import psycopg2.extras

from config import DATABASE_URL

def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set. This app now requires Postgres.")
    print("DB: USING POSTGRESQL")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = db_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            clinic_id uuid,
            user_number TEXT,
            role TEXT,
            content TEXT,
            created_at TIMESTAMP,
            twilio_sid TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            clinic_id uuid,
            user_number TEXT,
            context TEXT,
            current_state text DEFAULT 'idle',
            draft jsonb DEFAULT '{}'::jsonb,
            PRIMARY KEY (clinic_id, user_number)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS appointments (
            id SERIAL PRIMARY KEY,
            clinic_id uuid,
            user_number TEXT,
            name TEXT,
            date TEXT,
            time TEXT,
            status TEXT,
            source TEXT,
            created_at TIMESTAMP,
            sheet_sync_status text DEFAULT 'pending',
            sheet_sync_error text,
            sheet_synced_at timestamptz,
            cancelled_at timestamptz,
            ref_code text
        )
    """)

    try:
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_appointments_ref_code
            ON appointments (clinic_id, ref_code)
            WHERE ref_code IS NOT NULL
        """)
    except Exception as e:
        print("Index create uq_appointments_ref_code failed:", repr(e))

    c.execute("""
        CREATE TABLE IF NOT EXISTS clinic_settings (
            clinic_id uuid PRIMARY KEY REFERENCES clinics(id) ON DELETE CASCADE,
            settings jsonb NOT NULL DEFAULT '{}'::jsonb,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    """)

    # -------------------------------------------------
    # JOB QUEUE (Postgres-backed)
    # -------------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id BIGSERIAL PRIMARY KEY,
            job_type TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NOT NULL DEFAULT 'queued',   -- queued, running, done, failed
            run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            attempts INT NOT NULL DEFAULT 0,
            max_attempts INT NOT NULL DEFAULT 8,
            last_error TEXT,
            locked_at TIMESTAMPTZ,
            locked_by TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    try:
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_jobs_status_runat
            ON jobs(status, run_at)
        """)
    except Exception as e:
        print("Index create idx_jobs_status_runat failed:", repr(e))

    conn.commit()
    conn.close()
    print("DB tables checked/created successfully")

# -------------------------------------------------
# DB Helpers
# -------------------------------------------------
def save_message(clinic_id, user, role, msg, twilio_sid=None):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO messages (clinic_id, user_number, role, content, created_at, twilio_sid)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        """,
        (clinic_id, user, role, msg, datetime.datetime.utcnow(), twilio_sid)
    )
    conn.commit()
    conn.close()

def already_processed_twilio_sid(twilio_sid: str) -> bool:
    if not twilio_sid:
        return False
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM messages WHERE twilio_sid=%s LIMIT 1", (twilio_sid,))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def load_recent_messages(clinic_id, user, limit=12):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        f"SELECT role, content FROM messages WHERE clinic_id=%s AND user_number=%s ORDER BY id DESC LIMIT {limit}",
        (clinic_id, user)
    )
    rows = c.fetchall()
    conn.close()
    rows.reverse()
    return [{"role": r, "content": t} for r, t in rows]

def update_sheet_sync_status(appointment_id, status, error=None):
    try:
        conn = db_conn()
        c = conn.cursor()
        if status == "synced":
            c.execute(
                """
                UPDATE appointments
                SET sheet_sync_status=%s,
                    sheet_sync_error=NULL,
                    sheet_synced_at=now()
                WHERE id=%s
                """,
                (status, appointment_id)
            )
        else:
            err = (error or "")[:800]
            c.execute(
                """
                UPDATE appointments
                SET sheet_sync_status=%s,
                    sheet_sync_error=%s
                WHERE id=%s
                """,
                (status, err, appointment_id)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print("update_sheet_sync_status FAILED:", repr(e))

def get_unsynced_appointments(clinic_id, limit=20):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, user_number, name, date, time, sheet_sync_status
        FROM appointments
        WHERE clinic_id=%s
          AND status='Booked'
          AND sheet_sync_status IN ('failed','pending')
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (clinic_id, limit)
    )
    rows = c.fetchall()
    conn.close()
    return rows

def cancel_latest_appointment(clinic_id, user):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, name, date, time, ref_code
        FROM appointments
        WHERE clinic_id=%s AND user_number=%s AND status='Booked'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (clinic_id, user)
    )
    row = c.fetchone()
    if not row:
        conn.close()
        return None

    appt_id, name, date, time, ref_code = row
    c.execute(
        """
        UPDATE appointments
        SET status='Cancelled', cancelled_at=now()
        WHERE id=%s
        """,
        (appt_id,)
    )
    conn.commit()
    conn.close()
    return {"id": appt_id, "name": name, "date": date, "time": time, "ref_code": ref_code}

def cancel_by_ref(clinic_id, user, ref_code):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, name, date, time, user_number
        FROM appointments
        WHERE clinic_id=%s AND ref_code=%s AND status='Booked'
        LIMIT 1
        """,
        (clinic_id, ref_code)
    )
    row = c.fetchone()
    if not row:
        conn.close()
        return None

    appt_id, name, date, time, booked_user = row

    if (booked_user or "") != (user or ""):
        conn.close()
        return "not_owner"

    c.execute(
        """
        UPDATE appointments
        SET status='Cancelled', cancelled_at=now()
        WHERE id=%s
        """,
        (appt_id,)
    )
    conn.commit()
    conn.close()
    return {"id": appt_id, "name": name, "date": date, "time": time}

def get_latest_booked_appointment(clinic_id, user):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, name, date, time, created_at, ref_code
        FROM appointments
        WHERE clinic_id=%s AND user_number=%s AND status='Booked'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (clinic_id, user)
    )
    row = c.fetchone()
    conn.close()
    return row

def get_todays_appointments(clinic_id, date_str):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT name, user_number, time, sheet_sync_status, ref_code
        FROM appointments
        WHERE clinic_id=%s AND date=%s AND status='Booked'
        ORDER BY time ASC
        """,
        (clinic_id, date_str)
    )
    rows = c.fetchall()
    conn.close()
    return rows

def load_clinic_settings(clinic_id):
    try:
        conn = db_conn()
        c = conn.cursor()
        c.execute("SELECT settings FROM clinic_settings WHERE clinic_id=%s", (clinic_id,))
        row = c.fetchone()
        conn.close()
        if not row or row[0] is None:
            return {}
        if isinstance(row[0], str):
            try:
                return json.loads(row[0])
            except:
                return {}
        return row[0] if isinstance(row[0], dict) else {}
    except Exception as e:
        print("load_clinic_settings FAILED:", repr(e))
        return {}

def get_state_and_draft(clinic_id, user):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        "SELECT current_state, draft FROM conversations WHERE clinic_id=%s AND user_number=%s",
        (clinic_id, user)
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return ("idle", {})
    state, draft = row[0], row[1]
    if draft is None:
        draft = {}
    if isinstance(draft, str):
        try:
            draft = json.loads(draft)
        except:
            draft = {}
    return (state or "idle", draft if isinstance(draft, dict) else {})

def set_state_and_draft(clinic_id, user, state, draft):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO conversations (clinic_id, user_number, context, current_state, draft)
        VALUES (%s,%s,'',%s,%s)
        ON CONFLICT (clinic_id, user_number)
        DO UPDATE SET current_state=EXCLUDED.current_state,
                      draft=EXCLUDED.draft
        """,
        (clinic_id, user, state, psycopg2.extras.Json(draft or {}))
    )
    conn.commit()
    conn.close()

def clear_state_machine(clinic_id, user):
    set_state_and_draft(clinic_id, user, "idle", {})

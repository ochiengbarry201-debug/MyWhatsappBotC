import os
import psycopg2
import psycopg2.extras
import datetime
import re
import json
import secrets
import string
from zoneinfo import ZoneInfo
from flask import Flask, request, Response
from twilio.twiml.messaging_response import MessagingResponse
from dotenv import load_dotenv

# Google Sheets
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# OpenAI
from openai import OpenAI

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

print("LOCAL DATABASE_URL exists?", bool(os.getenv("DATABASE_URL")))
print("LOCAL SERVICE_ACCOUNT_JSON exists?", bool(os.getenv("SERVICE_ACCOUNT_JSON")))
print("LOCAL SERVICE_ACCOUNT_FILE exists?", bool(os.getenv("SERVICE_ACCOUNT_FILE")))
print("LOCAL GOOGLE_SHEETS_ID exists?", bool(os.getenv("GOOGLE_SHEETS_ID")))

# -------------------------------------------------
# Google Sheets (Local file OR Render-safe JSON)
# -------------------------------------------------
SERVICE_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "").strip()
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "").strip()
SHEET_TAB = os.getenv("GOOGLE_SHEETS_TAB", "Sheet1").strip()

DEFAULT_SHEET_ID = "15W9oICScP7ecJvacczeuCmlHVAvJ2QmVSH9tJgSiQBo"
DEFAULT_SHEET_TAB = "Sheet1"

sheets_api = None

def load_service_info():
    if SERVICE_JSON:
        return json.loads(SERVICE_JSON)

    if SERVICE_FILE and os.path.exists(SERVICE_FILE):
        with open(SERVICE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    return None

def a1(tab: str, cells: str) -> str:
    safe = tab.replace("'", "''")
    return f"'{safe}'!{cells}"

def _norm_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _index_to_col(idx: int) -> str:
    idx += 1
    out = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        out = chr(65 + r) + out
    return out

def _col_to_idx(col: str) -> int:
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - 64)
    return n - 1

def get_sheet_header_map(spreadsheet_id=None, sheet_tab=None):
    if not sheets_api:
        return None

    sid = (spreadsheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return None

    try:
        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A1:Z1")
        ).execute()
        header_row = (res.get("values") or [[]])[0]

        header_index = {}
        for i, cell in enumerate(header_row):
            key = _norm_header(cell)
            if key:
                header_index[key] = i

        wanted = {
            "date": ["date", "appointment date", "booking date"],
            "time": ["time", "appointment time", "booking time"],
            "name": ["name", "patient name", "full name"],
            "phone": ["phone", "phone number", "mobile", "number"],
            "status": ["status"],
            "source": ["source"],
        }

        out = {}
        for field, variants in wanted.items():
            for v in variants:
                vkey = _norm_header(v)
                if vkey in header_index:
                    out[field] = _index_to_col(header_index[vkey])
                    break

        return out
    except Exception as e:
        print("Header map read failed:", repr(e))
        return None

# Init Sheets client
service_info = None
try:
    service_info = load_service_info()
except Exception as e:
    print("Service account load failed:", repr(e))
    service_info = None

if service_info and (GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID):
    try:
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)
        sheets_service = build("sheets", "v4", credentials=creds)
        sheets_api = sheets_service.spreadsheets()
        print("Google Sheets initialized")
    except Exception as e:
        print("Google Sheets init failed:", repr(e))
else:
    print("Service account not set or sheet id not set — Sheets disabled")

# -------------------------------------------------
# Environment variables
# -------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "").strip()
CLINIC_NAME = os.getenv("CLINIC_NAME", "PrimeCare Medical Centre")

def normalize_admin_number(s: str) -> str:
    raw = (s or "").strip().replace("whatsapp:", "").strip()
    digits = re.sub(r"[^\d+]", "", raw)

    if digits.startswith("0") and len(digits) == 10:
        return "+254" + digits[1:]

    if digits.startswith("254") and not digits.startswith("+"):
        return "+" + digits

    return digits

def is_admin(user_number: str, clinic_settings: dict) -> bool:
    admins = clinic_settings.get("admins", [])
    user_norm = normalize_admin_number(user_number)

    for a in admins:
        if user_norm == normalize_admin_number(str(a)):
            return True

    if ADMIN_WHATSAPP:
        return user_norm == normalize_admin_number(ADMIN_WHATSAPP)

    return False

# -------------------------------------------------
# OpenAI Client
# -------------------------------------------------
openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        print("OpenAI client initialized")
    except Exception as e:
        print("OpenAI init error:", repr(e))
else:
    print("Warning: OPENAI_API_KEY not set — AI replies disabled")

# -------------------------------------------------
# Database (PostgreSQL ONLY)
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

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

    # Unique index for ref_code (safe)
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

    conn.commit()
    conn.close()
    print("DB tables checked/created successfully")

init_db()

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

    # Safety: only allow the same user to cancel by ref
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

def get_clinic_sheet_config(clinic_settings: dict):
    sheet = clinic_settings.get("sheet", {}) if isinstance(clinic_settings, dict) else {}
    sid = (sheet.get("spreadsheet_id") or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet.get("tab") or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    return sid, tab

# -------------------------------------------------
# Business Hours (per clinic settings)
# -------------------------------------------------
DEFAULT_HOURS = {
    "timezone": "Africa/Nairobi",
    "slot_minutes": 30,
    "weekly": {
        "mon": [{"start": "09:00", "end": "17:00"}],
        "tue": [{"start": "09:00", "end": "17:00"}],
        "wed": [{"start": "09:00", "end": "17:00"}],
        "thu": [{"start": "09:00", "end": "17:00"}],
        "fri": [{"start": "09:00", "end": "17:00"}],
        "sat": [{"start": "09:00", "end": "13:00"}],
        "sun": []
    }
}

def get_hours_settings(clinic_settings: dict):
    hours = clinic_settings.get("hours") if isinstance(clinic_settings, dict) else None
    if not isinstance(hours, dict):
        hours = DEFAULT_HOURS
    timezone = (hours.get("timezone") or DEFAULT_HOURS["timezone"]).strip()
    slot_minutes = hours.get("slot_minutes", DEFAULT_HOURS["slot_minutes"])
    try:
        slot_minutes = int(slot_minutes)
        if slot_minutes <= 0:
            slot_minutes = DEFAULT_HOURS["slot_minutes"]
    except:
        slot_minutes = DEFAULT_HOURS["slot_minutes"]
    weekly = hours.get("weekly", DEFAULT_HOURS["weekly"])
    if not isinstance(weekly, dict):
        weekly = DEFAULT_HOURS["weekly"]
    return timezone, slot_minutes, weekly

def parse_hhmm_to_minutes(hhmm: str):
    hhmm = (hhmm or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", hhmm)
    if not m:
        return None
    h = int(m.group(1))
    mi = int(m.group(2))
    if h < 0 or h > 23 or mi < 0 or mi > 59:
        return None
    return h * 60 + mi

def normalize_time_to_24h(s: str):
    s = (s or "").strip()
    try:
        t = datetime.datetime.strptime(s, "%H:%M").time()
        return f"{t.hour:02d}:{t.minute:02d}"
    except:
        pass
    try:
        t = datetime.datetime.strptime(s, "%I:%M %p").time()
        return f"{t.hour:02d}:{t.minute:02d}"
    except:
        return None

def weekday_key_from_date(date_str: str, tz_name: str):
    tz = ZoneInfo(tz_name)
    d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    idx = datetime.datetime(d.year, d.month, d.day, 12, 0, tzinfo=tz).weekday()
    keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    return keys[idx]

def is_open_on_date(date_str: str, tz_name: str, weekly: dict):
    try:
        day_key = weekday_key_from_date(date_str, tz_name)
        intervals = weekly.get(day_key, [])
        return isinstance(intervals, list) and len(intervals) > 0
    except:
        return True

def is_time_within_hours(date_str: str, time_24h: str, tz_name: str, weekly: dict):
    try:
        day_key = weekday_key_from_date(date_str, tz_name)
        intervals = weekly.get(day_key, [])
        if not isinstance(intervals, list) or len(intervals) == 0:
            return False

        tmin = parse_hhmm_to_minutes(time_24h)
        if tmin is None:
            return False

        for it in intervals:
            if not isinstance(it, dict):
                continue
            start = parse_hhmm_to_minutes(it.get("start", ""))
            end = parse_hhmm_to_minutes(it.get("end", ""))
            if start is None or end is None:
                continue
            if start <= tmin < end:
                return True
        return False
    except:
        return True

def is_slot_aligned(time_24h: str, slot_minutes: int):
    tmin = parse_hhmm_to_minutes(time_24h)
    if tmin is None:
        return False
    return (tmin % slot_minutes) == 0

def format_opening_hours_for_day(date_str: str, tz_name: str, weekly: dict):
    try:
        day_key = weekday_key_from_date(date_str, tz_name)
        intervals = weekly.get(day_key, [])
        if not isinstance(intervals, list) or len(intervals) == 0:
            return "Closed"
        parts = []
        for it in intervals:
            if isinstance(it, dict) and it.get("start") and it.get("end"):
                parts.append(f"{it['start']}-{it['end']}")
        return ", ".join(parts) if parts else "Closed"
    except:
        return ""

# -------------------------------------------------
# State machine
# -------------------------------------------------
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

# -------------------------------------------------
# Clinic resolver
# -------------------------------------------------
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
# Booking intent + validators
# -------------------------------------------------
BOOKING_KEYWORDS = [
    "book", "booking", "appointment", "schedule", "reschedule", "cancel",
    "doctor", "clinic", "visit",
    "dentist", "dental", "tooth", "teeth", "toothache", "gum", "braces",
    "cleaning", "checkup", "check-up", "pain", "ache"
]

def is_booking_intent(text):
    t = text.lower()
    return any(k in t for k in BOOKING_KEYWORDS)

def looks_like_date(s):
    try:
        datetime.datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except:
        return False

# -------------------------------------------------
# AI Reply
# -------------------------------------------------
SYSTEM_PROMPT = f"""
You are a medical clinic receptionist for {CLINIC_NAME}.
Keep replies short, polite, and helpful.
"""

def ai_reply(clinic_id, user, msg):
    if not openai_client:
        return f"This is {CLINIC_NAME}. Type 'book' to make an appointment."

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages += load_recent_messages(clinic_id, user)
    messages.append({"role": "user", "content": msg})

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=200
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print("AI error:", repr(e))
        return "Sorry, something went wrong."

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
            # 23505 unique violation (ref collision or slot collision)
            if getattr(e, "pgcode", None) == "23505":
                try:
                    conn.close()
                except:
                    pass
                continue
            raise
    raise RuntimeError("Failed to generate a unique appointment reference. Try again.")

# -------------------------------------------------
# Sheets append
# -------------------------------------------------
def append_to_sheet(date, time, name, phone, sheet_id=None, sheet_tab=None):
    if not sheets_api:
        return False

    sid = (sheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return False

    try:
        header_map = get_sheet_header_map(sid, tab)

        if header_map:
            required = ["date", "time", "name", "phone", "status", "source"]
            missing = [k for k in required if k not in header_map]
            if not missing:
                date_i = _col_to_idx(header_map["date"])
                time_i = _col_to_idx(header_map["time"])
                name_i = _col_to_idx(header_map["name"])
                phone_i = _col_to_idx(header_map["phone"])
                status_i = _col_to_idx(header_map["status"])
                source_i = _col_to_idx(header_map["source"])

                max_i = max(date_i, time_i, name_i, phone_i, status_i, source_i)
                row_values = [""] * (max_i + 1)

                row_values[date_i] = date
                row_values[time_i] = time
                row_values[name_i] = name
                row_values[phone_i] = phone
                row_values[status_i] = "Booked"
                row_values[source_i] = "WhatsApp"

                sheets_api.values().append(
                    spreadsheetId=sid,
                    range=a1(tab, "A:F"),
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row_values]}
                ).execute()
                return True

        # fallback A–F
        row_values = [date, time, name, phone, "Booked", "WhatsApp"]
        sheets_api.values().append(
            spreadsheetId=sid,
            range=a1(tab, "A:F"),
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]}
        ).execute()
        return True

    except Exception as e:
        print("Sheets append FAILED:", repr(e))
        return False

# -------------------------------------------------
# Flask App
# -------------------------------------------------
app = Flask(__name__)

@app.get("/")
def home():
    return "OK", 200

@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    incoming = request.values.get("Body", "").strip()
    raw_from = request.values.get("From", "")
    user = raw_from.replace("whatsapp:", "")

    resp = MessagingResponse()
    msg = resp.message()

    to_number = request.values.get("To", "").strip()
    clinic_id = resolve_clinic_id(to_number)
    print("Resolved clinic_id:", clinic_id, "To:", to_number)

    if not clinic_id:
        msg.body("This WhatsApp line is not linked to a clinic yet.")
        return Response(str(resp), mimetype="application/xml")

    clinic_settings = load_clinic_settings(clinic_id)
    clinic_sheet_id, clinic_sheet_tab = get_clinic_sheet_config(clinic_settings)
    tz_name, slot_minutes, weekly = get_hours_settings(clinic_settings)

    # Idempotency
    twilio_sid = (request.values.get("MessageSid") or "").strip()
    if twilio_sid and already_processed_twilio_sid(twilio_sid):
        msg.body("✅ Received.")
        return Response(str(resp), mimetype="application/xml")

    save_message(clinic_id, user, "user", incoming, twilio_sid=twilio_sid)

    # -------------------------
    # Commands
    # -------------------------
    if incoming.strip().lower() == "today":
        if not is_admin(user, clinic_settings):
            reply = "Not authorized."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        today = datetime.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
        rows = get_todays_appointments(clinic_id, today)
        if not rows:
            reply = f"No booked appointments for today ({today})."
        else:
            lines = [f"Today ({today}) appointments:"]
            for (name, phone, time, sync_status, ref_code) in rows[:30]:
                lines.append(f"- {time} | {name} | {phone} | ref:{ref_code} | sheets:{sync_status}")
            reply = "\n".join(lines)
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if incoming.strip().lower() == "retry sheets":
        if not is_admin(user, clinic_settings):
            reply = "Not authorized."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        rows = get_unsynced_appointments(clinic_id, limit=20)
        if not rows:
            reply = "No pending/failed sheet syncs found."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        attempted = synced = failed = 0
        for (appt_id, appt_user, appt_name, appt_date, appt_time, appt_status) in rows:
            attempted += 1
            ok = append_to_sheet(appt_date, appt_time, appt_name, appt_user, clinic_sheet_id, clinic_sheet_tab)
            if ok:
                synced += 1
                update_sheet_sync_status(appt_id, "synced")
            else:
                failed += 1
                update_sheet_sync_status(appt_id, "failed", "Retry sheets failed (see logs)")

        reply = f"Retry complete ✅\nAttempted: {attempted}\nSynced: {synced}\nFailed: {failed}"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if incoming.strip().lower() == "my appointment":
        appt = get_latest_booked_appointment(clinic_id, user)
        if not appt:
            reply = "You have no booked appointments right now."
        else:
            appt_id, name, date, time, created_at, ref_code = appt
            reply = f"Your next appointment is on {date} at {time} under the name {name}. Ref: {ref_code}"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    # cancel by reference: "cancel AP-XXXXXX"
    m = re.match(r"^cancel\s+(AP-[A-Z0-9]{6})$", incoming.strip().upper())
    if m:
        ref_code = m.group(1)
        result = cancel_by_ref(clinic_id, user, ref_code)
        if result == "not_owner":
            reply = "That reference code doesn’t belong to your number."
        elif not result:
            reply = "I couldn’t find an active booked appointment with that reference."
        else:
            reply = f"✅ Cancelled appointment on {result['date']} at {result['time']}."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    # cancel latest
    if incoming.strip().lower() == "cancel":
        clear_state_machine(clinic_id, user)
        cancelled = cancel_latest_appointment(clinic_id, user)
        if not cancelled:
            reply = "I couldn’t find an active booked appointment to cancel."
        else:
            reply = f"✅ Cancelled your appointment on {cancelled['date']} at {cancelled['time']}. Ref: {cancelled['ref_code']}"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if incoming.strip().lower() == "reschedule":
        clear_state_machine(clinic_id, user)
        cancelled = cancel_latest_appointment(clinic_id, user)
        set_state_and_draft(clinic_id, user, "collect_name", {})
        if cancelled:
            reply = f"✅ Cancelled {cancelled['date']} {cancelled['time']} (Ref: {cancelled['ref_code']}).\nLet’s reschedule. What’s your full name?"
        else:
            reply = "No active appointment found, but I can help you book a new one. What’s your full name?"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if incoming.strip().lower() == "reset":
        clear_state_machine(clinic_id, user)
        reply = "Session reset. You can start again."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    # -------------------------
    # Booking state machine
    # -------------------------
    state, draft = get_state_and_draft(clinic_id, user)

    if state in ["idle", None, ""] and is_booking_intent(incoming):
        set_state_and_draft(clinic_id, user, "collect_name", {})
        reply = "Sure. What's your full name?"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if state == "collect_name":
        draft["name"] = incoming.strip()
        set_state_and_draft(clinic_id, user, "collect_date", draft)
        reply = "What date would you like? (YYYY-MM-DD)"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if state == "collect_date":
        if looks_like_date(incoming):
            date_str = incoming.strip()

            if not is_open_on_date(date_str, tz_name, weekly):
                reply = "Sorry, we’re closed on that day. Please choose another date."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            draft["date"] = date_str
            set_state_and_draft(clinic_id, user, "collect_time", draft)
            reply = f"What time would you prefer? (HH:MM) e.g. 14:00. Slots are {slot_minutes} minutes."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        reply = "Please type the date like 2026-01-15 (YYYY-MM-DD)."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if state == "collect_time":
        time_24 = normalize_time_to_24h(incoming)
        if not time_24:
            reply = "Please type the time like 09:30 (HH:MM) or 2:30 PM."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        date = draft.get("date", "")

        if not is_time_within_hours(date, time_24, tz_name, weekly):
            hours_str = format_opening_hours_for_day(date, tz_name, weekly)
            reply = f"That time is outside working hours for {date}. Available: {hours_str}."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if not is_slot_aligned(time_24, slot_minutes):
            reply = f"Please choose a time that matches our {slot_minutes}-minute slots (e.g. 09:00, 09:30, 10:00)."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if check_double_booking(clinic_id, date, time_24, clinic_sheet_id, clinic_sheet_tab):
            reply = "That slot is already booked. Choose another time."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        draft["time"] = time_24
        set_state_and_draft(clinic_id, user, "confirm", draft)
        reply = f"Confirm appointment on {date} at {time_24}? (yes/no)"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    if state == "confirm":
        if incoming.lower() in ["yes", "y"]:
            name = draft.get("name", "").strip()
            date = draft.get("date", "").strip()
            time = draft.get("time", "").strip()

            # Save to DB + get ref
            appt_id, ref_code = save_appointment_local(clinic_id, user, name, date, time)

            ok = append_to_sheet(date, time, name, user, clinic_sheet_id, clinic_sheet_tab)
            if ok:
                update_sheet_sync_status(appt_id, "synced")
            else:
                update_sheet_sync_status(appt_id, "failed", "Sheets append failed (see logs)")

            clear_state_machine(clinic_id, user)

            reply = f"✅ Appointment confirmed for {date} at {time}\nRef: {ref_code}\nTo cancel: cancel {ref_code}"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if incoming.lower() in ["no", "n"]:
            clear_state_machine(clinic_id, user)
            reply = "No problem — booking cancelled. Type 'book' to start again."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        reply = "Please reply with 'yes' to confirm or 'no' to cancel."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    # Safety nudge
    maybe_booking_words = ["dent", "tooth", "teeth", "pain", "ache", "clean", "check", "braces", "gum"]
    if any(w in incoming.lower() for w in maybe_booking_words):
        reply = "If you'd like to book an appointment, please type 'book'."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

    # Otherwise AI
    reply = ai_reply(clinic_id, user, incoming)
    msg.body(reply)
    save_message(clinic_id, user, "assistant", reply)
    return Response(str(resp), mimetype="application/xml")

# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting {CLINIC_NAME} bot on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)



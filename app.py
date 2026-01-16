import os
import psycopg2
import datetime
import re
import json
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

# Debug: confirm env vars are loading (safe booleans only)
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

sheets_api = None

def load_service_info():
    # 1) Prefer JSON from env (Render style)
    if SERVICE_JSON:
        return json.loads(SERVICE_JSON)

    # 2) Fallback to file (Local dev style)
    if SERVICE_FILE and os.path.exists(SERVICE_FILE):
        with open(SERVICE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    return None

# ✅ FIX #1: Safe A1 range builder (quotes tab names with dots/spaces/parentheses)
def a1(tab: str, cells: str) -> str:
    safe = tab.replace("'", "''")  # escape single quotes for A1
    return f"'{safe}'!{cells}"

# ✅ PATCH: Header-driven mapping so we never drift to K–U again
def _norm_header(s: str) -> str:
    # normalize: lowercase, trim, collapse spaces
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _index_to_col(idx: int) -> str:
    # 0 -> A, 25 -> Z, 26 -> AA ...
    idx += 1
    out = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        out = chr(65 + r) + out
    return out

def get_sheet_header_map():
    """
    Reads row 1 (A1:Z1) and returns column letters for the fields we need:
    date, time, name, phone, status, source.
    This adapts even if your headers shift.
    """
    if not sheets_api:
        return None

    try:
        res = sheets_api.values().get(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range=a1(SHEET_TAB, "A1:Z1")
        ).execute()
        header_row = (res.get("values") or [[]])[0]

        # Map normalized header text -> index
        header_index = {}
        for i, cell in enumerate(header_row):
            key = _norm_header(cell)
            if key:
                header_index[key] = i

        # Adjust these variants to match your sheet header words
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
            found_idx = None
            for v in variants:
                vkey = _norm_header(v)
                if vkey in header_index:
                    found_idx = header_index[vkey]
                    break
            if found_idx is not None:
                out[field] = _index_to_col(found_idx)

        return out
    except Exception as e:
        print("Header map read failed:", repr(e))
        return None

def _col_to_idx(col: str) -> int:
    # A->0, Z->25, AA->26...
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - 64)
    return n - 1

if True:
    service_info = None
    try:
        service_info = load_service_info()
    except Exception as e:
        print("Service account load failed:", repr(e))
        service_info = None

    if service_info and GOOGLE_SHEETS_ID:
        try:
            print("Sheets target ID:", GOOGLE_SHEETS_ID)
            print("Service account email:", service_info.get("client_email"))
            print("Sheets tab:", SHEET_TAB)

            SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
            creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)
            sheets_service = build("sheets", "v4", credentials=creds)
            sheets_api = sheets_service.spreadsheets()
            print("Google Sheets initialized")

            # TEMP: Confirm access to the spreadsheet ID (helps debug 404 issues)
            try:
                meta = sheets_api.get(spreadsheetId=GOOGLE_SHEETS_ID).execute()
                print("Sheets access OK. Title:", meta.get("properties", {}).get("title"))
            except Exception as e:
                print("Sheets access TEST FAILED:", repr(e))

        except Exception as e:
            print("Google Sheets init failed:", repr(e))
    else:
        if not GOOGLE_SHEETS_ID:
            print("GOOGLE_SHEETS_ID not set — Sheets disabled")
        else:
            print("Service account not set — Sheets disabled (set SERVICE_ACCOUNT_JSON or SERVICE_ACCOUNT_FILE)")

# -------------------------------------------------
# Environment variables
# -------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "").strip()
CLINIC_NAME = os.getenv("CLINIC_NAME", "PrimeCare Medical Centre")

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
# Database (PostgreSQL ONLY)  ✅ Step 2: SQLite removed
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set. This app now requires Postgres (SQLite removed).")
    # Helpful debug so you always know what's being used
    print("DB: USING POSTGRESQL")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = db_conn()
    c = conn.cursor()

    # PostgreSQL tables (same schema you had) + clinic_id columns for future safety
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            clinic_id uuid,
            user_number TEXT,
            role TEXT,
            content TEXT,
            created_at TIMESTAMP
        )
    """)

    # ✅ Add-on: ensure twilio_sid column exists (you already added it via SQL, this is just safety)
    c.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS twilio_sid TEXT")

    # ✅ PATCH: conversations uses composite primary key for multi-clinic safety
    c.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            clinic_id uuid,
            user_number TEXT,
            context TEXT,
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
            created_at TIMESTAMP
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
    # ✅ Add-on: store twilio_sid for inbound messages; ON CONFLICT DO NOTHING prevents duplicate sid inserts
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

# ✅ Add-on: idempotency check
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

# ✅ PATCH: clinic-aware context functions
def get_context(clinic_id, user):
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT context FROM conversations WHERE clinic_id=%s AND user_number=%s", (clinic_id, user))
    r = c.fetchone()
    conn.close()
    return r[0] if r else ""

def set_context(clinic_id, user, ctx):
    conn = db_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO conversations (clinic_id, user_number, context)
        VALUES (%s,%s,%s)
        ON CONFLICT (clinic_id, user_number)
        DO UPDATE SET context=EXCLUDED.context
    """, (clinic_id, user, ctx))
    conn.commit()
    conn.close()

def clear_context(clinic_id, user):
    set_context(clinic_id, user, "")

# -------------------------------------------------
# ✅ PATCH: Clinic resolver (multi-clinic routing foundation)
# -------------------------------------------------
def resolve_clinic_id(to_number: str):
    """
    Looks up which clinic owns this Twilio 'To' number.
    Requires channels table to be created and populated.
    """
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
# ✅ Add-on: clinic-aware DB check (important now)
# -------------------------------------------------
def check_double_booking(clinic_id, date, time):
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

    try:
        header_map = get_sheet_header_map()

        # If header map works, use it (bulletproof)
        if header_map and "date" in header_map and "time" in header_map:
            date_i = _col_to_idx(header_map["date"])
            time_i = _col_to_idx(header_map["time"])

            res = sheets_api.values().get(
                spreadsheetId=GOOGLE_SHEETS_ID,
                range=a1(SHEET_TAB, "A2:Z")
            ).execute()

            for row in res.get("values", []):
                d = row[date_i] if len(row) > date_i else ""
                t = row[time_i] if len(row) > time_i else ""
                if d == date and t == time:
                    return True

            return False

        # Fallback to A–F assumption (A=date, B=time)
        res = sheets_api.values().get(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range=a1(SHEET_TAB, "A2:F")
        ).execute()

        for row in res.get("values", []):
            if len(row) >= 2 and row[0] == date and row[1] == time:
                return True

    except Exception as e:
        print("Sheets check error:", repr(e))

    return False

# -------------------------------------------------
# Validators
# -------------------------------------------------
def looks_like_date(s):
    try:
        datetime.datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except:
        return False

def looks_like_time(s):
    try:
        datetime.datetime.strptime(s.strip(), "%H:%M")
        return True
    except:
        try:
            datetime.datetime.strptime(s.strip(), "%I:%M %p")
            return True
        except:
            return False

# -------------------------------------------------
# Booking intent
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

# -------------------------------------------------
# AI Reply
# -------------------------------------------------
SYSTEM_PROMPT = f"""
You are a medical clinic receptionist for {CLINIC_NAME}.
Keep replies short, polite, and helpful.

CRITICAL RULES:
- Never claim an appointment is booked, confirmed, set, or scheduled.
- Only confirm appointments after the booking flow asks for date, time, and receives a "yes".
- If a user wants an appointment, tell them to type "book" to start the booking.
"""

def ai_reply(clinic_id, user, msg):
    if not openai_client:
        return f"This is {CLINIC_NAME}. Say 'book' to make an appointment."

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
# Save appointment (clinic-aware insert; protected by your unique index)
# -------------------------------------------------
def save_appointment_local(clinic_id, user, name, date, time):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO appointments (clinic_id, user_number, name, date, time, status, source, created_at) VALUES (%s,%s,%s,%s,%s,'Booked','WhatsApp',%s)",
        (clinic_id, user, name, date, time, datetime.datetime.utcnow())
    )
    conn.commit()
    conn.close()
    return None

# ✅ FIX #3: Mapping append to lock values into A–F cleanly (DATE,TIME,NAME,PHONE,STATUS,SOURCE)
def col_to_index(col: str) -> int:
    col = col.strip().upper()
    return ord(col) - ord("A")  # A-Z

def build_row_from_map(column_map: dict, data: dict) -> list:
    max_index = max(col_to_index(c) for c in column_map.values())
    row = [""] * (max_index + 1)
    for field, col in column_map.items():
        row[col_to_index(col)] = data.get(field, "")
    return row

def append_to_sheet(date, time, name, phone):
    # If Sheets wasn't initialized, skip but LOG why
    if not sheets_api:
        print("Sheets append skipped: sheets_api is None (Sheets not initialized).")
        print("Check SERVICE_ACCOUNT_JSON or SERVICE_ACCOUNT_FILE and GOOGLE_SHEETS_ID env vars.")
        return

    if not GOOGLE_SHEETS_ID:
        print("Sheets append skipped: GOOGLE_SHEETS_ID is empty.")
        return

    try:
        header_map = get_sheet_header_map()

        # If header map works, write EXACTLY under the real headers (bulletproof)
        if header_map:
            required = ["date", "time", "name", "phone", "status", "source"]
            missing = [k for k in required if k not in header_map]
            if missing:
                print("Sheets append FAILED: Missing headers in row 1:", missing)
                print("Detected header map:", header_map)
                # fall back below
            else:
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

                print("APPEND HEADER MAP:", header_map)
                print("APPEND RANGE:", a1(SHEET_TAB, "A:F"))
                print("APPEND VALUES:", row_values)

                sheets_api.values().append(
                    spreadsheetId=GOOGLE_SHEETS_ID,
                    range=a1(SHEET_TAB, "A:F"),
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row_values]}
                ).execute()
                print("Sheets append OK:", date, time, name, phone)
                return

        # Fallback to clean A–F layout
        column_map = {
            "date": "A",
            "time": "B",
            "name": "C",
            "phone": "D",
            "status": "E",
            "source": "F",
        }

        data = {
            "date": date,
            "time": time,
            "name": name,
            "phone": phone,
            "status": "Booked",
            "source": "WhatsApp",
        }

        row_values = build_row_from_map(column_map, data)

        print("APPEND RANGE:", a1(SHEET_TAB, "A:F"))
        print("APPEND VALUES:", row_values)

        sheets_api.values().append(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range=a1(SHEET_TAB, "A:F"),
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]}
        ).execute()
        print("Sheets append OK:", date, time, name, phone)

    except Exception as e:
        print("Sheets append FAILED:", repr(e))

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

    # Resolve clinic_id by Twilio To number
    to_number = request.values.get("To", "").strip()
    clinic_id = resolve_clinic_id(to_number)
    print("Resolved clinic_id:", clinic_id, "To:", to_number)

    if not clinic_id:
        msg.body("This WhatsApp line is not linked to a clinic yet.")
        return Response(str(resp), mimetype="application/xml")

    # ✅ Idempotency: ignore Twilio retries
    twilio_sid = (request.values.get("MessageSid") or "").strip()
    if twilio_sid and already_processed_twilio_sid(twilio_sid):
        msg.body("✅ Received.")
        return Response(str(resp), mimetype="application/xml")

    # Save inbound message with sid
    save_message(clinic_id, user, "user", incoming, twilio_sid=twilio_sid)

    context = get_context(clinic_id, user)

    if incoming.lower() == "reset":
        clear_context(clinic_id, user)
        reply = "Session reset. You can start again."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        print("TWIML OUT:", str(resp))
        return Response(str(resp), mimetype="application/xml")

    # Booking flow
    if context == "awaiting_name":
        set_context(clinic_id, user, f"name:{incoming}")
        reply = "What date would you like? (YYYY-MM-DD)"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        print("TWIML OUT:", str(resp))
        return Response(str(resp), mimetype="application/xml")

    if context.startswith("name:") and "|date:" not in context:
        if looks_like_date(incoming):
            name = context.split("name:", 1)[1]
            set_context(clinic_id, user, f"name:{name}|date:{incoming}")
            reply = "What time would you prefer? (HH:MM) e.g. 14:00"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            print("TWIML OUT:", str(resp))
            return Response(str(resp), mimetype="application/xml")
        else:
            reply = "Please type the date like 2026-01-15 (YYYY-MM-DD)."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            print("TWIML OUT:", str(resp))
            return Response(str(resp), mimetype="application/xml")

    if "|date:" in context and "|time:" not in context:
        if looks_like_time(incoming):
            parts = context.split("|")
            name = parts[0].split("name:", 1)[1]
            date = parts[1].split("date:", 1)[1]

            if check_double_booking(clinic_id, date, incoming):
                reply = "That slot is already booked. Choose another time."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                print("TWIML OUT:", str(resp))
                return Response(str(resp), mimetype="application/xml")

            set_context(clinic_id, user, f"name:{name}|date:{date}|time:{incoming}")
            reply = f"Confirm appointment on {date} at {incoming}? (yes/no)"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            print("TWIML OUT:", str(resp))
            return Response(str(resp), mimetype="application/xml")
        else:
            reply = "Please type the time like 09:30 (HH:MM) e.g. 14:00."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            print("TWIML OUT:", str(resp))
            return Response(str(resp), mimetype="application/xml")

    if "|time:" in context:
        if incoming.lower() in ["yes", "y"]:
            parts = context.split("|")
            name = parts[0].split("name:", 1)[1]
            date = parts[1].split("date:", 1)[1]
            time = parts[2].split("time:", 1)[1]

            # ✅ DB-safe insert (protected by unique index). Catch race duplicates.
            try:
                save_appointment_local(clinic_id, user, name, date, time)
            except psycopg2.Error as e:
                # 23505 = unique_violation
                if getattr(e, "pgcode", None) == "23505":
                    reply = "That slot is already booked. Choose another time."
                    msg.body(reply)
                    save_message(clinic_id, user, "assistant", reply)
                    return Response(str(resp), mimetype="application/xml")
                raise

            append_to_sheet(date, time, name, user)
            clear_context(clinic_id, user)

            reply = f"✅ Appointment confirmed for {date} at {time}"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            print("TWIML OUT:", str(resp))
            return Response(str(resp), mimetype="application/xml")

        if incoming.lower() in ["no", "n"]:
            clear_context(clinic_id, user)
            reply = "No problem — booking cancelled. Type 'book' to start again."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            print("TWIML OUT:", str(resp))
            return Response(str(resp), mimetype="application/xml")

        reply = "Please reply with 'yes' to confirm or 'no' to cancel."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        print("TWIML OUT:", str(resp))
        return Response(str(resp), mimetype="application/xml")

    # If user is trying to book, start the real booking flow
    if is_booking_intent(incoming):
        set_context(clinic_id, user, "awaiting_name")
        reply = "Sure. What's your full name?"
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        print("TWIML OUT:", str(resp))
        return Response(str(resp), mimetype="application/xml")

    # Safety nudge
    maybe_booking_words = ["dent", "tooth", "teeth", "pain", "ache", "clean", "check", "braces", "gum"]
    if any(w in incoming.lower() for w in maybe_booking_words):
        reply = "If you'd like to book an appointment, please type 'book'."
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        print("TWIML OUT:", str(resp))
        return Response(str(resp), mimetype="application/xml")

    # Otherwise, use AI for general replies
    reply = ai_reply(clinic_id, user, incoming)
    msg.body(reply)
    save_message(clinic_id, user, "assistant", reply)
    print("TWIML OUT:", str(resp))
    return Response(str(resp), mimetype="application/xml")

# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting {CLINIC_NAME} bot on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)

import os
import sqlite3
import psycopg2
import datetime
import re
import json
from flask import Flask, request
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

# -------------------------------------------------
# Google Sheets (Render-safe)
# -------------------------------------------------
SERVICE_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "").strip()

sheets_api = None

if SERVICE_JSON:
    try:
        service_info = json.loads(SERVICE_JSON)
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)
        sheets_service = build("sheets", "v4", credentials=creds)
        sheets_api = sheets_service.spreadsheets()
        print("Google Sheets initialized")
    except Exception as e:
        print("Google Sheets init failed:", e)
else:
    print("SERVICE_ACCOUNT_JSON not set — Sheets disabled")

# -------------------------------------------------
# Environment variables
# -------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "")
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
        print("OpenAI init error:", e)
else:
    print("Warning: OPENAI_API_KEY not set — AI replies disabled")

# -------------------------------------------------
# Database (SQLite local → PostgreSQL on Render)
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
LOCAL_DB = "clinic_local.db"

def db_conn():
    # Helpful debug so you always know what's being used
    if DATABASE_URL:
        print("DB: USING POSTGRESQL")
        return psycopg2.connect(DATABASE_URL)
    print("DB: USING SQLITE:", os.path.abspath(LOCAL_DB))
    return sqlite3.connect(LOCAL_DB)

def init_db():
    conn = db_conn()
    c = conn.cursor()

    if DATABASE_URL:
        # PostgreSQL
        c.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                user_number TEXT,
                role TEXT,
                content TEXT,
                created_at TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                user_number TEXT PRIMARY KEY,
                context TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id SERIAL PRIMARY KEY,
                user_number TEXT,
                name TEXT,
                date TEXT,
                time TEXT,
                status TEXT,
                source TEXT,
                created_at TIMESTAMP
            )
        """)
    else:
        # SQLite
        c.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_number TEXT,
                role TEXT,
                content TEXT,
                created_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                user_number TEXT PRIMARY KEY,
                context TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_number TEXT,
                name TEXT,
                date TEXT,
                time TEXT,
                status TEXT,
                source TEXT,
                created_at TEXT
            )
        """)

    conn.commit()
    conn.close()
    print("DB tables checked/created successfully")

init_db()

# -------------------------------------------------
# DB Helpers
# -------------------------------------------------
def save_message(user, role, msg):
    conn = db_conn()
    c = conn.cursor()
    q = (
        "INSERT INTO messages (user_number, role, content, created_at) VALUES (%s,%s,%s,%s)"
        if DATABASE_URL else
        "INSERT INTO messages (user_number, role, content, created_at) VALUES (?,?,?,?)"
    )
    c.execute(q, (user, role, msg, datetime.datetime.utcnow()))
    conn.commit()
    conn.close()

def load_recent_messages(user, limit=12):
    conn = db_conn()
    c = conn.cursor()
    q = (
        f"SELECT role, content FROM messages WHERE user_number=%s ORDER BY id DESC LIMIT {limit}"
        if DATABASE_URL else
        f"SELECT role, content FROM messages WHERE user_number=? ORDER BY id DESC LIMIT {limit}"
    )
    c.execute(q, (user,))
    rows = c.fetchall()
    conn.close()
    rows.reverse()
    return [{"role": r, "content": t} for r, t in rows]

def get_context(user):
    conn = db_conn()
    c = conn.cursor()
    q = (
        "SELECT context FROM conversations WHERE user_number=%s"
        if DATABASE_URL else
        "SELECT context FROM conversations WHERE user_number=?"
    )
    c.execute(q, (user,))
    r = c.fetchone()
    conn.close()
    return r[0] if r else ""

def set_context(user, ctx):
    conn = db_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("""
            INSERT INTO conversations (user_number, context)
            VALUES (%s,%s)
            ON CONFLICT (user_number)
            DO UPDATE SET context=EXCLUDED.context
        """, (user, ctx))
    else:
        c.execute("""
            INSERT OR REPLACE INTO conversations (user_number, context)
            VALUES (?,?)
        """, (user, ctx))
    conn.commit()
    conn.close()

def clear_context(user):
    set_context(user, "")

# -------------------------------------------------
# Double booking check (DB + Google Sheets)
# -------------------------------------------------
def check_double_booking(date, time):
    conn = db_conn()
    c = conn.cursor()
    q = (
        "SELECT id FROM appointments WHERE date=%s AND time=%s AND status='Booked'"
        if DATABASE_URL else
        "SELECT id FROM appointments WHERE date=? AND time=? AND status='Booked'"
    )
    c.execute(q, (date, time))
    exists = c.fetchone()
    conn.close()

    if exists:
        return True

    if not sheets_api:
        return False

    try:
        res = sheets_api.values().get(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range="Sheet1!A2:B"
        ).execute()
        for row in res.get("values", []):
            if len(row) >= 2 and row[0] == date and row[1] == time:
                return True
    except Exception as e:
        print("Sheets check error:", e)

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
# EXPANDED to catch dental/clinic style messages better
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
# UPDATED so AI never “pretends” booking happened
SYSTEM_PROMPT = f"""
You are a medical clinic receptionist for {CLINIC_NAME}.
Keep replies short, polite, and helpful.

CRITICAL RULES:
- Never claim an appointment is booked, confirmed, set, or scheduled.
- Only confirm appointments after the booking flow asks for date, time, and receives a "yes".
- If a user wants an appointment, tell them to type "book" to start the booking.
"""

def ai_reply(user, msg):
    if not openai_client:
        return f"This is {CLINIC_NAME}. Say 'book' to make an appointment."

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages += load_recent_messages(user)
    messages.append({"role": "user", "content": msg})

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=200
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print("AI error:", e)
        return "Sorry, something went wrong."

# -------------------------------------------------
# Save appointment
# -------------------------------------------------
def save_appointment_local(user, name, date, time):
    conn = db_conn()
    c = conn.cursor()
    q = (
        "INSERT INTO appointments (user_number,name,date,time,status,source,created_at) VALUES (%s,%s,%s,%s,'Booked','WhatsApp',%s)"
        if DATABASE_URL else
        "INSERT INTO appointments (user_number,name,date,time,status,source,created_at) VALUES (?,?,?,?, 'Booked','WhatsApp',?)"
    )
    c.execute(q, (user, name, date, time, datetime.datetime.utcnow()))
    conn.commit()
    appt_id = c.lastrowid if not DATABASE_URL else None
    conn.close()
    return appt_id

def append_to_sheet(date, time, name, phone):
    if not sheets_api:
        return
    sheets_api.values().append(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range="Sheet1!A:E",
        valueInputOption="RAW",
        body={"values": [[date, time, name, phone, "WhatsApp"]]}
    ).execute()

# -------------------------------------------------
# Flask App
# -------------------------------------------------
app = Flask(__name__)

# Health route (stops Render GET / 404 spam)
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

    save_message(user, "user", incoming)
    context = get_context(user)

    if incoming.lower() == "reset":
        clear_context(user)
        reply = "Session reset. You can start again."
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    # Booking flow
    if context == "awaiting_name":
        set_context(user, f"name:{incoming}")
        reply = "What date would you like? (YYYY-MM-DD)"
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    if context.startswith("name:") and "|date:" not in context:
        if looks_like_date(incoming):
            name = context.split("name:", 1)[1]
            set_context(user, f"name:{name}|date:{incoming}")
            reply = "What time would you prefer? (HH:MM) e.g. 14:00"
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)
        else:
            # NEW: don’t silently fall through
            reply = "Please type the date like 2026-01-15 (YYYY-MM-DD)."
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)

    if "|date:" in context and "|time:" not in context:
        if looks_like_time(incoming):
            parts = context.split("|")
            name = parts[0].split("name:", 1)[1]
            date = parts[1].split("date:", 1)[1]

            if check_double_booking(date, incoming):
                reply = "That slot is already booked. Choose another time."
                msg.body(reply)
                save_message(user, "assistant", reply)
                return str(resp)

            set_context(user, f"name:{name}|date:{date}|time:{incoming}")
            reply = f"Confirm appointment on {date} at {incoming}? (yes/no)"
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)
        else:
            # NEW: don’t silently fall through
            reply = "Please type the time like 09:30 (HH:MM) e.g. 14:00."
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)

    if "|time:" in context:
        if incoming.lower() in ["yes", "y"]:
            parts = context.split("|")
            name = parts[0].split("name:", 1)[1]
            date = parts[1].split("date:", 1)[1]
            time = parts[2].split("time:", 1)[1]

            save_appointment_local(user, name, date, time)
            append_to_sheet(date, time, name, user)
            clear_context(user)

            reply = f"✅ Appointment confirmed for {date} at {time}"
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)

        if incoming.lower() in ["no", "n"]:
            clear_context(user)
            reply = "No problem — booking cancelled. Type 'book' to start again."
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)

        # NEW: if they type something else at confirm stage
        reply = "Please reply with 'yes' to confirm or 'no' to cancel."
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    # If user is trying to book, start the real booking flow
    if is_booking_intent(incoming):
        set_context(user, "awaiting_name")
        reply = "Sure. What's your full name?"
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    # NEW: safety nudge to prevent AI “fake booking” if they mention dental/pain etc.
    maybe_booking_words = ["dent", "tooth", "teeth", "pain", "ache", "clean", "check", "braces", "gum"]
    if any(w in incoming.lower() for w in maybe_booking_words):
        reply = "If you'd like to book an appointment, please type 'book'."
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    # Otherwise, use AI for general replies (but AI is now instructed not to confirm bookings)
    reply = ai_reply(user, incoming)
    msg.body(reply)
    save_message(user, "assistant", reply)
    return str(resp)

# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting {CLINIC_NAME} bot on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)


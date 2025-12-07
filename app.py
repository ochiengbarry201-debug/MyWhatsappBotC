import os
import sqlite3
import datetime
import re
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from dotenv import load_dotenv

# Google Sheets
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# OpenAI
from openai import OpenAI

# --- Load environment ---
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
GOOGLE_SERVICE_JSON = os.getenv("GOOGLE_SERVICE_JSON", "service_account.json")
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "").strip()
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "whatsapp:+254746346234")  
CLINIC_NAME = os.getenv("CLINIC_NAME", "PrimeCare Medical Centre")

# --- Initialize clients & services ---
openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        print("OpenAI client initialized")
    except Exception as e:
        print("OpenAI init error:", e)
else:
    print("Warning: OPENAI_API_KEY not set — fallback assistant only.")

sheets_api = None
if GOOGLE_SHEETS_ID and os.path.exists(GOOGLE_SERVICE_JSON):
    try:
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(GOOGLE_SERVICE_JSON, scopes=SCOPES)
        sheets_service = build("sheets", "v4", credentials=creds)
        sheets_api = sheets_service.spreadsheets()
        print("Google Sheets initialized")
    except Exception as e:
        print("Google Sheets init failed:", e)
else:
    if not GOOGLE_SHEETS_ID:
        print("GOOGLE_SHEETS_ID not set; Sheets disabled.")
    else:
        print("Google service JSON not found at", GOOGLE_SERVICE_JSON)

# --- Database ---
DB_FILE = "clinic_advanced.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

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

init_db()

# --- Helpers ---
def db_conn():
    return sqlite3.connect(DB_FILE)

def save_message(user, role, msg):
    conn = db_conn()
    c = conn.cursor()
    c.execute("INSERT INTO messages (user_number, role, content, created_at) VALUES (?, ?, ?, ?)",
              (user, role, msg, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def load_recent_messages(user, limit=12):
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT role, content FROM messages WHERE user_number=? ORDER BY id DESC LIMIT ?", (user, limit))
    rows = c.fetchall()
    conn.close()
    rows.reverse()
    return [{"role": r, "content": t} for r, t in rows]

def get_context(user):
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT context FROM conversations WHERE user_number=?", (user,))
    r = c.fetchone()
    conn.close()
    return r[0] if r else ""

def set_context(user, ctx):
    conn = db_conn()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO conversations (user_number, context) VALUES (?, ?)", (user, ctx))
    conn.commit()
    conn.close()

def clear_context(user):
    set_context(user, "")

# === MUST BE ABOVE BOOKING FLOW (THE FIX) ===
def check_double_booking(date, time):
    # local DB check
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM appointments WHERE date=? AND time=? AND status='Booked'", (date, time))
    r = c.fetchone()
    conn.close()
    if r:
        return True

    # Google Sheets check
    if not sheets_api:
        return False

    try:
        r = sheets_api.values().get(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range="Sheet1!A2:B"
        ).execute()
        rows = r.get("values", [])
        for row in rows:
            if len(row) >= 2 and row[0] == date and row[1] == time:
                return True
    except Exception as e:
        print("Google check error:", e)

    return False

# --- Validators ---
def looks_like_date(s):
    try:
        datetime.datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except:
        return False

def looks_like_time(s):
    s = s.strip()
    try:
        datetime.datetime.strptime(s, "%H:%M")
        return True
    except:
        try:
            datetime.datetime.strptime(s, "%I:%M %p")
            return True
        except:
            return False

# --- Booking intent detection ---
BOOKING_KEYWORDS = [
    "book", "appointment", "schedule", "dentist", "doctor",
    "see a doctor", "clinic", "visit", "pain", "booking"
]

def is_booking_intent(text):
    t = text.lower()
    for k in BOOKING_KEYWORDS:
        if k in t:
            return True
    return False

# --- AI Replies ---
SYSTEM_PROMPT = f"""
You are the friendly receptionist bot for {CLINIC_NAME}.
Keep replies short and helpful. Do NOT book appointments yourself —
the backend handles booking. If user mentions pain or asks to see a doctor,
be comforting and guide them naturally.
"""

def ai_reply(user, msg):
    if not openai_client:
        return f"This is {CLINIC_NAME}. How can I help you? Say 'book' to make an appointment."

    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msgs += load_recent_messages(user)
    msgs.append({"role": "user", "content": msg})

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=msgs,
            max_tokens=200
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print("AI error:", e)
        return f"Sorry, I'm having trouble. You can say 'book' to start an appointment."

# --- Save appointment ---
def save_appointment_local(user, name, date, time):
    conn = db_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO appointments (user_number, name, date, time, status, source, created_at)
        VALUES (?, ?, ?, ?, 'Booked', 'WhatsApp', ?)
    """, (user, name, date, time, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    appt_id = c.lastrowid
    conn.close()
    return appt_id

def append_to_sheet(date, time, name, phone):
    if not sheets_api:
        return False
    try:
        sheets_api.values().append(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range="Sheet1!A:E",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [[date, time, name, phone, "WhatsApp"]]}
        ).execute()
        return True
    except:
        return False

# --- Flask App ---
app = Flask(__name__)

@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    incoming = request.values.get("Body", "").strip()
    raw_from = request.values.get("From", "")
    user = raw_from.replace("whatsapp:", "")

    resp = MessagingResponse()
    msg = resp.message()

    save_message(user, "user", incoming)
    context = get_context(user)

    # Reset
    if incoming.lower() == "reset":
        clear_context(user)
        reply = f"Your session has been reset. You may start booking again at {CLINIC_NAME}."
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    # Admin View
    if (incoming.lower().startswith("show appts") or "show appointments" in incoming.lower()) and \
        f"whatsapp:{user}" == ADMIN_WHATSAPP:
        conn = db_conn()
        c = conn.cursor()
        c.execute("SELECT id, name, date, time, status FROM appointments ORDER BY created_at DESC LIMIT 30")
        rows = c.fetchall()
        conn.close()

        if not rows:
            msg.body("No appointments found.")
        else:
            text = "\n".join([f"{r[0]} - {r[1]} - {r[2]} {r[3]} ({r[4]})" for r in rows])
            msg.body("Recent appointments:\n" + text)
        return str(resp)

    # Admin Cancel
    m = re.match(r"cancel\s+(\d+)", incoming.lower())
    if m and f"whatsapp:{user}" == ADMIN_WHATSAPP:
        appt_id = int(m.group(1))
        conn = db_conn()
        c = conn.cursor()
        c.execute("UPDATE appointments SET status='Cancelled' WHERE id=?", (appt_id,))
        conn.commit()
        if c.rowcount > 0:
            msg.body(f"Appointment {appt_id} cancelled.")
        else:
            msg.body("Not found.")
        return str(resp)

    # === BOOKING FLOW ===

    if context == "awaiting_name":
        name = incoming.strip()
        set_context(user, f"name:{name}")
        reply = f"Thanks {name}. What date would you like? (YYYY-MM-DD)"
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    if context.startswith("name:") and "|date:" not in context:
        name = context.split("name:", 1)[1]
        if looks_like_date(incoming):
            date = incoming
            set_context(user, f"name:{name}|date:{date}")
            reply = "Great. What time would you prefer? (HH:MM)"
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)
        else:
            ai = ai_reply(user, incoming)
            msg.body(ai)
            save_message(user, "assistant", ai)
            return str(resp)

    if "|date:" in context and "|time:" not in context:
        name = context.split("name:", 1)[1].split("|")[0]
        date = context.split("date:", 1)[1]

        if looks_like_time(incoming):
            time = incoming.strip()

            if check_double_booking(date, time):
                reply = "Sorry, that slot is already booked. Please choose another time."
                msg.body(reply)
                save_message(user, "assistant", reply)
                return str(resp)

            set_context(user, f"name:{name}|date:{date}|time:{time}")
            reply = f"Confirm appointment for {date} at {time}? (yes/no)"
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)
        else:
            ai = ai_reply(user, incoming)
            msg.body(ai)
            save_message(user, "assistant", ai)
            return str(resp)

    if "|time:" in context:
        parts = context.split("|")
        name = parts[0].split("name:", 1)[1]
        date = parts[1].split("date:", 1)[1]
        time = parts[2].split("time:", 1)[1]

        if incoming.lower() in ["yes", "y", "confirm", "sure"]:
            # final double booking check
            if check_double_booking(date, time):
                set_context(user, f"name:{name}|date:{date}")
                reply = "Sorry — this slot was just taken. Please choose another time."
                msg.body(reply)
                save_message(user, "assistant", reply)
                return str(resp)

            appt_id = save_appointment_local(user, name, date, time)
            append_to_sheet(date, time, name, user)

            set_context(user, "done")
            reply = f"✔️ Appointment confirmed for {date} at {time}. Your ID is {appt_id}. Thank you for choosing {CLINIC_NAME}."
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)

        if incoming.lower() in ["no", "n"]:
            set_context(user, f"name:{name}|date:{date}")
            reply = "Okay. Please provide another time."
            msg.body(reply)
            save_message(user, "assistant", reply)
            return str(resp)

        msg.body("Please reply 'yes' or 'no'.")
        return str(resp)

    # NEW BOOKING FLOW START
    if is_booking_intent(incoming):
        set_context(user, "awaiting_name")
        reply = f"Sure — I can help you book an appointment at {CLINIC_NAME}. What's your full name?"
        msg.body(reply)
        save_message(user, "assistant", reply)
        return str(resp)

    # Default assistant
    reply = ai_reply(user, incoming)
    msg.body(reply)
    save_message(user, "assistant", reply)
    return str(resp)

# Admin web view
@app.route("/appointments", methods=["GET"])
def view_appointments():
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT id, user_number, name, date, time, status FROM appointments ORDER BY created_at DESC LIMIT 100")
    rows = c.fetchall()
    conn.close()

    html = "<h2>Appointments</h2><table border='1'>"
    html += "<tr><th>ID</th><th>Phone</th><th>Name</th><th>Date</th><th>Time</th><th>Status</th></tr>"

    for r in rows:
        html += "<tr>" + "".join([f"<td>{x}</td>" for x in r]) + "</tr>"

    html += "</table>"
    return html

# Run
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting {CLINIC_NAME} bot on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)

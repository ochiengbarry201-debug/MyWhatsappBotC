from openai import OpenAI
from config import OPENAI_API_KEY, CLINIC_NAME
from db import load_recent_messages
import json

# ✅ Marker that routes.py can use if AI explicitly offers booking in normal chat replies
OFFER_BOOKING_MARKER = "<<OFFER_BOOKING>>"

openai_client = None


def init_ai():
    global openai_client
    if OPENAI_API_KEY:
        try:
            openai_client = OpenAI(api_key=OPENAI_API_KEY)
            print("OpenAI client initialized")
        except Exception as e:
            print("OpenAI init error:", repr(e))
    else:
        print("Warning: OPENAI_API_KEY not set — AI replies disabled")


def _build_system_prompt(clinic: dict):
    clinic_name = clinic.get("name") or CLINIC_NAME

    return f"""
You are a polite, professional, and friendly dental clinic receptionist for {clinic_name}.

Guidelines:
- Greet patients warmly and naturally
- Answer general dental questions clearly and simply
- Be empathetic if someone mentions pain or discomfort
- Do NOT diagnose or give medical treatment advice
- Encourage booking politely when appropriate, but do not force it
- Keep responses human, calm, and helpful (not robotic)

Booking handoff rule (IMPORTANT):
- If you are offering to help the patient book an appointment, include the exact marker {OFFER_BOOKING_MARKER} anywhere in your reply.
- Only include it when you are explicitly asking if they want to book / want help booking.
- Do NOT explain the marker.
- Do NOT include it for general dental Q&A.

If the patient wants to book:
- Ask for their full name
- Ask for preferred date
- Ask for preferred time
- Confirm before booking
""".strip()


def ai_reply(clinic: dict, user: str, msg: str):
    clinic_id = clinic.get("id")
    clinic_name = clinic.get("name") or CLINIC_NAME

    if not openai_client:
        return f"This is {clinic_name}. How may we help you today?"

    messages = [
        {"role": "system", "content": _build_system_prompt(clinic)}
    ]

    # Keep conversation memory EXACTLY as before
    messages += load_recent_messages(clinic_id, user)
    messages.append({"role": "user", "content": msg})

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=250,
            temperature=0.6
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print("AI error:", repr(e))
        return "Sorry, something went wrong. Please try again."


def ai_extract_booking_signal(clinic: dict, user_text: str):
    """
    Extract structured intent + booking info from a free-form user message.

    Returns:
    {
      "intent": "greeting|general|book|cancel|reschedule",
      "name": None | str,
      "date": None | str,
      "time": None | str
    }
    """
    clinic_name = clinic.get("name") or CLINIC_NAME

    if not openai_client:
        return {
            "intent": "general",
            "name": None,
            "date": None,
            "time": None,
        }

    system = f"""
You extract structured appointment information for {clinic_name}.

Return ONLY valid JSON with exactly these keys:
- intent
- name
- date
- time

Rules:
- intent must be one of: greeting, general, book, cancel, reschedule
- Use intent="greeting" only for simple greetings with no real request
- Use intent="general" for dental questions, normal chat, or anything that is not clearly booking/cancel/reschedule
- Use intent="book" when the user wants a new appointment / visit / consultation / checkup / booking / to see the dentist
- Use intent="cancel" when the user wants to cancel an appointment
- Use intent="reschedule" when the user wants to move/change an existing appointment
- name should be null if not clearly provided
- date should be null unless the user clearly provided a date
- time should be null unless the user clearly provided a time
- If the user says something vague like "morning", "afternoon", "evening", do not invent an exact clock time
- Do not add any extra keys
- Do not explain anything

Important booking detection:
- Detect booking intent even if mixed with a greeting
- Detect booking intent even in casual wording
- Detect booking intent in both English and common Swahili phrasing

Examples of booking intent:
- I want an appointment
- Can I book for tomorrow?
- I need to see the dentist
- I want to come on Tuesday
- Book me for Friday
- nataka appointment
- nataka booking
- nataka kuona dentist
- naweza kuja kesho
- nipatie appointment
- I want to visit the clinic
- can I come tomorrow at 10
""".strip()

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_text},
            ],
            temperature=0,
            max_tokens=120,
            response_format={"type": "json_object"},
        )

        raw = res.choices[0].message.content.strip()
        print("EXTRACT RAW:", raw)

        data = json.loads(raw)

        intent = str(data.get("intent") or "general").strip().lower()
        if intent not in {"greeting", "general", "book", "cancel", "reschedule"}:
            intent = "general"

        name = data.get("name")
        date = data.get("date")
        time = data.get("time")

        result = {
            "intent": intent,
            "name": str(name).strip() if name else None,
            "date": str(date).strip() if date else None,
            "time": str(time).strip() if time else None,
        }

        print("EXTRACT PARSED:", result)
        return result

    except Exception as e:
        print("AI extract error:", repr(e))
        return {
            "intent": "general",
            "name": None,
            "date": None,
            "time": None,
        }
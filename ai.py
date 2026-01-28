from openai import OpenAI
from config import OPENAI_API_KEY, CLINIC_NAME
from db import load_recent_messages

# ✅ Marker that routes.py will detect and remove before sending to the user
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



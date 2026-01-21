from openai import OpenAI
from config import OPENAI_API_KEY, CLINIC_NAME
from db import load_recent_messages

SYSTEM_PROMPT = f"""
You are a medical clinic receptionist for {CLINIC_NAME}.
Keep replies short, polite, and helpful.
"""

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

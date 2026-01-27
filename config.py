import os
from dotenv import load_dotenv

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

# -------------------------------------------------
# Environment variables
# -------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "").strip()
CLINIC_NAME = os.getenv("CLINIC_NAME", "PrimeCare Dental Clinic")

# -------------------------------------------------
# Database (PostgreSQL ONLY)
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

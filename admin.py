import re
from config import ADMIN_WHATSAPP

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

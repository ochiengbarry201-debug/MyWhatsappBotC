import datetime

# -------------------------------------------------
# Booking intent keywords
# IMPORTANT:
# - Keep this list focused on CLEAR booking actions
# - General dental terms are handled by AI, not booking
# -------------------------------------------------
BOOKING_KEYWORDS = [
    "book",
    "booking",
    "appointment",
    "schedule",
    "reschedule",
    "cancel appointment",
    "cancel booking",
    "make appointment",
    "see dentist",
    "visit clinic"
]

def is_booking_intent(text):
    """
    Returns True only if the user clearly intends to book
    or manage an appointment.
    """
    if not text:
        return False

    t = text.lower().strip()
    return any(k in t for k in BOOKING_KEYWORDS)


def looks_like_date(s):
    """
    Detects YYYY-MM-DD date format.
    Kept exactly as-is to avoid breaking existing logic.
    """
    try:
        datetime.datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except:
        return False

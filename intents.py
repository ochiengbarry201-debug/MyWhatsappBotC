import datetime

# -------------------------------------------------
# Booking intent keywords
# IMPORTANT:
# - Keep this list focused on CLEAR booking actions
# - CANCEL/RESCHEDULE are handled separately below
# -------------------------------------------------
BOOKING_KEYWORDS = [
    "book",
    "booking",
    "appointment",
    "schedule",
    "make appointment",
    "see dentist",
    "visit clinic"
]

# -------------------------------------------------
# Cancel / Reschedule intent keywords (separate)
# -------------------------------------------------
CANCEL_KEYWORDS = [
    "cancel",
    "cancel appointment",
    "cancel booking",
    "i want to cancel",
    "i would like to cancel",
    "remove appointment",
    "delete appointment"
]

RESCHEDULE_KEYWORDS = [
    "reschedule",
    "change appointment",
    "move appointment",
    "postpone appointment",
    "push appointment",
    "change time",
    "change date"
]


def is_booking_intent(text):
    """
    Returns True only if the user clearly intends to book
    or manage a NEW appointment.
    """
    if not text:
        return False

    t = text.lower().strip()
    return any(k in t for k in BOOKING_KEYWORDS)


def is_cancel_intent(text):
    """
    Returns True if the user is trying to cancel an appointment.
    """
    if not text:
        return False

    t = text.lower().strip()
    return any(k in t for k in CANCEL_KEYWORDS)


def is_reschedule_intent(text):
    """
    Returns True if the user is trying to reschedule an appointment.
    """
    if not text:
        return False

    t = text.lower().strip()
    return any(k in t for k in RESCHEDULE_KEYWORDS)


def looks_like_date(s):
    """
    Detects YYYY-MM-DD date format.
    Kept to avoid breaking existing logic.
    """
    try:
        datetime.datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except:
        return False

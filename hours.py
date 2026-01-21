import datetime
import re
from zoneinfo import ZoneInfo

DEFAULT_HOURS = {
    "timezone": "Africa/Nairobi",
    "slot_minutes": 30,
    "weekly": {
        "mon": [{"start": "09:00", "end": "17:00"}],
        "tue": [{"start": "09:00", "end": "17:00"}],
        "wed": [{"start": "09:00", "end": "17:00"}],
        "thu": [{"start": "09:00", "end": "17:00"}],
        "fri": [{"start": "09:00", "end": "17:00"}],
        "sat": [{"start": "09:00", "end": "13:00"}],
        "sun": []
    }
}

def get_hours_settings(clinic_settings: dict):
    hours = clinic_settings.get("hours") if isinstance(clinic_settings, dict) else None
    if not isinstance(hours, dict):
        hours = DEFAULT_HOURS
    timezone = (hours.get("timezone") or DEFAULT_HOURS["timezone"]).strip()
    slot_minutes = hours.get("slot_minutes", DEFAULT_HOURS["slot_minutes"])
    try:
        slot_minutes = int(slot_minutes)
        if slot_minutes <= 0:
            slot_minutes = DEFAULT_HOURS["slot_minutes"]
    except:
        slot_minutes = DEFAULT_HOURS["slot_minutes"]
    weekly = hours.get("weekly", DEFAULT_HOURS["weekly"])
    if not isinstance(weekly, dict):
        weekly = DEFAULT_HOURS["weekly"]
    return timezone, slot_minutes, weekly

def parse_hhmm_to_minutes(hhmm: str):
    hhmm = (hhmm or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", hhmm)
    if not m:
        return None
    h = int(m.group(1))
    mi = int(m.group(2))
    if h < 0 or h > 23 or mi < 0 or mi > 59:
        return None
    return h * 60 + mi

def normalize_time_to_24h(s: str):
    s = (s or "").strip()
    try:
        t = datetime.datetime.strptime(s, "%H:%M").time()
        return f"{t.hour:02d}:{t.minute:02d}"
    except:
        pass
    try:
        t = datetime.datetime.strptime(s, "%I:%M %p").time()
        return f"{t.hour:02d}:{t.minute:02d}"
    except:
        return None

def weekday_key_from_date(date_str: str, tz_name: str):
    tz = ZoneInfo(tz_name)
    d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    idx = datetime.datetime(d.year, d.month, d.day, 12, 0, tzinfo=tz).weekday()
    keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    return keys[idx]

def is_open_on_date(date_str: str, tz_name: str, weekly: dict):
    try:
        day_key = weekday_key_from_date(date_str, tz_name)
        intervals = weekly.get(day_key, [])
        return isinstance(intervals, list) and len(intervals) > 0
    except:
        return True

def is_time_within_hours(date_str: str, time_24h: str, tz_name: str, weekly: dict):
    try:
        day_key = weekday_key_from_date(date_str, tz_name)
        intervals = weekly.get(day_key, [])
        if not isinstance(intervals, list) or len(intervals) == 0:
            return False

        tmin = parse_hhmm_to_minutes(time_24h)
        if tmin is None:
            return False

        for it in intervals:
            if not isinstance(it, dict):
                continue
            start = parse_hhmm_to_minutes(it.get("start", ""))
            end = parse_hhmm_to_minutes(it.get("end", ""))
            if start is None or end is None:
                continue
            if start <= tmin < end:
                return True
        return False
    except:
        return True

def is_slot_aligned(time_24h: str, slot_minutes: int):
    tmin = parse_hhmm_to_minutes(time_24h)
    if tmin is None:
        return False
    return (tmin % slot_minutes) == 0

def format_opening_hours_for_day(date_str: str, tz_name: str, weekly: dict):
    try:
        day_key = weekday_key_from_date(date_str, tz_name)
        intervals = weekly.get(day_key, [])
        if not isinstance(intervals, list) or len(intervals) == 0:
            return "Closed"
        parts = []
        for it in intervals:
            if isinstance(it, dict) and it.get("start") and it.get("end"):
                parts.append(f"{it['start']}-{it['end']}")
        return ", ".join(parts) if parts else "Closed"
    except:
        return ""

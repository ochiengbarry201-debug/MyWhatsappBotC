import os
import re
import json

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from config import (
    SERVICE_JSON, SERVICE_FILE,
    GOOGLE_SHEETS_ID, SHEET_TAB,
    DEFAULT_SHEET_ID, DEFAULT_SHEET_TAB,
)

sheets_api = None

def load_service_info():
    if SERVICE_JSON:
        return json.loads(SERVICE_JSON)

    if SERVICE_FILE and os.path.exists(SERVICE_FILE):
        with open(SERVICE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    return None

def a1(tab: str, cells: str) -> str:
    safe = tab.replace("'", "''")
    return f"'{safe}'!{cells}"

def _norm_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _index_to_col(idx: int) -> str:
    idx += 1
    out = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        out = chr(65 + r) + out
    return out

def _col_to_idx(col: str) -> int:
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - 64)
    return n - 1

def get_sheet_header_map(spreadsheet_id=None, sheet_tab=None):
    global sheets_api
    if not sheets_api:
        return None

    sid = (spreadsheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return None

    try:
        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A1:Z1")
        ).execute()
        header_row = (res.get("values") or [[]])[0]

        header_index = {}
        for i, cell in enumerate(header_row):
            key = _norm_header(cell)
            if key:
                header_index[key] = i

        wanted = {
            "date": ["date", "appointment date", "booking date"],
            "time": ["time", "appointment time", "booking time"],
            "name": ["name", "patient name", "full name"],
            "phone": ["phone", "phone number", "mobile", "number"],
            "status": ["status"],
            "source": ["source"],
            # ✅ PATCH: support REF column for cancel/reschedule syncing
            "ref": ["ref", "reference", "reference code", "ref code"],
        }

        out = {}
        for field, variants in wanted.items():
            for v in variants:
                vkey = _norm_header(v)
                if vkey in header_index:
                    out[field] = _index_to_col(header_index[vkey])
                    break

        return out
    except Exception as e:
        print("Header map read failed:", repr(e))
        return None

def init_sheets():
    """
    Initializes sheets_api if credentials + sheet id exist.
    Keeps your exact behavior + logs.
    """
    global sheets_api

    service_info = None
    try:
        service_info = load_service_info()
    except Exception as e:
        print("Service account load failed:", repr(e))
        service_info = None

    if service_info and (GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID):
        try:
            SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
            creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)
            sheets_service = build("sheets", "v4", credentials=creds)
            sheets_api = sheets_service.spreadsheets()
            print("Google Sheets initialized")
        except Exception as e:
            print("Google Sheets init failed:", repr(e))
    else:
        print("Service account not set or sheet id not set — Sheets disabled")

def append_to_sheet(date, time, name, phone, sheet_id=None, sheet_tab=None):
    global sheets_api
    if not sheets_api:
        return False

    sid = (sheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return False

    try:
        header_map = get_sheet_header_map(sid, tab)

        if header_map:
            required = ["date", "time", "name", "phone", "status", "source"]
            missing = [k for k in required if k not in header_map]
            if not missing:
                date_i = _col_to_idx(header_map["date"])
                time_i = _col_to_idx(header_map["time"])
                name_i = _col_to_idx(header_map["name"])
                phone_i = _col_to_idx(header_map["phone"])
                status_i = _col_to_idx(header_map["status"])
                source_i = _col_to_idx(header_map["source"])

                max_i = max(date_i, time_i, name_i, phone_i, status_i, source_i)
                row_values = [""] * (max_i + 1)

                row_values[date_i] = date
                row_values[time_i] = time
                row_values[name_i] = name
                row_values[phone_i] = phone
                row_values[status_i] = "Booked"
                row_values[source_i] = "WhatsApp"

                sheets_api.values().append(
                    spreadsheetId=sid,
                    range=a1(tab, "A:F"),
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row_values]}
                ).execute()
                return True

        # fallback A–F
        row_values = [date, time, name, phone, "Booked", "WhatsApp"]
        sheets_api.values().append(
            spreadsheetId=sid,
            range=a1(tab, "A:F"),
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]}
        ).execute()
        return True

    except Exception as e:
        print("Sheets append FAILED:", repr(e))
        return False


# =========================================================
# ✅ PATCH HELPERS (added only — does not change existing logic)
# =========================================================

def append_ref_to_latest_row(ref_code: str, sheet_id=None, sheet_tab=None) -> bool:
    """
    After append_to_sheet() succeeds, call this to store REF on the latest row.
    Safe approach: finds the last non-empty DATE row and writes REF there.
    Returns True if written.
    """
    global sheets_api
    if not sheets_api:
        return False

    ref_code = (ref_code or "").strip()
    if not ref_code:
        return False

    sid = (sheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return False

    try:
        header_map = get_sheet_header_map(sid, tab) or {}
        if "ref" not in header_map:
            print("Sheets REF write skipped: no REF column detected in header.")
            return False

        ref_col_letter = header_map["ref"]
        ref_i = _col_to_idx(ref_col_letter)

        # We locate "latest row" by scanning DATE column (or column A) from A2:Z
        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A2:Z")
        ).execute()

        rows = res.get("values", [])
        if not rows:
            return False

        # last row with any content in first 1-3 columns (common safe heuristic)
        last_idx = None
        for i in range(len(rows) - 1, -1, -1):
            r = rows[i]
            # consider row "real" if it has a date/time/name/phone in first few cols
            head = " ".join([str(x).strip() for x in r[:4] if str(x).strip()])
            if head:
                last_idx = i
                break

        if last_idx is None:
            return False

        row_num = last_idx + 2  # because A2 starts row 2
        target_range = f"{tab}!{ref_col_letter}{row_num}"

        sheets_api.values().update(
            spreadsheetId=sid,
            range=target_range,
            valueInputOption="RAW",
            body={"values": [[ref_code]]}
        ).execute()

        return True

    except Exception as e:
        print("Sheets REF write error:", repr(e))
        return False


def update_sheet_status_by_ref(ref_code: str, new_status: str, sheet_id=None, sheet_tab=None) -> bool:
    """
    Finds a row by REF and updates STATUS column.
    Returns True if updated, False if not found or error.
    """
    global sheets_api
    if not sheets_api:
        return False

    ref_code = (ref_code or "").strip()
    if not ref_code:
        return False

    sid = (sheet_id or GOOGLE_SHEETS_ID or DEFAULT_SHEET_ID or "").strip()
    tab = (sheet_tab or SHEET_TAB or DEFAULT_SHEET_TAB or "Sheet1").strip()
    if not sid:
        return False

    try:
        header_map = get_sheet_header_map(sid, tab) or {}
        if "ref" not in header_map or "status" not in header_map:
            print("Sheets status update skipped: missing REF/STATUS columns.")
            return False

        ref_i = _col_to_idx(header_map["ref"])
        status_col_letter = header_map["status"]

        res = sheets_api.values().get(
            spreadsheetId=sid,
            range=a1(tab, "A2:Z")
        ).execute()

        rows = res.get("values", [])
        for idx, row in enumerate(rows):
            sheet_ref = row[ref_i] if len(row) > ref_i else ""
            if str(sheet_ref).strip().upper() == ref_code.upper():
                row_num = idx + 2
                target_range = f"{tab}!{status_col_letter}{row_num}"

                sheets_api.values().update(
                    spreadsheetId=sid,
                    range=target_range,
                    valueInputOption="RAW",
                    body={"values": [[new_status]]}
                ).execute()

                return True

        return False

    except Exception as e:
        print("Sheets status update error:", repr(e))
        return False


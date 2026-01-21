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

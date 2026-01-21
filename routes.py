import datetime
import re
from zoneinfo import ZoneInfo

from flask import request, Response
from twilio.twiml.messaging_response import MessagingResponse

from admin import is_admin
from ai import ai_reply
from booking import check_double_booking, save_appointment_local
from clinic import resolve_clinic_id, get_clinic_sheet_config
from db import (
    save_message, already_processed_twilio_sid,
    load_clinic_settings,
    get_todays_appointments, get_unsynced_appointments,
    update_sheet_sync_status,
    cancel_by_ref, cancel_latest_appointment,
    get_latest_booked_appointment,
    clear_state_machine, get_state_and_draft, set_state_and_draft,
)
from hours import (
    get_hours_settings,
    normalize_time_to_24h,
    is_open_on_date,
    is_time_within_hours,
    is_slot_aligned,
    format_opening_hours_for_day,
)
from intents import is_booking_intent, looks_like_date
from sheets import append_to_sheet

def register_routes(app):

    @app.get("/")
    def home():
        return "OK", 200

    @app.route("/whatsapp", methods=["POST"])
    def whatsapp_webhook():
        incoming = request.values.get("Body", "").strip()
        raw_from = request.values.get("From", "")
        user = raw_from.replace("whatsapp:", "")

        resp = MessagingResponse()
        msg = resp.message()

        to_number = request.values.get("To", "").strip()
        clinic_id = resolve_clinic_id(to_number)
        print("Resolved clinic_id:", clinic_id, "To:", to_number)

        if not clinic_id:
            msg.body("This WhatsApp line is not linked to a clinic yet.")
            return Response(str(resp), mimetype="application/xml")

        clinic_settings = load_clinic_settings(clinic_id)
        clinic_sheet_id, clinic_sheet_tab = get_clinic_sheet_config(clinic_settings)
        tz_name, slot_minutes, weekly = get_hours_settings(clinic_settings)

        # Idempotency
        twilio_sid = (request.values.get("MessageSid") or "").strip()
        if twilio_sid and already_processed_twilio_sid(twilio_sid):
            msg.body("✅ Received.")
            return Response(str(resp), mimetype="application/xml")

        save_message(clinic_id, user, "user", incoming, twilio_sid=twilio_sid)

        # -------------------------
        # Commands
        # -------------------------
        if incoming.strip().lower() == "today":
            if not is_admin(user, clinic_settings):
                reply = "Not authorized."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            today = datetime.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
            rows = get_todays_appointments(clinic_id, today)
            if not rows:
                reply = f"No booked appointments for today ({today})."
            else:
                lines = [f"Today ({today}) appointments:"]
                for (name, phone, time, sync_status, ref_code) in rows[:30]:
                    lines.append(f"- {time} | {name} | {phone} | ref:{ref_code} | sheets:{sync_status}")
                reply = "\n".join(lines)
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if incoming.strip().lower() == "retry sheets":
            if not is_admin(user, clinic_settings):
                reply = "Not authorized."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            rows = get_unsynced_appointments(clinic_id, limit=20)
            if not rows:
                reply = "No pending/failed sheet syncs found."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            attempted = synced = failed = 0
            for (appt_id, appt_user, appt_name, appt_date, appt_time, appt_status) in rows:
                attempted += 1
                ok = append_to_sheet(appt_date, appt_time, appt_name, appt_user, clinic_sheet_id, clinic_sheet_tab)
                if ok:
                    synced += 1
                    update_sheet_sync_status(appt_id, "synced")
                else:
                    failed += 1
                    update_sheet_sync_status(appt_id, "failed", "Retry sheets failed (see logs)")

            reply = f"Retry complete ✅\nAttempted: {attempted}\nSynced: {synced}\nFailed: {failed}"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if incoming.strip().lower() == "my appointment":
            appt = get_latest_booked_appointment(clinic_id, user)
            if not appt:
                reply = "You have no booked appointments right now."
            else:
                appt_id, name, date, time, created_at, ref_code = appt
                reply = f"Your next appointment is on {date} at {time} under the name {name}. Ref: {ref_code}"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        m = re.match(r"^cancel\s+(AP-[A-Z0-9]{6})$", incoming.strip().upper())
        if m:
            ref_code = m.group(1)
            result = cancel_by_ref(clinic_id, user, ref_code)
            if result == "not_owner":
                reply = "That reference code doesn’t belong to your number."
            elif not result:
                reply = "I couldn’t find an active booked appointment with that reference."
            else:
                reply = f"✅ Cancelled appointment on {result['date']} at {result['time']}."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if incoming.strip().lower() == "cancel":
            clear_state_machine(clinic_id, user)
            cancelled = cancel_latest_appointment(clinic_id, user)
            if not cancelled:
                reply = "I couldn’t find an active booked appointment to cancel."
            else:
                reply = f"✅ Cancelled your appointment on {cancelled['date']} at {cancelled['time']}. Ref: {cancelled['ref_code']}"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if incoming.strip().lower() == "reschedule":
            clear_state_machine(clinic_id, user)
            cancelled = cancel_latest_appointment(clinic_id, user)
            set_state_and_draft(clinic_id, user, "collect_name", {})
            if cancelled:
                reply = f"✅ Cancelled {cancelled['date']} {cancelled['time']} (Ref: {cancelled['ref_code']}).\nLet’s reschedule. What’s your full name?"
            else:
                reply = "No active appointment found, but I can help you book a new one. What’s your full name?"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if incoming.strip().lower() == "reset":
            clear_state_machine(clinic_id, user)
            reply = "Session reset. You can start again."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        # -------------------------
        # Booking state machine
        # -------------------------
        state, draft = get_state_and_draft(clinic_id, user)

        if state in ["idle", None, ""] and is_booking_intent(incoming):
            set_state_and_draft(clinic_id, user, "collect_name", {})
            reply = "Sure. What's your full name?"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if state == "collect_name":
            draft["name"] = incoming.strip()
            set_state_and_draft(clinic_id, user, "collect_date", draft)
            reply = "What date would you like? (YYYY-MM-DD)"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if state == "collect_date":
            if looks_like_date(incoming):
                date_str = incoming.strip()

                if not is_open_on_date(date_str, tz_name, weekly):
                    reply = "Sorry, we’re closed on that day. Please choose another date."
                    msg.body(reply)
                    save_message(clinic_id, user, "assistant", reply)
                    return Response(str(resp), mimetype="application/xml")

                draft["date"] = date_str
                set_state_and_draft(clinic_id, user, "collect_time", draft)
                reply = f"What time would you prefer? (HH:MM) e.g. 14:00. Slots are {slot_minutes} minutes."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            reply = "Please type the date like 2026-01-15 (YYYY-MM-DD)."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if state == "collect_time":
            time_24 = normalize_time_to_24h(incoming)
            if not time_24:
                reply = "Please type the time like 09:30 (HH:MM) or 2:30 PM."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            date = draft.get("date", "")

            if not is_time_within_hours(date, time_24, tz_name, weekly):
                hours_str = format_opening_hours_for_day(date, tz_name, weekly)
                reply = f"That time is outside working hours for {date}. Available: {hours_str}."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            if not is_slot_aligned(time_24, slot_minutes):
                reply = f"Please choose a time that matches our {slot_minutes}-minute slots (e.g. 09:00, 09:30, 10:00)."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            if check_double_booking(clinic_id, date, time_24, clinic_sheet_id, clinic_sheet_tab):
                reply = "That slot is already booked. Choose another time."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            draft["time"] = time_24
            set_state_and_draft(clinic_id, user, "confirm", draft)
            reply = f"Confirm appointment on {date} at {time_24}? (yes/no)"
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        if state == "confirm":
            if incoming.lower() in ["yes", "y"]:
                name = draft.get("name", "").strip()
                date = draft.get("date", "").strip()
                time = draft.get("time", "").strip()

                appt_id, ref_code = save_appointment_local(clinic_id, user, name, date, time)

                ok = append_to_sheet(date, time, name, user, clinic_sheet_id, clinic_sheet_tab)
                if ok:
                    update_sheet_sync_status(appt_id, "synced")
                else:
                    update_sheet_sync_status(appt_id, "failed", "Sheets append failed (see logs)")

                clear_state_machine(clinic_id, user)

                reply = f"✅ Appointment confirmed for {date} at {time}\nRef: {ref_code}\nTo cancel: cancel {ref_code}"
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            if incoming.lower() in ["no", "n"]:
                clear_state_machine(clinic_id, user)
                reply = "No problem — booking cancelled. Type 'book' to start again."
                msg.body(reply)
                save_message(clinic_id, user, "assistant", reply)
                return Response(str(resp), mimetype="application/xml")

            reply = "Please reply with 'yes' to confirm or 'no' to cancel."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        maybe_booking_words = ["dent", "tooth", "teeth", "pain", "ache", "clean", "check", "braces", "gum"]
        if any(w in incoming.lower() for w in maybe_booking_words):
            reply = "If you'd like to book an appointment, please type 'book'."
            msg.body(reply)
            save_message(clinic_id, user, "assistant", reply)
            return Response(str(resp), mimetype="application/xml")

        reply = ai_reply(clinic_id, user, incoming)
        msg.body(reply)
        save_message(clinic_id, user, "assistant", reply)
        return Response(str(resp), mimetype="application/xml")

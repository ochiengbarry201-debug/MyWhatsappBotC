import datetime
import json
import re
import traceback
from zoneinfo import ZoneInfo

from flask import request, Response
from twilio.twiml.messaging_response import MessagingResponse

from admin import is_admin
from ai import ai_reply, ai_extract_booking_signal, OFFER_BOOKING_MARKER
from booking import check_double_booking, save_appointment_local
from clinic import resolve_clinic_id, get_clinic_sheet_config, validate_clinic_settings
from db import (
    save_message, save_incoming_message_if_new,
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
from intents import is_booking_intent, looks_like_date, is_cancel_intent, is_reschedule_intent
from sheets import append_to_sheet, append_ref_to_latest_row, update_sheet_status_by_ref
from jobs import get_job_counts, count_stale_running_jobs, list_failed_jobs
from jobs import enqueue_job, cancel_jobs_for_appointment


REMINDER_MINUTES_BEFORE = 120  # 2 hours before appointment


def log_event(tag, **kwargs):
    try:
        parts = [f"[{tag}]"]
        for k, v in kwargs.items():
            if isinstance(v, (dict, list, tuple)):
                try:
                    v = json.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    v = str(v)
            else:
                v = str(v)

            if len(v) > 300:
                v = v[:300] + "..."

            parts.append(f"{k}={v}")
        print(" | ".join(parts))
    except Exception as e:
        print(f"[LOG_EVENT_FAILED] tag={tag} error={repr(e)}")


def _reply_and_return(resp, msg, clinic_id, user, reply, action=None, **extra):
    msg.body(reply)
    try:
        if clinic_id:
            save_message(clinic_id, user, "assistant", reply)
    except Exception as e:
        log_event("SAVE_ASSISTANT_MESSAGE_FAILED", clinic_id=clinic_id, user=user, error=repr(e))

    log_event(
        "REPLY",
        clinic_id=clinic_id,
        user=user,
        action=action or "",
        reply=reply,
        **extra
    )
    return Response(str(resp), mimetype="application/xml")


def _normalize_phone_for_lookup(raw: str) -> str:
    s = (raw or "").strip()
    s = s.replace("whatsapp:", "").strip()
    s = re.sub(r"\s+", "", s)
    return s


def _extract_state_target(incoming: str, fallback_user: str) -> str:
    parts = (incoming or "").strip().split(None, 1)
    if len(parts) == 1:
        return fallback_user
    candidate = _normalize_phone_for_lookup(parts[1])
    return candidate or fallback_user


def _is_greeting(text: str) -> bool:
    if not text:
        return False

    t = text.lower().strip()
    t_norm = re.sub(r"[^a-z0-9\s]", " ", t)
    t_norm = re.sub(r"\s+", " ", t_norm).strip()

    if len(t_norm) > 30:
        return False

    phrases = {
        "good morning", "good afternoon", "good evening", "good day",
        "morning", "afternoon", "evening",
        "habari", "niaje", "sasa", "mambo",
        "goodmorning", "goodafternoon", "goodevening"
    }
    if t_norm in phrases:
        return True

    words = set(t_norm.split())
    single_words = {"hi", "hello", "hey", "yo"}

    if len(words) <= 3 and not any(ch.isdigit() for ch in t_norm) and (words & single_words):
        return True

    return False


def _looks_like_booking_agree(text: str) -> bool:
    if not text:
        return False

    t = text.lower().strip()
    t_norm = re.sub(r"[^a-z0-9\s]", " ", t)
    t_norm = re.sub(r"\s+", " ", t_norm).strip()

    positive = [
        "yes", "yeah", "yep", "sure", "okay", "ok", "alright", "proceed", "go ahead",
        "please", "kindly", "sounds good", "that works", "i would", "i want", "i need",
        "help me", "can you", "could you"
    ]

    bookingish = [
        "book", "booking", "appointment", "schedule", "reschedule", "visit", "come in",
        "see dentist", "see the dentist", "consultation", "checkup", "check-up"
    ]

    return any(p in t_norm for p in positive) or any(b in t_norm for b in bookingish)


def _looks_like_booking_decline(text: str) -> bool:
    if not text:
        return False

    t = text.lower().strip()
    t_norm = re.sub(r"[^a-z0-9\s]", " ", t)
    t_norm = re.sub(r"\s+", " ", t_norm).strip()

    decline = [
        "no", "nope", "not now", "later", "maybe later", "another time",
        "not today", "no thanks", "dont", "do not", "just asking"
    ]
    return any(d in t_norm for d in decline)


def _safe_admin_numbers(clinic_settings: dict):
    admins = []
    if isinstance(clinic_settings, dict):
        raw = clinic_settings.get("admins", [])
        if isinstance(raw, list):
            admins = [str(x).strip() for x in raw if str(x).strip()]

    seen = set()
    out = []
    for a in admins:
        if a not in seen:
            out.append(a)
            seen.add(a)
    return out


def _enqueue_admin_notify(clinic_id, clinic_settings: dict, body: str, appointment_id=None):
    admins = _safe_admin_numbers(clinic_settings)
    if not admins:
        log_event("ADMIN_NOTIFY_SKIPPED", clinic_id=clinic_id, reason="no_admins")
        return

    for a in admins:
        enqueue_job(
            "notify_admin",
            {
                "to": a,
                "body": body,
                "clinic_id": str(clinic_id),
                "appointment_id": str(appointment_id) if appointment_id is not None else None
            }
        )

    log_event(
        "ADMIN_NOTIFY_ENQUEUED",
        clinic_id=clinic_id,
        admins=admins,
        appointment_id=appointment_id,
        body=body
    )


def _schedule_patient_reminder(
    clinic_id,
    user_number: str,
    clinic_settings: dict,
    appointment_id: int,
    patient_name: str,
    date: str,
    time_24h: str,
    ref_code: str = None,
    tz_name: str = "Africa/Nairobi"
):
    try:
        tz = ZoneInfo(tz_name or "Africa/Nairobi")
        dt = datetime.datetime.strptime(f"{date} {time_24h}", "%Y-%m-%d %H:%M")
        appt_local = dt.replace(tzinfo=tz)
        run_at_local = appt_local - datetime.timedelta(minutes=REMINDER_MINUTES_BEFORE)
        run_at_utc = run_at_local.astimezone(datetime.timezone.utc).replace(tzinfo=None)

        if run_at_utc <= datetime.datetime.utcnow():
            log_event(
                "REMINDER_SKIPPED",
                clinic_id=clinic_id,
                appointment_id=appointment_id,
                date=date,
                time=time_24h,
                reason="run_at_already_passed"
            )
            return

        clinic_name = "Our Clinic"
        if isinstance(clinic_settings, dict):
            clinic_name = clinic_settings.get("name", "Our Clinic")

        enqueue_job(
            "patient_reminder",
            {
                "clinic_id": str(clinic_id),
                "appointment_id": str(appointment_id),
                "to": user_number,
                "patient_name": patient_name or "Patient",
                "clinic_name": clinic_name,
                "date": date,
                "time": time_24h,
                "ref_code": ref_code or ""
            },
            run_at=run_at_utc
        )

        log_event(
            "REMINDER_ENQUEUED",
            clinic_id=clinic_id,
            appointment_id=appointment_id,
            to=user_number,
            patient_name=patient_name,
            date=date,
            time=time_24h,
            ref_code=ref_code,
            run_at_utc=run_at_utc
        )

    except Exception as e:
        log_event(
            "REMINDER_FAILED",
            clinic_id=clinic_id,
            appointment_id=appointment_id,
            to=user_number,
            error=repr(e),
            traceback=traceback.format_exc()
        )


def register_routes(app):

    @app.get("/")
    def home():
        return "OK", 200

    @app.route("/whatsapp", methods=["POST"])
    def whatsapp_webhook():
        resp = MessagingResponse()
        msg = resp.message()

        try:
            incoming = request.values.get("Body", "").strip()
            raw_from = request.values.get("From", "")
            user = raw_from.replace("whatsapp:", "")
            to_number = request.values.get("To", "").strip()
            twilio_sid = (request.values.get("MessageSid") or "").strip()

            log_event(
                "WEBHOOK_IN",
                sid=twilio_sid,
                from_number=raw_from,
                user=user,
                to_number=to_number,
                incoming=incoming
            )

            clinic_id = resolve_clinic_id(to_number)
            log_event("CLINIC_RESOLVED", sid=twilio_sid, to_number=to_number, clinic_id=clinic_id)

            if not clinic_id:
                return _reply_and_return(
                    resp, msg, None, user,
                    "This WhatsApp line is not linked to a clinic yet.",
                    action="clinic_not_linked",
                    sid=twilio_sid,
                    to_number=to_number
                )

            clinic_settings = load_clinic_settings(clinic_id)
            clinic_settings, config_errors, config_warnings = validate_clinic_settings(clinic_settings)

            if config_warnings:
                for w in config_warnings:
                    log_event("CONFIG_WARNING", clinic_id=clinic_id, sid=twilio_sid, warning=w)

            if config_errors:
                log_event("CONFIG_ERROR", clinic_id=clinic_id, sid=twilio_sid, errors=config_errors)
                return _reply_and_return(
                    resp, msg, clinic_id, user,
                    "This clinic setup is incomplete right now. Please contact support.",
                    action="config_error",
                    sid=twilio_sid
                )

            clinic_sheet_id, clinic_sheet_tab = get_clinic_sheet_config(clinic_settings)
            tz_name, slot_minutes, weekly = get_hours_settings(clinic_settings)

            clinic = {
                "id": clinic_id,
                "name": clinic_settings.get("name", "PrimeCare Dental Clinic"),
                "timezone": tz_name
            }

            log_event(
                "CLINIC_CONTEXT",
                sid=twilio_sid,
                clinic_id=clinic_id,
                clinic_name=clinic.get("name"),
                timezone=tz_name,
                slot_minutes=slot_minutes,
                sheet_id_present=bool(clinic_sheet_id),
                sheet_tab=clinic_sheet_tab
            )

            is_new_inbound = save_incoming_message_if_new(
                clinic_id=clinic_id,
                user=user,
                msg=incoming,
                twilio_sid=twilio_sid
            )
            if not is_new_inbound:
                log_event("DUPLICATE_WEBHOOK_IGNORED", sid=twilio_sid, clinic_id=clinic_id, user=user)
                return Response(str(resp), mimetype="application/xml")

            # -------------------------------------------------
            # Admin debug commands
            # -------------------------------------------------
            if incoming.strip().lower().startswith("state"):
                if not is_admin(user, clinic_settings):
                    return _reply_and_return(
                        resp, msg, clinic_id, user,
                        "Not authorized.",
                        action="state_unauthorized",
                        sid=twilio_sid
                    )

                target_user = _extract_state_target(incoming, user)
                state_view, draft_view = get_state_and_draft(clinic_id, target_user)

                reply = (
                    f"Debug state ✅\n"
                    f"Clinic: {clinic_settings.get('name', 'Unknown')}\n"
                    f"Target user: {target_user}\n"
                    f"State: {state_view}\n"
                    f"Draft: {json.dumps(draft_view, ensure_ascii=False)}"
                )

                log_event(
                    "STATE_DEBUG_COMMAND",
                    clinic_id=clinic_id,
                    sid=twilio_sid,
                    admin=user,
                    target_user=target_user,
                    state=state_view,
                    draft=draft_view
                )
                return _reply_and_return(
                    resp, msg, clinic_id, user,
                    reply,
                    action="state_debug_success",
                    sid=twilio_sid,
                    target_user=target_user
                )

            if incoming.strip().lower() == "clinic check":
                if not is_admin(user, clinic_settings):
                    return _reply_and_return(
                        resp, msg, clinic_id, user,
                        "Not authorized.",
                        action="clinic_check_unauthorized",
                        sid=twilio_sid
                    )

                admins = clinic_settings.get("admins", [])
                hours_cfg = clinic_settings.get("hours", {}) if isinstance(clinic_settings.get("hours"), dict) else {}
                weekly_cfg = hours_cfg.get("weekly", {}) if isinstance(hours_cfg.get("weekly"), dict) else {}
                days_present = sorted(list(weekly_cfg.keys())) if weekly_cfg else []

                reply = (
                    "Clinic check ✅\n"
                    f"Clinic: {clinic_settings.get('name', 'Unknown')}\n"
                    f"Clinic ID: {clinic_id}\n"
                    f"Timezone: {tz_name}\n"
                    f"Slot minutes: {slot_minutes}\n"
                    f"Admins count: {len(admins) if isinstance(admins, list) else 0}\n"
                    f"Sheet ID set: {'yes' if clinic_sheet_id else 'no'}\n"
                    f"Sheet tab: {clinic_sheet_tab or 'N/A'}\n"
                    f"Hours days set: {', '.join(days_present) if days_present else 'none'}\n"
                    f"Config warnings: {len(config_warnings)}\n"
                    f"Config errors: {len(config_errors)}"
                )

                log_event(
                    "CLINIC_CHECK_COMMAND",
                    clinic_id=clinic_id,
                    sid=twilio_sid,
                    admin=user,
                    clinic_name=clinic_settings.get("name", ""),
                    timezone=tz_name,
                    slot_minutes=slot_minutes,
                    admins_count=len(admins) if isinstance(admins, list) else 0,
                    sheet_id_present=bool(clinic_sheet_id),
                    sheet_tab=clinic_sheet_tab,
                    weekly_days=days_present,
                    config_warnings=config_warnings,
                    config_errors=config_errors
                )
                return _reply_and_return(
                    resp, msg, clinic_id, user,
                    reply,
                    action="clinic_check_success",
                    sid=twilio_sid
                )

            if incoming.strip().lower() == "today":
                if not is_admin(user, clinic_settings):
                    return _reply_and_return(resp, msg, clinic_id, user, "Not authorized.", action="today_unauthorized", sid=twilio_sid)

                today = datetime.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
                rows = get_todays_appointments(clinic_id, today)
                log_event("TODAY_COMMAND", clinic_id=clinic_id, sid=twilio_sid, rows_count=len(rows), today=today)

                if not rows:
                    reply = f"No booked appointments for today ({today})."
                else:
                    lines = [f"Today ({today}) appointments:"]
                    for (name, phone, time, sync_status, ref_code) in rows[:30]:
                        lines.append(f"- {time} | {name} | {phone} | ref:{ref_code} | sheets:{sync_status}")
                    reply = "\n".join(lines)
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="today_success", sid=twilio_sid)

            if incoming.strip().lower() == "retry sheets":
                if not is_admin(user, clinic_settings):
                    return _reply_and_return(resp, msg, clinic_id, user, "Not authorized.", action="retry_sheets_unauthorized", sid=twilio_sid)

                rows = get_unsynced_appointments(clinic_id, limit=20)
                log_event("RETRY_SHEETS_START", clinic_id=clinic_id, sid=twilio_sid, rows_count=len(rows))

                if not rows:
                    return _reply_and_return(resp, msg, clinic_id, user, "No pending/failed sheet syncs found.", action="retry_sheets_none", sid=twilio_sid)

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
                log_event("RETRY_SHEETS_DONE", clinic_id=clinic_id, sid=twilio_sid, attempted=attempted, synced=synced, failed=failed)
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="retry_sheets_done", sid=twilio_sid)

            if incoming.strip().lower() == "jobs":
                if not is_admin(user, clinic_settings):
                    return _reply_and_return(resp, msg, clinic_id, user, "Not authorized.", action="jobs_unauthorized", sid=twilio_sid)

                all_counts = get_job_counts()
                sheet_counts = get_job_counts("sync_sheet")
                stale_all = count_stale_running_jobs(minutes=5)
                stale_sheet = count_stale_running_jobs(minutes=5, job_type="sync_sheet")

                def fmt_counts(d):
                    return f"queued:{d.get('queued',0)} running:{d.get('running',0)} done:{d.get('done',0)} failed:{d.get('failed',0)}"

                reply = (
                    "Job status ✅\n"
                    f"All jobs -> {fmt_counts(all_counts)} | stale_running(>5m): {stale_all}\n"
                    f"sync_sheet -> {fmt_counts(sheet_counts)} | stale_running(>5m): {stale_sheet}\n"
                    "Commands: jobs, failed jobs"
                )
                log_event("JOBS_COMMAND", clinic_id=clinic_id, sid=twilio_sid, all_counts=all_counts, sheet_counts=sheet_counts, stale_all=stale_all, stale_sheet=stale_sheet)
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="jobs_success", sid=twilio_sid)

            if incoming.strip().lower() in ["failed jobs", "jobs failed"]:
                if not is_admin(user, clinic_settings):
                    return _reply_and_return(resp, msg, clinic_id, user, "Not authorized.", action="failed_jobs_unauthorized", sid=twilio_sid)

                rows = list_failed_jobs(job_type="sync_sheet", limit=10)
                log_event("FAILED_JOBS_COMMAND", clinic_id=clinic_id, sid=twilio_sid, failed_count=len(rows))

                if not rows:
                    reply = "No failed sync_sheet jobs ✅"
                else:
                    lines = ["Failed sync_sheet jobs (latest 10):"]
                    for r in rows:
                        jid = r.get("id")
                        att = r.get("attempts")
                        mx = r.get("max_attempts")
                        err = (r.get("last_error") or "").replace("\n", " ")
                        err = (err[:120] + "…") if len(err) > 120 else err
                        lines.append(f"- id:{jid} attempts:{att}/{mx} err:{err}")
                    reply = "\n".join(lines)

                return _reply_and_return(resp, msg, clinic_id, user, reply, action="failed_jobs_success", sid=twilio_sid)

            if incoming.strip().lower() == "my appointment":
                appt = get_latest_booked_appointment(clinic_id, user)
                log_event("MY_APPOINTMENT_COMMAND", clinic_id=clinic_id, sid=twilio_sid, found=bool(appt))

                if not appt:
                    reply = "You have no booked appointments right now."
                else:
                    appt_id, name, date, time_, created_at, ref_code = appt
                    reply = f"Your next appointment is on {date} at {time_} under the name {name}. Ref: {ref_code}"
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="my_appointment", sid=twilio_sid)

            m = re.match(r"^cancel\s+(AP-[A-Z0-9]{6})$", incoming.strip().upper())
            if m:
                ref_code = m.group(1)
                result = cancel_by_ref(clinic_id, user, ref_code)
                log_event("CANCEL_BY_REF", clinic_id=clinic_id, sid=twilio_sid, ref_code=ref_code, result=result)

                if result == "not_owner":
                    return _reply_and_return(resp, msg, clinic_id, user, "That reference code doesn’t belong to your number.", action="cancel_ref_not_owner", sid=twilio_sid)

                if not result:
                    return _reply_and_return(resp, msg, clinic_id, user, "I couldn’t find an active booked appointment with that reference.", action="cancel_ref_not_found", sid=twilio_sid)

                try:
                    update_sheet_status_by_ref(ref_code, "Cancelled", clinic_sheet_id, clinic_sheet_tab)
                    log_event("SHEETS_CANCEL_BY_REF_OK", clinic_id=clinic_id, sid=twilio_sid, ref_code=ref_code)
                except Exception as e:
                    log_event("SHEETS_CANCEL_BY_REF_FAILED", clinic_id=clinic_id, sid=twilio_sid, ref_code=ref_code, error=repr(e))

                reply = f"✅ Cancelled appointment on {result['date']} at {result['time']}."

                _enqueue_admin_notify(
                    clinic_id,
                    clinic_settings,
                    f"📌 Appointment CANCELLED\nDate: {result['date']}\nTime: {result['time']}\nRef: {ref_code}\nPatient: {user}",
                    appointment_id=None
                )

                return _reply_and_return(resp, msg, clinic_id, user, reply, action="cancel_ref_success", sid=twilio_sid, ref_code=ref_code)

            if incoming.strip().lower() == "cancel":
                clear_state_machine(clinic_id, user)
                cancelled = cancel_latest_appointment(clinic_id, user)
                log_event("CANCEL_LATEST", clinic_id=clinic_id, sid=twilio_sid, cancelled=cancelled)

                if not cancelled:
                    return _reply_and_return(resp, msg, clinic_id, user, "I couldn’t find an active booked appointment to cancel.", action="cancel_latest_not_found", sid=twilio_sid)

                try:
                    cancel_jobs_for_appointment("patient_reminder", cancelled["id"])
                    log_event("REMINDER_CANCELLED_FOR_APPOINTMENT", clinic_id=clinic_id, sid=twilio_sid, appointment_id=cancelled["id"])
                except Exception as e:
                    log_event("CANCEL_REMINDER_JOBS_FAILED", clinic_id=clinic_id, sid=twilio_sid, appointment_id=cancelled["id"], error=repr(e))

                try:
                    update_sheet_status_by_ref(cancelled.get("ref_code"), "Cancelled", clinic_sheet_id, clinic_sheet_tab)
                    log_event("SHEETS_CANCEL_LATEST_OK", clinic_id=clinic_id, sid=twilio_sid, ref_code=cancelled.get("ref_code"))
                except Exception as e:
                    log_event("SHEETS_CANCEL_LATEST_FAILED", clinic_id=clinic_id, sid=twilio_sid, ref_code=cancelled.get("ref_code"), error=repr(e))

                reply = f"✅ Cancelled your appointment on {cancelled['date']} at {cancelled['time']}. Ref: {cancelled['ref_code']}"

                _enqueue_admin_notify(
                    clinic_id,
                    clinic_settings,
                    f"📌 Appointment CANCELLED\nDate: {cancelled['date']}\nTime: {cancelled['time']}\nRef: {cancelled['ref_code']}\nPatient: {user}",
                    appointment_id=cancelled["id"]
                )

                return _reply_and_return(resp, msg, clinic_id, user, reply, action="cancel_latest_success", sid=twilio_sid)

            if incoming.strip().lower() == "reschedule":
                clear_state_machine(clinic_id, user)
                cancelled = cancel_latest_appointment(clinic_id, user)
                set_state_and_draft(clinic_id, user, "collect_name", {})
                log_event("RESCHEDULE_COMMAND", clinic_id=clinic_id, sid=twilio_sid, cancelled=cancelled)

                if cancelled:
                    try:
                        cancel_jobs_for_appointment("patient_reminder", cancelled["id"])
                        log_event("REMINDER_CANCELLED_FOR_RESCHEDULE", clinic_id=clinic_id, sid=twilio_sid, appointment_id=cancelled["id"])
                    except Exception as e:
                        log_event("CANCEL_REMINDER_JOBS_FAILED", clinic_id=clinic_id, sid=twilio_sid, appointment_id=cancelled["id"], error=repr(e))

                    try:
                        update_sheet_status_by_ref(cancelled.get("ref_code"), "Rescheduled", clinic_sheet_id, clinic_sheet_tab)
                        log_event("SHEETS_RESCHEDULE_OK", clinic_id=clinic_id, sid=twilio_sid, ref_code=cancelled.get("ref_code"))
                    except Exception as e:
                        log_event("SHEETS_RESCHEDULE_FAILED", clinic_id=clinic_id, sid=twilio_sid, ref_code=cancelled.get("ref_code"), error=repr(e))

                    reply = f"✅ Cancelled {cancelled['date']} {cancelled['time']} (Ref: {cancelled['ref_code']}).\nLet’s reschedule. What’s your full name?"

                    _enqueue_admin_notify(
                        clinic_id,
                        clinic_settings,
                        f"📌 Appointment RESCHEDULE requested (cancelled old)\nOld Date: {cancelled['date']}\nOld Time: {cancelled['time']}\nRef: {cancelled['ref_code']}\nPatient: {user}",
                        appointment_id=cancelled["id"]
                    )
                else:
                    reply = "No active appointment found, but I can help you book a new one. What’s your full name?"

                    _enqueue_admin_notify(
                        clinic_id,
                        clinic_settings,
                        f"📌 Appointment RESCHEDULE requested (no prior booking found)\nPatient: {user}",
                        appointment_id=None
                    )

                return _reply_and_return(resp, msg, clinic_id, user, reply, action="reschedule_start", sid=twilio_sid)

            if incoming.strip().lower() == "reset":
                clear_state_machine(clinic_id, user)
                log_event("RESET_COMMAND", clinic_id=clinic_id, sid=twilio_sid, user=user)
                return _reply_and_return(resp, msg, clinic_id, user, "Session reset. You can start again.", action="reset", sid=twilio_sid)

            state, draft = get_state_and_draft(clinic_id, user)
            log_event("STATE_LOADED", clinic_id=clinic_id, sid=twilio_sid, state=state, draft=draft)

            extracted = ai_extract_booking_signal(clinic, incoming)
            extracted_intent = extracted.get("intent", "general")
            log_event("AI_EXTRACTED", clinic_id=clinic_id, sid=twilio_sid, extracted=extracted)

            if state in [None, "", "idle"] and (extracted_intent in ["cancel", "reschedule"] or is_cancel_intent(incoming) or is_reschedule_intent(incoming)):
                if extracted_intent == "reschedule" or is_reschedule_intent(incoming):
                    clear_state_machine(clinic_id, user)
                    cancelled = cancel_latest_appointment(clinic_id, user)
                    set_state_and_draft(clinic_id, user, "collect_name", {})
                    log_event("IDLE_RESCHEDULE_INTENT", clinic_id=clinic_id, sid=twilio_sid, cancelled=cancelled)

                    if cancelled:
                        try:
                            cancel_jobs_for_appointment("patient_reminder", cancelled["id"])
                        except Exception as e:
                            log_event("CANCEL_REMINDER_JOBS_FAILED", clinic_id=clinic_id, sid=twilio_sid, appointment_id=cancelled["id"], error=repr(e))

                        try:
                            update_sheet_status_by_ref(cancelled.get("ref_code"), "Rescheduled", clinic_sheet_id, clinic_sheet_tab)
                        except Exception as e:
                            log_event("SHEETS_RESCHEDULE_FAILED", clinic_id=clinic_id, sid=twilio_sid, ref_code=cancelled.get("ref_code"), error=repr(e))

                        reply = f"✅ Cancelled {cancelled['date']} {cancelled['time']} (Ref: {cancelled['ref_code']}).\nLet’s reschedule. What’s your full name?"
                    else:
                        reply = "No active appointment found, but I can help you book a new one. What’s your full name?"

                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="idle_reschedule_intent", sid=twilio_sid)

                set_state_and_draft(clinic_id, user, "await_cancel_ref", {})
                reply = (
                    "Sure — I can cancel it.\n"
                    "If you have your reference code, reply like: cancel AP-XXXXXX\n"
                    "If you don’t have it, reply: cancel"
                )
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="await_cancel_ref", sid=twilio_sid)

            if state == "await_cancel_ref":
                if incoming.strip().lower() == "cancel":
                    clear_state_machine(clinic_id, user)
                    cancelled = cancel_latest_appointment(clinic_id, user)
                    log_event("AWAIT_CANCEL_REF_CANCEL", clinic_id=clinic_id, sid=twilio_sid, cancelled=cancelled)

                    if not cancelled:
                        return _reply_and_return(resp, msg, clinic_id, user, "I couldn’t find an active booked appointment to cancel.", action="await_cancel_ref_not_found", sid=twilio_sid)

                    try:
                        cancel_jobs_for_appointment("patient_reminder", cancelled["id"])
                    except Exception as e:
                        log_event("CANCEL_REMINDER_JOBS_FAILED", clinic_id=clinic_id, sid=twilio_sid, appointment_id=cancelled["id"], error=repr(e))

                    try:
                        update_sheet_status_by_ref(cancelled.get("ref_code"), "Cancelled", clinic_sheet_id, clinic_sheet_tab)
                    except Exception as e:
                        log_event("SHEETS_CANCEL_AWAIT_FAILED", clinic_id=clinic_id, sid=twilio_sid, ref_code=cancelled.get("ref_code"), error=repr(e))

                    reply = f"✅ Cancelled your appointment on {cancelled['date']} at {cancelled['time']}. Ref: {cancelled['ref_code']}"
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="await_cancel_ref_success", sid=twilio_sid)

                reply = "Please reply with your reference like: cancel AP-XXXXXX — or reply: cancel (to cancel your latest appointment)."
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="await_cancel_ref_prompt", sid=twilio_sid)

            if state in [None, "", "idle"] and (extracted_intent == "greeting" or _is_greeting(incoming)):
                clinic_name = clinic_settings.get("name", "PrimeCare Dental Clinic")
                reply = f"Hello 👋 Welcome to {clinic_name}. How may we help you today?"
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="greeting", sid=twilio_sid)

            if state == "offer_booking":
                if _looks_like_booking_agree(incoming):
                    set_state_and_draft(clinic_id, user, "collect_name", {})
                    return _reply_and_return(resp, msg, clinic_id, user, "Great — what’s your full name?", action="offer_booking_yes", sid=twilio_sid)

                if _looks_like_booking_decline(incoming):
                    clear_state_machine(clinic_id, user)
                    log_event("OFFER_BOOKING_DECLINED", clinic_id=clinic_id, sid=twilio_sid, user=user)
                else:
                    reply = "No problem. Would you like me to help you book an appointment? (yes/no)"
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="offer_booking_reprompt", sid=twilio_sid)

            if state in ["idle", None, ""] and (extracted_intent == "book" or is_booking_intent(incoming)):
                draft = draft or {}

                if extracted.get("name"):
                    draft["name"] = str(extracted["name"]).strip()

                if extracted.get("date"):
                    draft["date"] = str(extracted["date"]).strip()

                if extracted.get("time"):
                    extracted_time = normalize_time_to_24h(str(extracted["time"]).strip())
                    draft["time"] = extracted_time if extracted_time else str(extracted["time"]).strip()

                log_event("BOOKING_START", clinic_id=clinic_id, sid=twilio_sid, draft=draft)

                if not draft.get("name"):
                    set_state_and_draft(clinic_id, user, "collect_name", draft)
                    return _reply_and_return(resp, msg, clinic_id, user, "Sure. What's your full name?", action="collect_name", sid=twilio_sid)

                if not draft.get("date"):
                    set_state_and_draft(clinic_id, user, "collect_date", draft)
                    return _reply_and_return(resp, msg, clinic_id, user, "What date would you like? (YYYY-MM-DD)", action="collect_date", sid=twilio_sid)

                date = draft.get("date", "").strip()
                if not is_open_on_date(date, tz_name, weekly):
                    set_state_and_draft(clinic_id, user, "collect_date", draft)
                    return _reply_and_return(resp, msg, clinic_id, user, "Sorry, we’re closed on that day. Please choose another date.", action="closed_on_date", sid=twilio_sid, date=date)

                if not draft.get("time"):
                    set_state_and_draft(clinic_id, user, "collect_time", draft)
                    reply = f"What time would you prefer? (HH:MM) e.g. 14:00. Slots are {slot_minutes} minutes."
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="collect_time", sid=twilio_sid)

                time_24 = normalize_time_to_24h(draft.get("time", ""))
                if not time_24:
                    draft.pop("time", None)
                    set_state_and_draft(clinic_id, user, "collect_time", draft)
                    return _reply_and_return(resp, msg, clinic_id, user, "Please type the time like 09:30 (HH:MM) or 2:30 PM.", action="invalid_time_format", sid=twilio_sid)

                if not is_time_within_hours(date, time_24, tz_name, weekly):
                    set_state_and_draft(clinic_id, user, "collect_time", draft)
                    hours_str = format_opening_hours_for_day(date, tz_name, weekly)
                    reply = f"That time is outside working hours for {date}. Available: {hours_str}."
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="time_outside_hours", sid=twilio_sid, date=date, time=time_24)

                if not is_slot_aligned(time_24, slot_minutes):
                    set_state_and_draft(clinic_id, user, "collect_time", draft)
                    reply = f"Please choose a time that matches our {slot_minutes}-minute slots (e.g. 09:00, 09:30, 10:00)."
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="slot_not_aligned", sid=twilio_sid, time=time_24)

                is_taken = check_double_booking(clinic_id, date, time_24, clinic_sheet_id, clinic_sheet_tab)
                log_event("DOUBLE_BOOKING_CHECK", clinic_id=clinic_id, sid=twilio_sid, date=date, time=time_24, taken=is_taken)

                if is_taken:
                    set_state_and_draft(clinic_id, user, "collect_time", draft)
                    return _reply_and_return(resp, msg, clinic_id, user, "That slot is already booked. Choose another time.", action="slot_taken", sid=twilio_sid, date=date, time=time_24)

                draft["time"] = time_24
                set_state_and_draft(clinic_id, user, "confirm", draft)
                reply = f"Confirm appointment on {date} at {time_24}? (yes/no)"
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="confirm_prompt", sid=twilio_sid, draft=draft)

            if state == "collect_name":
                draft["name"] = incoming.strip()
                set_state_and_draft(clinic_id, user, "collect_date", draft)
                return _reply_and_return(resp, msg, clinic_id, user, "What date would you like? (YYYY-MM-DD)", action="name_collected", sid=twilio_sid, draft=draft)

            if state == "collect_date":
                if looks_like_date(incoming):
                    date_str = incoming.strip()

                    if not is_open_on_date(date_str, tz_name, weekly):
                        return _reply_and_return(resp, msg, clinic_id, user, "Sorry, we’re closed on that day. Please choose another date.", action="collect_date_closed", sid=twilio_sid, date=date_str)

                    draft["date"] = date_str
                    set_state_and_draft(clinic_id, user, "collect_time", draft)
                    reply = f"What time would you prefer? (HH:MM) e.g. 14:00. Slots are {slot_minutes} minutes."
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="date_collected", sid=twilio_sid, draft=draft)

                reply = "Please confirm the date in this format: YYYY-MM-DD (example: 2026-01-30)."
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="collect_date_invalid", sid=twilio_sid)

            if state == "collect_time":
                time_24 = normalize_time_to_24h(incoming)
                if not time_24:
                    return _reply_and_return(resp, msg, clinic_id, user, "Please type the time like 09:30 (HH:MM) or 2:30 PM.", action="collect_time_invalid", sid=twilio_sid)

                date = draft.get("date", "")

                if not is_time_within_hours(date, time_24, tz_name, weekly):
                    hours_str = format_opening_hours_for_day(date, tz_name, weekly)
                    reply = f"That time is outside working hours for {date}. Available: {hours_str}."
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="collect_time_outside_hours", sid=twilio_sid, date=date, time=time_24)

                if not is_slot_aligned(time_24, slot_minutes):
                    reply = f"Please choose a time that matches our {slot_minutes}-minute slots (e.g. 09:00, 09:30, 10:00)."
                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="collect_time_not_aligned", sid=twilio_sid, time=time_24)

                is_taken = check_double_booking(clinic_id, date, time_24, clinic_sheet_id, clinic_sheet_tab)
                log_event("DOUBLE_BOOKING_CHECK", clinic_id=clinic_id, sid=twilio_sid, date=date, time=time_24, taken=is_taken)

                if is_taken:
                    return _reply_and_return(resp, msg, clinic_id, user, "That slot is already booked. Choose another time.", action="collect_time_slot_taken", sid=twilio_sid, date=date, time=time_24)

                draft["time"] = time_24
                set_state_and_draft(clinic_id, user, "confirm", draft)
                reply = f"Confirm appointment on {date} at {time_24}? (yes/no)"
                return _reply_and_return(resp, msg, clinic_id, user, reply, action="collect_time_confirm", sid=twilio_sid, draft=draft)

            if state == "confirm":
                if incoming.lower() in ["yes", "y"]:
                    name = draft.get("name", "").strip()
                    date = draft.get("date", "").strip()
                    time_24 = draft.get("time", "").strip()

                    log_event("BOOKING_CONFIRM_START", clinic_id=clinic_id, sid=twilio_sid, name=name, date=date, time=time_24)

                    appt_id, ref_code = save_appointment_local(
                        clinic_id,
                        user,
                        name,
                        date,
                        time_24,
                        source_message_sid=twilio_sid
                    )
                    log_event("BOOKING_SAVED_DB", clinic_id=clinic_id, sid=twilio_sid, appointment_id=appt_id, ref_code=ref_code)

                    ok = append_to_sheet(date, time_24, name, user, clinic_sheet_id, clinic_sheet_tab)
                    log_event("SHEETS_APPEND_RESULT", clinic_id=clinic_id, sid=twilio_sid, appointment_id=appt_id, ref_code=ref_code, ok=ok)

                    if ok:
                        try:
                            append_ref_to_latest_row(ref_code, clinic_sheet_id, clinic_sheet_tab)
                            log_event("SHEETS_APPEND_REF_OK", clinic_id=clinic_id, sid=twilio_sid, ref_code=ref_code)
                        except Exception as e:
                            log_event("SHEETS_APPEND_REF_FAILED", clinic_id=clinic_id, sid=twilio_sid, ref_code=ref_code, error=repr(e))

                    if ok:
                        update_sheet_sync_status(appt_id, "synced")
                    else:
                        update_sheet_sync_status(appt_id, "failed", "Sheets append failed (see logs)")

                    clear_state_machine(clinic_id, user)

                    reply = f"✅ Appointment confirmed for {date} at {time_24}\nRef: {ref_code}\nTo cancel: cancel {ref_code}"

                    _enqueue_admin_notify(
                        clinic_id,
                        clinic_settings,
                        f"✅ Appointment BOOKED\nDate: {date}\nTime: {time_24}\nName: {name}\nPatient: {user}\nRef: {ref_code}",
                        appointment_id=appt_id
                    )

                    _schedule_patient_reminder(
                        clinic_id=clinic_id,
                        user_number=user,
                        clinic_settings=clinic_settings,
                        appointment_id=appt_id,
                        patient_name=name,
                        date=date,
                        time_24h=time_24,
                        ref_code=ref_code,
                        tz_name=tz_name
                    )

                    return _reply_and_return(resp, msg, clinic_id, user, reply, action="booking_confirmed", sid=twilio_sid, appointment_id=appt_id, ref_code=ref_code)

                if incoming.lower() in ["no", "n"]:
                    clear_state_machine(clinic_id, user)
                    return _reply_and_return(resp, msg, clinic_id, user, "No problem — booking cancelled. Type 'book' to start again.", action="booking_cancelled_at_confirm", sid=twilio_sid)

                return _reply_and_return(resp, msg, clinic_id, user, "Please reply with 'yes' to confirm or 'no' to cancel.", action="confirm_reprompt", sid=twilio_sid)

            reply = ai_reply(clinic, user, incoming)
            log_event("AI_REPLY_RAW", clinic_id=clinic_id, sid=twilio_sid, reply=reply)

            offered_booking = False
            if OFFER_BOOKING_MARKER in reply:
                offered_booking = True
                reply = reply.replace(OFFER_BOOKING_MARKER, "").strip()
                reply = re.sub(r"\n{3,}", "\n\n", reply).strip()
                log_event("AI_REPLY_OFFER_BOOKING", clinic_id=clinic_id, sid=twilio_sid)

            if state in ["idle", None, ""] and offered_booking:
                set_state_and_draft(clinic_id, user, "offer_booking", {})

            return _reply_and_return(resp, msg, clinic_id, user, reply, action="ai_reply", sid=twilio_sid, offered_booking=offered_booking)

        except Exception as e:
            tb = traceback.format_exc()
            log_event("WEBHOOK_FATAL_ERROR", error=repr(e), traceback=tb)

            try:
                clinic_id_safe = locals().get("clinic_id")
                user_safe = locals().get("user", "")
                reply = "Sorry, something went wrong on our side. Please try again in a moment."
                return _reply_and_return(
                    resp,
                    msg,
                    clinic_id_safe,
                    user_safe,
                    reply,
                    action="fatal_error"
                )
            except Exception:
                fallback_resp = MessagingResponse()
                fallback_msg = fallback_resp.message()
                fallback_msg.body("Sorry, something went wrong on our side. Please try again in a moment.")
                return Response(str(fallback_resp), mimetype="application/xml")
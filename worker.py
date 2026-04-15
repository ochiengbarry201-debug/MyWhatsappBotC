import time
import traceback

from jobs import fetch_and_lock_jobs, mark_done, reschedule_or_fail, enqueue_job, has_pending_sync_job
from sheets import append_to_sheet
from db import db_conn, update_sheet_sync_status, load_clinic_settings
from clinic import get_clinic_sheet_config

# ✅ Keep for notify_admin jobs
from notifier import send_whatsapp

# ✅ NEW: clinic-based template reminder sender
from send_clinic_reminder import send_appointment_reminder
from clinic_readiness import clinic_can_send_reminders

SWEEP_EVERY_SECONDS = 120
SWEEP_LIMIT = 50


def handle_job(job):
    job_type = job["job_type"]
    payload = job.get("payload") or {}

    if job_type == "sync_sheet":
        appointment_id = payload.get("appointment_id")
        date = payload.get("date")
        time_ = payload.get("time")
        name = payload.get("name")
        phone = payload.get("phone")
        sheet_id = payload.get("sheet_id")
        sheet_tab = payload.get("sheet_tab")

        # ✅ PATCH: expose the REAL reason Sheets fails
        try:
            ok = append_to_sheet(date, time_, name, phone, sheet_id, sheet_tab)
        except Exception as e:
            update_sheet_sync_status(appointment_id, "failed", f"Sheets exception: {repr(e)}")
            raise  # bubbles up so jobs.last_error captures traceback

        if ok:
            update_sheet_sync_status(appointment_id, "synced")
            return True
        else:
            # If append_to_sheet returns False without throwing, force a real error
            update_sheet_sync_status(appointment_id, "failed", "Sheets append returned False (check worker logs)")
            raise RuntimeError("Sheets append returned False")

    # ✅ Keep admin notifications on normal WhatsApp send
    if job_type == "notify_admin":
        to_number = payload.get("to")
        body = payload.get("body", "")
        print(f"[ADMIN_NOTIFY] Sending admin notification to={to_number}")
        send_whatsapp(to_number, body)
        print(f"[ADMIN_NOTIFY] Sent admin notification to={to_number}")
        return True

    # ✅ NEW: patient reminder job now uses clinic-specific template send
    if job_type == "patient_reminder":
        clinic_id = payload.get("clinic_id")
        to_number = payload.get("to")
        patient_name = payload.get("patient_name") or payload.get("name") or "Patient"
        clinic_name = payload.get("clinic_name") or "Our Clinic"
        appt_date = payload.get("date")
        appt_time = payload.get("time")

        print(
            f"[REMINDER] Preparing clinic reminder "
            f"clinic_id={clinic_id} to={to_number} patient_name={patient_name} "
            f"date={appt_date} time={appt_time}"
        )

        if not clinic_id:
            raise RuntimeError("patient_reminder missing clinic_id")

        if not to_number:
            raise RuntimeError("patient_reminder missing to number")

        if not appt_date:
            raise RuntimeError("patient_reminder missing date")

        if not appt_time:
            raise RuntimeError("patient_reminder missing time")

        ok, reason = clinic_can_send_reminders(clinic_id)
        if not ok:
            raise RuntimeError(f"Reminder blocked: {reason}")

        result = send_appointment_reminder(
            clinic_id=clinic_id,
            to_number=to_number,
            patient_name=patient_name,
            clinic_name=clinic_name,
            appt_date=appt_date,
            appt_time=appt_time
        )

        print(
            f"[REMINDER] Sent successfully "
            f"clinic_id={clinic_id} to={to_number} "
            f"sid={result.get('sid')} status={result.get('status')}"
        )
        return True

    print("Unknown job_type:", job_type, "job_id:", job["id"])
    return True


def sweep_and_enqueue_unsynced():
    """
    Auto-retry: find unsynced appointments and enqueue sync_sheet jobs.
    Won't enqueue duplicates if one is already queued/running for the same appointment_id.
    """
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, clinic_id, user_number, name, date, time, sheet_sync_status
        FROM appointments
        WHERE status='Booked'
          AND sheet_sync_status IN ('failed','pending')
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (SWEEP_LIMIT,)
    )
    rows = c.fetchall()
    conn.close()

    enqueued = 0
    skipped = 0

    for (appt_id, clinic_id, user_number, name, date, time_, sync_status) in rows:
        if has_pending_sync_job(appt_id):
            skipped += 1
            continue

        clinic_settings = load_clinic_settings(clinic_id)
        sheet_id, sheet_tab = get_clinic_sheet_config(clinic_settings)

        enqueue_job("sync_sheet", {
            "appointment_id": appt_id,
            "date": date,
            "time": time_,
            "name": name,
            "phone": user_number,
            "sheet_id": sheet_id,
            "sheet_tab": sheet_tab
        })
        enqueued += 1

    if enqueued or skipped:
        print(f"[SWEEP] Enqueued: {enqueued}, Skipped(existing pending): {skipped}, Checked: {len(rows)}")


def main():
    print("Worker started ✅ (with auto-retry sweeper)")
    last_sweep = 0

    while True:
        now = time.time()
        if now - last_sweep >= SWEEP_EVERY_SECONDS:
            try:
                sweep_and_enqueue_unsynced()
            except Exception as e:
                print("[SWEEP] failed:", repr(e))
            last_sweep = now

        jobs = fetch_and_lock_jobs(limit=5)
        if not jobs:
            time.sleep(2)
            continue

        for job in jobs:
            try:
                ok = handle_job(job)
                if ok:
                    mark_done(job["id"])
                else:
                    # With the patch, handle_job won't return False for sync_sheet.
                    reschedule_or_fail(
                        job["id"],
                        job.get("attempts", 0),
                        job.get("max_attempts", 8),
                        "Job handler returned False"
                    )
            except Exception as e:
                err = repr(e) + "\n" + traceback.format_exc()
                print("Job failed:", job["id"], err)
                reschedule_or_fail(
                    job["id"],
                    job.get("attempts", 0),
                    job.get("max_attempts", 8),
                    err
                )


if __name__ == "__main__":
    main()
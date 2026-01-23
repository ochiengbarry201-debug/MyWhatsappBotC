import time
import traceback

from jobs import fetch_and_lock_jobs, mark_done, reschedule_or_fail, enqueue_job, has_pending_sync_job
from sheets import append_to_sheet
from db import db_conn, update_sheet_sync_status, load_clinic_settings
from clinic import get_clinic_sheet_config


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

        ok = append_to_sheet(date, time_, name, phone, sheet_id, sheet_tab)
        if ok:
            update_sheet_sync_status(appointment_id, "synced")
            return True
        else:
            update_sheet_sync_status(appointment_id, "failed", "Worker sheets append failed (see logs)")
            return False

    # Unknown job types: mark done so it doesn't loop forever
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
        # Avoid spamming duplicates
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
        # Periodic sweeper
        now = time.time()
        if now - last_sweep >= SWEEP_EVERY_SECONDS:
            try:
                sweep_and_enqueue_unsynced()
            except Exception as e:
                print("[SWEEP] failed:", repr(e))
            last_sweep = now

        # Normal job processing
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


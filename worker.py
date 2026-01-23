import time
import traceback

from jobs import fetch_and_lock_jobs, mark_done, reschedule_or_fail
from sheets import init_sheets, append_to_sheet
from db import update_sheet_sync_status

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

    # Unknown job types: treat as done to avoid infinite retry
    print("Unknown job_type:", job_type, "job_id:", job["id"])
    return True

def main():
    print("Worker starting... initializing Google Sheets client...")
    init_sheets()
    print("Worker started ✅")

    while True:
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

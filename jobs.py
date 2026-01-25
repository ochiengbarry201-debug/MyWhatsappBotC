import os
import datetime
import psycopg2.extras

from db import db_conn

WORKER_NAME = os.getenv("WORKER_NAME", "worker-1")


def enqueue_job(job_type: str, payload: dict, run_at=None, max_attempts=8):
    run_at = run_at or datetime.datetime.utcnow()
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO jobs (job_type, payload, status, run_at, max_attempts)
        VALUES (%s, %s, 'queued', %s, %s)
        RETURNING id
        """,
        (job_type, psycopg2.extras.Json(payload or {}), run_at, int(max_attempts))
    )
    job_id = c.fetchone()[0]
    conn.commit()
    conn.close()
    return job_id


def fetch_and_lock_jobs(limit=5):
    """
    Atomically claim jobs using SKIP LOCKED.
    """
    conn = db_conn()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute(
        """
        WITH picked AS (
          SELECT id
          FROM jobs
          WHERE status='queued'
            AND run_at <= now()
          ORDER BY run_at ASC, id ASC
          FOR UPDATE SKIP LOCKED
          LIMIT %s
        )
        UPDATE jobs j
        SET status='running',
            locked_at=now(),
            locked_by=%s,
            updated_at=now()
        FROM picked
        WHERE j.id = picked.id
        RETURNING j.*
        """,
        (limit, WORKER_NAME)
    )
    rows = c.fetchall()
    conn.commit()
    conn.close()
    return rows


def mark_done(job_id: int):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        UPDATE jobs
        SET status='done',
            locked_at=NULL,
            locked_by=NULL,
            updated_at=now()
        WHERE id=%s
        """,
        (job_id,)
    )
    conn.commit()
    conn.close()


def reschedule_or_fail(job_id: int, attempts: int, max_attempts: int, error: str):
    """
    Exponential backoff: 30s, 60s, 120s, ...
    """
    attempts = int(attempts) + 1
    max_attempts = int(max_attempts)
    err = (error or "")[:1200]

    if attempts >= max_attempts:
        conn = db_conn()
        c = conn.cursor()
        c.execute(
            """
            UPDATE jobs
            SET status='failed',
                attempts=%s,
                last_error=%s,
                updated_at=now()
            WHERE id=%s
            """,
            (attempts, err, job_id)
        )
        conn.commit()
        conn.close()
        return

    delay = 30 * (2 ** (attempts - 1))
    run_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)

    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        UPDATE jobs
        SET status='queued',
            attempts=%s,
            last_error=%s,
            run_at=%s,
            locked_at=NULL,
            locked_by=NULL,
            updated_at=now()
        WHERE id=%s
        """,
        (attempts, err, run_at, job_id)
    )
    conn.commit()
    conn.close()


def has_pending_sync_job(appointment_id: int) -> bool:
    """
    Returns True if there's already a queued/running sync_sheet job for this appointment_id.
    """
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT 1
        FROM jobs
        WHERE job_type='sync_sheet'
          AND status IN ('queued','running')
          AND (payload->>'appointment_id')::text = %s
        LIMIT 1
        """,
        (str(appointment_id),)
    )
    exists = c.fetchone() is not None
    conn.close()
    return exists


# ✅ NEW: generic pending-job check for appointment-based jobs (reminders, etc.)
def has_pending_job_for_appointment(job_type: str, appointment_id: int) -> bool:
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT 1
        FROM jobs
        WHERE job_type=%s
          AND status IN ('queued','running')
          AND (payload->>'appointment_id')::text = %s
        LIMIT 1
        """,
        (job_type, str(appointment_id))
    )
    exists = c.fetchone() is not None
    conn.close()
    return exists


# ✅ NEW: cancel queued/running jobs tied to an appointment (e.g., reminders)
def cancel_jobs_for_appointment(job_type: str, appointment_id: int):
    conn = db_conn()
    c = conn.cursor()
    c.execute(
        """
        UPDATE jobs
        SET status='cancelled',
            locked_at=NULL,
            locked_by=NULL,
            updated_at=now()
        WHERE job_type=%s
          AND status IN ('queued','running')
          AND (payload->>'appointment_id')::text = %s
        """,
        (job_type, str(appointment_id))
    )
    conn.commit()
    conn.close()


# ============================================================
# ✅ PATCH: Job status helpers required by routes.py imports
#     routes.py imports:
#       get_job_counts, count_stale_running_jobs, list_failed_jobs
# ============================================================

def get_job_counts(job_type=None):
    """
    Returns counts grouped by status.
    If job_type is provided, filters to that job_type.
    """
    conn = db_conn()
    c = conn.cursor()
    if job_type:
        c.execute(
            """
            SELECT status, COUNT(*)
            FROM jobs
            WHERE job_type=%s
            GROUP BY status
            """,
            (job_type,)
        )
    else:
        c.execute(
            """
            SELECT status, COUNT(*)
            FROM jobs
            GROUP BY status
            """
        )
    rows = c.fetchall()
    conn.close()
    return {status: count for status, count in rows}


def count_stale_running_jobs(minutes=5, job_type=None):
    """
    Counts running jobs whose locked_at is older than N minutes.
    """
    conn = db_conn()
    c = conn.cursor()
    interval = f"{int(minutes)} minutes"

    if job_type:
        c.execute(
            """
            SELECT COUNT(*)
            FROM jobs
            WHERE status='running'
              AND job_type=%s
              AND locked_at IS NOT NULL
              AND locked_at < now() - (%s)::interval
            """,
            (job_type, interval)
        )
    else:
        c.execute(
            """
            SELECT COUNT(*)
            FROM jobs
            WHERE status='running'
              AND locked_at IS NOT NULL
              AND locked_at < now() - (%s)::interval
            """,
            (interval,)
        )

    count = c.fetchone()[0]
    conn.close()
    return count


def list_failed_jobs(job_type=None, limit=10):
    """
    Returns latest failed jobs (optionally filtered by job_type).
    """
    conn = db_conn()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if job_type:
        c.execute(
            """
            SELECT *
            FROM jobs
            WHERE status='failed'
              AND job_type=%s
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
            """,
            (job_type, int(limit))
        )
    else:
        c.execute(
            """
            SELECT *
            FROM jobs
            WHERE status='failed'
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
            """,
            (int(limit),)
        )

    rows = c.fetchall()
    conn.close()
    return rows

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

    delay = 30 * (2 ** (attempts - 1))  # 30s, 60s, 120s...
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


# ✅ NEW: prevent duplicate retries for the same appointment
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

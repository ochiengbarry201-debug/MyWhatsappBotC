import json
import os
from functools import wraps

from flask import (
    Blueprint, request, redirect, url_for,
    session, render_template_string
)

from db import db_conn
from clinic import validate_clinic_settings


admin_dashboard_bp = Blueprint("admin_dashboard", __name__)


BASE_HTML = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>{{ title }}</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
            background: #f7f7f7;
            color: #222;
        }
        h1, h2, h3 {
            margin-top: 0;
        }
        .nav {
            margin-bottom: 20px;
            padding: 12px;
            background: white;
            border-radius: 10px;
            border: 1px solid #ddd;
        }
        .nav a {
            margin-right: 14px;
            text-decoration: none;
            color: #0b57d0;
            font-weight: bold;
        }
        .card {
            background: white;
            padding: 16px;
            border-radius: 10px;
            border: 1px solid #ddd;
            margin-bottom: 16px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 10px;
            overflow: hidden;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
            vertical-align: top;
            font-size: 14px;
        }
        th {
            background: #f0f4f8;
        }
        code, pre {
            background: #f3f3f3;
            padding: 2px 4px;
            border-radius: 4px;
        }
        pre {
            padding: 12px;
            overflow-x: auto;
            white-space: pre-wrap;
        }
        .muted {
            color: #666;
        }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 999px;
            font-size: 12px;
            background: #eef3ff;
            color: #174ea6;
            margin-right: 6px;
        }
        .error {
            color: #b3261e;
            font-weight: bold;
        }
        .ok {
            color: #137333;
            font-weight: bold;
        }
        .warn {
            color: #b06000;
            font-weight: bold;
        }
        input, button, select {
            padding: 8px 10px;
            font-size: 14px;
            margin-right: 8px;
        }
        .small {
            font-size: 12px;
        }
    </style>
</head>
<body>
    {% if logged_in %}
    <div class="nav">
        <a href="{{ url_for('admin_dashboard.admin_clinics') }}">Clinics</a>
        <a href="{{ url_for('admin_dashboard.admin_bookings') }}">Bookings</a>
        <a href="{{ url_for('admin_dashboard.admin_jobs') }}">Jobs</a>
        <a href="{{ url_for('admin_dashboard.admin_logout') }}">Logout</a>
    </div>
    {% endif %}

    {{ content|safe }}
</body>
</html>
"""


def _dashboard_password():
    return os.getenv("DASHBOARD_PASSWORD", "").strip()


def dashboard_login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("dashboard_logged_in"):
            return redirect(url_for("admin_dashboard.admin_login"))
        return fn(*args, **kwargs)
    return wrapper


def _render_page(title, content):
    return render_template_string(
        BASE_HTML,
        title=title,
        content=content,
        logged_in=bool(session.get("dashboard_logged_in"))
    )


def _fetchall(query, params=None):
    conn = db_conn()
    c = conn.cursor()
    c.execute(query, params or ())
    rows = c.fetchall()
    conn.close()
    return rows


def _fetchone(query, params=None):
    conn = db_conn()
    c = conn.cursor()
    c.execute(query, params or ())
    row = c.fetchone()
    conn.close()
    return row


@admin_dashboard_bp.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = ""

    if request.method == "POST":
        supplied = (request.form.get("password") or "").strip()
        expected = _dashboard_password()

        if not expected:
            error = "DASHBOARD_PASSWORD is not set in environment."
        elif supplied == expected:
            session["dashboard_logged_in"] = True
            return redirect(url_for("admin_dashboard.admin_clinics"))
        else:
            error = "Invalid password."

    content = f"""
    <div class="card" style="max-width:420px;">
        <h1>Admin Dashboard Login</h1>
        <form method="post">
            <p><input type="password" name="password" placeholder="Dashboard password" style="width:100%;"></p>
            <p><button type="submit">Login</button></p>
            {'<p class="error">' + error + '</p>' if error else ''}
        </form>
    </div>
    """
    return _render_page("Admin Login", content)


@admin_dashboard_bp.route("/admin/logout")
def admin_logout():
    session.pop("dashboard_logged_in", None)
    return redirect(url_for("admin_dashboard.admin_login"))


@admin_dashboard_bp.route("/admin")
@dashboard_login_required
def admin_home():
    return redirect(url_for("admin_dashboard.admin_clinics"))


@admin_dashboard_bp.route("/admin/clinics")
@dashboard_login_required
def admin_clinics():
    rows = _fetchall(
        """
        SELECT
            cl.id,
            cl.name,
            ch.to_number,
            ch.is_active,
            cs.settings
        FROM clinics cl
        LEFT JOIN channels ch
            ON ch.clinic_id = cl.id
           AND ch.provider = 'twilio'
        LEFT JOIN clinic_settings cs
            ON cs.clinic_id = cl.id
        ORDER BY cl.name ASC
        """
    )

    table_rows = []
    for row in rows:
        clinic_id, clinic_name, to_number, is_active, settings = row
        settings = settings or {}
        if isinstance(settings, str):
            try:
                settings = json.loads(settings)
            except Exception:
                settings = {}

        cleaned, errors, warnings = validate_clinic_settings(settings)

        admins = cleaned.get("admins", [])
        sheet = cleaned.get("sheet", {})
        hours = cleaned.get("hours", {})
        weekly = hours.get("weekly", {})

        table_rows.append(f"""
        <tr>
            <td><a href="{url_for('admin_dashboard.admin_clinic_detail', clinic_id=clinic_id)}">{clinic_name or 'Unnamed Clinic'}</a></td>
            <td><code>{clinic_id}</code></td>
            <td>{to_number or '<span class="muted">No channel</span>'}</td>
            <td>{'Yes' if is_active else 'No'}</td>
            <td>{len(admins) if isinstance(admins, list) else 0}</td>
            <td>{'Yes' if sheet.get('spreadsheet_id') else 'No'}</td>
            <td>{sheet.get('tab') or 'N/A'}</td>
            <td>{', '.join(sorted(list(weekly.keys()))) if weekly else 'none'}</td>
            <td><span class="warn">{len(warnings)}</span></td>
            <td><span class="error">{len(errors)}</span></td>
        </tr>
        """)

    content = f"""
    <div class="card">
        <h1>Clinics</h1>
        <p class="muted">Internal view of clinic routing and settings health.</p>
    </div>

    <table>
        <tr>
            <th>Clinic</th>
            <th>Clinic ID</th>
            <th>WhatsApp Number</th>
            <th>Active</th>
            <th>Admins</th>
            <th>Sheet ID</th>
            <th>Sheet Tab</th>
            <th>Hours Days</th>
            <th>Warnings</th>
            <th>Errors</th>
        </tr>
        {''.join(table_rows) if table_rows else '<tr><td colspan="10">No clinics found.</td></tr>'}
    </table>
    """
    return _render_page("Clinics", content)


@admin_dashboard_bp.route("/admin/clinic/<clinic_id>")
@dashboard_login_required
def admin_clinic_detail(clinic_id):
    row = _fetchone(
        """
        SELECT
            cl.id,
            cl.name,
            ch.to_number,
            ch.is_active,
            cs.settings
        FROM clinics cl
        LEFT JOIN channels ch
            ON ch.clinic_id = cl.id
           AND ch.provider = 'twilio'
        LEFT JOIN clinic_settings cs
            ON cs.clinic_id = cl.id
        WHERE cl.id = %s
        LIMIT 1
        """,
        (clinic_id,)
    )

    if not row:
        return _render_page("Clinic Not Found", "<div class='card'><h1>Clinic not found</h1></div>")

    clinic_id, clinic_name, to_number, is_active, settings = row
    settings = settings or {}
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except Exception:
            settings = {}

    cleaned, errors, warnings = validate_clinic_settings(settings)

    recent_bookings = _fetchall(
        """
        SELECT id, name, user_number, date, time, status, ref_code, sheet_sync_status
        FROM appointments
        WHERE clinic_id = %s
        ORDER BY created_at DESC
        LIMIT 15
        """,
        (clinic_id,)
    )

    booking_rows = []
    for b in recent_bookings:
        booking_rows.append(f"""
        <tr>
            <td>{b[0]}</td>
            <td>{b[1] or ''}</td>
            <td>{b[2] or ''}</td>
            <td>{b[3] or ''}</td>
            <td>{b[4] or ''}</td>
            <td>{b[5] or ''}</td>
            <td>{b[6] or ''}</td>
            <td>{b[7] or ''}</td>
        </tr>
        """)

    content = f"""
    <div class="card">
        <h1>{clinic_name or 'Unnamed Clinic'}</h1>
        <p><span class="badge">Clinic ID: {clinic_id}</span></p>
        <p><b>WhatsApp Number:</b> {to_number or 'Not mapped'}</p>
        <p><b>Channel Active:</b> {'Yes' if is_active else 'No'}</p>
    </div>

    <div class="card">
        <h2>Validation</h2>
        <p><b>Warnings:</b> <span class="warn">{len(warnings)}</span></p>
        <pre>{json.dumps(warnings, indent=2, ensure_ascii=False)}</pre>
        <p><b>Errors:</b> <span class="error">{len(errors)}</span></p>
        <pre>{json.dumps(errors, indent=2, ensure_ascii=False)}</pre>
    </div>

    <div class="card">
        <h2>Cleaned Settings</h2>
        <pre>{json.dumps(cleaned, indent=2, ensure_ascii=False)}</pre>
    </div>

    <div class="card">
        <h2>Raw Settings</h2>
        <pre>{json.dumps(settings, indent=2, ensure_ascii=False)}</pre>
    </div>

    <div class="card">
        <h2>Recent Bookings</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>User Number</th>
                <th>Date</th>
                <th>Time</th>
                <th>Status</th>
                <th>Ref Code</th>
                <th>Sheet Sync</th>
            </tr>
            {''.join(booking_rows) if booking_rows else '<tr><td colspan="8">No bookings found.</td></tr>'}
        </table>
    </div>
    """
    return _render_page("Clinic Detail", content)


@admin_dashboard_bp.route("/admin/bookings")
@dashboard_login_required
def admin_bookings():
    clinic_id = (request.args.get("clinic_id") or "").strip()
    status = (request.args.get("status") or "").strip()
    date = (request.args.get("date") or "").strip()

    query = """
        SELECT
            a.id, a.clinic_id, c.name, a.name, a.user_number,
            a.date, a.time, a.status, a.ref_code, a.sheet_sync_status
        FROM appointments a
        LEFT JOIN clinics c ON c.id = a.clinic_id
        WHERE 1=1
    """
    params = []

    if clinic_id:
        query += " AND a.clinic_id = %s"
        params.append(clinic_id)

    if status:
        query += " AND a.status = %s"
        params.append(status)

    if date:
        query += " AND a.date = %s"
        params.append(date)

    query += " ORDER BY a.created_at DESC LIMIT 100"

    rows = _fetchall(query, tuple(params))

    table_rows = []
    for r in rows:
        table_rows.append(f"""
        <tr>
            <td>{r[0]}</td>
            <td><code>{r[1]}</code></td>
            <td>{r[2] or ''}</td>
            <td>{r[3] or ''}</td>
            <td>{r[4] or ''}</td>
            <td>{r[5] or ''}</td>
            <td>{r[6] or ''}</td>
            <td>{r[7] or ''}</td>
            <td>{r[8] or ''}</td>
            <td>{r[9] or ''}</td>
        </tr>
        """)

    content = f"""
    <div class="card">
        <h1>Bookings</h1>
        <form method="get">
            <input type="text" name="clinic_id" placeholder="Clinic ID" value="{clinic_id}">
            <input type="text" name="status" placeholder="Status e.g. Booked" value="{status}">
            <input type="text" name="date" placeholder="YYYY-MM-DD" value="{date}">
            <button type="submit">Filter</button>
        </form>
    </div>

    <table>
        <tr>
            <th>ID</th>
            <th>Clinic ID</th>
            <th>Clinic</th>
            <th>Name</th>
            <th>User Number</th>
            <th>Date</th>
            <th>Time</th>
            <th>Status</th>
            <th>Ref Code</th>
            <th>Sheet Sync</th>
        </tr>
        {''.join(table_rows) if table_rows else '<tr><td colspan="10">No bookings found.</td></tr>'}
    </table>
    """
    return _render_page("Bookings", content)


@admin_dashboard_bp.route("/admin/jobs")
@dashboard_login_required
def admin_jobs():
    rows = _fetchall(
        """
        SELECT id, job_type, status, run_at, attempts, max_attempts, last_error, locked_by
        FROM jobs
        ORDER BY created_at DESC
        LIMIT 100
        """
    )

    table_rows = []
    for r in rows:
        last_error = (r[6] or "")
        if len(last_error) > 120:
            last_error = last_error[:120] + "..."
        table_rows.append(f"""
        <tr>
            <td>{r[0]}</td>
            <td>{r[1] or ''}</td>
            <td>{r[2] or ''}</td>
            <td>{r[3] or ''}</td>
            <td>{r[4]}</td>
            <td>{r[5]}</td>
            <td>{last_error}</td>
            <td>{r[7] or ''}</td>
        </tr>
        """)

    counts = _fetchall(
        """
        SELECT status, COUNT(*)
        FROM jobs
        GROUP BY status
        ORDER BY status
        """
    )

    count_html = "".join(
        [f"<span class='badge'>{status}: {count}</span>" for status, count in counts]
    )

    content = f"""
    <div class="card">
        <h1>Jobs</h1>
        <p>{count_html or 'No jobs found.'}</p>
    </div>

    <table>
        <tr>
            <th>ID</th>
            <th>Job Type</th>
            <th>Status</th>
            <th>Run At</th>
            <th>Attempts</th>
            <th>Max Attempts</th>
            <th>Last Error</th>
            <th>Locked By</th>
        </tr>
        {''.join(table_rows) if table_rows else '<tr><td colspan="8">No jobs found.</td></tr>'}
    </table>
    """
    return _render_page("Jobs", content)


def register_admin_dashboard(app):
    app.register_blueprint(admin_dashboard_bp)
import os
from flask import Flask

from config import CLINIC_NAME
from db import init_db
from sheets import init_sheets
from ai import init_ai
from routes import register_routes
from admin_dashboard import register_admin_dashboard

# -------------------------------------------------
# Bootstrap (keeps your init behavior)
# -------------------------------------------------
init_db()
init_sheets()
init_ai()

# -------------------------------------------------
# Flask App
# -------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-this-secret-key")
register_routes(app)
register_admin_dashboard(app)

# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting {CLINIC_NAME} bot on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)

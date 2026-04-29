from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
import keyring
from juniper.config import KEYRING_SERVICE, KEYRING_EMAIL_KEY, KEYRING_PASSWORD_KEY, EXPORTS
from juniper.refresh import start_refresh, get_refresh_status
from storage import (
    ArtifactNotFoundError,
    StorageConfigurationError,
    build_artifact_store,
    cache_source_file,
    get_local_cache_dir,
    hydrate_source_files,
)

app = Flask(__name__)


def _env_flag(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name):
    raw = os.environ.get(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


CORS_ALLOWED_ORIGINS = _env_list("CORS_ALLOWED_ORIGINS")
if CORS_ALLOWED_ORIGINS:
    CORS(app, resources={r"/api/*": {"origins": CORS_ALLOWED_ORIGINS}})

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(BASE_DIR, "outputs"))
LOCAL_CACHE_DIR = get_local_cache_dir(DATA_DIR)
LOCAL_WORK_DIR = os.environ.get("LOCAL_WORK_DIR")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
if LOCAL_WORK_DIR:
    os.makedirs(LOCAL_WORK_DIR, exist_ok=True)

ARTIFACT_STORE = build_artifact_store(DATA_DIR)


ENABLE_JUNIPER_REFRESH = _env_flag("ENABLE_JUNIPER_REFRESH", default=True)
ENABLE_SOURCE_UPLOADS = _env_flag("ENABLE_SOURCE_UPLOADS", default=True)
ENABLE_CREDENTIAL_SETUP = _env_flag("ENABLE_CREDENTIAL_SETUP", default=True)

# Define the 4 source files and 4 reports
SOURCE_FILES = [
    {"id": "file1", "label": "Accounts", "filename": "Accounts.csv"},
    {"id": "file2", "label": "Entities",    "filename": "Entity_overview.xlsx"},
    {"id": "file3", "label": "Contacts", "filename": "js_contacts.csv"},
    {"id": "file4", "label": "Contact Export",    "filename": "Contact Export.csv"},
]

REPORTS = [
    {"id": "report1", "label": "Accumulators Report",  "script": "scripts/accumulators_report.py", "output": "Accumulator_Report.pdf"},
    {"id": "report2", "label": "Entities Report",  "script": "scripts/entities_report.py", "output": "Entity_Metrics_Report.pdf"},
    {"id": "report3", "label": "HNW Report",  "script": "scripts/hnw_report.py", "output": "HNW_Report.pdf"},
    {"id": "report4", "label": "Institutional Report",      "script": "scripts/institutional_report.py", "output": "Institutional_Report.pdf"},
]

def _parse_modified_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def get_file_info(filename, stored_metadata=None):
    metadata = stored_metadata or {}
    if metadata.get("exists"):
        dt = _parse_modified_timestamp(metadata.get("modified"))
        if dt is None:
            age_label = "Updated recently"
            status = "fresh"
            modified = None
        else:
            dt_local = dt.astimezone()
            now = datetime.now(dt_local.tzinfo)
            delta = now - dt_local
            if delta.days <= 0:
                age_label = "Updated today"
                status = "fresh"
            elif delta.days <= 7:
                age_label = f"Updated {delta.days}d ago"
                status = "fresh"
            elif delta.days <= 30:
                age_label = f"Updated {delta.days}d ago"
                status = "warning"
            else:
                age_label = f"Updated {delta.days}d ago"
                status = "stale"
            modified = dt_local.strftime("%b %d, %Y %H:%M")
        return {
            "exists": True,
            "age_label": age_label,
            "status": status,
            "modified": modified,
            "backend": metadata.get("backend", ARTIFACT_STORE.backend_name),
            "origin": metadata.get("origin"),
            "resolved_name": metadata.get("resolved_name", filename),
            "size": metadata.get("size"),
            "web_url": metadata.get("web_url"),
        }
    return {"exists": False, "age_label": "Not uploaded", "status": "missing", "modified": None}


def get_report_info(output_filename):
    path = os.path.join(OUTPUT_DIR, output_filename)
    if os.path.exists(path):
        mtime = os.path.getmtime(path)
        dt = datetime.fromtimestamp(mtime)
        return {"exists": True, "generated": dt.strftime("%b %d, %Y %H:%M")}
    return {"exists": False, "generated": None}


def _cache_source_locally(filename, content):
    cache_source_file(filename, content, DATA_DIR)
    if LOCAL_CACHE_DIR != DATA_DIR:
        cache_source_file(filename, content, LOCAL_CACHE_DIR)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def status():
    # Build a map of filename -> export name for files that have automated refresh
    refreshable = {e["save_as"]: e["name"] for e in EXPORTS}
    stored_files = ARTIFACT_STORE.list_source_files()

    files = []
    for f in SOURCE_FILES:
        info = get_file_info(f["filename"], stored_files.get(f["filename"]))
        export_name = refreshable.get(f["filename"])
        files.append({**f, **info, "refreshable": export_name is not None, "export_name": export_name})

    reports = []
    for r in REPORTS:
        info = get_report_info(r["output"])
        reports.append({**r, **info})

    return jsonify(
        {
            "files": files,
            "reports": reports,
            "storage_backend": ARTIFACT_STORE.backend_name,
            "refresh_enabled": ENABLE_JUNIPER_REFRESH,
            "upload_enabled": ENABLE_SOURCE_UPLOADS,
            "credential_setup_enabled": ENABLE_CREDENTIAL_SETUP,
        }
    )


@app.route("/api/upload", methods=["POST"])
def upload():
    if not ENABLE_SOURCE_UPLOADS:
        return jsonify({"error": "Source uploads are disabled on this instance."}), 403

    uploaded = []
    for f in SOURCE_FILES:
        key = f["id"]
        if key in request.files:
            file = request.files[key]
            if file.filename:
                content = file.read()
                ARTIFACT_STORE.put_source_file(
                    f["filename"],
                    content,
                    content_type=file.mimetype or "application/octet-stream",
                    metadata={"origin": "manual_upload", "label": f["label"]},
                )
                _cache_source_locally(f["filename"], content)
                uploaded.append(f["label"])
    return jsonify({"uploaded": uploaded, "count": len(uploaded)})


def _hydrate_required_sources(target_dir):
    filenames = [f["filename"] for f in SOURCE_FILES]
    hydrated = hydrate_source_files(ARTIFACT_STORE, filenames, target_dir)
    if LOCAL_CACHE_DIR != target_dir:
        for filename in filenames:
            with open(os.path.join(target_dir, filename), "rb") as f:
                cache_source_file(filename, f.read(), LOCAL_CACHE_DIR)
    return hydrated


def _run_report(report, work_data_dir, work_output_dir):
    script_path = os.path.join(BASE_DIR, report["script"])
    output_path = os.path.join(work_output_dir, report["output"])
    if not os.path.exists(script_path):
        _generate_dummy_pdf_at_path(output_path, report["label"])
        return {"success": True, "report": report["label"], "mode": "demo", "output_path": output_path}

    result = subprocess.run(
        [
            sys.executable,
            script_path,
            "--base-path",
            work_data_dir,
            "--output-path",
            output_path,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=BASE_DIR,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or f"{report['label']} failed.")
    if not os.path.exists(output_path):
        raise RuntimeError(f"{report['label']} did not produce {output_path}.")
    return {"success": True, "report": report["label"], "output_path": output_path}


def _run_report_in_workspace(report):
    with tempfile.TemporaryDirectory(prefix="report-run-", dir=LOCAL_WORK_DIR or None) as work_dir:
        work_data_dir = os.path.join(work_dir, "data")
        work_output_dir = os.path.join(work_dir, "outputs")
        os.makedirs(work_data_dir, exist_ok=True)
        os.makedirs(work_output_dir, exist_ok=True)

        hydrated = _hydrate_required_sources(work_data_dir)
        result = _run_report(report, work_data_dir, work_output_dir)

        final_output_path = os.path.join(OUTPUT_DIR, report["output"])
        shutil.copy2(result["output_path"], final_output_path)

        return {
            "success": result["success"],
            "report": result["report"],
            "mode": result.get("mode"),
            "output": report["output"],
            "hydrated_files": sorted(hydrated),
        }


@app.route("/api/generate/<report_id>", methods=["POST"])
def generate(report_id):
    report = next((r for r in REPORTS if r["id"] == report_id), None)
    if not report:
        return jsonify({"error": "Report not found"}), 404

    try:
        result = _run_report_in_workspace(report)
        return jsonify(result)
    except ArtifactNotFoundError as e:
        return jsonify({"error": f"Missing source file in {ARTIFACT_STORE.backend_name}: {e}"}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Script timed out"}), 500


@app.route("/api/generate-all", methods=["POST"])
def generate_all():
    try:
        results = []
        hydrated_files = None
        with tempfile.TemporaryDirectory(prefix="report-run-", dir=LOCAL_WORK_DIR or None) as work_dir:
            work_data_dir = os.path.join(work_dir, "data")
            work_output_dir = os.path.join(work_dir, "outputs")
            os.makedirs(work_data_dir, exist_ok=True)
            os.makedirs(work_output_dir, exist_ok=True)

            hydrated = _hydrate_required_sources(work_data_dir)
            hydrated_files = sorted(hydrated)

            for report in REPORTS:
                result = _run_report(report, work_data_dir, work_output_dir)
                shutil.copy2(result["output_path"], os.path.join(OUTPUT_DIR, report["output"]))
                results.append(result["report"])

        return jsonify({"success": True, "reports": results, "hydrated_files": hydrated_files})
    except ArtifactNotFoundError as e:
        return jsonify({"error": f"Missing source file in {ARTIFACT_STORE.backend_name}: {e}"}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Shell script timed out"}), 500


@app.route("/api/report/<report_id>")
def view_report(report_id):
    report = next((r for r in REPORTS if r["id"] == report_id), None)
    if not report:
        return jsonify({"error": "Not found"}), 404
    path = os.path.join(OUTPUT_DIR, report["output"])
    if not os.path.exists(path):
        return jsonify({"error": "PDF not generated yet"}), 404
    return send_file(path, mimetype="application/pdf")


@app.route("/api/download/<report_id>")
def download_report(report_id):
    report = next((r for r in REPORTS if r["id"] == report_id), None)
    if not report:
        return jsonify({"error": "Not found"}), 404
    path = os.path.join(OUTPUT_DIR, report["output"])
    if not os.path.exists(path):
        return jsonify({"error": "PDF not generated yet"}), 404
    return send_file(path, mimetype="application/pdf", as_attachment=True, download_name=report["output"])


# ── Juniper Square Refresh Endpoints ─────────────────────────────────────────

@app.route("/api/refresh", methods=["POST"])
def refresh_data():
    if not ENABLE_JUNIPER_REFRESH:
        return jsonify({"error": "Juniper refresh is disabled on this instance.", "refresh_enabled": False}), 503

    data = request.get_json(silent=True) or {}
    export_name = data.get("export_name")

    if export_name:
        known = {e["name"] for e in EXPORTS}
        if export_name not in known:
            return jsonify({"error": f"Unknown export: {export_name}"}), 400

    started = start_refresh(ARTIFACT_STORE, DATA_DIR, export_name=export_name)
    if not started:
        return jsonify({"error": "A refresh is already in progress."}), 409
    mode = "all" if export_name is None else "single"
    return jsonify(
        {
            "status": "started",
            "mode": mode,
            "export_name": export_name,
            "storage_backend": ARTIFACT_STORE.backend_name,
        }
    )


@app.route("/api/refresh/status")
def refresh_status():
    state = get_refresh_status()
    state["storage_backend"] = ARTIFACT_STORE.backend_name
    state["refresh_enabled"] = ENABLE_JUNIPER_REFRESH
    return jsonify(state)


@app.route("/api/credentials/setup", methods=["POST"])
def setup_credentials():
    if not ENABLE_CREDENTIAL_SETUP:
        return jsonify({"error": "Credential setup is disabled on this instance.", "credential_setup_enabled": False}), 403

    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body must be JSON."}), 400
    email = data.get("email", "").strip()
    password = data.get("password", "").strip()
    if not email or not password:
        return jsonify({"error": "Both email and password are required."}), 400
    try:
        keyring.set_password(KEYRING_SERVICE, KEYRING_EMAIL_KEY, email)
        keyring.set_password(KEYRING_SERVICE, KEYRING_PASSWORD_KEY, password)
        return jsonify({"success": True, "message": "Credentials saved to system keychain."})
    except Exception as e:
        return jsonify({"error": f"Failed to store credentials: {e}"}), 500


@app.route("/api/credentials/check")
def check_credentials():
    if not ENABLE_CREDENTIAL_SETUP:
        return jsonify({"configured": False, "credential_setup_enabled": False}), 403

    email = keyring.get_password(KEYRING_SERVICE, KEYRING_EMAIL_KEY)
    return jsonify({"configured": email is not None, "credential_setup_enabled": True})


def _generate_dummy_pdf_at_path(path, title):
    """Generate a placeholder PDF for demo/prototype purposes."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        from reportlab.lib import colors

        output_dir = os.path.dirname(path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        c = canvas.Canvas(path, pagesize=letter)
        w, h = letter

        c.setFillColor(colors.HexColor("#0f172a"))
        c.rect(0, 0, w, h, fill=1, stroke=0)

        c.setFillColor(colors.HexColor("#38bdf8"))
        c.setFont("Helvetica-Bold", 28)
        c.drawCentredString(w / 2, h / 2 + 40, title)

        c.setFillColor(colors.HexColor("#94a3b8"))
        c.setFont("Helvetica", 14)
        c.drawCentredString(w / 2, h / 2, "Demo PDF — Replace with actual script output")

        c.setFillColor(colors.HexColor("#475569"))
        c.setFont("Helvetica", 11)
        c.drawCentredString(w / 2, h / 2 - 30, f"Generated: {datetime.now().strftime('%B %d, %Y %H:%M')}")

        c.save()
    except ImportError:
        # Fallback: write a minimal valid PDF manually
        output_dir = os.path.dirname(path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        pdf_content = f"""%PDF-1.4
1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj
2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj
3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]
/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj
4 0 obj << /Length 100 >>
stream
BT /F1 20 Tf 100 400 Td ({title} - Demo PDF) Tj ET
endstream
endobj
5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj
xref
0 6
trailer << /Size 6 /Root 1 0 R >>
startxref
0
%%EOF"""
        with open(path, "w") as f:
            f.write(pdf_content)


def _generate_dummy_pdf(filename, title):
    _generate_dummy_pdf_at_path(os.path.join(OUTPUT_DIR, filename), title)


if __name__ == "__main__":
    try:
        app.run(debug=True, port=5000)
    except StorageConfigurationError as e:
        raise SystemExit(str(e)) from e

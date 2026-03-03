"""
AntiGravity SAP Report Portal
==============================
A web portal for uploading SAP P&L and Balance Sheet files, processing them,
and viewing/printing the output PDF reports.

Usage:
    python portal.py
    Then open http://localhost:8080 in your browser.

Users are managed via the users.xlsx file in this same folder.
"""

import os, sys, re, json, uuid, hashlib, time, cgi, io, mimetypes, urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from http.cookies import SimpleCookie
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import openpyxl
import process_reports

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

PORT = 8080
BASE_FOLDER = SCRIPT_DIR
INPUT_DIR = os.path.join(BASE_FOLDER, "Input Files")
USERS_FILE = os.path.join(BASE_FOLDER, "users.xlsx")

SESSIONS = {}
SESSION_DURATION = 3600 * 8


# ═══════════════════════════════════════════════════════════════════════════════
#  USER AUTHENTICATION
# ═══════════════════════════════════════════════════════════════════════════════

def load_users():
    users = {}
    if not os.path.exists(USERS_FILE):
        return users
    try:
        wb = openpyxl.load_workbook(USERS_FILE, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0] and row[1]:
                users[str(row[0]).strip().lower()] = {
                    "password": str(row[1]).strip(),
                    "fullname": str(row[2]).strip() if row[2] else str(row[0]).strip(),
                    "role": str(row[3]).strip() if len(row) > 3 and row[3] else "User",
                }
        wb.close()
    except Exception as e:
        print(f"ERROR reading users.xlsx: {e}")
    return users


def authenticate(username, password):
    users = load_users()
    user = users.get(username.strip().lower())
    if user and user["password"] == password:
        return user
    return None


def create_session(username, user_info):
    sid = uuid.uuid4().hex
    SESSIONS[sid] = {
        "username": username,
        "fullname": user_info["fullname"],
        "role": user_info["role"],
        "expires": time.time() + SESSION_DURATION,
    }
    return sid


def get_session(cookie_header):
    if not cookie_header:
        return None
    cookie = SimpleCookie()
    cookie.load(cookie_header)
    if "session" not in cookie:
        return None
    sid = cookie["session"].value
    sess = SESSIONS.get(sid)
    if sess and sess["expires"] > time.time():
        return sess
    if sid in SESSIONS:
        del SESSIONS[sid]
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  FILE SCANNING
# ═══════════════════════════════════════════════════════════════════════════════

def get_output_folders():
    """Find output folders organized by month, combining P&L and Bal-Sht.
    Returns a dict: {month_key: {"pl": {...}, "bs": {...}, "sort_key": (year, month)}}"""
    monthly = {}  # e.g. {"Jan 26": {"pl": {name, files}, "bs": {name, files}, "sort_key": (26, 1)}}
    month_abbrevs = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                     "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}

    for item in sorted(os.listdir(BASE_FOLDER)):
        full = os.path.join(BASE_FOLDER, item)
        if not os.path.isdir(full):
            continue
        pdfs = sorted([f for f in os.listdir(full) if f.lower().endswith('.pdf')])
        if not pdfs:
            continue

        folder_info = {"name": item, "files": pdfs}
        ftype = None
        month_key = None

        if item.startswith("P&L "):
            ftype = "pl"
            month_key = item[4:]  # e.g. "Jan 26"
        elif item.startswith("Bal-Sht "):
            ftype = "bs"
            month_key = item[8:]  # e.g. "Jan 26"
        elif re.match(r'^[A-Z][a-z]{2} \d{2} Financial$', item):
            ftype = "pl"
            month_key = item.replace(" Financial", "")

        if ftype and month_key:
            if month_key not in monthly:
                # Parse sort key from month_key like "Jan 26"
                parts = month_key.split()
                yr = int(parts[1]) if len(parts) > 1 else 0
                mn = month_abbrevs.get(parts[0], 0)
                monthly[month_key] = {"pl": None, "bs": None, "sort_key": (yr, mn)}
            monthly[month_key][ftype] = folder_info

    # Sort by year then month
    sorted_keys = sorted(monthly.keys(), key=lambda k: monthly[k]["sort_key"])
    return {k: monthly[k] for k in sorted_keys}


def get_input_files_organized():
    """Organize input files by month and type (PL/BS). Scans subfolders too."""
    if not os.path.isdir(INPUT_DIR):
        return {}, []
    organized = {}  # {month_key: {"PL": [...], "BS": [...]}}
    unmatched = []
    for dirpath, dirnames, filenames in os.walk(INPUT_DIR):
        for f in sorted(filenames):
            if not f.lower().endswith('.xlsx'):
                continue
            result = process_reports.parse_filename(f)
            if result:
                ftype, company, month_num, year = result
                _, month_name = process_reports.MONTH_ROWS[month_num]
                short_month = month_name[:3].title()
                yr_short = str(year)[2:]
                key = f"{short_month} {yr_short}"
                if key not in organized:
                    organized[key] = {"PL": [], "BS": [], "sort_key": (year, month_num)}
                organized[key][ftype].append(f)
            else:
                unmatched.append(f)
    sorted_months = sorted(organized.keys(), key=lambda k: organized[k]["sort_key"])
    return {k: organized[k] for k in sorted_months}, unmatched


def get_all_input_files():
    if not os.path.isdir(INPUT_DIR):
        return []
    all_files = []
    for dirpath, dirnames, filenames in os.walk(INPUT_DIR):
        for f in filenames:
            if f.lower().endswith('.xlsx'):
                all_files.append(f)
    return sorted(all_files)


# ═══════════════════════════════════════════════════════════════════════════════
#  PROCESSING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def run_processing(force=False):
    import logging
    import io as _io

    log_capture = _io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                            datefmt="%Y-%m-%d %H:%M:%S"))
    logger = logging.getLogger("process_reports")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    file_handler = logging.FileHandler(os.path.join(BASE_FOLDER, "process_log.txt"), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                                 datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)

    tracker = process_reports.load_tracker(BASE_FOLDER)
    agg_path = os.path.join(BASE_FOLDER, process_reports.AGGREGATE_RELATIVE)
    input_files = process_reports.find_input_files(BASE_FOLDER)

    results = {"processed": [], "skipped": [], "errors": [], "log": ""}

    if not input_files:
        results["log"] = "No SAP input files found."
        return results

    logger.info(f"Portal processing: found {len(input_files)} input file(s)")

    for filepath, file_type, company, month_num, year in input_files:
        fname = os.path.basename(filepath)
        fp = process_reports.file_fingerprint(filepath)
        _, month_name = process_reports.MONTH_ROWS[month_num]
        period = f"{month_name} {year}"

        if not force and tracker.get(fname) == fp:
            logger.info(f"SKIP (unchanged): {fname}")
            results["skipped"].append(fname)
            continue

        logger.info(f"Processing: {fname} → {file_type} / {company} / {period}")

        try:
            short_month = month_name[:3].title()
            yr_short = str(year)[2:]
            month_folder_name = process_reports.get_output_folder_name(file_type, short_month, yr_short)
            month_folder = os.path.join(BASE_FOLDER, month_folder_name)
            os.makedirs(month_folder, exist_ok=True)

            update_date = datetime.fromtimestamp(os.path.getmtime(filepath)).strftime("%m/%d/%Y %I:%M %p")
            pdf_name = f"{company} {short_month} {yr_short}.pdf"
            pdf_path = os.path.join(month_folder, pdf_name)

            if file_type == "BS":
                rows = process_reports.parse_bs_excel(filepath)
                process_reports.build_pdf(rows, company, period, update_date, pdf_path, report_type="BS")
                logger.info(f"  ✓ BS PDF → {month_folder_name}/{pdf_name}")
            else:
                rows = process_reports.parse_sap_excel(filepath)
                process_reports.build_pdf(rows, company, period, update_date, pdf_path, report_type="PL")
                logger.info(f"  ✓ PL PDF → {month_folder_name}/{pdf_name}")

                metrics = process_reports.extract_metrics(filepath)
                ok = process_reports.update_aggregate(agg_path, company, month_num, metrics, logger)
                if ok:
                    logger.info(f"  ✓ Aggregate updated — {company} / {period}")
                else:
                    raise RuntimeError("Aggregate update failed")

            tracker[fname] = fp
            results["processed"].append(fname)

        except Exception as e:
            logger.error(f"  ✗ FAILED: {fname} — {e}")
            results["errors"].append({"file": fname, "error": str(e)})

    process_reports.save_tracker(BASE_FOLDER, tracker)

    logger.removeHandler(handler)
    logger.removeHandler(file_handler)

    results["log"] = log_capture.getvalue()
    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  HTML TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

def login_page(error=""):
    error_html = f'<div class="error">{error}</div>' if error else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AntiGravity SAP Portal - Login</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
       background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
       min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
.login-box {{ background: white; border-radius: 16px; padding: 48px 40px; width: 400px;
              box-shadow: 0 20px 60px rgba(0,0,0,0.3); }}
.logo {{ text-align: center; margin-bottom: 32px; }}
.logo h1 {{ color: #0f3460; font-size: 24px; margin-bottom: 4px; }}
.logo p {{ color: #888; font-size: 14px; }}
.form-group {{ margin-bottom: 20px; }}
label {{ display: block; color: #555; font-size: 13px; font-weight: 600;
         margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }}
input {{ width: 100%; padding: 12px 16px; border: 2px solid #e0e0e0; border-radius: 8px;
         font-size: 15px; transition: border-color 0.3s; }}
input:focus {{ outline: none; border-color: #0f3460; }}
button {{ width: 100%; padding: 14px; background: #0f3460; color: white; border: none;
          border-radius: 8px; font-size: 16px; font-weight: 600; cursor: pointer;
          transition: background 0.3s; }}
button:hover {{ background: #1a4a8a; }}
.error {{ background: #ffe0e0; color: #c00; padding: 12px; border-radius: 8px;
          margin-bottom: 20px; font-size: 14px; text-align: center; }}
</style>
</head>
<body>
<div class="login-box">
  <div class="logo">
    <h1>AntiGravity SAP Portal</h1>
    <p>Financial Report Processing System</p>
  </div>
  {error_html}
  <form method="POST" action="/login">
    <div class="form-group">
      <label>Username</label>
      <input type="text" name="username" required autofocus placeholder="Enter your username">
    </div>
    <div class="form-group">
      <label>Password</label>
      <input type="password" name="password" required placeholder="Enter your password">
    </div>
    <button type="submit">Sign In</button>
  </form>
</div>
</body>
</html>"""


def main_page(session):
    monthly_output = get_output_folders()
    organized_inputs, unmatched = get_input_files_organized()
    all_inputs = get_all_input_files()

    # Check if Aggregate file exists
    agg_path = os.path.join(BASE_FOLDER, "Annual Financial Data", "Aggregate P&L Output.xlsx")
    agg_exists = os.path.exists(agg_path)

    # ── Build OUTPUT section — Monthly Financial groups ─────────────
    def build_folder_files_html(folder_info):
        if not folder_info:
            return ""
        html = ""
        for f in folder_info["files"]:
            encoded = urllib.parse.quote(f)
            folder_encoded = urllib.parse.quote(folder_info["name"])
            html += f"""
                <div class="file-row">
                  <span class="file-name">{f}</span>
                  <div class="file-actions">
                    <a href="/view/{folder_encoded}/{encoded}" target="_blank" class="btn btn-view">View</a>
                    <a href="/view/{folder_encoded}/{encoded}" target="_blank"
                       onclick="window.open(this.href).print(); return false;" class="btn btn-print">Print</a>
                  </div>
                </div>"""
        return html

    monthly_output_html = ""
    if not monthly_output:
        monthly_output_html = '<div class="empty-state">No output files yet.</div>'
    else:
        for month_key, data in monthly_output.items():
            pl_info = data["pl"]
            bs_info = data["bs"]
            total_files = (len(pl_info["files"]) if pl_info else 0) + (len(bs_info["files"]) if bs_info else 0)

            inner_html = ""
            if pl_info:
                inner_html += f'<div class="input-type-label">P&L ({len(pl_info["files"])} files)</div>'
                inner_html += build_folder_files_html(pl_info)
            if bs_info:
                inner_html += f'<div class="input-type-label">Balance Sheet ({len(bs_info["files"])} files)</div>'
                inner_html += build_folder_files_html(bs_info)

            monthly_output_html += f"""
            <div class="folder-section collapsed">
              <div class="folder-header" onclick="this.parentElement.classList.toggle('collapsed')">
                <span>{month_key}</span>
                <span class="file-count">{total_files}</span>
              </div>
              <div class="folder-files">{inner_html}</div>
            </div>"""

    # ── Build INPUT section (right panel) — collapsed by default ─────────────
    input_html = ""
    for month_key, data in organized_inputs.items():
        pl_files = data["PL"]
        bs_files = data["BS"]
        files_html = ""
        if pl_files:
            files_html += '<div class="input-type-label">P&L Files</div>'
            for f in pl_files:
                files_html += f'<div class="input-file">{f}</div>'
        if bs_files:
            files_html += '<div class="input-type-label">Balance Sheet Files</div>'
            for f in bs_files:
                files_html += f'<div class="input-file">{f}</div>'
        total = len(pl_files) + len(bs_files)
        input_html += f"""
        <div class="folder-section collapsed">
          <div class="folder-header" onclick="this.parentElement.classList.toggle('collapsed')">
            <span>{month_key}</span>
            <span class="file-count">{total}</span>
          </div>
          <div class="folder-files">{files_html}</div>
        </div>"""

    if unmatched:
        files_html = ""
        for f in unmatched:
            files_html += f'<div class="input-file">{f}</div>'
        input_html += f"""
        <div class="folder-section collapsed">
          <div class="folder-header" onclick="this.parentElement.classList.toggle('collapsed')">
            <span>Other Files</span>
            <span class="file-count">{len(unmatched)}</span>
          </div>
          <div class="folder-files">{files_html}</div>
        </div>"""

    if not input_html:
        input_html = '<div class="empty-state">No input files uploaded yet.</div>'

    # Aggregate file link
    agg_link_html = ""
    if agg_exists:
        agg_encoded = urllib.parse.quote("Annual Financial Data") + "/" + urllib.parse.quote("Aggregate P&L Output.xlsx")
        agg_link_html = f"""
        <a href="/download/{agg_encoded}" target="_blank" class="agg-link" title="Open Aggregate P&L Output">
          <span class="agg-icon">📊</span>
          <span class="agg-text">Annual Financial Data</span>
        </a>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AntiGravity SAP Portal</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f0f2f5;
       display: flex; flex-direction: column; min-height: 100vh; }}

/* Header */
.header {{ background: linear-gradient(135deg, #0f3460, #1a4a8a); color: white;
           padding: 14px 32px; display: flex; align-items: center; justify-content: space-between;
           box-shadow: 0 2px 10px rgba(0,0,0,0.15); flex-shrink: 0; }}
.header h1 {{ font-size: 20px; }}
.header-right {{ display: flex; align-items: center; gap: 16px; }}
.user-info {{ font-size: 14px; opacity: 0.9; }}
.logout-btn {{ background: rgba(255,255,255,0.15); color: white; border: 1px solid rgba(255,255,255,0.3);
               padding: 6px 14px; border-radius: 6px; text-decoration: none; font-size: 13px; }}
.logout-btn:hover {{ background: rgba(255,255,255,0.25); }}

/* Top section — Upload/Process + Annual Data */
.top-section {{ padding: 16px 24px; display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
                max-width: 1400px; margin: 0 auto; width: 100%; flex-shrink: 0; }}

/* Bottom section — Dashboard split view */
.dashboard {{ flex: 1; display: grid; grid-template-columns: 1fr 1fr; gap: 0;
              max-width: 1400px; margin: 0 auto 16px; width: 100%; padding: 0 24px;
              min-height: 400px; }}

/* Cards */
.card {{ background: white; border-radius: 10px; overflow: hidden;
         box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; }}
.card-header {{ background: #f8f9fa; padding: 12px 16px; border-bottom: 1px solid #e9ecef;
                display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }}
.card-header h2 {{ font-size: 15px; color: #333; }}
.card-body {{ padding: 12px 16px; overflow-y: auto; flex: 1; }}

/* Dashboard panels */
.dash-panel {{ background: white; border-radius: 10px; overflow: hidden;
               box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; }}
.dash-panel:first-child {{ border-radius: 10px 0 0 10px; border-right: 1px solid #e0e0e0; }}
.dash-panel:last-child {{ border-radius: 0 10px 10px 0; }}
.dash-panel .panel-header {{ background: #f8f9fa; padding: 12px 16px; border-bottom: 1px solid #e9ecef;
                             flex-shrink: 0; display: flex; align-items: center; justify-content: space-between; }}
.dash-panel .panel-header h2 {{ font-size: 15px; color: #333; }}
.dash-panel .panel-body {{ overflow-y: auto; flex: 1; padding: 8px; }}

/* Type section headers */
.type-section-header {{ background: #e8f0fe; padding: 10px 14px; font-weight: 700; font-size: 14px;
                        color: #0f3460; border-bottom: 1px solid #d0ddf0; margin-top: 4px;
                        border-radius: 6px 6px 0 0; }}

/* Drop Zone */
.drop-zone {{ border: 2px dashed #ccc; border-radius: 10px; padding: 20px; text-align: center;
              transition: all 0.3s; cursor: pointer; background: #fafbfc; }}
.drop-zone:hover, .drop-zone.dragover {{ border-color: #0f3460; background: #e8f0fe; }}
.drop-zone h3 {{ color: #555; margin-bottom: 4px; font-size: 15px; }}
.drop-zone p {{ color: #888; font-size: 12px; }}
.drop-zone .icon {{ font-size: 32px; margin-bottom: 6px; }}
#fileInput {{ display: none; }}

/* Upload list */
.upload-list {{ margin-top: 8px; }}
.upload-item {{ display: flex; align-items: center; justify-content: space-between; gap: 8px;
                padding: 5px 10px; background: #f8f9fa; border-radius: 5px; margin-bottom: 3px;
                font-size: 12px; }}
.upload-item .status {{ font-weight: 600; }}
.upload-item .status.success {{ color: #28a745; }}
.upload-item .status.error {{ color: #dc3545; }}
.upload-item .status.uploading {{ color: #ffc107; }}

/* Process buttons */
.btn-row {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }}
.process-btn {{ background: #28a745; color: white; border: none; padding: 8px 16px;
                border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; }}
.process-btn:hover {{ background: #218838; }}
.process-btn:disabled {{ background: #ccc; cursor: not-allowed; }}
.process-btn.force {{ background: #e67e22; }}
.process-btn.force:hover {{ background: #d35400; }}

/* Aggregate link */
.agg-link {{ display: flex; align-items: center; gap: 12px; padding: 16px 20px;
             background: linear-gradient(135deg, #f8f9fa, #e8f0fe); border: 2px solid #d0ddf0;
             border-radius: 10px; text-decoration: none; color: #0f3460;
             transition: all 0.3s; cursor: pointer; }}
.agg-link:hover {{ background: linear-gradient(135deg, #e8f0fe, #d0ddf0); border-color: #0f3460;
                   box-shadow: 0 4px 12px rgba(15,52,96,0.15); }}
.agg-icon {{ font-size: 36px; }}
.agg-text {{ font-size: 15px; font-weight: 700; }}
.agg-sub {{ font-size: 12px; color: #666; font-weight: normal; display: block; margin-top: 2px; }}

/* Folder sections */
.folder-section {{ margin-bottom: 6px; border: 1px solid #e9ecef; border-radius: 6px; overflow: hidden; }}
.folder-section.collapsed .folder-files {{ display: none; }}
.folder-header {{ background: #f8f9fa; padding: 10px 14px; cursor: pointer;
                  display: flex; justify-content: space-between; align-items: center;
                  font-weight: 600; color: #333; font-size: 13px; }}
.folder-header:hover {{ background: #e9ecef; }}
.file-count {{ background: #0f3460; color: white; padding: 1px 8px; border-radius: 10px;
               font-size: 11px; font-weight: normal; min-width: 22px; text-align: center; }}
.file-row {{ display: flex; align-items: center; justify-content: space-between;
             padding: 7px 14px; border-top: 1px solid #f0f0f0; font-size: 13px; }}
.file-row:hover {{ background: #f8f9fa; }}
.file-name {{ color: #333; }}
.file-actions {{ display: flex; gap: 6px; }}
.btn {{ padding: 4px 10px; border-radius: 4px; text-decoration: none; font-size: 12px; font-weight: 500; }}
.btn-view {{ background: #0f3460; color: white; }}
.btn-view:hover {{ background: #1a4a8a; }}
.btn-print {{ background: #6c757d; color: white; }}
.btn-print:hover {{ background: #555; }}

/* Input file sections */
.input-file {{ padding: 5px 14px; border-top: 1px solid #f5f5f5; font-size: 12px; color: #555; }}
.input-type-label {{ padding: 5px 14px; font-size: 11px; font-weight: 700; color: #0f3460;
                     background: #f0f4fa; text-transform: uppercase; letter-spacing: 0.5px;
                     border-top: 1px solid #e0e8f0; }}

/* Log area */
.empty-state {{ color: #999; text-align: center; padding: 20px; font-size: 13px; }}
#processLog {{ background: #1e1e1e; color: #d4d4d4; padding: 10px; border-radius: 6px;
               font-family: 'Consolas', 'Courier New', monospace; font-size: 11px;
               max-height: 150px; overflow-y: auto; display: none; white-space: pre-wrap; }}
#processResult {{ margin-top: 6px; display: none; padding: 8px; border-radius: 6px; font-size: 12px; }}
#processResult.success {{ background: #d4edda; color: #155724; display: block; }}
#processResult.error {{ background: #f8d7da; color: #721c24; display: block; }}

.refresh-btn {{ background: #0f3460; color: white; border: none; padding: 5px 12px;
                border-radius: 5px; cursor: pointer; font-size: 12px; }}

@media (max-width: 900px) {{
  .top-section {{ grid-template-columns: 1fr; }}
  .dashboard {{ grid-template-columns: 1fr; }}
  .dash-panel:first-child {{ border-radius: 10px; border-right: none; border-bottom: 1px solid #e0e0e0; }}
  .dash-panel:last-child {{ border-radius: 10px; }}
}}
</style>
</head>
<body>

<div class="header">
  <h1>AntiGravity SAP Portal</h1>
  <div class="header-right">
    <span class="user-info">Welcome, {session["fullname"]}</span>
    <a href="/logout" class="logout-btn">Sign Out</a>
  </div>
</div>

<!-- Top Section: Upload/Process (left) + Annual Financial (right) -->
<div class="top-section">

  <!-- LEFT: Upload + Process buttons combined -->
  <div class="card">
    <div class="card-header"><h2>Upload & Process</h2></div>
    <div class="card-body">
      <div class="drop-zone" id="dropZone" onclick="document.getElementById('fileInput').click()">
        <div class="icon">📂</div>
        <h3>Drag & Drop Files Here</h3>
        <p>Accepts .xlsx SAP files (PL and BS)</p>
      </div>
      <input type="file" id="fileInput" multiple accept=".xlsx">
      <div class="upload-list" id="uploadList"></div>
      <div class="btn-row">
        <button class="process-btn" onclick="processFiles(false)">Process New Files</button>
        <button class="process-btn force" onclick="processFiles(true)">Re-Process All</button>
      </div>
      <div id="processResult"></div>
      <pre id="processLog"></pre>
    </div>
  </div>

  <!-- RIGHT: Annual Financial Data -->
  <div class="card">
    <div class="card-header"><h2>Annual Financial Data</h2></div>
    <div class="card-body" style="display:flex; align-items:center; justify-content:center;">
      {agg_link_html if agg_exists else '<div class="empty-state">No Aggregate file found.</div>'}
    </div>
  </div>

</div>

<!-- Dashboard: Output (left) | Input (right) -->
<div class="dashboard">

  <!-- LEFT: Output files by month -->
  <div class="dash-panel">
    <div class="panel-header">
      <h2>Output Reports</h2>
      <button class="refresh-btn" onclick="location.reload()">Refresh</button>
    </div>
    <div class="panel-body" id="outputSection">
      <div class="type-section-header">Monthly Financial</div>
      {monthly_output_html}
    </div>
  </div>

  <!-- RIGHT: Input files by month -->
  <div class="dash-panel">
    <div class="panel-header">
      <h2>Input Files</h2>
      <span style="font-size:12px; color:#888;">{len(all_inputs)} file(s)</span>
    </div>
    <div class="panel-body" id="inputSection">
      {input_html}
    </div>
  </div>

</div>

<script>
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const uploadList = document.getElementById('uploadList');

['dragenter','dragover'].forEach(e => {{
  dropZone.addEventListener(e, ev => {{ ev.preventDefault(); dropZone.classList.add('dragover'); }});
}});
['dragleave','drop'].forEach(e => {{
  dropZone.addEventListener(e, ev => {{ ev.preventDefault(); dropZone.classList.remove('dragover'); }});
}});

dropZone.addEventListener('drop', e => {{ handleFiles(e.dataTransfer.files); }});
fileInput.addEventListener('change', e => {{ handleFiles(e.target.files); }});

function handleFiles(files) {{
  for (let f of files) {{
    if (!f.name.toLowerCase().endsWith('.xlsx')) {{
      addUploadItem(f.name, 'error', 'Not .xlsx');
      continue;
    }}
    uploadFile(f);
  }}
}}

function addUploadItem(name, status, message) {{
  const div = document.createElement('div');
  div.className = 'upload-item';
  div.innerHTML = '<span>' + name + '</span><span class="status ' + status + '">' + message + '</span>';
  uploadList.prepend(div);
  return div;
}}

function uploadFile(file) {{
  const item = addUploadItem(file.name, 'uploading', 'Uploading...');
  const formData = new FormData();
  formData.append('file', file);

  fetch('/upload', {{ method: 'POST', body: formData }})
    .then(r => r.json())
    .then(data => {{
      const status = item.querySelector('.status');
      if (data.success) {{
        status.className = 'status success';
        status.textContent = 'Uploaded';
      }} else {{
        status.className = 'status error';
        status.textContent = data.error || 'Failed';
      }}
    }})
    .catch(err => {{
      const status = item.querySelector('.status');
      status.className = 'status error';
      status.textContent = 'Network error';
    }});
}}

function processFiles(force) {{
  const btns = document.querySelectorAll('.process-btn');
  btns.forEach(b => b.disabled = true);

  const log = document.getElementById('processLog');
  const result = document.getElementById('processResult');
  log.style.display = 'block';
  log.textContent = 'Processing...\\n';
  result.style.display = 'none';

  fetch('/process' + (force ? '?force=1' : ''), {{ method: 'POST' }})
    .then(r => r.json())
    .then(data => {{
      log.textContent = data.log || 'Done.';
      result.style.display = 'block';
      if (data.errors && data.errors.length > 0) {{
        result.className = 'error';
        result.textContent = 'Processed: ' + data.processed.length +
          ' | Skipped: ' + data.skipped.length +
          ' | Errors: ' + data.errors.length;
      }} else {{
        result.className = 'success';
        result.textContent = 'Processed: ' + data.processed.length +
          ' | Skipped (unchanged): ' + data.skipped.length + ' | No errors';
      }}
      btns.forEach(b => b.disabled = false);
      setTimeout(() => location.reload(), 2000);
    }})
    .catch(err => {{
      log.textContent += '\\nError: ' + err;
      result.className = 'error';
      result.style.display = 'block';
      result.textContent = 'Processing failed.';
      btns.forEach(b => b.disabled = false);
    }});
}}
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
#  HTTP REQUEST HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

class PortalHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]}")

    def _send(self, code, content, content_type="text/html", headers=None):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        if isinstance(content, str):
            content = content.encode("utf-8")
        self.wfile.write(content)

    def _redirect(self, url, cookie=None):
        self.send_response(302)
        self.send_header("Location", url)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()

    def _get_session(self):
        return get_session(self.headers.get("Cookie"))

    def _require_auth(self):
        session = self._get_session()
        if not session:
            self._redirect("/login")
            return None
        return session

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path

        if path == "/login" or path == "/":
            session = self._get_session()
            if session and path == "/":
                self._send(200, main_page(session))
            elif session:
                self._redirect("/")
            else:
                if path == "/":
                    self._redirect("/login")
                else:
                    self._send(200, login_page())
            return

        if path == "/logout":
            cookie = SimpleCookie()
            cookie.load(self.headers.get("Cookie", ""))
            if "session" in cookie:
                sid = cookie["session"].value
                SESSIONS.pop(sid, None)
            self._redirect("/login", "session=; Max-Age=0; Path=/")
            return

        if path.startswith("/view/"):
            session = self._require_auth()
            if not session:
                return
            parts = path[6:].split("/", 1)
            if len(parts) == 2:
                folder_name = urllib.parse.unquote(parts[0])
                file_name = urllib.parse.unquote(parts[1])
                file_path = os.path.join(BASE_FOLDER, folder_name, file_name)
                real_path = os.path.realpath(file_path)
                if real_path.startswith(os.path.realpath(BASE_FOLDER)) and os.path.isfile(real_path):
                    with open(real_path, "rb") as f:
                        content = f.read()
                    self._send(200, content, "application/pdf",
                               {"Content-Disposition": f"inline; filename=\"{file_name}\""})
                    return
            self._send(404, "File not found")
            return

        # Download file (for Aggregate xlsx etc.)
        if path.startswith("/download/"):
            session = self._require_auth()
            if not session:
                return
            rel_path = urllib.parse.unquote(path[10:])
            file_path = os.path.join(BASE_FOLDER, rel_path)
            real_path = os.path.realpath(file_path)
            if real_path.startswith(os.path.realpath(BASE_FOLDER)) and os.path.isfile(real_path):
                file_name = os.path.basename(real_path)
                with open(real_path, "rb") as f:
                    content = f.read()
                # Determine content type
                if file_name.lower().endswith('.xlsx'):
                    ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                elif file_name.lower().endswith('.pdf'):
                    ct = "application/pdf"
                else:
                    ct = "application/octet-stream"
                self._send(200, content, ct,
                           {"Content-Disposition": f"attachment; filename=\"{file_name}\""})
                return
            self._send(404, "File not found")
            return

        if path == "/api/input-files":
            session = self._require_auth()
            if not session:
                return
            self._send(200, json.dumps(get_all_input_files()), "application/json")
            return

        session = self._get_session()
        if session:
            self._send(200, main_page(session))
        else:
            self._redirect("/login")

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path

        if path == "/login":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            params = urllib.parse.parse_qs(body)
            username = params.get("username", [""])[0]
            password = params.get("password", [""])[0]

            user = authenticate(username, password)
            if user:
                sid = create_session(username, user)
                self._redirect("/", f"session={sid}; Path=/; HttpOnly; Max-Age={SESSION_DURATION}")
            else:
                self._send(200, login_page("Invalid username or password. Please try again."))
            return

        if path == "/upload":
            session = self._require_auth()
            if not session:
                self._send(401, json.dumps({"success": False, "error": "Not logged in"}), "application/json")
                return

            content_type = self.headers.get("Content-Type", "")
            if "multipart/form-data" not in content_type:
                self._send(400, json.dumps({"success": False, "error": "Invalid request"}), "application/json")
                return

            try:
                boundary = content_type.split("boundary=")[1].strip()
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)

                file_data, file_name = self._parse_multipart(body, boundary)

                if not file_name or not file_data:
                    self._send(400, json.dumps({"success": False, "error": "No file received"}),
                               "application/json")
                    return

                if not file_name.lower().endswith(".xlsx"):
                    self._send(400, json.dumps({"success": False, "error": "Only .xlsx files accepted"}),
                               "application/json")
                    return

                os.makedirs(INPUT_DIR, exist_ok=True)
                save_path = os.path.join(INPUT_DIR, file_name)
                with open(save_path, "wb") as f:
                    f.write(file_data)

                self._send(200, json.dumps({"success": True, "file": file_name}), "application/json")

            except Exception as e:
                self._send(500, json.dumps({"success": False, "error": str(e)}), "application/json")
            return

        if path == "/process":
            session = self._require_auth()
            if not session:
                self._send(401, json.dumps({"error": "Not logged in"}), "application/json")
                return

            query = urllib.parse.urlparse(self.path).query
            force = "force=1" in query

            try:
                results = run_processing(force=force)
                self._send(200, json.dumps(results), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e), "processed": [], "skipped": [],
                                            "errors": [{"file": "system", "error": str(e)}],
                                            "log": str(e)}), "application/json")
            return

        self._send(404, "Not found")

    def _parse_multipart(self, body, boundary):
        boundary_bytes = boundary.encode()
        parts = body.split(b"--" + boundary_bytes)

        for part in parts:
            if b"filename=" not in part:
                continue
            header_end = part.find(b"\r\n\r\n")
            if header_end == -1:
                continue
            header = part[:header_end].decode("utf-8", errors="ignore")
            match = re.search(r'filename="([^"]+)"', header)
            if not match:
                continue
            filename = os.path.basename(match.group(1))
            file_data = part[header_end + 4:]
            if file_data.endswith(b"\r\n"):
                file_data = file_data[:-2]
            return file_data, filename

        return None, None


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  AntiGravity SAP Report Portal")
    print("=" * 60)
    print(f"  Base folder: {BASE_FOLDER}")
    print(f"  Users file:  {USERS_FILE}")
    print(f"  Input dir:   {INPUT_DIR}")
    print()

    users = load_users()
    if users:
        print(f"  Loaded {len(users)} user(s): {', '.join(users.keys())}")
    else:
        print("  WARNING: No users found! Create users.xlsx first.")

    print()
    print(f"  Starting server on http://localhost:{PORT}")
    print(f"  Press Ctrl+C to stop")
    print("=" * 60)

    server = HTTPServer(("0.0.0.0", PORT), PortalHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()

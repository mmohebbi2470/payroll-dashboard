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
SAP_DIR = os.path.join(BASE_FOLDER, "SAP Reports")   # Module 1 data folder
INPUT_DIR = os.path.join(SAP_DIR, "Input Files")
USERS_FILE = os.path.join(BASE_FOLDER, "users.xlsx")

SESSIONS = {}
SESSION_DURATION = 3600 * 8

BACKEND_DIR = os.path.join(BASE_FOLDER, "backend")

# Known payroll processing scripts (in the backend/ folder)
PAYROLL_SCRIPT_INFO = {
    "auto_fill_vijay_payroll_from_all_tables_v8_final.py": {
        "name": "Auto Fill Payroll",
        "description": "Automatically fills payroll data from all input tables",
        "icon": "🔄",
    },
    "new_Earnings_Same_FIXED_v2.py": {
        "name": "Process Earnings",
        "description": "Process and calculate employee earnings",
        "icon": "💰",
    },
    "employee_deductions_universal_v22_FIXED_v2.py": {
        "name": "Employee Deductions",
        "description": "Calculate all employee deductions",
        "icon": "➖",
    },
    "reimbursements_otheritems_universal_FINAL_v3.py": {
        "name": "Reimbursements & Other Items",
        "description": "Process reimbursements and other payroll items",
        "icon": "📋",
    },
    "withholdings_employerliab_v4d_nocalc.py": {
        "name": "Withholdings & Employer Liabilities",
        "description": "Calculate withholdings and employer liabilities",
        "icon": "🏛️",
    },
}

def get_available_payroll_scripts():
    """Return list of payroll scripts that exist in backend/ folder."""
    available = []
    for filename, info in PAYROLL_SCRIPT_INFO.items():
        filepath = os.path.join(BACKEND_DIR, filename)
        available.append({
            "filename": filename,
            "name": info["name"],
            "description": info["description"],
            "icon": info["icon"],
            "available": os.path.exists(filepath),
            "filepath": filepath,
        })
    return available


# ═══════════════════════════════════════════════════════════════════════════════
#  USER AUTHENTICATION
# ═══════════════════════════════════════════════════════════════════════════════

PERM_COLUMNS = ['sap_reports', 'payroll_reports', 'ob_accounts', 'ob_orders', 'ob_invoices', 'ob_reports']

def load_users():
    users = {}
    if not os.path.exists(USERS_FILE):
        return users
    try:
        wb = openpyxl.load_workbook(USERS_FILE, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0] and row[1]:
                perms = {}
                for i, key in enumerate(PERM_COLUMNS):
                    val = str(row[4 + i]).strip() if len(row) > 4 + i and row[4 + i] else "Edit"
                    perms[key] = val  # "Edit" or "Read"
                # Column K (index 10) = company_access
                company_access = str(row[10]).strip() if len(row) > 10 and row[10] else "all"
                users[str(row[0]).strip().lower()] = {
                    "password": str(row[1]).strip(),
                    "fullname": str(row[2]).strip() if row[2] else str(row[0]).strip(),
                    "role": str(row[3]).strip() if len(row) > 3 and row[3] else "User",
                    "permissions": perms,
                    "company_access": company_access,
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
        "permissions": user_info.get("permissions", {}),
        "company_access": user_info.get("company_access", "all"),
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
    """Find output folders organized by type (P&L and Bal-Sht) and month.
    Also finds legacy 'Mon YY Financial' folders."""
    pl_folders = []
    bs_folders = []
    scan_dir = SAP_DIR if os.path.isdir(SAP_DIR) else BASE_FOLDER
    for item in sorted(os.listdir(scan_dir)):
        full = os.path.join(scan_dir, item)
        if not os.path.isdir(full):
            continue
        pdfs = sorted([f for f in os.listdir(full) if f.lower().endswith('.pdf')])
        if not pdfs:
            continue
        if item.startswith("P&L "):
            pl_folders.append({"name": item, "files": pdfs})
        elif item.startswith("Bal-Sht "):
            bs_folders.append({"name": item, "files": pdfs})
        elif re.match(r'^[A-Z][a-z]{2} \d{2} Financial$', item):
            # Legacy folder format — treat as P&L
            pl_folders.append({"name": item, "files": pdfs})
    return pl_folders, bs_folders


def _scan_input_files():
    """Scan INPUT_DIR recursively and return all .xlsx filenames found."""
    if not os.path.isdir(INPUT_DIR):
        return []
    files = []
    for root, dirs, fnames in os.walk(INPUT_DIR):
        for f in fnames:
            if f.lower().endswith('.xlsx') and not f.startswith('~$'):
                files.append(f)
    return sorted(files)


def get_input_files_organized():
    """Organize input files by month and type (PL/BS).
    Returns: (known_organized, other_organized, unmatched)
      - known_organized: dict of month→{PL:[], BS:[]} for known aggregate companies
      - other_organized: dict of month→{PL:[], BS:[]} for other companies
      - unmatched: list of filenames that couldn't be parsed at all
    """
    known = {}
    other = {}
    unmatched = []
    for f in _scan_input_files():
        result = process_reports.parse_filename(f)
        if result:
            ftype, company, month_num, year = result
            _, month_name = process_reports.MONTH_ROWS[month_num]
            short_month = month_name[:3].title()
            yr_short = str(year)[2:]
            key = f"{short_month} {yr_short}"
            target = known if process_reports.is_known_company(company) else other
            if key not in target:
                target[key] = {"PL": [], "BS": [], "sort_key": (year, month_num)}
            target[key][ftype].append(f)
        else:
            unmatched.append(f)
    sorted_known = sorted(known.keys(), key=lambda k: known[k]["sort_key"])
    sorted_other = sorted(other.keys(), key=lambda k: other[k]["sort_key"])
    return (
        {k: known[k] for k in sorted_known},
        {k: other[k] for k in sorted_other},
        unmatched
    )


def get_all_input_files():
    return _scan_input_files()


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

    file_handler = logging.FileHandler(os.path.join(SAP_DIR, "process_log.txt"), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                                 datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)

    tracker = process_reports.load_tracker(SAP_DIR)
    agg_path = os.path.join(SAP_DIR, process_reports.AGGREGATE_RELATIVE)
    input_files = process_reports.find_input_files(SAP_DIR)

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
            month_folder = os.path.join(SAP_DIR, month_folder_name)
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

                # Only update Aggregate P&L for known companies
                if process_reports.is_known_company(company):
                    metrics = process_reports.extract_metrics(filepath)
                    ok = process_reports.update_aggregate(agg_path, company, month_num, metrics, logger)
                    if ok:
                        logger.info(f"  ✓ Aggregate updated — {company} / {period}")
                    else:
                        raise RuntimeError("Aggregate update failed")
                else:
                    logger.info(f"  ⊘ Aggregate skipped (other company) — {company}")

            tracker[fname] = fp
            results["processed"].append(fname)

        except Exception as e:
            logger.error(f"  ✗ FAILED: {fname} — {e}")
            results["errors"].append({"file": fname, "error": str(e)})

    process_reports.save_tracker(SAP_DIR, tracker)

    logger.removeHandler(handler)
    logger.removeHandler(file_handler)

    results["log"] = log_capture.getvalue()
    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  HTML TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

def _change_password_modal_html():
    """Returns the HTML + CSS + JS for the Change Password modal, to embed in any page."""
    return """
<!-- Change Password Modal -->
<div id="cpModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%;
     background:rgba(0,0,0,0.5); z-index:9999; justify-content:center; align-items:center;">
  <div style="background:white; border-radius:12px; padding:28px; width:380px; box-shadow:0 20px 60px rgba(0,0,0,0.3);">
    <h3 style="color:#0f3460; margin-bottom:16px; font-size:18px;">Change Password</h3>
    <div id="cpError" style="display:none; background:#ffe0e0; color:#c00; padding:10px; border-radius:6px; margin-bottom:12px; font-size:13px;"></div>
    <div id="cpSuccess" style="display:none; background:#d4edda; color:#155724; padding:10px; border-radius:6px; margin-bottom:12px; font-size:13px;"></div>
    <div style="margin-bottom:14px;">
      <label style="display:block; font-size:12px; font-weight:600; color:#475569; margin-bottom:4px;">Current Password</label>
      <input type="password" id="cpCurrent" style="width:100%; padding:8px 12px; border:1px solid #d1d5db; border-radius:6px; font-size:13px;">
    </div>
    <div style="margin-bottom:14px;">
      <label style="display:block; font-size:12px; font-weight:600; color:#475569; margin-bottom:4px;">New Password</label>
      <input type="password" id="cpNew" style="width:100%; padding:8px 12px; border:1px solid #d1d5db; border-radius:6px; font-size:13px;">
    </div>
    <div style="margin-bottom:18px;">
      <label style="display:block; font-size:12px; font-weight:600; color:#475569; margin-bottom:4px;">Confirm New Password</label>
      <input type="password" id="cpConfirm" style="width:100%; padding:8px 12px; border:1px solid #d1d5db; border-radius:6px; font-size:13px;">
    </div>
    <div style="display:flex; gap:10px; justify-content:flex-end;">
      <button onclick="closeCpModal()" style="padding:8px 16px; border:1px solid #d1d5db; border-radius:6px; background:white; cursor:pointer; font-size:13px;">Cancel</button>
      <button onclick="submitChangePassword()" style="padding:8px 16px; border:none; border-radius:6px; background:#0f3460; color:white; cursor:pointer; font-size:13px; font-weight:600;">Change Password</button>
    </div>
  </div>
</div>
<script>
function openCpModal() {
  document.getElementById('cpCurrent').value = '';
  document.getElementById('cpNew').value = '';
  document.getElementById('cpConfirm').value = '';
  document.getElementById('cpError').style.display = 'none';
  document.getElementById('cpSuccess').style.display = 'none';
  document.getElementById('cpModal').style.display = 'flex';
}
function closeCpModal() {
  document.getElementById('cpModal').style.display = 'none';
}
async function submitChangePassword() {
  var errEl = document.getElementById('cpError');
  var succEl = document.getElementById('cpSuccess');
  errEl.style.display = 'none';
  succEl.style.display = 'none';
  var cur = document.getElementById('cpCurrent').value;
  var newP = document.getElementById('cpNew').value;
  var conf = document.getElementById('cpConfirm').value;
  if (!cur || !newP || !conf) { errEl.textContent = 'All fields are required'; errEl.style.display = 'block'; return; }
  if (newP !== conf) { errEl.textContent = 'New passwords do not match'; errEl.style.display = 'block'; return; }
  if (newP.length < 4) { errEl.textContent = 'New password must be at least 4 characters'; errEl.style.display = 'block'; return; }
  try {
    var resp = await fetch('/api/change-password', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ current_password: cur, new_password: newP, confirm_password: conf })
    });
    var data = await resp.json();
    if (!resp.ok) { errEl.textContent = data.error || 'Failed to change password'; errEl.style.display = 'block'; return; }
    succEl.textContent = 'Password changed successfully!';
    succEl.style.display = 'block';
    setTimeout(function() { closeCpModal(); }, 1500);
  } catch(e) { errEl.textContent = 'Network error'; errEl.style.display = 'block'; }
}
</script>
"""


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
    pl_folders, bs_folders = get_output_folders()
    organized_inputs, other_inputs, unmatched = get_input_files_organized()
    all_inputs = get_all_input_files()

    # ── Build COMBINED aligned dashboard (interleaved by month: P&L then BS) ─
    # Map output folders by month key: "Jan 26", "Feb 26", etc.
    # Separate output PDFs into known vs other companies
    pl_output_map = {}
    pl_other_output_map = {}
    for folder in pl_folders:
        key = folder["name"][4:] if folder["name"].startswith("P&L ") else folder["name"]
        # Split PDFs: known companies vs other companies
        known_pdfs = []
        other_pdfs = []
        for pdf in folder["files"]:
            # PDF name format: "COMPANY Mon YY.pdf"
            company_part = pdf.rsplit(" ", 2)[0] if len(pdf.rsplit(" ", 2)) == 3 else pdf
            if process_reports.is_known_company(company_part):
                known_pdfs.append(pdf)
            else:
                other_pdfs.append(pdf)
        if known_pdfs:
            pl_output_map[key] = {"name": folder["name"], "files": known_pdfs}
        if other_pdfs:
            pl_other_output_map[key] = {"name": folder["name"], "files": other_pdfs}

    bs_output_map = {}
    bs_other_output_map = {}
    for folder in bs_folders:
        key = folder["name"][8:] if folder["name"].startswith("Bal-Sht ") else folder["name"]
        known_pdfs = []
        other_pdfs = []
        for pdf in folder["files"]:
            company_part = pdf.rsplit(" ", 2)[0] if len(pdf.rsplit(" ", 2)) == 3 else pdf
            if process_reports.is_known_company(company_part):
                known_pdfs.append(pdf)
            else:
                other_pdfs.append(pdf)
        if known_pdfs:
            bs_output_map[key] = {"name": folder["name"], "files": known_pdfs}
        if other_pdfs:
            bs_other_output_map[key] = {"name": folder["name"], "files": other_pdfs}

    def _month_sort_key(k):
        if k in organized_inputs:
            return organized_inputs[k]["sort_key"]
        if k in other_inputs:
            return other_inputs[k]["sort_key"]
        parts = k.split()
        if len(parts) == 2:
            m = process_reports.MONTH_ABBREVS.get(parts[0].upper(), 99)
            y = 2000 + int(parts[1]) if parts[1].isdigit() else 9999
            return (y, m)
        return (9999, 99)

    all_month_keys = sorted(
        set(list(pl_output_map.keys()) + list(bs_output_map.keys()) + list(organized_inputs.keys())),
        key=_month_sort_key
    )

    # Month keys for "Other Companies" section
    all_other_month_keys = sorted(
        set(list(pl_other_output_map.keys()) + list(bs_other_output_map.keys()) + list(other_inputs.keys())),
        key=_month_sort_key
    )

    def _file_mod_time(full_path):
        """Get file modification date/time as a formatted string."""
        try:
            mtime = os.path.getmtime(full_path)
            return datetime.fromtimestamp(mtime).strftime("%m/%d/%Y %I:%M %p")
        except Exception:
            return ""

    def _out_rows(folder, folder_name):
        if not folder:
            return '<div class="align-empty">—</div>'
        html = ""
        for f in sorted(folder["files"]):
            enc = urllib.parse.quote(f)
            fn_enc = urllib.parse.quote(folder_name)
            full_path = os.path.join(SAP_DIR, folder_name, f)
            mod_time = _file_mod_time(full_path)
            time_html = f'<span style="color:#888;font-size:10px;margin-left:6px;">({mod_time})</span>' if mod_time else ''
            html += (f'<div class="align-cell">'
                     f'<span class="af-name" title="{f}">{f}{time_html}</span>'
                     f'<span class="af-actions">'
                     f'<a href="/view/{fn_enc}/{enc}" target="_blank" class="btn btn-view">View</a>'
                     f'<a href="/view/{fn_enc}/{enc}" target="_blank"'
                     f' onclick="window.open(this.href).print();return false;" class="btn btn-print">Print</a>'
                     f'</span></div>')
        return html

    def _find_input_file(filename):
        """Find full path of an input file within INPUT_DIR subdirectories."""
        for root, dirs, fnames in os.walk(INPUT_DIR):
            if filename in fnames:
                return os.path.join(root, filename)
        return None

    is_admin = session.get("role") == "Admin"

    def _in_rows(file_list):
        if not file_list:
            return '<div class="align-empty">—</div>'
        html = ""
        for f in sorted(file_list):
            enc = urllib.parse.quote(f)
            full_path = _find_input_file(f)
            mod_time = _file_mod_time(full_path) if full_path else ""
            time_html = f'<span style="color:#888;font-size:10px;margin-left:6px;">({mod_time})</span>' if mod_time else ''
            del_btn = (f'<button class="btn btn-del" onclick="deleteInputFile(\'{enc}\')" '
                       f'title="Delete this file">✕</button>') if is_admin else ''
            html += (f'<div class="align-cell" id="cell-{enc}">'
                     f'<span class="af-name" title="{f}">📗 {f}{time_html}</span>'
                     f'<span class="af-actions">'
                     f'{del_btn}'
                     f'<a href="/download-input/{enc}" class="btn btn-dl">Download</a>'
                     f'</span></div>')
        return html

    combined_html = ""

    # ── "Operating Companies" section title ──
    if all_month_keys:
        combined_html += (
            f'<div class="align-section">'
            f'<div class="align-hdr" style="background:#1a1a2e;color:white;border-bottom:2px solid #0f3460;font-size:15px;font-weight:700;padding:10px 14px;">'
            f'🏭 Operating Companies</div></div>'
        )

    for idx, month_key in enumerate(all_month_keys):
        # Collect P&L and BS content for this month
        month_inner = ""

        # P&L section for this month
        pl_out = pl_output_map.get(month_key)
        pl_in  = organized_inputs.get(month_key, {}).get("PL", [])
        pl_cnt_o = len(pl_out["files"]) if pl_out else 0
        pl_cnt_i = len(pl_in)
        if pl_out or pl_in:
            fn = pl_out["name"] if pl_out else ""
            month_inner += (
                f'<div class="align-section">'
                f'<div class="align-hdr pl-hdr">📊 P&amp;L {month_key}'
                f'<span class="ah-badge">Out: {pl_cnt_o} | In: {pl_cnt_i}</span></div>'
                f'<div class="align-body">'
                f'<div class="align-col">{_out_rows(pl_out, fn)}</div>'
                f'<div class="align-col align-col-r">{_in_rows(pl_in)}</div>'
                f'</div></div>'
            )

        # BS section for this month (immediately after P&L)
        bs_out = bs_output_map.get(month_key)
        bs_in  = organized_inputs.get(month_key, {}).get("BS", [])
        bs_cnt_o = len(bs_out["files"]) if bs_out else 0
        bs_cnt_i = len(bs_in)
        if bs_out or bs_in:
            fn = bs_out["name"] if bs_out else ""
            month_inner += (
                f'<div class="align-section">'
                f'<div class="align-hdr bs-hdr">📋 Bal-Sht {month_key}'
                f'<span class="ah-badge">Out: {bs_cnt_o} | In: {bs_cnt_i}</span></div>'
                f'<div class="align-body">'
                f'<div class="align-col">{_out_rows(bs_out, fn)}</div>'
                f'<div class="align-col align-col-r">{_in_rows(bs_in)}</div>'
                f'</div></div>'
            )

        if month_inner:
            total_out = pl_cnt_o + bs_cnt_o
            total_in  = pl_cnt_i + bs_cnt_i
            # All months default to collapsed
            combined_html += (
                f'<div class="month-group collapsed">'
                f'<div class="month-hdr" onclick="toggleMonth(this)">'
                f'<span class="month-arrow">&#9654;</span>'
                f'<span class="month-title">{month_key}</span>'
                f'<span class="month-summary">P&amp;L: {pl_cnt_o} out / {pl_cnt_i} in &nbsp; | &nbsp; '
                f'Bal-Sht: {bs_cnt_o} out / {bs_cnt_i} in</span>'
                f'</div>'
                f'<div class="month-content">{month_inner}</div>'
                f'</div>'
            )

    # ── "Other Companies" section — organized by month, same layout ──
    if all_other_month_keys:
        other_inner_html = ""
        for idx2, month_key in enumerate(all_other_month_keys):
            oc_month_inner = ""

            # Other P&L
            oc_pl_out = pl_other_output_map.get(month_key)
            oc_pl_in  = other_inputs.get(month_key, {}).get("PL", [])
            oc_pl_cnt_o = len(oc_pl_out["files"]) if oc_pl_out else 0
            oc_pl_cnt_i = len(oc_pl_in)
            if oc_pl_out or oc_pl_in:
                fn = oc_pl_out["name"] if oc_pl_out else ""
                oc_month_inner += (
                    f'<div class="align-section">'
                    f'<div class="align-hdr pl-hdr">📊 P&amp;L {month_key}'
                    f'<span class="ah-badge">Out: {oc_pl_cnt_o} | In: {oc_pl_cnt_i}</span></div>'
                    f'<div class="align-body">'
                    f'<div class="align-col">{_out_rows(oc_pl_out, fn)}</div>'
                    f'<div class="align-col align-col-r">{_in_rows(oc_pl_in)}</div>'
                    f'</div></div>'
                )

            # Other BS
            oc_bs_out = bs_other_output_map.get(month_key)
            oc_bs_in  = other_inputs.get(month_key, {}).get("BS", [])
            oc_bs_cnt_o = len(oc_bs_out["files"]) if oc_bs_out else 0
            oc_bs_cnt_i = len(oc_bs_in)
            if oc_bs_out or oc_bs_in:
                fn = oc_bs_out["name"] if oc_bs_out else ""
                oc_month_inner += (
                    f'<div class="align-section">'
                    f'<div class="align-hdr bs-hdr">📋 Bal-Sht {month_key}'
                    f'<span class="ah-badge">Out: {oc_bs_cnt_o} | In: {oc_bs_cnt_i}</span></div>'
                    f'<div class="align-body">'
                    f'<div class="align-col">{_out_rows(oc_bs_out, fn)}</div>'
                    f'<div class="align-col align-col-r">{_in_rows(oc_bs_in)}</div>'
                    f'</div></div>'
                )

            if oc_month_inner:
                oc_total_out = oc_pl_cnt_o + oc_bs_cnt_o
                oc_total_in  = oc_pl_cnt_i + oc_bs_cnt_i
                # All months default to collapsed
                other_inner_html += (
                    f'<div class="month-group collapsed">'
                    f'<div class="month-hdr" onclick="toggleMonth(this)">'
                    f'<span class="month-arrow">&#9654;</span>'
                    f'<span class="month-title">{month_key}</span>'
                    f'<span class="month-summary">P&amp;L: {oc_pl_cnt_o} out / {oc_pl_cnt_i} in &nbsp; | &nbsp; '
                    f'Bal-Sht: {oc_bs_cnt_o} out / {oc_bs_cnt_i} in</span>'
                    f'</div>'
                    f'<div class="month-content">{oc_month_inner}</div>'
                    f'</div>'
                )

        if other_inner_html:
            combined_html += (
                f'<div class="align-section" style="margin-top:16px;">'
                f'<div class="align-hdr" style="background:#e8f4fd;color:#1565c0;border-bottom:2px solid #90caf9;font-size:15px;font-weight:700;padding:10px 14px;">'
                f'🏢 Other Companies</div>'
                f'<div style="padding:4px 8px;">{other_inner_html}</div>'
                f'</div>'
            )

    # Show truly unmatched files (bad format / typos) if any
    if unmatched:
        combined_html += (
            f'<div class="align-section">'
            f'<div class="align-hdr" style="background:#fff3cd;color:#856404;border-bottom:1px solid #ffe69c;">'
            f'⚠ Unrecognized Files<span class="ah-badge">{len(unmatched)}</span></div>'
            f'<div class="align-body">'
            f'<div class="align-col"><div class="align-empty">—</div></div>'
            f'<div class="align-col align-col-r">{_in_rows(unmatched)}</div>'
            f'</div></div>'
        )

    # Aggregate P&L Output section
    agg_rel = process_reports.AGGREGATE_RELATIVE
    agg_full = os.path.join(SAP_DIR, agg_rel)
    if os.path.isfile(agg_full):
        import time as _time
        agg_size = os.path.getsize(agg_full)
        agg_mod  = datetime.fromtimestamp(os.path.getmtime(agg_full)).strftime("%b %d, %Y %I:%M %p")
        agg_kb   = f"{agg_size / 1024:.0f} KB"
        combined_html += (
            f'<div class="align-section">'
            f'<div class="align-hdr" style="background:#fff8e1;color:#6d4c00;border-bottom:1px solid #ffe082;">'
            f'📈 Annual Financial Data</div>'
            f'<div style="padding:10px 14px;display:flex;align-items:center;justify-content:space-between;font-size:13px;">'
            f'<span>📗 Aggregate P&amp;L Output.xlsx'
            f'<span style="color:#888;font-size:11px;margin-left:8px;">({agg_kb}, updated {agg_mod})</span></span>'
            f'<span style="display:flex;gap:5px;">'
            f'<a href="/download-aggregate" class="btn btn-dl" style="background:#28a745;color:white;padding:4px 10px;border-radius:4px;text-decoration:none;font-size:12px;">Download</a>'
            f'</span>'
            f'</div></div>'
        )

    if not combined_html:
        combined_html = '<div class="empty-state">No files yet. Upload input files and click Process.</div>'

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

/* Top section — Upload & Process */
.top-section {{ padding: 16px 24px; display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
                max-width: 1400px; margin: 0 auto; width: 100%; flex-shrink: 0; }}

/* Bottom section — Dashboard (single combined panel) */
.dashboard {{ flex: 1; max-width: 1400px; margin: 0 auto 16px; width: 100%;
              padding: 0 24px; min-height: 400px; display: flex; flex-direction: column; }}

/* Cards */
.card {{ background: white; border-radius: 10px; overflow: hidden;
         box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; }}
.card-header {{ background: #f8f9fa; padding: 12px 16px; border-bottom: 1px solid #e9ecef;
                display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }}
.card-header h2 {{ font-size: 15px; color: #333; }}
.card-body {{ padding: 12px 16px; overflow-y: auto; flex: 1; }}

/* Dashboard combined panel */
.dash-panel {{ background: white; border-radius: 10px; overflow: hidden;
               box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; flex: 1; }}
.dash-panel .panel-header {{ background: #f8f9fa; border-bottom: 2px solid #e0e0e0;
                              flex-shrink: 0; display: grid; grid-template-columns: 1fr 1fr; }}
.ph-left, .ph-right {{ padding: 12px 16px; display: flex; align-items: center; gap: 12px; }}
.ph-right {{ border-left: 1px solid #e0e0e0; justify-content: space-between; }}
.ph-left h2, .ph-right h2 {{ font-size: 15px; color: #333; }}
.dash-panel .panel-body {{ overflow-y: auto; flex: 1; padding: 8px; }}

/* Collapsible month groups */
.month-group {{ margin-bottom: 10px; border: 1px solid #d0d7de; border-radius: 8px; overflow: hidden; }}
.month-hdr {{ display: flex; align-items: center; gap: 10px; padding: 10px 14px;
              background: linear-gradient(135deg, #f0f4fa, #e8ecf2); cursor: pointer;
              border-bottom: 1px solid #d0d7de; user-select: none; }}
.month-hdr:hover {{ background: linear-gradient(135deg, #e4eaf2, #dce2ea); }}
.month-arrow {{ font-size: 12px; color: #555; width: 14px; text-align: center; }}
.month-title {{ font-weight: 700; font-size: 14px; color: #1a3a5c; }}
.month-summary {{ font-size: 11px; color: #666; margin-left: auto; }}
.month-content {{ padding: 6px; }}
.month-group.collapsed .month-content {{ display: none; }}
.month-group.collapsed .month-hdr {{ border-bottom: none; }}

/* Aligned sections (month-type groups) */
.align-section {{ margin-bottom: 8px; border: 1px solid #e9ecef; border-radius: 6px; overflow: hidden; }}
.align-hdr {{ display: flex; justify-content: space-between; align-items: center;
              padding: 9px 14px; font-weight: 600; font-size: 13px; }}
.pl-hdr {{ background: #e8f5e9; color: #1b5e20; border-bottom: 1px solid #c8e6c9; }}
.bs-hdr {{ background: #e8f0fe; color: #0f3460; border-bottom: 1px solid #d0ddf0; }}
.ah-badge {{ font-size: 11px; font-weight: normal; background: rgba(0,0,0,0.12);
             padding: 2px 8px; border-radius: 10px; }}
.align-body {{ display: grid; grid-template-columns: 1fr 1fr; }}
.align-col {{ border-right: 1px solid #e0e0e0; }}
.align-col-r {{ border-right: none; }}
.align-cell {{ display: flex; align-items: center; justify-content: space-between;
               padding: 7px 14px; border-top: 1px solid #f0f0f0; font-size: 13px; min-height: 36px; }}
.align-cell:hover {{ background: #f8f9fa; }}
.af-name {{ color: #333; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap; margin-right: 8px; }}
.af-actions {{ display: flex; gap: 5px; flex-shrink: 0; }}
.align-empty {{ padding: 12px 14px; color: #bbb; font-size: 12px; text-align: center;
                border-top: 1px solid #f0f0f0; }}
.btn-dl {{ background: #28a745; color: white; }}
.btn-dl:hover {{ background: #218838; }}
.btn-del {{ background: #dc3545; color: white; font-size: 11px; padding: 2px 7px;
            border: none; border-radius: 4px; cursor: pointer; font-weight: 700; }}
.btn-del:hover {{ background: #b02a37; }}

/* Drop Zone */
.drop-zone {{ border: 2px dashed #ccc; border-radius: 10px; padding: 24px; text-align: center;
              transition: all 0.3s; cursor: pointer; background: #fafbfc; }}
.drop-zone:hover, .drop-zone.dragover {{ border-color: #0f3460; background: #e8f0fe; }}
.drop-zone h3 {{ color: #555; margin-bottom: 4px; font-size: 16px; }}
.drop-zone p {{ color: #888; font-size: 13px; }}
.drop-zone .icon {{ font-size: 36px; margin-bottom: 8px; }}
#fileInput {{ display: none; }}

/* Upload list */
.upload-list {{ margin-top: 10px; }}
.upload-item {{ display: flex; align-items: center; justify-content: space-between; gap: 8px;
                padding: 6px 10px; background: #f8f9fa; border-radius: 5px; margin-bottom: 4px;
                font-size: 13px; }}
.upload-item .status {{ font-weight: 600; }}
.upload-item .status.success {{ color: #28a745; }}
.upload-item .status.error {{ color: #dc3545; }}
.upload-item .status.uploading {{ color: #ffc107; }}

/* Process buttons */
.btn-row {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }}
.process-btn {{ background: #28a745; color: white; border: none; padding: 10px 20px;
                border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; }}
.process-btn:hover {{ background: #218838; }}
.process-btn:disabled {{ background: #ccc; cursor: not-allowed; }}
.process-btn.force {{ background: #e67e22; }}
.process-btn.force:hover {{ background: #d35400; }}

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
#processLog {{ background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 6px;
               font-family: 'Consolas', 'Courier New', monospace; font-size: 12px;
               max-height: 200px; overflow-y: auto; display: none; white-space: pre-wrap; }}
#processResult {{ margin-top: 8px; display: none; padding: 10px; border-radius: 6px; font-size: 13px; }}
#processResult.success {{ background: #d4edda; color: #155724; display: block; }}
#processResult.error {{ background: #f8d7da; color: #721c24; display: block; }}

.refresh-btn {{ background: #0f3460; color: white; border: none; padding: 5px 12px;
                border-radius: 5px; cursor: pointer; font-size: 12px; }}

@media (max-width: 900px) {{
  .top-section {{ grid-template-columns: 1fr; }}
  .dash-panel .panel-header {{ grid-template-columns: 1fr; }}
  .ph-right {{ border-left: none; border-top: 1px solid #e0e0e0; }}
  .align-body {{ grid-template-columns: 1fr; }}
  .align-col {{ border-right: none; border-bottom: 1px solid #e0e0e0; }}
}}
</style>
</head>
<body>

<div class="header">
  <h1>AntiGravity SAP Portal</h1>
  <div class="header-right">
    <a href="/" class="logout-btn" style="background:rgba(255,255,255,0.4);font-weight:600;">SAP Reports</a>
    {"" if session.get("permissions", {}).get("payroll_reports") == "Hidden" else '<a href="/payroll" class="logout-btn" style="background:rgba(255,255,255,0.2);">Payroll</a>'}
    <a href="/orders" class="logout-btn" style="background:rgba(255,255,255,0.2);">Orders Backlog</a>
    {'<a href="/admin" class="logout-btn" style="background:rgba(240,230,211,0.9);color:#8b6914;">Admin</a>' if session.get("role") == "Admin" else ""}
    <span class="user-info">Welcome, {session["fullname"]}</span>
    <a href="javascript:void(0)" onclick="openCpModal()" class="logout-btn" style="background:rgba(255,255,255,0.15);font-size:12px;">Change Password</a>
    <a href="/logout" class="logout-btn">Sign Out</a>
  </div>
</div>

{_change_password_modal_html()}

<!-- Top Section: Upload + Process -->
<div class="top-section">
  <div class="card">
    <div class="card-header"><h2>Upload Input Files</h2></div>
    <div class="card-body">
      <div class="drop-zone" id="dropZone" onclick="document.getElementById('fileInput').click()">
        <div class="icon">📂</div>
        <h3>Drag & Drop Files Here</h3>
        <p>Accepts .xlsx SAP files (PL and BS)</p>
      </div>
      <input type="file" id="fileInput" multiple accept=".xlsx">
      <div class="upload-list" id="uploadList"></div>
    </div>
  </div>

  <div class="card">
    <div class="card-header"><h2>Process Files</h2></div>
    <div class="card-body">
      <div class="btn-row">
        <button class="process-btn" onclick="processFiles(false)">Process New Files</button>
        <button class="process-btn force" onclick="processFiles(true)">Re-Process All</button>
      </div>
      <div id="processResult"></div>
      <pre id="processLog"></pre>
    </div>
  </div>
</div>

<!-- Dashboard: Combined aligned view — Output (left) | Input (right) per month+type -->
<div class="dashboard">
  <div class="dash-panel">
    <div class="panel-header">
      <div class="ph-left">
        <h2>Output Reports</h2>
        <button class="refresh-btn" onclick="location.reload()">Refresh</button>
      </div>
      <div class="ph-right">
        <h2>Input Files</h2>
        <span style="font-size:12px;color:#888;">{len(all_inputs)} file(s)</span>
      </div>
    </div>
    <div class="panel-body">
      {combined_html}
    </div>
  </div>
</div>

<script>
function toggleMonth(hdr) {{
  const group = hdr.parentElement;
  const arrow = hdr.querySelector('.month-arrow');
  group.classList.toggle('collapsed');
  arrow.innerHTML = group.classList.contains('collapsed') ? '&#9654;' : '&#9660;';
}}

function deleteInputFile(encodedName) {{
  const displayName = decodeURIComponent(encodedName);
  if (!confirm('Delete input file "' + displayName + '"?\\n\\nThis cannot be undone.')) return;
  fetch('/delete-input/' + encodedName, {{ method: 'DELETE' }})
    .then(r => {{
      if (!r.ok) throw new Error('Server returned ' + r.status);
      return r.json();
    }})
    .then(data => {{
      if (data.success) {{
        const cell = document.getElementById('cell-' + encodedName);
        if (cell) {{
          cell.style.transition = 'opacity 0.3s';
          cell.style.opacity = '0';
          setTimeout(() => cell.remove(), 300);
        }}
        // Reload page to confirm file is gone from disk
        setTimeout(() => location.reload(), 800);
      }} else {{
        alert('Delete failed: ' + (data.detail || 'Unknown error'));
      }}
    }})
    .catch(err => alert('Delete failed: ' + err));
}}

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
      var nProc = (data.processed || []).length;
      var nSkip = (data.skipped || []).length;
      var nErr  = (data.errors || []).length;
      if (nErr > 0) {{
        result.className = 'error';
        result.textContent = 'Processed: ' + nProc +
          ' | Skipped: ' + nSkip +
          ' | Errors: ' + nErr;
      }} else {{
        result.className = 'success';
        result.textContent = 'Processed: ' + nProc +
          ' | Skipped (unchanged): ' + nSkip + ' | No errors';
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
#  PAYROLL PAGE
# ═══════════════════════════════════════════════════════════════════════════════

def payroll_page(session):
    scripts = get_available_payroll_scripts()

    script_cards_html = ""
    for s in scripts:
        avail_badge = (
            '<span style="color:#27ae60;font-weight:600;">✓ Ready</span>'
            if s["available"]
            else '<span style="color:#e74c3c;font-weight:600;">⚠ Script not found in backend/</span>'
        )
        btn_attrs = "" if s["available"] else 'disabled style="opacity:0.4;cursor:not-allowed;"'
        script_cards_html += f"""
        <div class="script-card" id="card-{s['filename']}">
          <div class="script-header">
            <span class="script-icon">{s['icon']}</span>
            <div class="script-title">
              <strong>{s['name']}</strong>
              <div class="script-status">{avail_badge}</div>
            </div>
          </div>
          <p class="script-desc">{s['description']}</p>
          <button class="run-btn" onclick="runScript('{s['filename']}', this)" {btn_attrs}>
            ▶ Run Script
          </button>
          <div class="output-box" id="out-{s['filename']}" style="display:none;"></div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Payroll — AntiGravity SAP Portal</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f0f2f5; color: #333; }}
    .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
               color: white; padding: 16px 32px;
               display: flex; justify-content: space-between; align-items: center; }}
    .header h1 {{ font-size: 1.4rem; font-weight: 700; letter-spacing: 0.5px; }}
    .header-right {{ display: flex; gap: 10px; align-items: center; }}
    .nav-btn {{ background: rgba(255,255,255,0.2); color: white; border: none;
                padding: 7px 16px; border-radius: 6px; cursor: pointer;
                font-size: 0.88rem; text-decoration: none; display: inline-block; }}
    .nav-btn:hover {{ background: rgba(255,255,255,0.35); }}
    .nav-btn.active {{ background: rgba(255,255,255,0.4); font-weight: 600; }}
    .user-info {{ font-size: 0.85rem; opacity: 0.85; margin: 0 8px; }}
    .logout-btn {{ background: rgba(231,76,60,0.8); color: white; border: none;
                   padding: 7px 16px; border-radius: 6px; cursor: pointer;
                   font-size: 0.88rem; text-decoration: none; }}
    .logout-btn:hover {{ background: rgba(231,76,60,1); }}

    .content {{ max-width: 960px; margin: 32px auto; padding: 0 20px; }}
    .page-title {{ font-size: 1.6rem; font-weight: 700; color: #1a1a2e;
                   margin-bottom: 8px; }}
    .page-subtitle {{ color: #666; margin-bottom: 28px; }}

    .scripts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
    @media (max-width: 700px) {{ .scripts-grid {{ grid-template-columns: 1fr; }} }}

    .script-card {{ background: white; border-radius: 12px; padding: 22px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                    border: 1px solid #e8ecef; }}
    .script-header {{ display: flex; gap: 14px; align-items: flex-start; margin-bottom: 10px; }}
    .script-icon {{ font-size: 2rem; line-height: 1; }}
    .script-title {{ flex: 1; }}
    .script-title strong {{ font-size: 1rem; color: #1a1a2e; display: block; margin-bottom: 4px; }}
    .script-status {{ font-size: 0.8rem; }}
    .script-desc {{ font-size: 0.88rem; color: #555; margin-bottom: 16px; }}
    .run-btn {{ background: linear-gradient(135deg, #0f3460, #16213e);
                color: white; border: none; padding: 9px 22px;
                border-radius: 7px; cursor: pointer; font-size: 0.9rem;
                font-weight: 600; width: 100%; transition: opacity 0.2s; }}
    .run-btn:hover:not([disabled]) {{ opacity: 0.85; }}
    .run-btn.running {{ background: #888; cursor: wait; }}

    .output-box {{ margin-top: 14px; background: #1a1a2e; color: #a8e6cf;
                   font-family: 'Courier New', monospace; font-size: 0.8rem;
                   padding: 14px; border-radius: 8px; max-height: 260px;
                   overflow-y: auto; white-space: pre-wrap; line-height: 1.5; }}
    .output-box .err {{ color: #ff8a80; }}

    .info-box {{ background: #fff9e6; border: 1px solid #ffe082;
                 border-radius: 10px; padding: 18px 22px; margin-bottom: 28px; }}
    .info-box h3 {{ color: #e67e22; margin-bottom: 8px; font-size: 1rem; }}
    .info-box p {{ font-size: 0.88rem; color: #555; line-height: 1.6; }}
  </style>
</head>
<body>

<div class="header">
  <h1>AntiGravity SAP Portal</h1>
  <div class="header-right">
    <a href="/" class="nav-btn">SAP Reports</a>
    <a href="/payroll" class="nav-btn active">Payroll</a>
    <a href="http://localhost:8001" target="_blank" class="nav-btn">Orders Backlog</a>
    <span class="user-info">Welcome, {session["fullname"]}</span>
    <a href="javascript:void(0)" onclick="openCpModal()" class="nav-btn" style="font-size:12px;padding:5px 12px;">Change Password</a>
    <a href="/logout" class="logout-btn">Sign Out</a>
  </div>
</div>

<div class="content">
  <div class="page-title">⚙️ Payroll Processing</div>
  <p class="page-subtitle">Run payroll processing scripts. Each script reads data from the Input Files folder.</p>

  <div class="info-box">
    <h3>📁 How it works</h3>
    <p>Each button runs the corresponding Python script from the <strong>backend/</strong> folder.
       Output and any errors will be shown below each button.
       Make sure your input files are placed in the <strong>Input Files</strong> folder before running.</p>
  </div>

  <div class="scripts-grid">
    {script_cards_html}
  </div>
</div>

<script>
async function runScript(filename, btn) {{
  const outBox = document.getElementById('out-' + filename);
  btn.classList.add('running');
  btn.disabled = true;
  btn.textContent = '⏳ Running...';
  outBox.style.display = 'block';
  outBox.innerHTML = 'Starting script...\n';

  try {{
    const resp = await fetch('/run-payroll', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{ script: filename }})
    }});
    const data = await resp.json();
    let html = '';
    if (data.stdout) html += data.stdout;
    if (data.stderr) html += '<span class="err">' + data.stderr + '</span>';
    if (!data.success) html += '<span class="err">\\n✗ Script exited with error code ' + data.returncode + '</span>';
    else html += '\\n✓ Completed successfully.';
    outBox.innerHTML = html;
  }} catch (e) {{
    outBox.innerHTML = '<span class="err">Error: ' + e.message + '</span>';
  }}

  btn.classList.remove('running');
  btn.disabled = false;
  btn.textContent = '▶ Run Script';
}}
</script>
{_change_password_modal_html()}
</body>
</html>"""


def _tab_wrapper_page(session, active_tab, iframe_src, title):
    """Shared wrapper: portal nav header + full-height iframe for a tab."""
    perms = session.get("permissions", {})
    tabs = []
    # Only show SAP Reports if not Hidden
    if perms.get("sap_reports") != "Hidden":
        tabs.append(("SAP Reports", "/", "sap"))
    # Only show Payroll if not Hidden
    if perms.get("payroll_reports") != "Hidden":
        tabs.append(("Payroll", "/payroll", "payroll"))
    # Orders Backlog — always shown (sub-tabs hidden inside the app)
    tabs.append(("Orders Backlog", "/orders", "orders"))
    # Admin tab — only show for Admin role users
    if session.get("role") == "Admin":
        tabs.append(("Admin", "/admin", "admin"))
    nav_links = ""
    for label, href, key in tabs:
        active_style = "background:rgba(255,255,255,0.4);font-weight:600;" if key == active_tab else "background:rgba(255,255,255,0.2);"
        if key == "admin":
            active_style = (active_style + "background:#f0e6d3;color:#8b6914;") if key == active_tab else "background:rgba(240,230,211,0.9);color:#8b6914;"
        nav_links += f'<a href="{href}" style="{active_style}{"color:white;" if key != "admin" else ""}padding:7px 16px;border-radius:6px;text-decoration:none;font-size:0.88rem;">{label}</a>\n    '

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>{title} — AntiGravity Portal</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ display: flex; flex-direction: column; height: 100vh; overflow: hidden;
           font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
    .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
               color: white; padding: 12px 28px;
               display: flex; justify-content: space-between; align-items: center;
               flex-shrink: 0; }}
    .header h1 {{ font-size: 1.2rem; font-weight: 700; }}
    .nav {{ display: flex; gap: 8px; align-items: center; }}
    .user-info {{ font-size: 0.82rem; opacity: 0.8; margin: 0 10px; }}
    .sign-out {{ background: rgba(231,76,60,0.8); color: white; padding: 6px 14px;
                 border-radius: 6px; text-decoration: none; font-size: 0.85rem; }}
    .sign-out:hover {{ background: rgba(231,76,60,1); }}
    iframe {{ flex: 1; width: 100%; border: none; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>AntiGravity SAP Portal</h1>
    <div class="nav">
      {nav_links}
      <span class="user-info">Welcome, {session["fullname"]}</span>
      <a href="javascript:void(0)" onclick="openCpModal()" style="background:rgba(255,255,255,0.15);color:white;padding:5px 12px;border-radius:6px;text-decoration:none;font-size:0.78rem;cursor:pointer;">Change Password</a>
      <a href="/logout" class="sign-out">Sign Out</a>
    </div>
  </div>
  {_change_password_modal_html()}
  <iframe src="{iframe_src}" allowfullscreen></iframe>
</body>
</html>"""


def payroll_tab_page(session):
    """Returns the Payroll tab — iframe loads the drag-and-drop payroll dashboard."""
    return _tab_wrapper_page(session, "payroll", "/payroll-app/", "Payroll")


def admin_tab_page(session):
    """Returns the Admin tab — iframe loads the orders app with admin tab auto-opened."""
    return _tab_wrapper_page(session, "admin", "/orders-app/?tab=admin", "Admin")


def orders_tab_page(session):
    """Returns the Orders Backlog tab — iframe loads the orders app."""
    return _tab_wrapper_page(session, "orders", "/orders-app/", "Orders Backlog")


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

        if path == "/api/input-files":
            session = self._require_auth()
            if not session:
                return
            self._send(200, json.dumps(get_all_input_files()), "application/json")
            return

        if path == "/payroll":
            session = self._require_auth()
            if not session:
                return
            self._send(200, payroll_page(session))
            return

        if path == "/api/payroll-scripts":
            session = self._require_auth()
            if not session:
                return
            self._send(200, json.dumps(get_available_payroll_scripts()), "application/json")
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

        if path == "/run-payroll":
            session = self._require_auth()
            if not session:
                self._send(401, json.dumps({"error": "Not logged in"}), "application/json")
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                data = json.loads(body)
                script_filename = data.get("script", "")
                # Validate script name is in our known list
                if script_filename not in PAYROLL_SCRIPT_INFO:
                    self._send(400, json.dumps({"error": "Unknown script"}), "application/json")
                    return
                script_path = os.path.join(BACKEND_DIR, script_filename)
                if not os.path.exists(script_path):
                    self._send(404, json.dumps({"error": f"Script not found: {script_filename}"}), "application/json")
                    return
                import subprocess
                result = subprocess.run(
                    [sys.executable, script_path],
                    capture_output=True, text=True, timeout=300,
                    cwd=SCRIPT_DIR
                )
                self._send(200, json.dumps({
                    "success": result.returncode == 0,
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }), "application/json")
            except subprocess.TimeoutExpired:
                self._send(200, json.dumps({
                    "success": False, "returncode": -1,
                    "stdout": "", "stderr": "Script timed out after 5 minutes.",
                }), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json")
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

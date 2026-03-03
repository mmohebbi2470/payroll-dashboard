import os
import shutil
import json
import traceback
import re
import urllib.parse
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, UploadFile, File, Request, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import openpyxl
import importlib.util
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime

app = FastAPI()

# Serving static files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "frontend")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/payroll-app", StaticFiles(directory=STATIC_DIR, html=True), name="payroll-app")

# --- IMPORT PORTAL LOGIC ---
import sys
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
import portal

def get_session_from_request(request: Request):
    cookie_header = request.headers.get("cookie")
    return portal.get_session(cookie_header)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return JSONResponse(status_code=204, content=None)

@app.get("/", response_class=HTMLResponse)
async def sap_index(request: Request):
    session = get_session_from_request(request)
    if session:
        return portal.main_page(session)
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
async def get_login(request: Request):
    session = get_session_from_request(request)
    if session:
        return RedirectResponse(url="/")
    return portal.login_page()

@app.post("/login", response_class=HTMLResponse)
async def post_login(username: str = Form(""), password: str = Form("")):
    user = portal.authenticate(username, password)
    if user:
        sid = portal.create_session(username, user)
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(key="session", value=sid, max_age=portal.SESSION_DURATION, path="/", httponly=True)
        return response
    return portal.login_page("Invalid username or password. Please try again.")

@app.get("/logout")
async def logout(request: Request):
    cookie_header = request.headers.get("cookie")
    if cookie_header:
        cookie = portal.SimpleCookie()
        cookie.load(cookie_header)
        if "session" in cookie:
            sid = cookie["session"].value
            portal.SESSIONS.pop(sid, None)
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session", path="/")
    return response

@app.get("/payroll", response_class=HTMLResponse)
async def get_payroll_tab(request: Request):
    session = get_session_from_request(request)
    if not session:
        return RedirectResponse(url="/login")
    return portal.payroll_tab_page(session)

@app.post("/upload-sap")
async def upload_sap(request: Request, file: UploadFile = File(...)):
    if not get_session_from_request(request):
        return JSONResponse(status_code=401, content={"success": False, "error": "Not logged in"})
    if not file.filename.lower().endswith(".xlsx"):
        return JSONResponse(status_code=400, content={"success": False, "error": "Only .xlsx files accepted"})
    os.makedirs(portal.INPUT_DIR, exist_ok=True)
    save_path = os.path.join(portal.INPUT_DIR, file.filename)
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"success": True, "file": file.filename}

@app.post("/process-sap")
async def process_sap(request: Request, force: bool = False):
    if not get_session_from_request(request):
        return JSONResponse(status_code=401, content={"error": "Not logged in"})
    try:
        results = portal.run_processing(force=force)
        return results
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "processed": [], "skipped": [], "errors": [{"file": "system", "error": str(e)}], "log": str(e)})

@app.get("/api/input-files-sap")
async def api_input_files_sap(request: Request):
    if not get_session_from_request(request):
        return JSONResponse(status_code=401, content={"error": "Not logged in"})
    return portal.get_all_input_files()

@app.get("/download-sap/{rel_path:path}")
async def download_sap(request: Request, rel_path: str):
    if not get_session_from_request(request):
        raise HTTPException(status_code=401, detail="Not logged in")
    file_path = os.path.join(portal.BASE_FOLDER, urllib.parse.unquote(rel_path))
    real_path = os.path.realpath(file_path)
    if real_path.startswith(os.path.realpath(portal.BASE_FOLDER)) and os.path.isfile(real_path):
        return FileResponse(real_path, filename=os.path.basename(real_path))
    raise HTTPException(status_code=404, detail="File not found")

@app.get("/view-sap/{folder_name}/{file_name}")
async def view_sap(request: Request, folder_name: str, file_name: str):
    if not get_session_from_request(request):
        raise HTTPException(status_code=401, detail="Not logged in")
    file_path = os.path.join(portal.BASE_FOLDER, urllib.parse.unquote(folder_name), urllib.parse.unquote(file_name))
    real_path = os.path.realpath(file_path)
    if real_path.startswith(os.path.realpath(portal.BASE_FOLDER)) and os.path.isfile(real_path):
        media_type = "application/pdf" if file_name.lower().endswith(".pdf") else None
        return FileResponse(real_path, filename=os.path.basename(real_path), media_type=media_type, content_disposition_type="inline")
    raise HTTPException(status_code=404, detail="File not found")


def format_date_long(date_str):
    try:
        # Standardize slashes
        date_str = date_str.replace("-", "/")
        dt = datetime.strptime(date_str, "%m/%d/%y")
        return dt.strftime("%B %Y")
    except:
        return "Unknown Date"

def format_date_short(date_str):
    return date_str.replace("/", "-")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = os.path.join(BASE_DIR, "uploaddepartment")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputdepartment")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Helper to load modules from existing scripts
def load_module_from_path(path: str, name_hint: str):
    import sys
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing required script: {path}")
    spec = importlib.util.spec_from_file_location(name_hint, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name_hint] = module  # Required for dataclasses and other module-refs
    spec.loader.exec_module(module)
    return module

MASTER_SCRIPT = os.path.join(BASE_DIR, "backend", "master_department_summary_to_excel_ALL_FIXED_v2.py")
PAYROLL_SCRIPT = os.path.join(BASE_DIR, "backend", "auto_fill_vijay_payroll_from_all_tables_v8_final.py")
PAYROLL_TEMPLATE = os.path.join(BASE_DIR, "Vijay Payroll.xlsx")

# State management (simplified for now, using a global variable/dict to store data in memory)
current_data: Dict[str, pd.DataFrame] = {}
current_output_xlsx: Optional[str] = None
final_payroll_data: Dict[str, pd.DataFrame] = {}

class CellUpdate(BaseModel):
    sheet: str
    target_date: Optional[str] = None
    row: int
    column: str  # Changed from col to match frontend
    value: Any

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    try:
        # Import master module
        master = load_module_from_path(MASTER_SCRIPT, "master")
        
        # We need to monkeypatch the scripts to run for this specific PDF and return the Excel path
        # For simplicity, we'll call process_one_pdf directly if available
        # But we need to make sure the sub-script paths are correct
        # The master script expects sub-scripts in the same folder as it
        
        # Load sub-modules for the master script
        earn_mod = master.__import_module(master.SCRIPT_EARNINGS, "earnings")
        ded_mod = master.__import_module(master.SCRIPT_DEDUCTIONS, "deductions")
        wl_mod = master.__import_module(master.SCRIPT_WITHHOLD_LIAB, "withhold_liab")
        r_mod = master.__import_module(master.SCRIPT_REIMB_OTHER, "reimb_other")

        # Initialize global flags in modules
        for mod in (earn_mod, ded_mod, wl_mod, r_mod):
            mod.INCLUDE_MTD = getattr(master, "INCLUDE_MTD", False)
            mod.INCLUDE_QTD = getattr(master, "INCLUDE_QTD", False)
            mod.INCLUDE_YTD = getattr(master, "INCLUDE_YTD", False)
        
        # Set output dir to our outputs folder
        master.OUTPUT_DIR = OUTPUT_DIR
        
        output_xlsx = master.process_one_pdf(file_path, earn_mod, ded_mod, wl_mod, r_mod)
        global current_output_xlsx, current_data
        current_output_xlsx = output_xlsx
        
        # Load data into memory and extract unique dates
        current_data = {}
        unique_dates = set()
        
        # Try to extract date from filename (e.g. PG January 2026 Department Summary_ALL_Tables_01-16-26.xlsx)
        fn_dates = re.findall(r"(\d{2}[-/]\d{2}[-/]\d{2})", file.filename)
        for fd in fn_dates:
            unique_dates.add(fd.replace("-", "/"))

        with pd.ExcelFile(output_xlsx) as xls:
            for sheet in xls.sheet_names:
                # fillna(0) to prevent JSON compliance errors (NaN is not allowed)
                df = pd.read_excel(xls, sheet_name=sheet).fillna(0)
                current_data[sheet] = df
                print(f"DEBUG: Loaded sheet {sheet}, current_data keys: {list(current_data.keys())}")
                
                # Look for date columns to extract unique values
                date_col = next((c for c in df.columns if str(c).upper() in ["CHECK DATE", "DATE"]), None)
                if date_col:
                    # Add non-summary unique dates (skip MTD/QTD/YTD)
                    vals = df[date_col].astype(str).unique()
                    for v in vals:
                        v_clean = v.strip()
                        # Filter out 0, empty, and common summary/label noise
                        if v_clean and v_clean not in ["0", "0.0", "nan", "None"]:
                            if not any(x in v_clean.upper() for x in ["MTD", "QTD", "YTD", "TOTAL", "NET"]):
                                if "/" in v_clean or "-" in v_clean:
                                    unique_dates.add(v_clean.replace("-", "/"))
            
        # Last resort: Try parsing the PDF directly for "Check Date" if Excel didn't provide any
        if not unique_dates:
            try:
                import pdfplumber
                with pdfplumber.open(file_path) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text() or ""
                        m = re.search(r"Check\s*Date\s+(\d{2}/\d{2}/\d{2})", text, re.I)
                        if m: unique_dates.add(m.group(1))
            except:
                pass

        original_base = os.path.splitext(file.filename)[0]
        new_filename = f"DS {original_base}.xlsx"
        new_path = os.path.join(OUTPUT_DIR, new_filename)
        
        # If a file with that name already exists, we might need to handle it 
        # (shutil.move will overwrite by default which is fine here)
        if os.path.exists(output_xlsx) and output_xlsx != new_path:
            shutil.move(output_xlsx, new_path)
            output_xlsx = new_path
            current_output_xlsx = output_xlsx # Update global path

        return {
            "status": "success", 
            "input_file": file.filename,    # Aligning keys with frontend
            "output_file": output_xlsx,     # Aligning keys with frontend
            "sheets": list(current_data.keys()),
            "available_dates": sorted(list(unique_dates)),
            "summary": calculate_actual_summary(sorted(list(unique_dates))[0] if unique_dates else "01/16/26")
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reprocess")
async def reprocess_pdf(request: Request):
    data = await request.json()
    filename = data.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename missing")
        
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    try:
        # Import master module
        master = load_module_from_path(MASTER_SCRIPT, "master")
        
        # Load sub-modules for the master script
        earn_mod = master.__import_module(master.SCRIPT_EARNINGS, "earnings")
        ded_mod = master.__import_module(master.SCRIPT_DEDUCTIONS, "deductions")
        wl_mod = master.__import_module(master.SCRIPT_WITHHOLD_LIAB, "withhold_liab")
        r_mod = master.__import_module(master.SCRIPT_REIMB_OTHER, "reimb_other")

        # Initialize global flags in modules
        for mod in (earn_mod, ded_mod, wl_mod, r_mod):
            mod.INCLUDE_MTD = getattr(master, "INCLUDE_MTD", False)
            mod.INCLUDE_QTD = getattr(master, "INCLUDE_QTD", False)
            mod.INCLUDE_YTD = getattr(master, "INCLUDE_YTD", False)
        
        # Set output dir to our outputs folder
        master.OUTPUT_DIR = OUTPUT_DIR
        
        output_xlsx = master.process_one_pdf(file_path, earn_mod, ded_mod, wl_mod, r_mod)
        global current_output_xlsx, current_data
        current_output_xlsx = output_xlsx
        
        # Load data into memory and extract unique dates
        current_data = {}
        unique_dates = set()
        
        # Try to extract date from filename
        fn_dates = re.findall(r"(\d{2}[-/]\d{2}[-/]\d{2})", filename)
        for fd in fn_dates:
            unique_dates.add(fd.replace("-", "/"))

        with pd.ExcelFile(output_xlsx) as xls:
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet).fillna(0)
                current_data[sheet] = df
                
                date_col = next((c for c in df.columns if str(c).upper() in ["CHECK DATE", "DATE"]), None)
                if date_col:
                    vals = df[date_col].astype(str).unique()
                    for v in vals:
                        v_clean = str(v).strip()
                        if v_clean and v_clean not in ["0", "0.0", "nan", "None"]:
                            if not any(x in v_clean.upper() for x in ["MTD", "QTD", "YTD", "TOTAL", "NET"]):
                                if "/" in v_clean or "-" in v_clean:
                                    unique_dates.add(v_clean.replace("-", "/"))
            
        if not unique_dates:
            try:
                import pdfplumber
                with pdfplumber.open(file_path) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text() or ""
                        m = re.search(r"Check\s*Date\s+(\d{2}/\d{2}/\d{2})", text, re.I)
                        if m: unique_dates.add(m.group(1))
            except:
                pass

        original_base = os.path.splitext(filename)[0]
        new_filename = f"DS {original_base}.xlsx"
        new_path = os.path.join(OUTPUT_DIR, new_filename)
        
        if os.path.exists(output_xlsx) and output_xlsx != new_path:
            shutil.move(output_xlsx, new_path)
            output_xlsx = new_path
            current_output_xlsx = output_xlsx

        return {
            "status": "success", 
            "input_file": filename,
            "output_file": output_xlsx,
            "sheets": list(current_data.keys()),
            "available_dates": sorted(list(unique_dates)),
            "summary": calculate_actual_summary(sorted(list(unique_dates))[0] if unique_dates else "01/16/26")
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

def calculate_actual_summary(pay_date: str):
    total_debit = 0
    total_credit = 0
    
    def norm_date(d):
        # Remove leading zeros from month/day to match consistently (e.g. 01/02/26 -> 1/2/26)
        s = str(d).strip()
        return re.sub(r'\b0+(\d)', r'\1', s)
    
    target = norm_date(pay_date)
    
    # In review stage, Debit is the Earnings sum
    if "Earnings" in current_data:
        df = current_data["Earnings"]
        # User manually updated Earnings to use 'Check Date' as the primary date identifier
        date_col = "Check Date" if "Check Date" in df.columns else next((c for c in df.columns if str(c).upper() in ["CHECK DATE", "DATE"]), None)
        total_col = next((c for c in df.columns if "EARNINGS TOTAL" in str(c).upper()), None)
        if date_col and total_col:
            def parse_val(v):
                try: return float(str(v).replace(',', ''))
                except: return 0.0
            mask = df[date_col].astype(str).apply(norm_date) == target
            total_debit = df[mask][total_col].apply(parse_val).sum()
            
    # For accounting view in review, Debit must equal Credit
    total_credit = total_debit
            
    return {
        "total_debit": round(total_debit, 2),
        "total_credit": round(total_credit, 2),
        "net_payroll": 0.0 # Will be updated after generation
    }

@app.get("/get-summary")
async def get_summary(pay_date: str):
    return calculate_actual_summary(pay_date)

@app.get("/list-files")
async def list_files():
    try:
        def get_files_with_mtime(directory):
            files = []
            if os.path.exists(directory):
                for f in os.listdir(directory):
                    if f.endswith((".pdf", ".xlsx")):
                        path = os.path.join(directory, f)
                        mtime = os.path.getmtime(path)
                        files.append({
                            "name": f,
                            "path": path,
                            "mtime": mtime,
                            "size": os.path.getsize(path)
                        })
            # Newest first
            return sorted(files, key=lambda x: x["mtime"], reverse=True)

        return {
            "inputs": get_files_with_mtime(UPLOAD_DIR),
            "outputs": get_files_with_mtime(OUTPUT_DIR)
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/load-file")
async def load_file(filename: str):
    """Loads a specific Excel file from outputs/ into memory."""
    global current_data, final_payroll_data
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    try:
        is_source = "Department Summary" in filename
        xl = pd.ExcelFile(path)
        data_dict = {}
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            df = df.where(pd.notnull(df), None)
            data_dict[sheet] = df
        if is_source:
            current_data = data_dict
            return {"status": "success", "source": "source", "sheets": list(data_dict.keys())}
        else:
            final_payroll_data = data_dict
            return {"status": "success", "source": "final", "sheets": list(data_dict.keys())}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download-file")
async def download_any_file(filename: str):
    """Downloads a specific file from outputs/ or uploads/."""
    # Check outputs first, then uploads
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        path = os.path.join(UPLOAD_DIR, filename)
        
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
        
    return FileResponse(path, filename=filename)

@app.get("/data/{sheet_name}")
async def get_sheet_data(sheet_name: str):
    # Robust matching: Strip and case-insensitive
    target = sheet_name.strip().upper()
    actual_key = next((k for k in current_data.keys() if k.strip().upper() == target), None)
    
    print(f"DEBUG: GET /data/{sheet_name} (target: {target}), found: {actual_key}, available: {list(current_data.keys())}")
    
    if not actual_key:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet_name}' not found")
    
    # fillna(0) to prevent JSON compliance errors (NaN is not allowed)
    df = current_data[actual_key].fillna(0)
    # Convert to JSON-friendly format
    data = df.to_dict(orient="records")
    columns = [{"headerName": col, "field": col, "editable": True} for col in df.columns]
    return {"columns": columns, "rowData": data}

@app.post("/update-cell")
async def update_cell(update: CellUpdate):
    if update.sheet not in current_data:
        raise HTTPException(status_code=404, detail="Sheet not found")
    
    df = current_data[update.sheet]
    try:
        # Assuming row is 0-indexed index from frontend
        df.at[update.row, update.column] = update.value # Use update.column
        # Save back to Excel
        with pd.ExcelWriter(current_output_xlsx, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=update.sheet, index=False)
        return {"status": "success"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

class GenerateRequest(BaseModel):
    target_date: str

@app.post("/generate-payroll")
async def generate_payroll(request: GenerateRequest):
    pay_date = request.target_date
    if not current_output_xlsx:
        raise HTTPException(status_code=400, detail="No source data available. Upload PDF first.")
    
    try:
        payroll = load_module_from_path(PAYROLL_SCRIPT, "payroll")
        payroll.ALL_TABLES_XLSX = current_output_xlsx
        payroll.PAYROLL_TEMPLATE_XLSX = PAYROLL_TEMPLATE
        payroll.TARGET_PAY_DATE = pay_date
        
        base_name = os.path.basename(current_output_xlsx)
        base_name = os.path.splitext(base_name)[0]
        if base_name.startswith("DS "):
            base_name = base_name[3:]
            
        short_date = format_date_short(pay_date)
        payroll.OUTPUT_XLSX = os.path.join(OUTPUT_DIR, f"PR {base_name} {short_date}.xlsx")
        
        payroll.main()
        
        # Load final payroll data into memory
        global final_payroll_data
        final_payroll_data = {}
        final_sum = {"total_debit": 0, "total_credit": 0, "net_payroll": 0}
        
        if os.path.exists(payroll.OUTPUT_XLSX):
            xls = pd.ExcelFile(payroll.OUTPUT_XLSX)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet).fillna(0)
                final_payroll_data[sheet] = df
                
                if sheet == "_Calc Debug":
                    continue
                
                # Priority 1: Exact matches for DB and CR (standard for this template)
                col_f = next((c for c in df.columns if str(c).strip().upper() in ["CR", "DB"]), None)
                col_g = next((c for c in df.columns if str(c).strip().upper() in ["DB", "CR"] and c != col_f), None)
                
                # Double check: F is usually Credit, G is usually Debit in this accounting template
                # But in the user screenshot, DB is before CR.
                # Let's just find columns that specifically contain JUST DB or CR
                headers_upper = [str(c).strip().upper() for c in df.columns]
                
                if "CR" in headers_upper:
                    col_f = df.columns[headers_upper.index("CR")]
                if "DB" in headers_upper:
                    col_g = df.columns[headers_upper.index("DB")]

                # Fallback to previously successful search keywords if exact match failed
                if not col_f: col_f = next((c for c in df.columns if any(x == str(c).strip().upper() for x in ["CREDIT", "CR (F)", "UNNAMED: 5"])), None)
                if not col_g: col_g = next((c for c in df.columns if any(x == str(c).strip().upper() for x in ["DEBIT", "DB (G)", "UNNAMED: 6"])), None)
                
                # If still not found, then use substrings but be very careful
                if not col_f: col_f = next((c for c in df.columns if "CREDIT" in str(c).upper() and "(FORMULA)" not in str(c).upper()), None)
                if not col_g: col_g = next((c for c in df.columns if "DEBIT" in str(c).upper() and "(FORMULA)" not in str(c).upper()), None)

                if col_f and col_g:
                    def to_f(v):
                        try:
                            # Clean up strings with commas, dollar signs, or blanks
                            v_str = str(v).replace(',', '').replace('$', '').strip()
                            return float(v_str) if v_str else 0.0
                        except: return 0.0
                    
                    # Search specifically for the total labels in ALL columns 
                    # (in case the index shifted or Unnamed: 1 detection failed)
                    found_total = False
                    for _, row in df.iterrows():
                        # Join all cell values in the row to find the label search string
                        row_text = " ".join([str(val).upper() for val in row.values])
                        
                        if "TOTAL PAYROLL" in row_text:
                            final_sum["total_credit"] = to_f(row.get(col_f, 0))
                            final_sum["total_debit"] = to_f(row.get(col_g, 0))
                            found_total = True
                        elif "NET OF PAYROLL" in row_text:
                            final_sum["net_payroll"] = to_f(row.get(col_g, 0))
                    
                    # Fallback ONLY if the labels were completely missing: 
                    # take the largest number that is at least a significant amount (to ignore random 55s)
                    if not found_total or final_sum["total_debit"] < 100:
                        potential_totals = df[col_g].apply(to_f).tolist()
                        if potential_totals:
                            final_sum["total_debit"] = max(potential_totals)
                            final_sum["total_credit"] = df[col_f].apply(to_f).max()

            final_sum["total_debit"] = round(final_sum["total_debit"], 2)
            final_sum["total_credit"] = round(final_sum["total_credit"], 2)
            final_sum["net_payroll"] = round(final_sum["net_payroll"], 2)
        
        return {
            "status": "success", 
            "file": payroll.OUTPUT_XLSX, 
            "sheets": list(final_payroll_data.keys()),
            "summary": final_sum
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/final-data/{sheet_name}")
async def get_final_sheet_data(sheet_name: str):
    target = sheet_name.strip().upper()
    actual_key = next((k for k in final_payroll_data.keys() if k.strip().upper() == target), None)
    
    if not actual_key:
        raise HTTPException(status_code=404, detail="Sheet not found")
    
    df = final_payroll_data[actual_key].fillna(0)
    # Convert to JSON-friendly format
    data = df.to_dict(orient="records")
    columns = [{"headerName": col, "field": col, "editable": True} for col in df.columns]
    return {"columns": columns, "rowData": data}

@app.post("/update-final")
async def update_final_cell(update: CellUpdate):
    if update.sheet not in final_payroll_data:
        raise HTTPException(status_code=404, detail="Sheet not found")
    
    df = final_payroll_data[update.sheet]
    try:
        # Update in memory
        df.at[update.row, update.column] = update.value # Use update.column
        
        # Determine the latest payroll file to save to
        # Updated naming convention search
        short_date = format_date_short(pay_date) if update.target_date else ""
        files = [os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.startswith("DS CHALL ") and not f.endswith("Department Summary.xlsx")]
        
        if not files:
             # Fallback to old naming just in case
             files = [os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.startswith("PayrollFilled_")]
             
        if not files:
            raise HTTPException(status_code=404, detail="Payroll file not found")
        
        latest_file = max(files, key=os.path.getctime)
        
        # Save back to Excel
        with pd.ExcelWriter(latest_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=update.sheet, index=False)
            
        return {"status": "success"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/source")
async def download_source():
    if not current_output_xlsx or not os.path.exists(current_output_xlsx):
        raise HTTPException(status_code=404, detail="Source Excel file not found")
    return FileResponse(current_output_xlsx, filename=os.path.basename(current_output_xlsx))

@app.get("/download/payroll")
async def download_payroll(pay_date: str = None):
    if pay_date:
        short_date = format_date_short(pay_date)
        filename = f"DS CHALL {short_date}.xlsx"
        filepath = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(filepath):
            return FileResponse(filepath, filename=filename)
        
        # Try alternate naming
        filename_alt = f"PayrollFilled_{pay_date.replace('/', '-')}.xlsx"
        filepath_alt = os.path.join(OUTPUT_DIR, filename_alt)
        if os.path.exists(filepath_alt):
            return FileResponse(filepath_alt, filename=filename_alt)
        
    # Fallback to most recently generated if no date or file doesn't exist
    files = [os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.startswith("PayrollFilled_")]
    if not files:
        raise HTTPException(status_code=404, detail="Payroll file not found. Generate it first.")
    
    latest_file = max(files, key=os.path.getctime)
    return FileResponse(latest_file, filename=os.path.basename(latest_file))

if __name__ == "__main__":
    import uvicorn
    import os
    
    host = os.environ.get("AG_HOST", "0.0.0.0")
    port = int(os.environ.get("AG_PAYROLL_PORT", "8001"))
    
    uvicorn.run(app, host=host, port=port)

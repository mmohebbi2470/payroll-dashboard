# AntiGravity SAP Portal - Project Context

## Overview
Unified web application with 3 business modules running on a single FastAPI server at port 8001.
- **SAP Reports** - Process P&L and Balance Sheet Excel files into PDF reports
- **Payroll** - Process employee payroll PDFs into DS/PR Excel files using Vijay Payroll template
- **Orders Backlog** - Manage orders, milestones, invoices, receipts (SQLite database)

**IMPORTANT**: Always use `run.py` as the entry point. Never use `portal.py` directly as a server.

## Directory Structure
```
AntiGravity-SAP report/
├── run.py                          # ENTRY POINT - starts backend.payroll:app on port 8001
├── portal.py                       # Auth + HTML templates (imported as MODULE, not run standalone)
├── process_reports.py              # SAP Excel→PDF engine
├── orders_app.py                   # Standalone orders server (pure stdlib, legacy)
├── users.xlsx                      # Login credentials
├── START PORTAL.command            # macOS launcher (runs python3 run.py)
├── requirements.txt                # Dependencies
│
├── frontend/
│   ├── index.html                  # Payroll data explorer UI (served at /payroll-app/)
│   └── orders-app/
│       └── index.html              # Orders backlog UI (served at /orders-app/)
│
├── backend/
│   ├── payroll.py                  # MAIN FastAPI app (~923 lines) - all routes
│   ├── orders.py                   # Orders CRUD routes (~929 lines)
│   ├── orders_server.py            # Standalone orders server (stdlib)
│   ├── database/
│   │   ├── db.py                   # SQLAlchemy setup, migrations, seeding
│   │   ├── models.py              # ORM models (clients, orders, milestones, invoices, receipts)
│   │   └── __init__.py
│   ├── master_department_summary_to_excel_ALL_FIXED_v2.py   # PDF→Excel orchestrator
│   ├── auto_fill_vijay_payroll_from_all_tables_v8_final.py  # DS→PR generator
│   ├── new_Earnings_Same_FIXED_v2.py                        # Earnings calc
│   ├── employee_deductions_universal_v22_FIXED_v2.py        # Deductions
│   ├── reimbursements_otheritems_universal_FINAL_v3.py      # Reimbursements
│   └── withholdings_employerliab_v4d_nocalc.py              # Withholdings
│
├── SAP Reports/
│   ├── Input Files/               # Upload SAP Excel files here
│   ├── P&L {Mon} {YY}/           # Output P&L PDFs
│   ├── Bal-Sht {Mon} {YY}/       # Output Balance Sheet PDFs
│   └── Annual Financial Data/
│       └── Aggregate P&L Output.xlsx
│
├── Payroll/
│   ├── Vijay Payroll.xlsx         # Template for PR generation
│   ├── uploaddepartment/          # PDF input storage
│   └── outputdepartment/
│       └── {Month Year}/          # DS and PR output files
│
└── Orders/
    ├── orders.db                  # SQLite database
    └── uploads/                   # Contract/PO/Invoice PDFs
```

## How to Start
```bash
cd ~/Documents/AntiGravity-SAP\ report
python3 run.py
# Server runs at http://localhost:8001, accessible on LAN at http://192.168.8.128:8001
```

## Architecture

### Entry Point: run.py
- Starts `backend.payroll:app` via uvicorn on port 8001 with `--host 0.0.0.0` and `--reload`

### Main Server: backend/payroll.py
- FastAPI app that serves ALL three modules
- Imports `portal` module for HTML generation (login_page, main_page, payroll_tab_page, orders_tab_page)
- Imports `orders` module and copies its routes into the main app
- Mounts static files: `/payroll-app/` → frontend/, `/orders-app/` → frontend/orders-app/
- Includes NoCacheMiddleware for browser cache busting on remote stations

### Authentication: portal.py
- Reads credentials from `users.xlsx`
- In-memory session store (SESSIONS dict, 8-hour TTL)
- Cookie-based: `session={uuid}; HttpOnly`

## API Endpoints

### SAP Reports
```
GET  /                    # Main dashboard (requires auth)
GET  /login, POST /login  # Authentication
POST /upload, /upload-sap # Upload Excel files
POST /process             # Process new/changed files
GET  /view/{folder}/{fn}  # View PDF report
GET  /api/input-files     # List input files
```

### Payroll
```
GET  /payroll             # Payroll tab (iframe to /payroll-app/)
POST /upload-payroll      # Upload PDF → creates DS file
GET  /list-files          # List uploads/outputs
GET  /load-file           # Load Excel into memory for grid editing
GET  /data/{sheet}        # Get sheet data as JSON
POST /update-cell         # Edit cell in DS file
POST /generate-payroll    # Create PR file from DS + Vijay Payroll template
GET  /final-data/{sheet}  # Get PR sheet data
GET  /download/source     # Download DS Excel
GET  /download/payroll    # Download PR Excel
```

### Orders
```
GET  /orders              # Orders tab (iframe to /orders-app/)
CRUD /api/clients         # Client management
CRUD /api/contract-types  # Contract type management
CRUD /api/companies       # Company management
CRUD /api/orders          # Order management (soft delete)
POST /api/orders/{id}/notes     # Add note (max 50/order)
CRUD /api/milestones      # Milestone management (max 15/order, audit trail)
GET  /api/milestones/{id}/audit # View audit log
CRUD /api/invoices        # Invoice creation (marks milestone billed)
POST /api/receipts        # Record payment receipt
GET  /api/reports/backlog # Unbilled milestones report
```

## Payroll Processing Pipeline
1. **Upload PDF** → `/upload-payroll` → `master_department_summary_to_excel_ALL_FIXED_v2.process_one_pdf()`
2. **Creates DS file** → `DS {name}.xlsx` in `Payroll/outputdepartment/{Month Year}/`
3. **User edits** via grid UI → `/update-cell`
4. **Generate Payroll** → `/generate-payroll` → `auto_fill_vijay_payroll_from_all_tables_v8_final.main()`
   - Uses `Payroll/Vijay Payroll.xlsx` as template
   - Runs 5 sub-modules: earnings, deductions, reimbursements, withholdings, employer liabilities
5. **Creates PR file** → `PR {name} {date}.xlsx` in same month folder

## SAP Report Processing Pipeline
1. **Upload Excel** → `/upload` → file saved to `SAP Reports/Input Files/{subfolder}/`
2. **Process** → `/process` → `process_reports.py` detects new/changed files via MD5 fingerprint
3. **Parse** → `parse_sap_excel()` or `parse_bs_excel()` extracts rows
4. **Generate PDF** → `build_pdf()` using ReportLab → saved to output folder
5. **Update Aggregate** → metrics written to `Aggregate P&L Output.xlsx`

## Key Configuration
- **UPLOAD_DIR**: `Payroll/uploaddepartment`
- **OUTPUT_DIR**: `Payroll/outputdepartment`
- **PAYROLL_TEMPLATE**: `Payroll/Vijay Payroll.xlsx`
- **MASTER_SCRIPT**: `backend/master_department_summary_to_excel_ALL_FIXED_v2.py`
- **PAYROLL_SCRIPT**: `backend/auto_fill_vijay_payroll_from_all_tables_v8_final.py`
- **Database**: SQLite at `Orders/orders.db` (configurable via AG_DB_TYPE env var)

## Database Schema (Orders Module)
- **clients**: id, client_id(8char), name, billing_address, contacts, billing_name/email
- **orders**: id, client_id(FK), order_name(12char), contract_type_id(FK), company_id(FK), contract_amount, po_number, is_deleted(soft delete)
- **milestones**: id, order_id(FK), milestone_name(12char), scheduled_date, payment_amount, milestone_type(Estimate/Confirmed), is_billed
- **milestone_audit**: tracks all changes to milestones (field, old_value, new_value, reason, who, when)
- **invoices**: id, order_id(FK), milestone_id(FK), invoice_number(unique), invoice_amount
- **receipts**: id, invoice_id(FK), receipt_date, receipt_amount, difference
- **contract_types**: category + subcategory (12 seeded types)
- **companies**: name (seeded: IT Curves, AI Dev Lab)

## Important Technical Notes
- All HTML routes return `HTMLResponse(content=html.encode("utf-8"))` to handle emoji characters
- Favicon returns `Response(status_code=204)` (not JSONResponse) to avoid Content-Length issues
- NoCacheMiddleware adds no-cache headers to HTML/JS/CSS for remote station freshness
- Orders module loaded via sys.path manipulation: `sys.path.insert(0, backend_dir)`
- `--reload` flag on uvicorn means file changes auto-restart the server
- Payroll uses in-memory state: `current_data`, `current_output_xlsx`, `final_payroll_data`

## GitHub
- Repo: https://github.com/mmohebbi2470/payroll-dashboard
- Branch: main

"""
Orders Backlog & Billing System — FastAPI Backend
Serves the Orders Backlog tab in the AntiGravity Portal.
"""

import os
import sys
import shutil
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Request, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Path setup ──────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import portal  # shared auth

# ── Database ────────────────────────────────
from database.db import init_db, SessionLocal
from database.models import (
    Client, ContractType, Company,
    Order, OrderNote, Milestone, MilestoneAudit,
    Invoice, Receipt,
)

# ── FastAPI app ─────────────────────────────
app = FastAPI(title="Orders Backlog API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the orders frontend
ORDERS_STATIC = os.path.join(BASE_DIR, "frontend", "orders-app")
os.makedirs(ORDERS_STATIC, exist_ok=True)
app.mount("/orders-app", StaticFiles(directory=ORDERS_STATIC, html=True), name="orders-app")

# Upload directories
UPLOAD_BASE = os.path.join(BASE_DIR, "Orders", "uploads")
for sub in ("contracts", "po", "invoices"):
    os.makedirs(os.path.join(UPLOAD_BASE, sub), exist_ok=True)


# ═══════════════════════════════════════════
#  Auth helpers
# ═══════════════════════════════════════════

def get_session_from_request(request: Request):
    cookie_header = request.headers.get("cookie")
    return portal.get_session(cookie_header)


def require_auth(request: Request):
    session = get_session_from_request(request)
    if not session:
        raise HTTPException(status_code=401, detail="Not logged in")
    return session


def require_admin(request: Request):
    session = require_auth(request)
    if session.get("role", "").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return session


# ═══════════════════════════════════════════
#  Portal-level routes (login/logout/tabs)
# ═══════════════════════════════════════════

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return JSONResponse(status_code=204, content=None)


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
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


@app.get("/orders", response_class=HTMLResponse)
async def get_orders_tab(request: Request):
    session = get_session_from_request(request)
    if not session:
        return RedirectResponse(url="/login")
    return portal.orders_tab_page(session)


# ═══════════════════════════════════════════
#  ADMIN — Client CRUD
# ═══════════════════════════════════════════

@app.get("/api/clients")
async def list_clients(request: Request):
    require_auth(request)
    db = SessionLocal()
    try:
        clients = db.query(Client).order_by(Client.name).all()
        return [c.to_dict() for c in clients]
    finally:
        db.close()


@app.post("/api/clients")
async def create_client(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        # Check unique client_id
        existing = db.query(Client).filter(Client.client_id == data.get("client_id", "")).first()
        if existing:
            return JSONResponse(status_code=400, content={"error": "Client ID already exists"})
        client = Client(
            client_id=data.get("client_id", "")[:8],
            name=data.get("name", ""),
            billing_address=data.get("billing_address", ""),
            contact_names=data.get("contact_names", ""),
            billing_name=data.get("billing_name", ""),
            billing_email=data.get("billing_email", ""),
        )
        db.add(client)
        db.commit()
        db.refresh(client)
        return client.to_dict()
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.put("/api/clients/{client_db_id}")
async def update_client(client_db_id: int, request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        client = db.query(Client).filter(Client.id == client_db_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        for field in ("client_id", "name", "billing_address", "contact_names", "billing_name", "billing_email"):
            if field in data:
                val = data[field]
                if field == "client_id":
                    val = val[:8]
                setattr(client, field, val)
        db.commit()
        db.refresh(client)
        return client.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.delete("/api/clients/{client_db_id}")
async def delete_client(client_db_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        client = db.query(Client).filter(Client.id == client_db_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        db.delete(client)
        db.commit()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


# ═══════════════════════════════════════════
#  ADMIN — Contract Type CRUD
# ═══════════════════════════════════════════

@app.get("/api/contract-types")
async def list_contract_types(request: Request):
    require_auth(request)
    db = SessionLocal()
    try:
        types = db.query(ContractType).order_by(ContractType.category, ContractType.subcategory).all()
        return [t.to_dict() for t in types]
    finally:
        db.close()


@app.post("/api/contract-types")
async def create_contract_type(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        ct = ContractType(
            category=data.get("category", ""),
            subcategory=data.get("subcategory", ""),
        )
        db.add(ct)
        db.commit()
        db.refresh(ct)
        return ct.to_dict()
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.put("/api/contract-types/{type_id}")
async def update_contract_type(type_id: int, request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        ct = db.query(ContractType).filter(ContractType.id == type_id).first()
        if not ct:
            raise HTTPException(status_code=404, detail="Contract type not found")
        if "category" in data:
            ct.category = data["category"]
        if "subcategory" in data:
            ct.subcategory = data["subcategory"]
        db.commit()
        db.refresh(ct)
        return ct.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.delete("/api/contract-types/{type_id}")
async def delete_contract_type(type_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        ct = db.query(ContractType).filter(ContractType.id == type_id).first()
        if not ct:
            raise HTTPException(status_code=404, detail="Contract type not found")
        db.delete(ct)
        db.commit()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


# ═══════════════════════════════════════════
#  ADMIN — Company CRUD
# ═══════════════════════════════════════════

@app.get("/api/companies")
async def list_companies(request: Request):
    require_auth(request)
    db = SessionLocal()
    try:
        companies = db.query(Company).order_by(Company.name).all()
        return [c.to_dict() for c in companies]
    finally:
        db.close()


@app.post("/api/companies")
async def create_company(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        company = Company(name=data.get("name", ""))
        db.add(company)
        db.commit()
        db.refresh(company)
        return company.to_dict()
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.put("/api/companies/{company_id}")
async def update_company(company_id: int, request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        company = db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        if "name" in data:
            company.name = data["name"]
        db.commit()
        db.refresh(company)
        return company.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.delete("/api/companies/{company_id}")
async def delete_company(company_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        company = db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        db.delete(company)
        db.commit()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


# ═══════════════════════════════════════════
#  USER — Orders CRUD
# ═══════════════════════════════════════════

@app.get("/api/orders")
async def list_orders(request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        orders = (db.query(Order)
                  .filter(Order.is_deleted == False)
                  .order_by(Order.date_of_order.desc())
                  .all())
        return [o.to_dict() for o in orders]
    finally:
        db.close()


@app.get("/api/orders/{order_id}")
async def get_order(order_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        order = db.query(Order).filter(Order.id == order_id, Order.is_deleted == False).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        return order.to_dict(include_related=True)
    except HTTPException:
        raise
    finally:
        db.close()


@app.post("/api/orders")
async def create_order(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        order = Order(
            client_id=data["client_id"],
            order_name=data.get("order_name", "")[:12],
            contract_type_id=data["contract_type_id"],
            company_id=data["company_id"],
            date_of_order=datetime.strptime(data["date_of_order"], "%Y-%m-%d"),
            po_number=data.get("po_number", ""),
            contract_amount=Decimal(str(data.get("contract_amount", 0))),
            created_by=session["username"],
        )
        db.add(order)
        db.commit()
        db.refresh(order)
        return order.to_dict()
    except KeyError as e:
        return JSONResponse(status_code=400, content={"error": f"Missing field: {e}"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.put("/api/orders/{order_id}")
async def update_order(order_id: int, request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        for field in ("client_id", "order_name", "contract_type_id", "company_id",
                      "po_number", "contract_amount"):
            if field in data:
                val = data[field]
                if field == "order_name":
                    val = val[:12]
                if field == "contract_amount":
                    val = Decimal(str(val))
                setattr(order, field, val)
        if "date_of_order" in data:
            order.date_of_order = datetime.strptime(data["date_of_order"], "%Y-%m-%d")
        db.commit()
        db.refresh(order)
        return order.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.delete("/api/orders/{order_id}")
async def delete_order(order_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        order.is_deleted = True
        db.commit()
        return {"success": True}
    finally:
        db.close()


# ── Order Notes ─────────────────────────────

@app.post("/api/orders/{order_id}/notes")
async def add_note(order_id: int, request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        # Check note limit (max 50)
        count = db.query(OrderNote).filter(OrderNote.order_id == order_id).count()
        if count >= 50:
            return JSONResponse(status_code=400, content={"error": "Maximum 50 notes per order"})
        note = OrderNote(
            order_id=order_id,
            note_text=data.get("note_text", "")[:50],
            login_name=session["username"],
        )
        db.add(note)
        db.commit()
        db.refresh(note)
        return note.to_dict()
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


# ═══════════════════════════════════════════
#  USER — Milestones CRUD (with audit)
# ═══════════════════════════════════════════

@app.post("/api/milestones")
async def create_milestone(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        # Check milestone limit (max 15 per order)
        count = db.query(Milestone).filter(Milestone.order_id == data["order_id"]).count()
        if count >= 15:
            return JSONResponse(status_code=400, content={"error": "Maximum 15 milestones per order"})
        ms = Milestone(
            order_id=data["order_id"],
            milestone_name=data.get("milestone_name", "")[:12],
            scheduled_date=datetime.strptime(data["scheduled_date"], "%Y-%m-%d"),
            payment_amount=Decimal(str(data.get("payment_amount", 0))),
            milestone_type=data.get("milestone_type", "Estimate"),
            description=data.get("description", "")[:50],
            modified_by=session["username"],
        )
        db.add(ms)
        db.commit()
        db.refresh(ms)
        return ms.to_dict()
    except KeyError as e:
        return JSONResponse(status_code=400, content={"error": f"Missing field: {e}"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.put("/api/milestones/{milestone_id}")
async def update_milestone(milestone_id: int, request: Request):
    session = require_auth(request)
    data = await request.json()
    change_reason = data.pop("change_reason", "")
    db = SessionLocal()
    try:
        ms = db.query(Milestone).filter(Milestone.id == milestone_id).first()
        if not ms:
            raise HTTPException(status_code=404, detail="Milestone not found")

        # Track changes in audit log
        trackable = {
            "milestone_name": "milestone_name",
            "scheduled_date": "scheduled_date",
            "payment_amount": "payment_amount",
            "milestone_type": "milestone_type",
            "rescheduled_date": "rescheduled_date",
            "rescheduling_reason": "rescheduling_reason",
            "description": "description",
        }
        for field, attr in trackable.items():
            if field not in data:
                continue
            old_val = str(getattr(ms, attr, ""))
            new_val = str(data[field])
            if old_val != new_val:
                audit = MilestoneAudit(
                    milestone_id=milestone_id,
                    field_changed=field,
                    old_value=old_val,
                    new_value=new_val,
                    change_reason=change_reason,
                    changed_by=session["username"],
                )
                db.add(audit)

        # Apply updates
        for field in ("milestone_name", "milestone_type", "rescheduling_reason", "description"):
            if field in data:
                val = data[field]
                if field == "milestone_name":
                    val = val[:12]
                if field == "description":
                    val = val[:50]
                setattr(ms, field, val)
        if "payment_amount" in data:
            ms.payment_amount = Decimal(str(data["payment_amount"]))
        if "scheduled_date" in data and data["scheduled_date"]:
            ms.scheduled_date = datetime.strptime(data["scheduled_date"], "%Y-%m-%d")
        if "rescheduled_date" in data and data["rescheduled_date"]:
            ms.rescheduled_date = datetime.strptime(data["rescheduled_date"], "%Y-%m-%d")
        ms.modified_by = session["username"]

        db.commit()
        db.refresh(ms)
        return ms.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.delete("/api/milestones/{milestone_id}")
async def delete_milestone(milestone_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        ms = db.query(Milestone).filter(Milestone.id == milestone_id).first()
        if not ms:
            raise HTTPException(status_code=404, detail="Milestone not found")
        if ms.is_billed:
            return JSONResponse(status_code=400, content={"error": "Cannot delete a billed milestone"})
        db.delete(ms)
        db.commit()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.get("/api/milestones/{milestone_id}/audit")
async def get_milestone_audit(milestone_id: int, request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        audits = (db.query(MilestoneAudit)
                  .filter(MilestoneAudit.milestone_id == milestone_id)
                  .order_by(MilestoneAudit.changed_date.desc())
                  .all())
        return [a.to_dict() for a in audits]
    finally:
        db.close()


# ═══════════════════════════════════════════
#  USER — Invoices
# ═══════════════════════════════════════════

@app.get("/api/invoices")
async def list_invoices(request: Request, order_id: Optional[int] = None):
    session = require_auth(request)
    db = SessionLocal()
    try:
        q = db.query(Invoice)
        if order_id:
            q = q.filter(Invoice.order_id == order_id)
        invoices = q.order_by(Invoice.invoice_date.desc()).all()
        return [inv.to_dict() for inv in invoices]
    finally:
        db.close()


@app.post("/api/invoices")
async def create_invoice(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        ms = db.query(Milestone).filter(Milestone.id == data["milestone_id"]).first()
        if not ms:
            raise HTTPException(status_code=404, detail="Milestone not found")

        inv = Invoice(
            order_id=ms.order_id,
            milestone_id=data["milestone_id"],
            invoice_number=data["invoice_number"],
            invoice_date=datetime.strptime(data["invoice_date"], "%Y-%m-%d"),
            invoice_amount=Decimal(str(data.get("invoice_amount", ms.payment_amount))),
            created_by=session["username"],
        )
        db.add(inv)
        ms.is_billed = True
        db.commit()
        db.refresh(inv)
        return inv.to_dict()
    except KeyError as e:
        return JSONResponse(status_code=400, content={"error": f"Missing field: {e}"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


# ═══════════════════════════════════════════
#  USER — Receipts
# ═══════════════════════════════════════════

@app.post("/api/receipts")
async def create_receipt(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        inv = db.query(Invoice).filter(Invoice.id == data["invoice_id"]).first()
        if not inv:
            raise HTTPException(status_code=404, detail="Invoice not found")
        amount = Decimal(str(data.get("receipt_amount", 0)))
        diff = amount - inv.invoice_amount
        receipt = Receipt(
            invoice_id=data["invoice_id"],
            receipt_date=datetime.strptime(data["receipt_date"], "%Y-%m-%d"),
            receipt_amount=amount,
            difference=diff,
            receipt_notes=data.get("receipt_notes", ""),
            created_by=session["username"],
        )
        db.add(receipt)
        db.commit()
        db.refresh(receipt)
        return receipt.to_dict()
    except KeyError as e:
        return JSONResponse(status_code=400, content={"error": f"Missing field: {e}"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


# ═══════════════════════════════════════════
#  File Upload/Download
# ═══════════════════════════════════════════

@app.post("/api/upload/{file_type}/{ref_id}")
async def upload_file(file_type: str, ref_id: int, request: Request, file: UploadFile = File(...)):
    session = require_auth(request)
    if file_type not in ("contracts", "po", "invoices"):
        return JSONResponse(status_code=400, content={"error": "Invalid file type"})
    if not file.filename.lower().endswith(".pdf"):
        return JSONResponse(status_code=400, content={"error": "Only PDF files accepted"})

    dest_dir = os.path.join(UPLOAD_BASE, file_type)
    filename = f"{ref_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}"
    save_path = os.path.join(dest_dir, filename)
    with open(save_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Update the corresponding record
    db = SessionLocal()
    try:
        if file_type == "contracts":
            order = db.query(Order).filter(Order.id == ref_id).first()
            if order:
                order.contract_pdf = filename
                db.commit()
        elif file_type == "po":
            order = db.query(Order).filter(Order.id == ref_id).first()
            if order:
                order.po_pdf = filename
                db.commit()
        elif file_type == "invoices":
            inv = db.query(Invoice).filter(Invoice.id == ref_id).first()
            if inv:
                inv.invoice_pdf = filename
                db.commit()
    finally:
        db.close()

    return {"success": True, "filename": filename}


@app.get("/api/download/{file_type}/{filename}")
async def download_file(file_type: str, filename: str, request: Request):
    session = require_auth(request)
    path = os.path.join(UPLOAD_BASE, file_type, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename)


# ═══════════════════════════════════════════
#  Reports
# ═══════════════════════════════════════════

@app.get("/api/reports/backlog")
async def backlog_report(request: Request,
                         start_month: str = Query(..., description="YYYY-MM")):
    """Total backlog: unbilled milestones from start_month for 12 months."""
    session = require_auth(request)
    db = SessionLocal()
    try:
        start = datetime.strptime(start_month + "-01", "%Y-%m-%d")
        end = start.replace(year=start.year + 1) if start.month == 1 else start
        # Advance 12 months
        for _ in range(12):
            m = end.month + 1
            y = end.year
            if m > 12:
                m = 1
                y += 1
            end = end.replace(year=y, month=m)

        milestones = (
            db.query(Milestone)
            .join(Order)
            .filter(
                Order.is_deleted == False,
                Milestone.is_billed == False,
                Milestone.scheduled_date >= start,
                Milestone.scheduled_date < end,
            )
            .order_by(Milestone.scheduled_date)
            .all()
        )

        rows = []
        for ms in milestones:
            order = ms.order
            rows.append({
                "order_name": order.order_name,
                "client_name": order.client.name if order.client else "",
                "milestone_name": ms.milestone_name,
                "scheduled_date": ms.scheduled_date.strftime("%Y-%m-%d"),
                "payment_amount": float(ms.payment_amount or 0),
                "milestone_type": ms.milestone_type,
                "description": ms.description or "",
            })
        total = sum(r["payment_amount"] for r in rows)
        return {"rows": rows, "total_backlog": total, "start": start_month, "months": 12}
    finally:
        db.close()


@app.get("/api/reports/milestone-schedule")
async def milestone_schedule(request: Request,
                             start_date: str = Query(...),
                             end_date: str = Query(...)):
    """All scheduled milestone payments between two dates."""
    session = require_auth(request)
    db = SessionLocal()
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
        milestones = (
            db.query(Milestone)
            .join(Order)
            .filter(
                Order.is_deleted == False,
                Milestone.scheduled_date >= s,
                Milestone.scheduled_date <= e,
            )
            .order_by(Milestone.scheduled_date)
            .all()
        )
        rows = []
        for ms in milestones:
            order = ms.order
            rows.append({
                "order_name": order.order_name,
                "client_name": order.client.name if order.client else "",
                "milestone_name": ms.milestone_name,
                "scheduled_date": ms.scheduled_date.strftime("%Y-%m-%d"),
                "payment_amount": float(ms.payment_amount or 0),
                "milestone_type": ms.milestone_type,
                "is_billed": ms.is_billed,
                "description": ms.description or "",
            })
        total = sum(r["payment_amount"] for r in rows)
        return {"rows": rows, "total": total}
    finally:
        db.close()


# ═══════════════════════════════════════════
#  Session info endpoint (for frontend)
# ═══════════════════════════════════════════

@app.get("/api/session")
async def get_session_info(request: Request):
    session = get_session_from_request(request)
    if not session:
        return JSONResponse(status_code=401, content={"error": "Not logged in"})
    return {
        "username": session.get("username", ""),
        "fullname": session.get("fullname", ""),
        "role": session.get("role", ""),
    }


# ═══════════════════════════════════════════
#  Startup
# ═══════════════════════════════════════════

@app.on_event("startup")
async def on_startup():
    init_db()
    print("[orders.py] Database initialized, default data seeded.")


if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("AG_HOST", "0.0.0.0")
    port = int(os.environ.get("AG_ORDERS_PORT", "8002"))
    uvicorn.run(app, host=host, port=port)

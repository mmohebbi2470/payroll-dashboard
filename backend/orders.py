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
from sqlalchemy import func
from database.db import init_db, SessionLocal
from database.models import (
    Client, ContractType, Company,
    Order, OrderNote, Milestone, MilestoneAudit,
    Invoice, Receipt, OrderCommission,
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
for sub in ("contracts", "po", "invoices", "invoice_templates"):
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


def get_allowed_company_ids(session):
    """Return list of company IDs the user can access, or None if all."""
    ca = session.get("company_access", "all")
    if ca == "all" or not ca:
        return None  # No filter needed
    try:
        return [int(x.strip()) for x in ca.split(",") if x.strip()]
    except ValueError:
        return None


# ═══════════════════════════════════════════
#  ADMIN — Client CRUD
# ═══════════════════════════════════════════

@app.get("/api/clients")
async def list_clients(request: Request):
    session = require_auth(request)
    db = SessionLocal()
    try:
        allowed = get_allowed_company_ids(session)
        if allowed:
            # Only return clients who have at least one order in allowed companies
            client_ids = db.query(Order.client_id).filter(Order.company_id.in_(allowed)).distinct().all()
            client_ids = [cid[0] for cid in client_ids]
            clients = db.query(Client).filter(Client.id.in_(client_ids)).order_by(Client.name).all() if client_ids else []
        else:
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
            client_business=data.get("client_business", ""),
            address=data.get("address", ""),
            contact_names=data.get("contact_names", ""),
            contact_email=data.get("contact_email", ""),
            phone=data.get("phone", ""),
            general_info=data.get("general_info", "")[:80],
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
        for field in ("client_id", "name", "client_business", "address", "contact_names", "contact_email", "phone", "general_info"):
            if field in data:
                val = data[field]
                if field == "client_id":
                    val = val[:8]
                if field == "general_info":
                    val = val[:80]
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
        # Safety check: cannot delete client with existing orders
        order_count = db.query(Order).filter(
            Order.client_id == client_db_id, Order.is_deleted == False
        ).count()
        if order_count > 0:
            return JSONResponse(status_code=400, content={
                "error": f"Cannot delete client — has {order_count} active order(s). Remove all orders first."
            })
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
        q = db.query(Order).filter(Order.is_deleted == False)
        allowed = get_allowed_company_ids(session)
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        orders = q.order_by(Order.date_of_order.desc()).all()
        now = datetime.now()
        current_year = now.year
        year_start = datetime(current_year, 1, 1)
        year_end = datetime(current_year, 12, 31, 23, 59, 59)
        result = []
        for o in orders:
            d = o.to_dict()
            # Compute remaining (unbilled) milestones in current year for this order
            remaining = sum(
                float(m.payment_amount or 0)
                for m in o.milestones
                if not m.is_billed
                and m.scheduled_date is not None
                and year_start <= m.scheduled_date <= year_end
            )
            d["remaining_mls_year"] = remaining
            result.append(d)
        return result
    except Exception as e:
        print(f"[list_orders] Error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": f"Error loading orders: {str(e)}"})
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
            order_name=data.get("order_name", "")[:25],
            contract_type_id=data["contract_type_id"],
            company_id=data["company_id"],
            date_of_order=datetime.strptime(data["date_of_order"], "%Y-%m-%d"),
            delivery_start_date=datetime.strptime(data["delivery_start_date"], "%Y-%m-%d") if data.get("delivery_start_date") else None,
            order_end_date=datetime.strptime(data["order_end_date"], "%Y-%m-%d") if data.get("order_end_date") else None,
            po_number=data.get("po_number", ""),
            contract_amount=Decimal(str(data.get("contract_amount", 0))),
            is_estimate=data.get("is_estimate", True),
            order_description=data.get("order_description", "")[:80],
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
        # Simple string/numeric fields
        for field in ("client_id", "order_name", "contract_type_id", "company_id",
                      "po_number", "contract_amount", "is_estimate", "order_description",
                      "invoice_number_prefix", "invoice_contact_name", "invoice_mailing_addr",
                      "invoice_email", "invoice_method", "wire_ach_data"):
            if field in data:
                val = data[field]
                if field == "order_name":
                    val = val[:15]
                if field == "order_description":
                    val = val[:80]
                if field == "invoice_number_prefix":
                    val = val[:15]
                if field == "contract_amount":
                    val = Decimal(str(val))
                setattr(order, field, val)
        # Date fields
        for date_field in ("date_of_order", "delivery_start_date", "order_end_date"):
            if date_field in data:
                val = data[date_field]
                if val:
                    setattr(order, date_field, datetime.strptime(val, "%Y-%m-%d"))
                else:
                    setattr(order, date_field, None)
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
        # Safety check: cannot delete order with milestones that have amounts
        ms_with_amount = db.query(Milestone).filter(
            Milestone.order_id == order_id, Milestone.payment_amount > 0
        ).count()
        if ms_with_amount > 0:
            return JSONResponse(status_code=400, content={
                "error": f"Cannot delete order — has {ms_with_amount} milestone(s) with payment amounts. Remove milestones first."
            })
        # Safety check: cannot delete order with invoices
        invoice_count = db.query(Invoice).filter(Invoice.order_id == order_id).count()
        if invoice_count > 0:
            return JSONResponse(status_code=400, content={
                "error": f"Cannot delete order — has {invoice_count} invoice(s). Remove invoices first."
            })
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
#  ORDER COMMISSIONS
# ═══════════════════════════════════════════

@app.get("/api/orders/{order_id}/commissions")
async def get_order_commissions(order_id: int, request: Request):
    """Get commission structure for an order."""
    session = require_auth(request)
    db = SessionLocal()
    try:
        comms = db.query(OrderCommission).filter(
            OrderCommission.order_id == order_id
        ).order_by(OrderCommission.commission_type, OrderCommission.slot_number).all()
        return [c.to_dict() for c in comms]
    finally:
        db.close()


@app.put("/api/orders/{order_id}/commissions")
async def save_order_commissions(order_id: int, request: Request):
    """Save/replace the entire commission structure for an order (Admin only)."""
    session = require_auth(request)
    if session.get("role") != "Admin":
        return JSONResponse(status_code=403, content={"error": "Admin access required"})
    data = await request.json()
    entries = data.get("entries", [])
    db = SessionLocal()
    try:
        # Delete existing commissions for this order
        db.query(OrderCommission).filter(OrderCommission.order_id == order_id).delete()
        # Insert new ones
        for entry in entries:
            name = (entry.get("person_name") or "").strip()
            pct = float(entry.get("commission_pct") or 0)
            if not name and pct == 0:
                continue  # skip empty rows
            comm = OrderCommission(
                order_id=order_id,
                commission_type=entry.get("commission_type", "Sales"),
                person_name=name,
                commission_pct=pct,
                slot_number=int(entry.get("slot_number", 1)),
            )
            db.add(comm)
        db.commit()
        return {"status": "ok"}
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.get("/api/reports/commission")
async def commission_report(request: Request, year: int = 0, quarter: int = 0,
                            include_zero: bool = False, include_no_payment: bool = False):
    """
    Quarterly commission report.
    Takes all posted receipts in a calendar quarter and calculates each
    individual's commission based on the order's commission structure.
    If include_zero=True, also include receipts for orders with no commission setup.
    """
    session = require_auth(request)
    if not year or not quarter:
        return JSONResponse(status_code=400, content={"error": "year and quarter parameters required"})
    # Quarter date ranges
    q_start_month = (quarter - 1) * 3 + 1
    q_start = datetime(year, q_start_month, 1)
    if quarter == 4:
        q_end = datetime(year + 1, 1, 1)
    else:
        q_end = datetime(year, q_start_month + 3, 1)

    allowed = get_allowed_company_ids(session)
    db = SessionLocal()
    try:
        # Get all receipts in the quarter, joined to invoice → order
        query = (
            db.query(Receipt, Invoice, Order)
            .join(Invoice, Receipt.invoice_id == Invoice.id)
            .join(Order, Invoice.order_id == Order.id)
            .filter(Receipt.receipt_date >= q_start, Receipt.receipt_date < q_end)
            .filter(Order.is_deleted == False)
        )
        if allowed:
            query = query.filter(Order.company_id.in_(allowed))
        rows = query.all()

        # Get all commissions for the involved orders
        order_ids = list(set(r[2].id for r in rows))
        comms = db.query(OrderCommission).filter(
            OrderCommission.order_id.in_(order_ids) if order_ids else False
        ).all()
        # Build commission lookup: order_id -> list of commission entries
        comm_map = {}
        for c in comms:
            comm_map.setdefault(c.order_id, []).append(c)

        # Calculate commissions per person
        person_totals = {}  # person_name -> {type, total_commission, details: []}
        detail_rows = []
        for receipt, invoice, order in rows:
            order_comms = comm_map.get(order.id, [])
            if order_comms:
                for c in order_comms:
                    comm_amount = float(receipt.receipt_amount or 0) * float(c.commission_pct or 0) / 100
                    key = (c.person_name, c.commission_type)
                    if key not in person_totals:
                        person_totals[key] = {"person_name": c.person_name, "commission_type": c.commission_type,
                                              "total_commission": 0, "total_receipts": 0, "pct": float(c.commission_pct)}
                    person_totals[key]["total_commission"] += comm_amount
                    person_totals[key]["total_receipts"] += float(receipt.receipt_amount or 0)
                    detail_rows.append({
                        "person_name": c.person_name,
                        "commission_type": c.commission_type,
                        "commission_pct": float(c.commission_pct),
                        "order_name": order.order_name,
                        "client_name": order.client.name if order.client else "",
                        "invoice_number": invoice.invoice_number,
                        "receipt_date": receipt.receipt_date.strftime("%Y-%m-%d") if receipt.receipt_date else "",
                        "receipt_amount": float(receipt.receipt_amount or 0),
                        "commission_amount": round(comm_amount, 2),
                    })
            elif include_zero:
                # Order has no commission setup — include receipt with zero commission
                detail_rows.append({
                    "person_name": "(No Commission)",
                    "commission_type": "—",
                    "commission_pct": 0,
                    "order_name": order.order_name,
                    "client_name": order.client.name if order.client else "",
                    "invoice_number": invoice.invoice_number,
                    "receipt_date": receipt.receipt_date.strftime("%Y-%m-%d") if receipt.receipt_date else "",
                    "receipt_amount": float(receipt.receipt_amount or 0),
                    "commission_amount": 0,
                })
        if include_no_payment:
            # Query all commissions for active orders
            all_comms_query = db.query(OrderCommission, Order).join(Order).filter(Order.is_deleted == False)
            if allowed:
                all_comms_query = all_comms_query.filter(Order.company_id.in_(allowed))
            all_comms = all_comms_query.all()
            
            # Find which order commissions were already processed
            processed_comm_ids = set()
            for receipt, invoice, order in rows:
                for c in comm_map.get(order.id, []):
                    processed_comm_ids.add(c.id)

            for c, order in all_comms:
                if c.id not in processed_comm_ids:
                    # Add zero payment line
                    detail_rows.append({
                        "person_name": c.person_name,
                        "commission_type": c.commission_type,
                        "commission_pct": float(c.commission_pct),
                        "order_name": order.order_name,
                        "client_name": order.client.name if order.client else "",
                        "invoice_number": "—",
                        "receipt_date": "—",
                        "receipt_amount": 0.0,
                        "commission_amount": 0.0,
                    })
                    # Add to summary if not already there
                    key = (c.person_name, c.commission_type)
                    if key not in person_totals:
                        person_totals[key] = {"person_name": c.person_name, "commission_type": c.commission_type,
                                              "total_commission": 0, "total_receipts": 0, "pct": float(c.commission_pct)}
        
        summary = sorted(person_totals.values(), key=lambda x: (x["commission_type"], x["person_name"]))
        for s in summary:
            s["total_commission"] = round(s["total_commission"], 2)
            s["total_receipts"] = round(s["total_receipts"], 2)

        # Sort details again so the zero-payment lines are grouped with the correct person/client
        detail_rows = sorted(detail_rows, key=lambda x: (x.get("person_name", ""), x.get("client_name", ""), x.get("order_name", "")))

        return {
            "year": year,
            "quarter": quarter,
            "summary": summary,
            "details": detail_rows,
            "total_receipts": round(sum(float(r[0].receipt_amount or 0) for r in rows), 2),
            "total_commissions": round(sum(s["total_commission"] for s in summary), 2),
        }
    finally:
        db.close()


# ═══════════════════════════════════════════
#  USER — Milestones CRUD (with audit)
# ═══════════════════════════════════════════

@app.post("/api/milestones/batch")
async def create_milestones_batch(request: Request):
    """Create multiple milestones at once (for periodic milestones)."""
    session = require_auth(request)
    data = await request.json()
    order_id = data.get("order_id")
    milestones_data = data.get("milestones", [])
    if not order_id or not milestones_data:
        return JSONResponse(status_code=400, content={"error": "order_id and milestones are required"})
    db = SessionLocal()
    try:
        existing_count = db.query(Milestone).filter(Milestone.order_id == order_id).count()
        total_after = existing_count + len(milestones_data)
        if total_after > 60:
            return JSONResponse(status_code=400, content={
                "error": f"Would exceed maximum of 60 milestones per order ({existing_count} existing + {len(milestones_data)} new = {total_after})"
            })
        created = []
        for md in milestones_data:
            ms = Milestone(
                order_id=order_id,
                milestone_name=md.get("milestone_name", "")[:25],
                scheduled_date=datetime.strptime(md["scheduled_date"], "%Y-%m-%d"),
                payment_amount=Decimal(str(md.get("payment_amount", 0))),
                milestone_type=md.get("milestone_type", "Estimate"),
                description=md.get("description", "")[:50],
                modified_by=session["username"],
            )
            db.add(ms)
            created.append(ms)
        db.commit()
        return {"status": "ok", "count": len(created)}
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.post("/api/milestones")
async def create_milestone(request: Request):
    session = require_auth(request)
    data = await request.json()
    db = SessionLocal()
    try:
        # Check milestone limit (max 60 per order)
        count = db.query(Milestone).filter(Milestone.order_id == data["order_id"]).count()
        if count >= 60:
            return JSONResponse(status_code=400, content={"error": "Maximum 60 milestones per order"})
        ms = Milestone(
            order_id=data["order_id"],
            milestone_name=data.get("milestone_name", "")[:25],
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
                    val = val[:25]
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

@app.get("/api/milestones-ready-to-invoice")
async def milestones_ready_to_invoice(request: Request):
    """Return unbilled milestones that are past due, due this week, or due next week."""
    session = require_auth(request)
    db = SessionLocal()
    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # End of next week (Sunday)
        days_until_next_sunday = 13 - today.weekday()  # generous: ~2 weeks out
        cutoff = today + timedelta(days=days_until_next_sunday)

        q = (db.query(Milestone)
            .join(Order, Order.id == Milestone.order_id)
            .join(Client, Client.id == Order.client_id)
            .filter(Milestone.is_billed == False)
            .filter(Milestone.scheduled_date <= cutoff))
        allowed = get_allowed_company_ids(session)
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        milestones = q.order_by(Milestone.scheduled_date.asc()).all()
        results = []
        for ms in milestones:
            order = ms.order
            client = order.client if order else None
            label = "Past Due" if ms.scheduled_date.replace(hour=0, minute=0, second=0, microsecond=0) < today else (
                "Due This Week" if ms.scheduled_date.replace(hour=0, minute=0, second=0, microsecond=0) <= today + timedelta(days=(6 - today.weekday())) else "Due Next Week"
            )
            results.append({
                "milestone_id": ms.id,
                "milestone_name": ms.milestone_name,
                "payment_amount": float(ms.payment_amount) if ms.payment_amount else 0,
                "milestone_type": ms.milestone_type,
                "scheduled_date": ms.scheduled_date.strftime("%Y-%m-%d") if ms.scheduled_date else "",
                "order_id": order.id if order else None,
                "order_name": order.order_name if order else "",
                "client_name": client.name if client else "",
                "client_id": client.id if client else None,
                "due_label": label,
            })
        return results
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()


@app.get("/api/invoices")
async def list_invoices(request: Request, order_id: Optional[int] = None):
    session = require_auth(request)
    db = SessionLocal()
    try:
        q = db.query(Invoice).join(Milestone, Milestone.id == Invoice.milestone_id).join(Order, Order.id == Invoice.order_id).join(Client, Client.id == Order.client_id)
        if order_id:
            q = q.filter(Invoice.order_id == order_id)
        allowed = get_allowed_company_ids(session)
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        invoices = q.order_by(Invoice.invoice_date.desc()).all()
        results = []
        for inv in invoices:
            d = inv.to_dict()
            order = db.query(Order).filter(Order.id == inv.order_id).first()
            d["order_name"] = order.order_name if order else ""
            d["client_name"] = order.client.name if order and order.client else ""
            results.append(d)
        return results
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
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

        inv_date = datetime.strptime(data["invoice_date"], "%Y-%m-%d")
        due_date_str = data.get("payment_due_date")
        due_date = datetime.strptime(due_date_str, "%Y-%m-%d") if due_date_str else inv_date + timedelta(days=10)
        inv = Invoice(
            order_id=ms.order_id,
            milestone_id=data["milestone_id"],
            invoice_number=data["invoice_number"],
            invoice_date=inv_date,
            payment_due_date=due_date,
            invoice_amount=Decimal(str(data.get("invoice_amount", ms.payment_amount))),
            invoiced_by=data.get("invoiced_by", session.get("fullname", session["username"])),
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
    if file_type not in ("contracts", "po", "invoices", "invoice_templates"):
        return JSONResponse(status_code=400, content={"error": "Invalid file type"})
    allowed_ext = (".pdf", ".doc", ".docx")
    if file_type == "invoice_templates":
        if not any(file.filename.lower().endswith(ext) for ext in allowed_ext):
            return JSONResponse(status_code=400, content={"error": "Only PDF, DOC, or DOCX files accepted for templates"})
    else:
        if not file.filename.lower().endswith(".pdf"):
            return JSONResponse(status_code=400, content={"error": "Only PDF files accepted"})

    # File size limit: 50MB for PO (up to 80 pages), 10MB for others
    max_size = 50 * 1024 * 1024 if file_type == "po" else 10 * 1024 * 1024
    contents = await file.read()
    if len(contents) > max_size:
        max_mb = max_size // (1024 * 1024)
        return JSONResponse(status_code=400, content={"error": f"File too large. Maximum size is {max_mb}MB"})

    dest_dir = os.path.join(UPLOAD_BASE, file_type)
    filename = f"{ref_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}"
    save_path = os.path.join(dest_dir, filename)
    with open(save_path, "wb") as f:
        f.write(contents)

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
        elif file_type == "invoice_templates":
            order = db.query(Order).filter(Order.id == ref_id).first()
            if order:
                order.invoice_template_file = filename
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

        q = (db.query(Milestone)
            .join(Order)
            .filter(
                Order.is_deleted == False,
                Milestone.is_billed == False,
                Milestone.scheduled_date >= start,
                Milestone.scheduled_date < end,
            ))
        allowed = get_allowed_company_ids(session)
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        milestones = q.order_by(Milestone.scheduled_date).all()

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
        q = (db.query(Milestone)
            .join(Order)
            .filter(
                Order.is_deleted == False,
                Milestone.scheduled_date >= s,
                Milestone.scheduled_date <= e,
            ))
        allowed = get_allowed_company_ids(session)
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        milestones = q.order_by(Milestone.scheduled_date).all()
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
#  New Booking by Month Report
# ═══════════════════════════════════════════

@app.get("/api/reports/new-bookings-by-month")
async def new_bookings_by_month(request: Request, year: int = Query(...)):
    """New orders booked per month, grouped by client."""
    session = require_auth(request)
    db = SessionLocal()
    try:
        allowed = get_allowed_company_ids(session)
        q = db.query(Order).filter(
            Order.is_deleted == False,
            Order.date_of_order >= datetime(year, 1, 1),
            Order.date_of_order < datetime(year + 1, 1, 1),
        )
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        orders = q.order_by(Order.date_of_order).all()

        # Group by client
        client_map = {}  # client_name -> { month -> [orders] }
        for o in orders:
            cname = o.client.name if o.client else "Unknown"
            if cname not in client_map:
                client_map[cname] = {}
            m = o.date_of_order.month
            if m not in client_map[cname]:
                client_map[cname][m] = []
            client_map[cname][m].append({
                "order_name": o.order_name,
                "contract_amount": float(o.contract_amount or 0),
            })

        # Build rows: one per client with sub-rows for orders
        rows = []
        month_totals = {m: 0 for m in range(1, 13)}
        grand_total = 0
        for cname in sorted(client_map.keys()):
            months = client_map[cname]
            # Collect all orders under this client
            client_orders = []
            client_month_totals = {m: 0 for m in range(1, 13)}
            for m, olist in months.items():
                for o in olist:
                    client_orders.append({
                        "month": m,
                        "order_name": o["order_name"],
                        "amount": o["contract_amount"],
                    })
                    client_month_totals[m] += o["contract_amount"]
                    month_totals[m] += o["contract_amount"]
                    grand_total += o["contract_amount"]
            rows.append({
                "client": cname,
                "month_totals": client_month_totals,
                "orders": client_orders,
                "row_total": sum(client_month_totals.values()),
            })
        return {
            "year": year,
            "rows": rows,
            "month_totals": month_totals,
            "grand_total": grand_total,
        }
    finally:
        db.close()


# ═══════════════════════════════════════════
#  Billing Performance Report
# ═══════════════════════════════════════════

@app.get("/api/reports/billing-performance")
async def billing_performance(request: Request, year: int = Query(...)):
    """Monthly billing performance: scheduled vs invoiced vs paid amounts and timing."""
    session = require_auth(request)
    db = SessionLocal()
    try:
        allowed = get_allowed_company_ids(session)
        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31, 23, 59, 59)

        # Get all milestones scheduled in the year
        q = (db.query(Milestone)
            .join(Order, Order.id == Milestone.order_id)
            .filter(Order.is_deleted == False,
                    Milestone.scheduled_date >= year_start,
                    Milestone.scheduled_date <= year_end))
        if allowed:
            q = q.filter(Order.company_id.in_(allowed))
        milestones = q.order_by(Milestone.scheduled_date).all()

        # Monthly summary totals
        month_data = {}
        for m_num in range(1, 13):
            month_data[m_num] = {"scheduled": 0, "invoiced": 0, "paid": 0,
                                 "total_days": 0, "count_days": 0}

        # Order-level detail: order_id -> { client, order_name, months: {m -> {sched, inv, paid, days}} }
        order_map = {}
        seen_receipts = set()  # Track receipt IDs to avoid counting duplicates

        for ms in milestones:
            o = ms.order
            sched_month = ms.scheduled_date.month
            sched_amt = float(ms.payment_amount or 0)
            month_data[sched_month]["scheduled"] += sched_amt

            # Find associated invoice
            inv = None
            if ms.invoices:
                inv = ms.invoices[0]

            inv_amt = float(inv.invoice_amount or 0) if inv else 0
            inv_date = inv.invoice_date if inv else None

            # Calculate days difference (positive = early, negative = late)
            days_diff = None
            if inv_date and ms.scheduled_date:
                try:
                    if inv_date.year < 2000 or ms.scheduled_date.year < 2000:
                        days_diff = 0
                    else:
                        days_diff = (ms.scheduled_date - inv_date).days
                except Exception:
                    days_diff = None

            if inv:
                month_data[sched_month]["invoiced"] += inv_amt
                if days_diff is not None:
                    month_data[sched_month]["total_days"] += days_diff
                    month_data[sched_month]["count_days"] += 1

            # Paid amount = sum of ALL receipts on the invoice, bucketed into the
            # same month as the milestone (sched_month).  This shows how much of
            # each month's invoiced amount has been collected, regardless of when
            # the receipt was actually posted in the database.
            paid_amt = 0
            if inv and inv.receipts:
                for rcpt in inv.receipts:
                    if rcpt.id in seen_receipts:
                        continue
                    seen_receipts.add(rcpt.id)
                    paid_amt += float(rcpt.receipt_amount or 0)
            month_data[sched_month]["paid"] += paid_amt

            # Aggregate into order-level map
            oid = o.id if o else 0
            if oid not in order_map:
                order_map[oid] = {
                    "client": o.client.name if o and o.client else "",
                    "order_name": o.order_name if o else "",
                    "months": {m: {"scheduled": 0, "invoiced": 0, "paid": 0,
                                   "total_days": 0, "count_days": 0}
                               for m in range(1, 13)},
                }
            om = order_map[oid]["months"][sched_month]
            om["scheduled"] += sched_amt
            om["invoiced"] += inv_amt
            om["paid"] += paid_amt
            if days_diff is not None:
                om["total_days"] += days_diff
                om["count_days"] += 1

        # Compute monthly summaries
        monthly_summary = {}
        for m_num in range(1, 13):
            d = month_data[m_num]
            avg_days = round(d["total_days"] / d["count_days"]) if d["count_days"] > 0 else None
            monthly_summary[m_num] = {
                "scheduled": round(d["scheduled"], 2),
                "invoiced": round(d["invoiced"], 2),
                "paid": round(d["paid"], 2),
                "avg_days": avg_days,
            }

        # Build order-level rows for the detail grid
        order_rows = []
        for oid, odata in order_map.items():
            row = {
                "client": odata["client"],
                "order_name": odata["order_name"],
                "months": {},
                "total_scheduled": 0,
                "total_invoiced": 0,
                "total_paid": 0,
            }
            for m_num in range(1, 13):
                md = odata["months"][m_num]
                avg_d = round(md["total_days"] / md["count_days"]) if md["count_days"] > 0 else None
                row["months"][m_num] = {
                    "scheduled": round(md["scheduled"], 2),
                    "invoiced": round(md["invoiced"], 2),
                    "paid": round(md["paid"], 2),
                    "avg_days": avg_d,
                }
                row["total_scheduled"] += md["scheduled"]
                row["total_invoiced"] += md["invoiced"]
                row["total_paid"] += md["paid"]
            row["total_scheduled"] = round(row["total_scheduled"], 2)
            row["total_invoiced"] = round(row["total_invoiced"], 2)
            row["total_paid"] = round(row["total_paid"], 2)
            order_rows.append(row)
        # Sort by client then order name
        order_rows.sort(key=lambda r: (r["client"].lower(), r["order_name"].lower()))

        return {
            "year": year,
            "order_rows": order_rows,
            "monthly_summary": monthly_summary,
            "total_scheduled": round(sum(d["scheduled"] for d in month_data.values()), 2),
            "total_invoiced": round(sum(d["invoiced"] for d in month_data.values()), 2),
            "total_paid": round(sum(d["paid"] for d in month_data.values()), 2),
        }
    finally:
        db.close()


# ═══════════════════════════════════════════
#  Session info endpoint (for frontend)
# ═══════════════════════════════════════════

@app.get("/api/stats")
async def get_dashboard_stats(request: Request):
    """Return 7 dashboard summary statistics."""
    session = get_session_from_request(request)
    allowed = get_allowed_company_ids(session) if session else None
    db = SessionLocal()
    try:
        now = datetime.now()
        current_year = now.year
        thirty_days_later = now + timedelta(days=30)

        # 1. Total Clients (filtered by company access)
        if allowed:
            client_ids = db.query(Order.client_id).filter(Order.company_id.in_(allowed)).distinct().all()
            client_id_list = [cid[0] for cid in client_ids]
            total_clients = db.query(Client).filter(Client.id.in_(client_id_list)).count() if client_id_list else 0
        else:
            total_clients = db.query(Client).count()

        # 2. Total Orders = ALL non-deleted orders (not just active with unbilled milestones)
        #    This ensures bookings report and stat cards use the same population.
        q_all_orders = db.query(Order.id).filter(Order.is_deleted == False)
        if allowed:
            q_all_orders = q_all_orders.filter(Order.company_id.in_(allowed))
        all_order_ids = q_all_orders.all()
        all_order_id_list = [r[0] for r in all_order_ids]
        active_orders = len(all_order_id_list)

        # 3. Total Order Value = sum of contract amounts for ALL non-deleted orders
        total_order_value = 0
        if all_order_id_list:
            val = db.query(
                func.coalesce(func.sum(Order.contract_amount), 0)
            ).filter(Order.id.in_(all_order_id_list)).scalar()
            total_order_value = float(val) if val else 0

        # 4. MLS to Invoice = unbilled milestones due within next 30 days
        q_mls = (db.query(Milestone)
            .join(Order, Order.id == Milestone.order_id)
            .filter(Order.is_deleted == False, Milestone.is_billed == False, Milestone.scheduled_date <= thirty_days_later))
        if allowed:
            q_mls = q_mls.filter(Order.company_id.in_(allowed))
        mls_to_invoice = q_mls.count()

        # 5. Backlog current year = $ value of unbilled milestones in current calendar year
        year_start = datetime(current_year, 1, 1)
        year_end = datetime(current_year, 12, 31, 23, 59, 59)
        q_backlog = (db.query(func.coalesce(func.sum(Milestone.payment_amount), 0))
            .join(Order, Order.id == Milestone.order_id)
            .filter(Order.is_deleted == False, Milestone.is_billed == False,
                    Milestone.scheduled_date >= year_start, Milestone.scheduled_date <= year_end))
        if allowed:
            q_backlog = q_backlog.filter(Order.company_id.in_(allowed))
        backlog_val = q_backlog.scalar()
        backlog_year = float(backlog_val) if backlog_val else 0

        # 6 & 7. Open Invoices count + Outstanding Balance (Accounts Receivable)
        open_invoices = 0
        outstanding_balance = 0.0
        q_inv = db.query(Invoice).join(Order, Order.id == Invoice.order_id)
        if allowed:
            q_inv = q_inv.filter(Order.company_id.in_(allowed))
        invoices = q_inv.all()
        for inv in invoices:
            total_received = sum(float(r.receipt_amount or 0) for r in inv.receipts)
            balance = float(inv.invoice_amount or 0) - total_received
            if balance > 0.001:
                open_invoices += 1
                outstanding_balance += balance

        # 8. Total Invoiced (all time) = sum of billed milestone amounts for ALL non-deleted orders
        #    Not limited to active-only, so fully-invoiced orders are included too.
        q_invoiced_all = (db.query(func.coalesce(func.sum(Milestone.payment_amount), 0))
            .join(Order, Order.id == Milestone.order_id)
            .filter(Order.is_deleted == False, Milestone.is_billed == True))
        if allowed:
            q_invoiced_all = q_invoiced_all.filter(Order.company_id.in_(allowed))
        invoiced_all_val = q_invoiced_all.scalar()
        total_invoiced = float(invoiced_all_val) if invoiced_all_val else 0

        # 9. Backlog after current year = derived so the math always balances:
        #    Bklg after year = Total Order Value - Total Invoiced - Backlog current year
        #    (total_order_value already includes ALL non-deleted orders from step 3)
        backlog_after_year = round(float(total_order_value) - total_invoiced - backlog_year, 2)
        if backlog_after_year < 0:
            backlog_after_year = 0.0

        return {
            "total_clients": total_clients,
            "active_orders": active_orders,
            "total_order_value": float(total_order_value),
            "mls_to_invoice": mls_to_invoice,
            "backlog_year": backlog_year,
            "backlog_year_label": str(current_year),
            "open_invoices": open_invoices,
            "outstanding_balance": round(outstanding_balance, 2),
            "total_invoiced": total_invoiced,
            "backlog_after_year": backlog_after_year,
        }
    except Exception as e:
        print(f"[stats] Error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": f"Stats error: {str(e)}"})
    finally:
        db.close()


@app.get("/api/stats/drilldown")
async def stats_drilldown(request: Request, type: str = Query(...)):
    """Return detail rows behind each stat-card so users can verify the numbers."""
    session = get_session_from_request(request)
    allowed = get_allowed_company_ids(session) if session else None
    db = SessionLocal()
    try:
        now = datetime.now()
        current_year = now.year
        year_start = datetime(current_year, 1, 1)
        year_end = datetime(current_year, 12, 31, 23, 59, 59)
        thirty_days_later = now + timedelta(days=30)

        # Helper: all non-deleted order ids (includes fully-invoiced orders)
        q_all = db.query(Order.id).filter(Order.is_deleted == False)
        if allowed:
            q_all = q_all.filter(Order.company_id.in_(allowed))
        active_ids = [r[0] for r in q_all.all()]

        if type == "clients_orders":
            # List every order with client info
            rows = []
            if active_ids:
                orders = db.query(Order).filter(Order.id.in_(active_ids)).order_by(Order.date_of_order.desc()).all()
                for o in orders:
                    rows.append({
                        "Client": o.client.name if o.client else "",
                        "Order Name": o.order_name,
                        "Category": (f"{o.contract_type.category} - {o.contract_type.subcategory}" if o.contract_type else ""),
                        "Company": o.company.name if o.company else "",
                        "Date": o.date_of_order.strftime("%Y-%m-%d") if o.date_of_order else "",
                        "Contract Amount": float(o.contract_amount or 0),
                    })
            total = sum(r["Contract Amount"] for r in rows)
            return {"title": f"Clients & Active Orders ({len(rows)})", "rows": rows, "total": total, "total_label": "Total Contract Amount"}

        elif type == "orders_value":
            rows = []
            if active_ids:
                orders = db.query(Order).filter(Order.id.in_(active_ids)).order_by(Order.date_of_order.desc()).all()
                for o in orders:
                    ms_total = sum(float(m.payment_amount or 0) for m in o.milestones)
                    billed = sum(float(m.payment_amount or 0) for m in o.milestones if m.is_billed)
                    rows.append({
                        "Client": o.client.name if o.client else "",
                        "Order Name": o.order_name,
                        "Company": o.company.name if o.company else "",
                        "Contract Amount": float(o.contract_amount or 0),
                        "Milestones Total": ms_total,
                        "Billed": billed,
                        "Unbilled": ms_total - billed,
                    })
            total = sum(r["Contract Amount"] for r in rows)
            return {"title": f"Active Orders Value Detail ({len(rows)} orders)", "rows": rows, "total": total, "total_label": "Total Contract Amount"}

        elif type == "mls_to_invoice":
            q = (db.query(Milestone)
                .join(Order, Order.id == Milestone.order_id)
                .filter(Order.is_deleted == False, Milestone.is_billed == False,
                        Milestone.scheduled_date <= thirty_days_later))
            if allowed:
                q = q.filter(Order.company_id.in_(allowed))
            milestones = q.order_by(Milestone.scheduled_date).all()
            rows = []
            for m in milestones:
                o = m.order
                rows.append({
                    "Client": o.client.name if o and o.client else "",
                    "Order Name": o.order_name if o else "",
                    "Milestone": m.milestone_name,
                    "Scheduled Date": m.scheduled_date.strftime("%Y-%m-%d") if m.scheduled_date else "",
                    "Amount": float(m.payment_amount or 0),
                    "Type": m.milestone_type or "",
                })
            total = sum(r["Amount"] for r in rows)
            return {"title": f"Milestones to Invoice — Next 30 Days ({len(rows)})", "rows": rows, "total": total, "total_label": "Total Amount"}

        elif type == "total_invoiced":
            # All billed milestones joined with their invoices
            q = (db.query(Milestone, Invoice)
                .join(Order, Order.id == Milestone.order_id)
                .outerjoin(Invoice, Invoice.milestone_id == Milestone.id)
                .filter(Order.is_deleted == False, Milestone.is_billed == True))
            if allowed:
                q = q.filter(Order.company_id.in_(allowed))
            if active_ids:
                q = q.filter(Order.id.in_(active_ids))
            results = q.order_by(Milestone.scheduled_date).all()
            rows = []
            for m, inv in results:
                o = m.order
                sched_date = m.scheduled_date
                inv_date = inv.invoice_date if inv else None
                # Calculate days between scheduled and invoiced
                days_to_invoice = None
                if sched_date and inv_date:
                    try:
                        days_to_invoice = (inv_date - sched_date).days
                    except Exception:
                        days_to_invoice = None
                rows.append({
                    "Client": o.client.name if o and o.client else "",
                    "Order Name": o.order_name if o else "",
                    "Milestone": m.milestone_name,
                    "Scheduled Date": sched_date.strftime("%Y-%m-%d") if sched_date else "",
                    "Invoiced Date": inv_date.strftime("%Y-%m-%d") if inv_date else "",
                    "Days to Invoice": days_to_invoice if days_to_invoice is not None else "",
                    "Scheduled Amount": float(m.payment_amount or 0),
                    "Invoiced Amount": float(inv.invoice_amount or 0) if inv else 0,
                    "Type": m.milestone_type or "",
                })
            total_scheduled = sum(r["Scheduled Amount"] for r in rows)
            total_invoiced = sum(r["Invoiced Amount"] for r in rows)
            return {
                "title": f"Total Invoiced — All Billed Milestones ({len(rows)})",
                "rows": rows,
                "total": total_invoiced,
                "total_label": "Total Invoiced",
                "total_scheduled": total_scheduled
            }

        elif type == "backlog_year":
            q = (db.query(Milestone)
                .join(Order, Order.id == Milestone.order_id)
                .filter(Order.is_deleted == False, Milestone.is_billed == False,
                        Milestone.scheduled_date >= year_start, Milestone.scheduled_date <= year_end))
            if allowed:
                q = q.filter(Order.company_id.in_(allowed))
            milestones = q.order_by(Milestone.scheduled_date).all()
            rows = []
            for m in milestones:
                o = m.order
                rows.append({
                    "Client": o.client.name if o and o.client else "",
                    "Order Name": o.order_name if o else "",
                    "Milestone": m.milestone_name,
                    "Scheduled Date": m.scheduled_date.strftime("%Y-%m-%d") if m.scheduled_date else "",
                    "Amount": float(m.payment_amount or 0),
                    "Type": m.milestone_type or "",
                })
            total = sum(r["Amount"] for r in rows)
            return {"title": f"Backlog {current_year} — Unbilled Milestones ({len(rows)})", "rows": rows, "total": total, "total_label": "Total Backlog"}

        elif type == "backlog_after":
            # Show what makes up the "after year" number: derived = orders value - invoiced - backlog year
            # Show per-order breakdown
            rows = []
            if active_ids:
                orders = db.query(Order).filter(Order.id.in_(active_ids)).order_by(Order.date_of_order.desc()).all()
                for o in orders:
                    contract = float(o.contract_amount or 0)
                    billed = sum(float(m.payment_amount or 0) for m in o.milestones if m.is_billed)
                    unbilled_year = sum(
                        float(m.payment_amount or 0) for m in o.milestones
                        if not m.is_billed and m.scheduled_date and year_start <= m.scheduled_date <= year_end
                    )
                    remaining = contract - billed - unbilled_year
                    if abs(remaining) > 0.01:
                        rows.append({
                            "Client": o.client.name if o.client else "",
                            "Order Name": o.order_name,
                            "Contract Amount": contract,
                            "Billed": billed,
                            f"Backlog {current_year}": unbilled_year,
                            f"After {current_year}": round(remaining, 2),
                        })
            total = sum(r[f"After {current_year}"] for r in rows)
            return {"title": f"Backlog After {current_year} — Per Order ({len(rows)} orders)", "rows": rows, "total": round(total, 2), "total_label": f"Total After {current_year}"}

        elif type == "open_invoices":
            q = (db.query(Invoice, Order)
                .join(Order, Order.id == Invoice.order_id)
                .filter(Order.is_deleted == False))
            if allowed:
                q = q.filter(Order.company_id.in_(allowed))
            results = q.order_by(Invoice.invoice_date.desc()).all()
            rows = []
            for inv, o in results:
                total_received = sum(float(r.receipt_amount or 0) for r in inv.receipts)
                balance = float(inv.invoice_amount or 0) - total_received
                if balance > 0.001:
                    rows.append({
                        "Invoice #": inv.invoice_number,
                        "Client": o.client.name if o.client else "",
                        "Order Name": o.order_name,
                        "Invoice Date": inv.invoice_date.strftime("%Y-%m-%d") if inv.invoice_date else "",
                        "Due Date": inv.payment_due_date.strftime("%Y-%m-%d") if inv.payment_due_date else "",
                        "Invoice Amount": float(inv.invoice_amount or 0),
                        "Received": total_received,
                        "Balance": round(balance, 2),
                    })
            total = sum(r["Balance"] for r in rows)
            return {"title": f"Open Invoices ({len(rows)})", "rows": rows, "total": round(total, 2), "total_label": "Total Outstanding"}

        elif type == "accounts_receivable":
            # A/R with aging buckets: 30, 60, 90, 90+ days from invoice date
            q = (db.query(Invoice, Order)
                .join(Order, Order.id == Invoice.order_id)
                .filter(Order.is_deleted == False))
            if allowed:
                q = q.filter(Order.company_id.in_(allowed))
            results = q.order_by(Invoice.invoice_date).all()
            rows = []
            aging_totals = {"0-30 Days": 0, "31-60 Days": 0, "61-90 Days": 0, "90+ Days": 0}
            for inv, o in results:
                total_received = sum(float(r.receipt_amount or 0) for r in inv.receipts)
                balance = float(inv.invoice_amount or 0) - total_received
                if balance > 0.001:
                    inv_date = inv.invoice_date if inv.invoice_date else now
                    # Guard against bad dates (e.g. year 0031 instead of 2031)
                    if inv_date.year < 2000:
                        days_since = 0
                    else:
                        days_since = max((now - inv_date).days, 0)
                    # Compute aging buckets — balance goes into whichever bucket applies
                    b_30 = 0.0
                    b_60 = 0.0
                    b_90 = 0.0
                    b_over = 0.0
                    if days_since <= 30:
                        b_30 = balance
                    elif days_since <= 60:
                        b_60 = balance
                    elif days_since <= 90:
                        b_90 = balance
                    else:
                        b_over = balance
                    aging_totals["0-30 Days"] += b_30
                    aging_totals["31-60 Days"] += b_60
                    aging_totals["61-90 Days"] += b_90
                    aging_totals["90+ Days"] += b_over
                    rows.append({
                        "Invoice #": inv.invoice_number,
                        "Client": o.client.name if o.client else "",
                        "Order Name": o.order_name,
                        "Invoice Date": inv.invoice_date.strftime("%Y-%m-%d") if inv.invoice_date else "",
                        "Due Date": inv.payment_due_date.strftime("%Y-%m-%d") if inv.payment_due_date else "",
                        "Days Since Invoice": days_since,
                        "Invoice Amount": float(inv.invoice_amount or 0),
                        "Received": total_received,
                        "Balance": round(balance, 2),
                        "0-30 Days": round(b_30, 2),
                        "31-60 Days": round(b_60, 2),
                        "61-90 Days": round(b_90, 2),
                        "90+ Days": round(b_over, 2),
                    })
            total = sum(r["Balance"] for r in rows)
            return {
                "title": f"Accounts Receivable Aging ({len(rows)} invoices)",
                "rows": rows,
                "total": round(total, 2),
                "total_label": "Total A/R",
                "aging": {k: round(v, 2) for k, v in aging_totals.items()},
            }

        else:
            return JSONResponse(status_code=400, content={"error": f"Unknown drilldown type: {type}"})

    except Exception as e:
        print(f"[drilldown] Error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": f"Drilldown error: {str(e)}"})
    finally:
        db.close()


@app.get("/api/session")
async def get_session_info(request: Request):
    session = get_session_from_request(request)
    if not session:
        return JSONResponse(status_code=401, content={"error": "Not logged in"})
    return {
        "username": session.get("username", ""),
        "fullname": session.get("fullname", ""),
        "role": session.get("role", ""),
        "company_access": session.get("company_access", "all"),
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

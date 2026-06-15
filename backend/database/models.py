"""
SQLAlchemy models for Orders Backlog & Billing System.
Supports SQLite (local dev) and PostgreSQL (server deployment).
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Boolean,
    ForeignKey, Text, create_engine
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


# ─────────────────────────────────────────────
# Lookup / Reference Tables
# ─────────────────────────────────────────────

class Client(Base):
    __tablename__ = "clients"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    client_id       = Column(String(8), unique=True, nullable=False)
    name            = Column(String(255), nullable=False)
    client_business = Column(String(50), nullable=False, default="")
    address         = Column(Text, default="")
    contact_names   = Column(String(500), default="")
    contact_email   = Column(String(255), default="")
    phone           = Column(String(30), default="")
    general_info    = Column(String(80), default="")
    created_date    = Column(DateTime, default=datetime.utcnow)

    # Relationships
    orders = relationship("Order", back_populates="client", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "client_id": self.client_id,
            "name": self.name,
            "client_business": self.client_business,
            "address": self.address,
            "contact_names": self.contact_names,
            "contact_email": self.contact_email,
            "phone": self.phone,
            "general_info": self.general_info,
            "created_date": self.created_date.isoformat() if self.created_date else None,
        }


class ContractType(Base):
    __tablename__ = "contract_types"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    category    = Column(String(50), nullable=False)    # SaaS, Development Service, Support Services
    subcategory = Column(String(50), nullable=False)    # Telephony System, AI Agents, etc.
    created_date = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "subcategory": self.subcategory,
        }


class Company(Base):
    __tablename__ = "companies"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    name        = Column(String(255), unique=True, nullable=False)
    created_date = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {"id": self.id, "name": self.name}


# ─────────────────────────────────────────────
# Core Business Tables
# ─────────────────────────────────────────────

class Order(Base):
    __tablename__ = "orders"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    client_id        = Column(Integer, ForeignKey("clients.id"), nullable=False)
    order_name       = Column(String(25), nullable=False)
    contract_type_id = Column(Integer, ForeignKey("contract_types.id"), nullable=False)
    company_id       = Column(Integer, ForeignKey("companies.id"), nullable=False)
    date_of_order    = Column(DateTime, nullable=False)
    delivery_start_date = Column(DateTime, nullable=True)
    order_end_date   = Column(DateTime, nullable=True)
    po_number        = Column(String(100), default="")
    contract_amount  = Column(Numeric(12, 2), nullable=False, default=0)
    is_estimate      = Column(Boolean, default=True)
    order_description = Column(String(80), default="")
    contract_pdf     = Column(String(255), default="")
    po_pdf           = Column(String(255), default="")
    # Invoice Setup fields (section 3.2 — one-time per order)
    invoice_template_file = Column(String(255), default="")
    invoice_number_prefix = Column(String(15), default="")
    invoice_contact_name  = Column(String(255), default="")
    invoice_mailing_addr  = Column(String(500), default="")
    invoice_email         = Column(String(255), default="")
    invoice_method        = Column(String(50), default="")  # Email, Post, Others
    wire_ach_data         = Column(String(500), default="")
    is_deleted       = Column(Boolean, default=False)
    created_date     = Column(DateTime, default=datetime.utcnow)
    last_modified    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by       = Column(String(100), nullable=False)

    # Relationships
    client        = relationship("Client", back_populates="orders")
    contract_type = relationship("ContractType")
    company       = relationship("Company")
    milestones    = relationship("Milestone", back_populates="order", cascade="all, delete-orphan")
    notes         = relationship("OrderNote", back_populates="order", cascade="all, delete-orphan")
    commissions   = relationship("OrderCommission", back_populates="order", cascade="all, delete-orphan")

    def to_dict(self, include_related=False):
        d = {
            "id": self.id,
            "client_id": self.client_id,
            "client_name": self.client.name if self.client else "",
            "client_code": self.client.client_id if self.client else "",
            "order_name": self.order_name,
            "contract_type_id": self.contract_type_id,
            "contract_type": (f"{self.contract_type.category} - {self.contract_type.subcategory}"
                              if self.contract_type else ""),
            "company_id": self.company_id,
            "company_name": self.company.name if self.company else "",
            "date_of_order": self.date_of_order.strftime("%Y-%m-%d") if self.date_of_order else "",
            "delivery_start_date": self.delivery_start_date.strftime("%Y-%m-%d") if self.delivery_start_date else "",
            "order_end_date": self.order_end_date.strftime("%Y-%m-%d") if self.order_end_date else "",
            "po_number": self.po_number or "",
            "contract_amount": float(self.contract_amount) if self.contract_amount else 0,
            "is_estimate": self.is_estimate if self.is_estimate is not None else True,
            "order_description": self.order_description or "",
            "contract_pdf": self.contract_pdf or "",
            "po_pdf": self.po_pdf or "",
            # Invoice setup
            "invoice_template_file": self.invoice_template_file or "",
            "invoice_number_prefix": self.invoice_number_prefix or "",
            "invoice_contact_name": self.invoice_contact_name or "",
            "invoice_mailing_addr": self.invoice_mailing_addr or "",
            "invoice_email": self.invoice_email or "",
            "invoice_method": self.invoice_method or "",
            "wire_ach_data": self.wire_ach_data or "",
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "created_by": self.created_by,
        }
        if include_related:
            d["milestones"] = [m.to_dict() for m in self.milestones]
            d["notes"] = [n.to_dict() for n in self.notes]
            milestone_total = sum(float(m.payment_amount or 0) for m in self.milestones)
            d["milestone_total"] = milestone_total
            d["amount_difference"] = float(self.contract_amount or 0) - milestone_total
        return d


class OrderNote(Base):
    __tablename__ = "order_notes"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    order_id   = Column(Integer, ForeignKey("orders.id"), nullable=False)
    note_text  = Column(String(50), nullable=False)
    note_date  = Column(DateTime, default=datetime.utcnow)
    login_name = Column(String(100), nullable=False)

    order = relationship("Order", back_populates="notes")

    def to_dict(self):
        return {
            "id": self.id,
            "order_id": self.order_id,
            "note_text": self.note_text,
            "note_date": self.note_date.strftime("%Y-%m-%d %H:%M") if self.note_date else "",
            "login_name": self.login_name,
        }


class Milestone(Base):
    __tablename__ = "milestones"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    order_id            = Column(Integer, ForeignKey("orders.id"), nullable=False)
    milestone_name      = Column(String(25), nullable=False)
    scheduled_date      = Column(DateTime, nullable=False)
    payment_amount      = Column(Numeric(12, 2), nullable=False, default=0)
    milestone_type      = Column(String(20), nullable=False, default="Estimate")  # Estimate or Confirmed
    rescheduled_date    = Column(DateTime, nullable=True)
    rescheduling_reason = Column(String(255), default="")
    description         = Column(String(50), default="")
    is_billed           = Column(Boolean, default=False)
    created_date        = Column(DateTime, default=datetime.utcnow)
    last_modified       = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    modified_by         = Column(String(100), default="")

    # Relationships
    order     = relationship("Order", back_populates="milestones")
    invoices  = relationship("Invoice", back_populates="milestone", cascade="all, delete-orphan")
    audit_log = relationship("MilestoneAudit", back_populates="milestone", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "order_id": self.order_id,
            "milestone_name": self.milestone_name,
            "scheduled_date": self.scheduled_date.strftime("%Y-%m-%d") if self.scheduled_date else "",
            "payment_amount": float(self.payment_amount) if self.payment_amount else 0,
            "milestone_type": self.milestone_type,
            "rescheduled_date": self.rescheduled_date.strftime("%Y-%m-%d") if self.rescheduled_date else "",
            "rescheduling_reason": self.rescheduling_reason or "",
            "description": self.description or "",
            "is_billed": self.is_billed,
        }


class MilestoneAudit(Base):
    __tablename__ = "milestone_audit"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    milestone_id  = Column(Integer, ForeignKey("milestones.id"), nullable=False)
    field_changed = Column(String(100), nullable=False)
    old_value     = Column(String(255), default="")
    new_value     = Column(String(255), default="")
    change_reason = Column(String(255), default="")
    changed_by    = Column(String(100), nullable=False)
    changed_date  = Column(DateTime, default=datetime.utcnow)

    milestone = relationship("Milestone", back_populates="audit_log")

    def to_dict(self):
        return {
            "id": self.id,
            "milestone_id": self.milestone_id,
            "field_changed": self.field_changed,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "change_reason": self.change_reason,
            "changed_by": self.changed_by,
            "changed_date": self.changed_date.strftime("%Y-%m-%d %H:%M") if self.changed_date else "",
        }


class OrderCommission(Base):
    __tablename__ = "order_commissions"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    order_id      = Column(Integer, ForeignKey("orders.id"), nullable=False)
    commission_type = Column(String(20), nullable=False)   # "Sales" or "Support"
    person_name   = Column(String(255), nullable=False, default="")
    commission_pct = Column(Numeric(5, 2), nullable=False, default=0)
    slot_number   = Column(Integer, nullable=False, default=1)  # 1-2 for Sales, 1-4 for Support
    created_date  = Column(DateTime, default=datetime.utcnow)

    order = relationship("Order", back_populates="commissions")

    def to_dict(self):
        return {
            "id": self.id,
            "order_id": self.order_id,
            "commission_type": self.commission_type,
            "person_name": self.person_name,
            "commission_pct": float(self.commission_pct) if self.commission_pct else 0,
            "slot_number": self.slot_number,
        }


class Invoice(Base):
    __tablename__ = "invoices"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    order_id       = Column(Integer, ForeignKey("orders.id"), nullable=False)
    milestone_id   = Column(Integer, ForeignKey("milestones.id"), nullable=False)
    invoice_number = Column(String(100), unique=True, nullable=False)
    invoice_date   = Column(DateTime, nullable=False)
    payment_due_date = Column(DateTime, nullable=True)
    invoice_amount = Column(Numeric(12, 2), nullable=False)
    invoiced_by    = Column(String(100), default="")
    invoice_pdf    = Column(String(255), default="")
    created_date   = Column(DateTime, default=datetime.utcnow)
    created_by     = Column(String(100), nullable=False)

    # Relationships
    milestone = relationship("Milestone", back_populates="invoices")
    receipts  = relationship("Receipt", back_populates="invoice", cascade="all, delete-orphan")

    def to_dict(self):
        total_received = sum(float(r.receipt_amount or 0) for r in self.receipts)
        return {
            "id": self.id,
            "order_id": self.order_id,
            "milestone_id": self.milestone_id,
            "milestone_name": self.milestone.milestone_name if self.milestone else "",
            "invoice_number": self.invoice_number,
            "invoice_date": self.invoice_date.strftime("%Y-%m-%d") if self.invoice_date else "",
            "payment_due_date": self.payment_due_date.strftime("%Y-%m-%d") if self.payment_due_date else "",
            "invoice_amount": float(self.invoice_amount) if self.invoice_amount else 0,
            "invoiced_by": self.invoiced_by or "",
            "invoice_pdf": self.invoice_pdf or "",
            "total_received": total_received,
            "balance": float(self.invoice_amount or 0) - total_received,
            "receipts": [r.to_dict() for r in self.receipts],
        }


class Receipt(Base):
    __tablename__ = "receipts"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id     = Column(Integer, ForeignKey("invoices.id"), nullable=False)
    receipt_date   = Column(DateTime, nullable=False)
    receipt_amount = Column(Numeric(12, 2), nullable=False)
    difference     = Column(Numeric(12, 2), default=0)
    receipt_notes  = Column(String(255), default="")
    created_date   = Column(DateTime, default=datetime.utcnow)
    created_by     = Column(String(100), nullable=False)

    invoice = relationship("Invoice", back_populates="receipts")

    def to_dict(self):
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "invoice_number": self.invoice.invoice_number if self.invoice else "",
            "receipt_date": self.receipt_date.strftime("%Y-%m-%d") if self.receipt_date else "",
            "receipt_amount": float(self.receipt_amount) if self.receipt_amount else 0,
            "difference": float(self.difference) if self.difference else 0,
            "receipt_notes": self.receipt_notes or "",
        }

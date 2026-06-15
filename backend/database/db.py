"""
Database connection and session management.
Supports SQLite (local dev) and PostgreSQL (server deployment).

Set environment variables:
  AG_DB_TYPE=sqlite          (default) or postgresql
  AG_SQLITE_PATH=./orders.db (default path for SQLite)
  AG_DATABASE_URL=postgresql://user:pass@host/dbname  (for PostgreSQL)
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, ContractType, Company, OrderCommission

# ── Choose database engine ──────────────────
DB_TYPE = os.environ.get("AG_DB_TYPE", "sqlite").lower()

if DB_TYPE == "postgresql":
    DATABASE_URL = os.environ.get(
        "AG_DATABASE_URL",
        "postgresql://postgres:Regency1@postgres.itcurves.us:5432/Backlog"
    )
    engine = create_engine(DATABASE_URL)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DB_PATH = os.environ.get("AG_SQLITE_PATH", os.path.join(BASE_DIR, "Orders", "orders.db"))
    DATABASE_URL = f"sqlite:///{DB_PATH}"
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Create all tables, migrate schema, and seed default data if empty."""
    Base.metadata.create_all(bind=engine)
    _migrate_schema()
    _seed_defaults()


def _migrate_schema():
    """Add any missing columns to existing tables."""
    from sqlalchemy import text, inspect
    insp = inspect(engine)
    migrations = [
        # ── Contract Types table ──
        ("contract_types", "created_date", "DATETIME"),
        # ── Companies table ──
        ("companies", "created_date", "DATETIME"),
        # ── Client table — all non-original fields ──
        ("clients", "client_business", "VARCHAR(50) DEFAULT ''"),
        ("clients", "address", "VARCHAR(500) DEFAULT ''"),
        ("clients", "contact_email", "VARCHAR(255) DEFAULT ''"),
        ("clients", "phone", "VARCHAR(30) DEFAULT ''"),
        ("clients", "general_info", "VARCHAR(80) DEFAULT ''"),
        ("clients", "created_date", "DATETIME"),
        # ── Orders table — EVERY column that might be missing ──
        ("orders", "order_name", "VARCHAR(25) DEFAULT ''"),
        ("orders", "po_number", "VARCHAR(100) DEFAULT ''"),
        ("orders", "contract_amount", "NUMERIC(12,2) DEFAULT 0"),
        ("orders", "delivery_start_date", "DATETIME"),
        ("orders", "order_end_date", "DATETIME"),
        ("orders", "is_estimate", "BOOLEAN DEFAULT 1"),
        ("orders", "order_description", "VARCHAR(80) DEFAULT ''"),
        ("orders", "contract_pdf", "VARCHAR(255) DEFAULT ''"),
        ("orders", "po_pdf", "VARCHAR(255) DEFAULT ''"),
        ("orders", "invoice_template_file", "VARCHAR(255) DEFAULT ''"),
        ("orders", "invoice_number_prefix", "VARCHAR(15) DEFAULT ''"),
        ("orders", "invoice_contact_name", "VARCHAR(255) DEFAULT ''"),
        ("orders", "invoice_mailing_addr", "VARCHAR(500) DEFAULT ''"),
        ("orders", "invoice_email", "VARCHAR(255) DEFAULT ''"),
        ("orders", "invoice_method", "VARCHAR(50) DEFAULT ''"),
        ("orders", "wire_ach_data", "VARCHAR(500) DEFAULT ''"),
        ("orders", "is_deleted", "BOOLEAN DEFAULT 0"),
        ("orders", "last_modified", "DATETIME"),
        ("orders", "created_by", "VARCHAR(100) DEFAULT ''"),
        ("orders", "created_date", "DATETIME"),
        # ── Order Notes table ──
        ("order_notes", "note_text", "VARCHAR(50) DEFAULT ''"),
        ("order_notes", "note_date", "DATETIME"),
        ("order_notes", "login_name", "VARCHAR(100) DEFAULT ''"),
        # ── Milestones table — all fields ──
        ("milestones", "milestone_name", "VARCHAR(25) DEFAULT ''"),
        ("milestones", "scheduled_date", "DATETIME"),
        ("milestones", "payment_amount", "NUMERIC(12,2) DEFAULT 0"),
        ("milestones", "milestone_type", "VARCHAR(20) DEFAULT 'Estimate'"),
        ("milestones", "rescheduled_date", "DATETIME"),
        ("milestones", "rescheduling_reason", "VARCHAR(255) DEFAULT ''"),
        ("milestones", "description", "VARCHAR(50) DEFAULT ''"),
        ("milestones", "is_billed", "BOOLEAN DEFAULT 0"),
        ("milestones", "created_date", "DATETIME"),
        ("milestones", "last_modified", "DATETIME"),
        ("milestones", "modified_by", "VARCHAR(100) DEFAULT ''"),
        # ── Milestone Audit table ──
        ("milestone_audit", "field_changed", "VARCHAR(100) DEFAULT ''"),
        ("milestone_audit", "old_value", "VARCHAR(255) DEFAULT ''"),
        ("milestone_audit", "new_value", "VARCHAR(255) DEFAULT ''"),
        ("milestone_audit", "change_reason", "VARCHAR(255) DEFAULT ''"),
        ("milestone_audit", "changed_by", "VARCHAR(100) DEFAULT ''"),
        ("milestone_audit", "changed_date", "DATETIME"),
        # ── Invoices table — all fields ──
        ("invoices", "invoice_number", "VARCHAR(100) DEFAULT ''"),
        ("invoices", "invoice_date", "DATETIME"),
        ("invoices", "invoice_amount", "NUMERIC(12,2) DEFAULT 0"),
        ("invoices", "payment_due_date", "DATETIME"),
        ("invoices", "invoiced_by", "VARCHAR(100) DEFAULT ''"),
        ("invoices", "invoice_pdf", "VARCHAR(255) DEFAULT ''"),
        ("invoices", "created_date", "DATETIME"),
        ("invoices", "created_by", "VARCHAR(100) DEFAULT ''"),
        # ── Receipts table — all fields ──
        ("receipts", "receipt_date", "DATETIME"),
        ("receipts", "receipt_amount", "NUMERIC(12,2) DEFAULT 0"),
        ("receipts", "difference", "NUMERIC(12,2) DEFAULT 0"),
        ("receipts", "receipt_notes", "VARCHAR(255) DEFAULT ''"),
        ("receipts", "created_date", "DATETIME"),
        ("receipts", "created_by", "VARCHAR(100) DEFAULT ''"),
        # ── Order Commissions table ──
        ("order_commissions", "commission_type", "VARCHAR(20) DEFAULT ''"),
        ("order_commissions", "person_name", "VARCHAR(255) DEFAULT ''"),
        ("order_commissions", "commission_pct", "NUMERIC(5,2) DEFAULT 0"),
        ("order_commissions", "slot_number", "INTEGER DEFAULT 1"),
        ("order_commissions", "created_date", "DATETIME"),
    ]
    with engine.connect() as conn:
        for table, column, col_type in migrations:
            if table in insp.get_table_names():
                existing = [c["name"] for c in insp.get_columns(table)]
                if column not in existing:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                    print(f"[db] Added {column} to {table}")

        # Migrate old client data: copy billing_address → address, billing_email → contact_email
        if "clients" in insp.get_table_names():
            existing = [c["name"] for c in insp.get_columns("clients")]
            if "billing_address" in existing and "address" in existing:
                conn.execute(text("UPDATE clients SET address = billing_address WHERE (address IS NULL OR address = '') AND billing_address != ''"))
                print("[db] Migrated billing_address → address")
            if "billing_email" in existing and "contact_email" in existing:
                conn.execute(text("UPDATE clients SET contact_email = billing_email WHERE (contact_email IS NULL OR contact_email = '') AND billing_email != ''"))
                print("[db] Migrated billing_email → contact_email")

        conn.commit()


def get_db():
    """Dependency for FastAPI — yields a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _seed_defaults():
    """Seed contract types and companies if tables are empty."""
    db = SessionLocal()
    try:
        # Seed contract types
        if db.query(ContractType).count() == 0:
            defaults = [
                # SaaS
                ("SaaS", "Telephony System"),
                ("SaaS", "AI Agents"),
                ("SaaS", "Transit"),
                ("SaaS", "Others"),
                # Development Service
                ("Development Service", "Telephony System"),
                ("Development Service", "AI Agents"),
                ("Development Service", "Transit"),
                ("Development Service", "Others"),
                # Support Services
                ("Support Services", "Maintenance"),
                ("Support Services", "AI Agents"),
                ("Support Services", "Customer Support"),
                ("Support Services", "Others"),
            ]
            for cat, sub in defaults:
                db.add(ContractType(category=cat, subcategory=sub))

        # Seed companies
        if db.query(Company).count() == 0:
            for name in ["IT Curves", "AI Dev Lab"]:
                db.add(Company(name=name))

        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()

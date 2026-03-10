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
from .models import Base, ContractType, Company

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
        ("contract_types", "created_date", "DATETIME"),
        ("companies", "created_date", "DATETIME"),
    ]
    with engine.connect() as conn:
        for table, column, col_type in migrations:
            if table in insp.get_table_names():
                existing = [c["name"] for c in insp.get_columns(table)]
                if column not in existing:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                    print(f"[db] Added {column} to {table}")
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

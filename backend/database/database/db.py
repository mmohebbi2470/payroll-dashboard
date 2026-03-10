"""
Database connection and table management using built-in sqlite3.
No external dependencies needed.

For PostgreSQL deployment: install psycopg2 and update get_connection().
"""

import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.environ.get("AG_SQLITE_PATH", os.path.join(BASE_DIR, "orders.db"))


def get_connection():
    """Get a SQLite connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create all tables and seed default data if empty."""
    conn = get_connection()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        billing_address TEXT DEFAULT '',
        contact_names TEXT DEFAULT '',
        billing_name TEXT DEFAULT '',
        billing_email TEXT DEFAULT '',
        created_date TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS contract_types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT NOT NULL,
        subcategory TEXT NOT NULL,
        created_date TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        created_date TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL REFERENCES clients(id),
        order_name TEXT NOT NULL,
        contract_type_id INTEGER NOT NULL REFERENCES contract_types(id),
        company_id INTEGER NOT NULL REFERENCES companies(id),
        date_of_order TEXT NOT NULL,
        po_number TEXT DEFAULT '',
        contract_amount REAL DEFAULT 0,
        contract_pdf TEXT DEFAULT '',
        po_pdf TEXT DEFAULT '',
        is_deleted INTEGER DEFAULT 0,
        created_date TEXT DEFAULT (datetime('now')),
        last_modified TEXT DEFAULT (datetime('now')),
        created_by TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS order_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        note_text TEXT NOT NULL,
        note_date TEXT DEFAULT (datetime('now')),
        login_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS milestones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        milestone_name TEXT NOT NULL,
        scheduled_date TEXT NOT NULL,
        payment_amount REAL DEFAULT 0,
        milestone_type TEXT DEFAULT 'Estimate',
        rescheduled_date TEXT DEFAULT '',
        rescheduling_reason TEXT DEFAULT '',
        description TEXT DEFAULT '',
        is_billed INTEGER DEFAULT 0,
        created_date TEXT DEFAULT (datetime('now')),
        last_modified TEXT DEFAULT (datetime('now')),
        modified_by TEXT DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS milestone_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        milestone_id INTEGER NOT NULL REFERENCES milestones(id),
        field_changed TEXT NOT NULL,
        old_value TEXT DEFAULT '',
        new_value TEXT DEFAULT '',
        change_reason TEXT DEFAULT '',
        changed_by TEXT NOT NULL,
        changed_date TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        milestone_id INTEGER NOT NULL REFERENCES milestones(id),
        invoice_number TEXT UNIQUE NOT NULL,
        invoice_date TEXT NOT NULL,
        invoice_amount REAL NOT NULL,
        invoice_pdf TEXT DEFAULT '',
        created_date TEXT DEFAULT (datetime('now')),
        created_by TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER NOT NULL REFERENCES invoices(id),
        receipt_date TEXT NOT NULL,
        receipt_amount REAL NOT NULL,
        difference REAL DEFAULT 0,
        receipt_notes TEXT DEFAULT '',
        created_date TEXT DEFAULT (datetime('now')),
        created_by TEXT NOT NULL
    );
    """)

    # Seed contract types
    count = c.execute("SELECT COUNT(*) FROM contract_types").fetchone()[0]
    if count == 0:
        defaults = [
            ("SaaS", "Telephony System"), ("SaaS", "AI Agents"),
            ("SaaS", "Transit"), ("SaaS", "Others"),
            ("Development Service", "Telephony System"), ("Development Service", "AI Agents"),
            ("Development Service", "Transit"), ("Development Service", "Others"),
            ("Support Services", "Maintenance"), ("Support Services", "AI Agents"),
            ("Support Services", "Customer Support"), ("Support Services", "Others"),
        ]
        c.executemany("INSERT INTO contract_types (category, subcategory) VALUES (?, ?)", defaults)

    # Seed companies
    count = c.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    if count == 0:
        c.executemany("INSERT INTO companies (name) VALUES (?)", [("IT Curves",), ("AI Dev Lab",)])

    conn.commit()
    conn.close()

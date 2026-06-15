"""
Migrate local SQLite data → PostgreSQL server (sap_portal).
Usage:  python3 migrate_to_server.py

Reads from: Orders/orders.db (local SQLite)
Writes to:  postgresql://postgres:Regency1@192.168.13.75:5432/sap_portal
"""

import sqlite3
import psycopg2
import os

# ── Config ──
SQLITE_PATH = os.path.join(os.path.dirname(__file__), "Orders", "orders.db")
PG_URL = "postgresql://postgres:Regency1@192.168.13.75:5432/sap_portal"

# Tables in dependency order (parents before children)
TABLES = [
    "companies",
    "contract_types",
    "clients",
    "orders",
    "order_notes",
    "milestones",
    "milestone_audit",
    "invoices",
    "receipts",
    "order_commissions",
]


def get_sqlite_columns(cursor, table):
    """Get column names for a SQLite table."""
    cursor.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cursor.fetchall()]


def migrate():
    # Connect to both databases
    print(f"📂 Reading from: {SQLITE_PATH}")
    print(f"🐘 Writing to:   {PG_URL}")
    print()

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    sc = sqlite_conn.cursor()

    pg_conn = psycopg2.connect(PG_URL)
    pg_conn.autocommit = False
    pc = pg_conn.cursor()

    # Get PostgreSQL table columns for each target table
    pc.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """)
    pg_columns = {}
    for row in pc.fetchall():
        pg_columns.setdefault(row[0], []).append(row[1])

    total_migrated = 0

    for table in TABLES:
        # Get SQLite columns
        sqlite_cols = get_sqlite_columns(sc, table)

        # Find common columns (exist in both SQLite and PostgreSQL)
        pg_table_cols = pg_columns.get(table, [])
        if not pg_table_cols:
            print(f"⚠️  Table '{table}' not found in PostgreSQL — skipping")
            continue

        common_cols = [c for c in sqlite_cols if c in pg_table_cols]

        # Read all rows from SQLite
        sc.execute(f"SELECT * FROM {table}")
        rows = sc.fetchall()

        if not rows:
            print(f"  {table}: 0 rows (empty) — skipping")
            continue

        # Clear existing data in PostgreSQL (to avoid conflicts)
        pc.execute(f"DELETE FROM {table}")

        # Build INSERT statement
        col_list = ", ".join(common_cols)
        placeholders = ", ".join(["%s"] * len(common_cols))
        insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

        # Detect which columns are BOOLEAN in PostgreSQL
        pc.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
              AND data_type = 'boolean'
        """, (table,))
        bool_cols = {r[0] for r in pc.fetchall()}

        # Insert rows
        inserted = 0
        for row in rows:
            row_dict = dict(row)
            values = []
            for col in common_cols:
                v = row_dict.get(col)
                # Cast integer booleans (0/1) to Python bool for PostgreSQL
                if col in bool_cols and v is not None:
                    v = bool(v)
                values.append(v)
            try:
                pc.execute(insert_sql, values)
                inserted += 1
            except Exception as e:
                print(f"  ⚠️  Error inserting into {table}: {e}")
                pg_conn.rollback()
                raise

        # Reset the serial sequence to max(id) + 1
        if "id" in common_cols:
            pc.execute(f"SELECT MAX(id) FROM {table}")
            max_id = pc.fetchone()[0]
            if max_id:
                pc.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), {max_id})")

        print(f"  ✅ {table}: {inserted} rows migrated")
        total_migrated += inserted

    pg_conn.commit()
    print()
    print(f"🎉 Migration complete! {total_migrated} total rows migrated.")

    # Verify counts
    print()
    print("── Verification ──")
    for table in TABLES:
        sc.execute(f"SELECT COUNT(*) FROM {table}")
        sqlite_count = sc.fetchone()[0]
        pc.execute(f"SELECT COUNT(*) FROM {table}")
        pg_count = pc.fetchone()[0]
        match = "✅" if sqlite_count == pg_count else "❌"
        print(f"  {match} {table}: SQLite={sqlite_count}  PostgreSQL={pg_count}")

    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    migrate()

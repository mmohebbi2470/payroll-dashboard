"""
Orders Backlog Server (pure stdlib — zero external dependencies)
================================================================
Uses Python built-in http.server + sqlite3.
Runs on port 8001 by default.
"""

import os, sys, json, uuid, re, mimetypes, urllib.parse, traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(BACKEND_DIR)
sys.path.insert(0, BACKEND_DIR)

from database.db import get_connection, init_db

UPLOAD_DIR = os.path.join(BASE_DIR, "uploads", "orders")
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend", "orders-app")
os.makedirs(UPLOAD_DIR, exist_ok=True)

PORT = int(os.environ.get("AG_ORDERS_PORT", "8001"))


def row_to_dict(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
#  HTTP Handler
# ═══════════════════════════════════════════════════════════════
class OrdersHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[orders {datetime.now().strftime('%H:%M:%S')}] {args[0]}")

    def _send(self, code, body, content_type="application/json", headers=None):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.wfile.write(body)

    def _json(self, code, data):
        self._send(code, json.dumps(data))

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except:
            return {}

    def do_OPTIONS(self):
        self._send(200, "")

    # ─── GET ─────────────────────────────────────────────────
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/")
        qs = urllib.parse.parse_qs(parsed.query)

        # Serve frontend
        if path == "" or path == "/":
            self._serve_file(os.path.join(FRONTEND_DIR, "index.html"), "text/html")
            return
        if path.startswith("/static/"):
            fpath = os.path.join(FRONTEND_DIR, path[8:])
            mime = mimetypes.guess_type(fpath)[0] or "application/octet-stream"
            self._serve_file(fpath, mime)
            return

        conn = get_connection()
        c = conn.cursor()
        try:
            # Session info
            if path == "/api/orders/session-info":
                self._json(200, {"username": "User", "role": "Admin"})
                return

            # ── Clients ──
            if path == "/api/orders/clients":
                rows = c.execute("SELECT * FROM clients ORDER BY name").fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Contract Types ──
            if path == "/api/orders/contract-types":
                rows = c.execute("SELECT * FROM contract_types ORDER BY category, subcategory").fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Companies ──
            if path == "/api/orders/companies":
                rows = c.execute("SELECT * FROM companies ORDER BY name").fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Orders list ──
            if path == "/api/orders/orders":
                search = qs.get("search", [""])[0]
                sql = """SELECT o.*, cl.name as client_name, cl.client_id as client_code,
                         ct.category || ' - ' || ct.subcategory as contract_type,
                         co.name as company_name
                         FROM orders o
                         LEFT JOIN clients cl ON o.client_id = cl.id
                         LEFT JOIN contract_types ct ON o.contract_type_id = ct.id
                         LEFT JOIN companies co ON o.company_id = co.id
                         WHERE o.is_deleted = 0"""
                params = []
                if search:
                    sql += " AND o.order_name LIKE ?"
                    params.append(f"%{search}%")
                sql += " ORDER BY o.date_of_order DESC"
                rows = c.execute(sql, params).fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Single order detail ──
            m = re.match(r"^/api/orders/orders/(\d+)$", path)
            if m:
                oid = int(m.group(1))
                order = self._get_order_detail(c, oid)
                if not order:
                    self._json(404, {"detail": "Order not found"}); return
                self._json(200, order)
                return

            # ── Milestones for order ──
            m = re.match(r"^/api/orders/orders/(\d+)/milestones$", path)
            if m:
                rows = c.execute("SELECT * FROM milestones WHERE order_id=? ORDER BY scheduled_date",
                                 (int(m.group(1)),)).fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Notes for order ──
            m = re.match(r"^/api/orders/orders/(\d+)/notes$", path)
            if m:
                rows = c.execute("SELECT * FROM order_notes WHERE order_id=? ORDER BY note_date DESC",
                                 (int(m.group(1)),)).fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Invoices for order ──
            m = re.match(r"^/api/orders/orders/(\d+)/invoices$", path)
            if m:
                oid = int(m.group(1))
                invs = c.execute("""SELECT i.*, m.milestone_name
                    FROM invoices i LEFT JOIN milestones m ON i.milestone_id = m.id
                    WHERE i.order_id=? ORDER BY i.invoice_date DESC""", (oid,)).fetchall()
                result = []
                for inv in invs:
                    inv_d = dict(inv)
                    rcpts = c.execute("SELECT * FROM receipts WHERE invoice_id=?", (inv_d["id"],)).fetchall()
                    total_received = sum(r["receipt_amount"] or 0 for r in rcpts)
                    inv_d["total_received"] = total_received
                    inv_d["balance"] = (inv_d["invoice_amount"] or 0) - total_received
                    result.append(inv_d)
                self._json(200, result)
                return

            # ── Receipts for invoice ──
            m = re.match(r"^/api/orders/invoices/(\d+)/receipts$", path)
            if m:
                rows = c.execute("""SELECT r.*, i.invoice_number
                    FROM receipts r LEFT JOIN invoices i ON r.invoice_id = i.id
                    WHERE r.invoice_id=? ORDER BY r.receipt_date DESC""", (int(m.group(1)),)).fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Milestone audit ──
            m = re.match(r"^/api/orders/milestones/(\d+)/audit$", path)
            if m:
                rows = c.execute("SELECT * FROM milestone_audit WHERE milestone_id=? ORDER BY changed_date DESC",
                                 (int(m.group(1)),)).fetchall()
                self._json(200, rows_to_list(rows))
                return

            # ── Download file ──
            m = re.match(r"^/api/orders/download/(.+)$", path)
            if m:
                filename = urllib.parse.unquote(m.group(1))
                fpath = os.path.join(UPLOAD_DIR, filename)
                if os.path.isfile(fpath):
                    self._serve_file(fpath, "application/pdf")
                else:
                    self._json(404, {"detail": "File not found"})
                return

            # ── Backlog report ──
            if path == "/api/orders/reports/backlog":
                start_month = qs.get("start_month", [datetime.utcnow().strftime("%Y-%m")])[0]
                self._json(200, self._backlog_report(c, start_month))
                return

            # ── Milestone schedule report ──
            if path == "/api/orders/reports/milestone-schedule":
                date_from = qs.get("date_from", [datetime.utcnow().strftime("%Y-%m-%d")])[0]
                date_to = qs.get("date_to", [datetime(datetime.utcnow().year, 12, 31).strftime("%Y-%m-%d")])[0]
                self._json(200, self._schedule_report(c, date_from, date_to))
                return

            self._json(404, {"detail": "Not found"})

        except Exception as e:
            traceback.print_exc()
            self._json(500, {"detail": str(e)})
        finally:
            conn.close()

    # ─── POST ────────────────────────────────────────────────
    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        data = self._read_body()
        conn = get_connection()
        c = conn.cursor()
        try:
            # ── Create client ──
            if path == "/api/orders/clients":
                existing = c.execute("SELECT id FROM clients WHERE client_id=?", (data["client_id"],)).fetchone()
                if existing:
                    self._json(400, {"detail": f"Client ID '{data['client_id']}' already exists"}); return
                c.execute("""INSERT INTO clients (client_id, name, billing_address, contact_names, billing_name, billing_email)
                    VALUES (?,?,?,?,?,?)""",
                    (data["client_id"], data["name"], data.get("billing_address",""),
                     data.get("contact_names",""), data.get("billing_name",""), data.get("billing_email","")))
                conn.commit()
                row = c.execute("SELECT * FROM clients WHERE id=?", (c.lastrowid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            # ── Create contract type ──
            if path == "/api/orders/contract-types":
                c.execute("INSERT INTO contract_types (category, subcategory) VALUES (?,?)",
                    (data["category"], data["subcategory"]))
                conn.commit()
                row = c.execute("SELECT * FROM contract_types WHERE id=?", (c.lastrowid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            # ── Create company ──
            if path == "/api/orders/companies":
                existing = c.execute("SELECT id FROM companies WHERE name=?", (data["name"],)).fetchone()
                if existing:
                    self._json(400, {"detail": f"Company '{data['name']}' already exists"}); return
                c.execute("INSERT INTO companies (name) VALUES (?)", (data["name"],))
                conn.commit()
                row = c.execute("SELECT * FROM companies WHERE id=?", (c.lastrowid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            # ── Create order ──
            if path == "/api/orders/orders":
                c.execute("""INSERT INTO orders (client_id, order_name, contract_type_id, company_id,
                    date_of_order, po_number, contract_amount, created_by)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (data["client_id"], data["order_name"], data["contract_type_id"],
                     data["company_id"], data["date_of_order"], data.get("po_number",""),
                     data.get("contract_amount",0), data.get("created_by","system")))
                conn.commit()
                order = self._get_order_brief(c, c.lastrowid)
                self._json(200, order)
                return

            # ── Create milestone ──
            m = re.match(r"^/api/orders/orders/(\d+)/milestones$", path)
            if m:
                oid = int(m.group(1))
                count = c.execute("SELECT COUNT(*) FROM milestones WHERE order_id=?", (oid,)).fetchone()[0]
                if count >= 15:
                    self._json(400, {"detail": "Maximum 15 milestones per order"}); return
                c.execute("""INSERT INTO milestones (order_id, milestone_name, scheduled_date, payment_amount,
                    milestone_type, description, modified_by) VALUES (?,?,?,?,?,?,?)""",
                    (oid, data["milestone_name"], data["scheduled_date"],
                     data.get("payment_amount",0), data.get("milestone_type","Estimate"),
                     data.get("description",""), data.get("modified_by","system")))
                conn.commit()
                row = c.execute("SELECT * FROM milestones WHERE id=?", (c.lastrowid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            # ── Create note ──
            m = re.match(r"^/api/orders/orders/(\d+)/notes$", path)
            if m:
                oid = int(m.group(1))
                count = c.execute("SELECT COUNT(*) FROM order_notes WHERE order_id=?", (oid,)).fetchone()[0]
                if count >= 50:
                    self._json(400, {"detail": "Maximum 50 notes per order"}); return
                c.execute("INSERT INTO order_notes (order_id, note_text, login_name) VALUES (?,?,?)",
                    (oid, data["note_text"][:50], data.get("login_name","system")))
                conn.commit()
                row = c.execute("SELECT * FROM order_notes WHERE id=?", (c.lastrowid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            # ── Create invoice ──
            m = re.match(r"^/api/orders/orders/(\d+)/invoices$", path)
            if m:
                oid = int(m.group(1))
                existing = c.execute("SELECT id FROM invoices WHERE invoice_number=?",
                    (data["invoice_number"],)).fetchone()
                if existing:
                    self._json(400, {"detail": f"Invoice '{data['invoice_number']}' already exists"}); return
                c.execute("""INSERT INTO invoices (order_id, milestone_id, invoice_number, invoice_date,
                    invoice_amount, created_by) VALUES (?,?,?,?,?,?)""",
                    (oid, data["milestone_id"], data["invoice_number"], data["invoice_date"],
                     data["invoice_amount"], data.get("created_by","system")))
                # Mark milestone as billed
                c.execute("UPDATE milestones SET is_billed=1 WHERE id=?", (data["milestone_id"],))
                conn.commit()
                inv = c.execute("""SELECT i.*, m.milestone_name FROM invoices i
                    LEFT JOIN milestones m ON i.milestone_id=m.id WHERE i.id=?""", (c.lastrowid,)).fetchone()
                inv_d = dict(inv)
                inv_d["total_received"] = 0
                inv_d["balance"] = inv_d["invoice_amount"]
                self._json(200, inv_d)
                return

            # ── Create receipt ──
            m = re.match(r"^/api/orders/invoices/(\d+)/receipts$", path)
            if m:
                inv_id = int(m.group(1))
                inv = c.execute("SELECT * FROM invoices WHERE id=?", (inv_id,)).fetchone()
                if not inv:
                    self._json(404, {"detail": "Invoice not found"}); return
                total_received = c.execute("SELECT COALESCE(SUM(receipt_amount),0) FROM receipts WHERE invoice_id=?",
                    (inv_id,)).fetchone()[0]
                remaining = (inv["invoice_amount"] or 0) - total_received
                diff = data["receipt_amount"] - remaining
                c.execute("""INSERT INTO receipts (invoice_id, receipt_date, receipt_amount, difference,
                    receipt_notes, created_by) VALUES (?,?,?,?,?,?)""",
                    (inv_id, data["receipt_date"], data["receipt_amount"], diff,
                     data.get("receipt_notes",""), data.get("created_by","system")))
                conn.commit()
                row = c.execute("""SELECT r.*, i.invoice_number FROM receipts r
                    LEFT JOIN invoices i ON r.invoice_id=i.id WHERE r.id=?""", (c.lastrowid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            self._json(404, {"detail": "Not found"})

        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            self._json(500, {"detail": str(e)})
        finally:
            conn.close()

    # ─── PUT ─────────────────────────────────────────────────
    def do_PUT(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        data = self._read_body()
        conn = get_connection()
        c = conn.cursor()
        try:
            # ── Update client ──
            m = re.match(r"^/api/orders/clients/(\d+)$", path)
            if m:
                cid = int(m.group(1))
                sets, vals = [], []
                for k in ("client_id", "name", "billing_address", "contact_names", "billing_name", "billing_email"):
                    if k in data:
                        sets.append(f"{k}=?"); vals.append(data[k])
                if sets:
                    vals.append(cid)
                    c.execute(f"UPDATE clients SET {','.join(sets)} WHERE id=?", vals)
                    conn.commit()
                row = c.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            # ── Update order ──
            m = re.match(r"^/api/orders/orders/(\d+)$", path)
            if m:
                oid = int(m.group(1))
                sets, vals = ["last_modified=datetime('now')"], []
                for k in ("client_id", "order_name", "contract_type_id", "company_id",
                           "date_of_order", "po_number", "contract_amount"):
                    if k in data:
                        sets.append(f"{k}=?"); vals.append(data[k])
                vals.append(oid)
                c.execute(f"UPDATE orders SET {','.join(sets)} WHERE id=?", vals)
                conn.commit()
                order = self._get_order_brief(c, oid)
                self._json(200, order)
                return

            # ── Update milestone ──
            m = re.match(r"^/api/orders/milestones/(\d+)$", path)
            if m:
                ms_id = int(m.group(1))
                old = c.execute("SELECT * FROM milestones WHERE id=?", (ms_id,)).fetchone()
                if not old:
                    self._json(404, {"detail": "Milestone not found"}); return
                old_d = dict(old)
                change_reason = data.pop("change_reason", "")
                modified_by = data.pop("modified_by", "system")

                sets, vals = ["last_modified=datetime('now')", "modified_by=?"], [modified_by]
                for field in ("milestone_name", "scheduled_date", "payment_amount",
                              "milestone_type", "rescheduled_date", "rescheduling_reason", "description"):
                    if field not in data:
                        continue
                    new_val = data[field]
                    old_val = old_d.get(field, "")
                    if str(old_val or "") != str(new_val or ""):
                        c.execute("""INSERT INTO milestone_audit
                            (milestone_id, field_changed, old_value, new_value, change_reason, changed_by)
                            VALUES (?,?,?,?,?,?)""",
                            (ms_id, field, str(old_val or ""), str(new_val or ""), change_reason, modified_by))
                    sets.append(f"{field}=?"); vals.append(new_val)

                vals.append(ms_id)
                c.execute(f"UPDATE milestones SET {','.join(sets)} WHERE id=?", vals)
                conn.commit()
                row = c.execute("SELECT * FROM milestones WHERE id=?", (ms_id,)).fetchone()
                self._json(200, row_to_dict(row))
                return

            self._json(404, {"detail": "Not found"})

        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            self._json(500, {"detail": str(e)})
        finally:
            conn.close()

    # ─── DELETE ──────────────────────────────────────────────
    def do_DELETE(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        conn = get_connection()
        c = conn.cursor()
        try:
            # ── Delete client ──
            m = re.match(r"^/api/orders/clients/(\d+)$", path)
            if m:
                cid = int(m.group(1))
                has_orders = c.execute("SELECT COUNT(*) FROM orders WHERE client_id=? AND is_deleted=0", (cid,)).fetchone()[0]
                if has_orders:
                    self._json(400, {"detail": "Cannot delete client with existing orders"}); return
                c.execute("DELETE FROM clients WHERE id=?", (cid,))
                conn.commit()
                self._json(200, {"ok": True})
                return

            # ── Delete contract type ──
            m = re.match(r"^/api/orders/contract-types/(\d+)$", path)
            if m:
                c.execute("DELETE FROM contract_types WHERE id=?", (int(m.group(1)),))
                conn.commit()
                self._json(200, {"ok": True})
                return

            # ── Delete company ──
            m = re.match(r"^/api/orders/companies/(\d+)$", path)
            if m:
                c.execute("DELETE FROM companies WHERE id=?", (int(m.group(1)),))
                conn.commit()
                self._json(200, {"ok": True})
                return

            # ── Soft-delete order ──
            m = re.match(r"^/api/orders/orders/(\d+)$", path)
            if m:
                c.execute("UPDATE orders SET is_deleted=1, last_modified=datetime('now') WHERE id=?",
                    (int(m.group(1)),))
                conn.commit()
                self._json(200, {"ok": True})
                return

            # ── Delete milestone ──
            m = re.match(r"^/api/orders/milestones/(\d+)$", path)
            if m:
                ms_id = int(m.group(1))
                has_inv = c.execute("SELECT COUNT(*) FROM invoices WHERE milestone_id=?", (ms_id,)).fetchone()[0]
                if has_inv:
                    self._json(400, {"detail": "Cannot delete milestone with invoices"}); return
                c.execute("DELETE FROM milestone_audit WHERE milestone_id=?", (ms_id,))
                c.execute("DELETE FROM milestones WHERE id=?", (ms_id,))
                conn.commit()
                self._json(200, {"ok": True})
                return

            self._json(404, {"detail": "Not found"})

        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            self._json(500, {"detail": str(e)})
        finally:
            conn.close()

    # ─── Helpers ─────────────────────────────────────────────
    def _serve_file(self, fpath, content_type):
        if not os.path.isfile(fpath):
            self._json(404, {"detail": "File not found"})
            return
        with open(fpath, "rb") as f:
            data = f.read()
        self._send(200, data, content_type)

    def _get_order_brief(self, c, oid):
        """Get order with joined names (for list view)."""
        row = c.execute("""SELECT o.*, cl.name as client_name, cl.client_id as client_code,
            ct.category || ' - ' || ct.subcategory as contract_type,
            co.name as company_name
            FROM orders o
            LEFT JOIN clients cl ON o.client_id = cl.id
            LEFT JOIN contract_types ct ON o.contract_type_id = ct.id
            LEFT JOIN companies co ON o.company_id = co.id
            WHERE o.id=?""", (oid,)).fetchone()
        return row_to_dict(row) if row else None

    def _get_order_detail(self, c, oid):
        """Get full order detail with milestones, notes."""
        order = self._get_order_brief(c, oid)
        if not order:
            return None
        # Milestones
        ms_rows = c.execute("SELECT * FROM milestones WHERE order_id=? ORDER BY scheduled_date", (oid,)).fetchall()
        milestones = rows_to_list(ms_rows)
        milestone_total = sum(m.get("payment_amount", 0) or 0 for m in milestones)
        # Notes
        note_rows = c.execute("SELECT * FROM order_notes WHERE order_id=? ORDER BY note_date DESC", (oid,)).fetchall()
        notes = rows_to_list(note_rows)

        order["milestones"] = milestones
        order["notes"] = notes
        order["milestone_total"] = milestone_total
        order["amount_difference"] = (order.get("contract_amount", 0) or 0) - milestone_total
        return order

    def _backlog_report(self, c, start_month):
        start_date = start_month + "-01"
        # Approximate 12 months forward
        year = int(start_month[:4])
        month = int(start_month[5:7])
        end_month = month
        end_year = year + 1
        end_date = f"{end_year}-{end_month:02d}-28"

        rows = c.execute("""
            SELECT m.*, o.order_name, cl.name as client_name
            FROM milestones m
            JOIN orders o ON m.order_id = o.id
            LEFT JOIN clients cl ON o.client_id = cl.id
            WHERE o.is_deleted = 0 AND m.is_billed = 0
            AND m.scheduled_date >= ? AND m.scheduled_date <= ?
            ORDER BY m.scheduled_date
        """, (start_date, end_date)).fetchall()

        result_rows = []
        total = 0
        for r in rows:
            rd = dict(r)
            amt = rd.get("payment_amount", 0) or 0
            total += amt
            result_rows.append({
                "order_name": rd.get("order_name", ""),
                "client_name": rd.get("client_name", ""),
                "milestone_name": rd.get("milestone_name", ""),
                "scheduled_date": rd.get("scheduled_date", ""),
                "payment_amount": amt,
                "milestone_type": rd.get("milestone_type", ""),
            })

        return {"rows": result_rows, "total": total, "start_month": start_month,
                "period": f"{start_month} — {end_year}-{end_month:02d}"}

    def _schedule_report(self, c, date_from, date_to):
        rows = c.execute("""
            SELECT m.*, o.order_name, cl.name as client_name
            FROM milestones m
            JOIN orders o ON m.order_id = o.id
            LEFT JOIN clients cl ON o.client_id = cl.id
            WHERE o.is_deleted = 0
            AND m.scheduled_date >= ? AND m.scheduled_date <= ?
            ORDER BY m.scheduled_date
        """, (date_from, date_to)).fetchall()

        result_rows = []
        total = 0
        for r in rows:
            rd = dict(r)
            amt = rd.get("payment_amount", 0) or 0
            total += amt
            result_rows.append({
                "order_name": rd.get("order_name", ""),
                "client_name": rd.get("client_name", ""),
                "milestone_name": rd.get("milestone_name", ""),
                "scheduled_date": rd.get("scheduled_date", ""),
                "payment_amount": amt,
                "milestone_type": rd.get("milestone_type", ""),
                "is_billed": bool(rd.get("is_billed", 0)),
            })

        return {"rows": result_rows, "total": total, "date_from": date_from, "date_to": date_to}


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    from database.db import DB_PATH
    print("=" * 60)
    print("  Orders Backlog & Billing Server")
    print("=" * 60)
    print(f"  Database: {DB_PATH}")
    print(f"  Frontend: {FRONTEND_DIR}")
    print(f"  Uploads:  {UPLOAD_DIR}")
    print()

    print("  Initializing database...")
    init_db()
    print("  Database ready!")
    print()
    print(f"  Server: http://localhost:{PORT}")
    print(f"  Press Ctrl+C to stop")
    print("=" * 60)

    server = HTTPServer(("0.0.0.0", PORT), OrdersHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()

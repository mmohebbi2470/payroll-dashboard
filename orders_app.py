"""
Orders Backlog & Billing System — Standalone Server
====================================================
Self-contained: no external packages needed, no subfolders required.
Uses Python's built-in sqlite3 + http.server.

Usage:
    python3 orders_app.py

Opens at: http://localhost:8001
"""

import os, sys, json, re, mimetypes, urllib.parse, traceback, sqlite3
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────
PORT = 8001
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "orders.db")
UPLOAD_DIR = os.path.join(SCRIPT_DIR, "orders_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  DATABASE SETUP
# ══════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
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
        subcategory TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
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
        is_deleted INTEGER DEFAULT 0,
        created_date TEXT DEFAULT (datetime('now')),
        last_modified TEXT DEFAULT (datetime('now')),
        created_by TEXT NOT NULL DEFAULT 'system'
    );
    CREATE TABLE IF NOT EXISTS order_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        note_text TEXT NOT NULL,
        note_date TEXT DEFAULT (datetime('now')),
        login_name TEXT NOT NULL DEFAULT 'system'
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
        changed_by TEXT NOT NULL DEFAULT 'system',
        changed_date TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        milestone_id INTEGER NOT NULL REFERENCES milestones(id),
        invoice_number TEXT UNIQUE NOT NULL,
        invoice_date TEXT NOT NULL,
        invoice_amount REAL NOT NULL,
        created_date TEXT DEFAULT (datetime('now')),
        created_by TEXT NOT NULL DEFAULT 'system'
    );
    CREATE TABLE IF NOT EXISTS receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER NOT NULL REFERENCES invoices(id),
        receipt_date TEXT NOT NULL,
        receipt_amount REAL NOT NULL,
        difference REAL DEFAULT 0,
        receipt_notes TEXT DEFAULT '',
        created_date TEXT DEFAULT (datetime('now')),
        created_by TEXT NOT NULL DEFAULT 'system'
    );
    """)

    # Seed contract types if empty
    if c.execute("SELECT COUNT(*) FROM contract_types").fetchone()[0] == 0:
        c.executemany("INSERT INTO contract_types (category, subcategory) VALUES (?,?)", [
            ("SaaS","Telephony System"),("SaaS","AI Agents"),("SaaS","Transit"),("SaaS","Others"),
            ("Development Service","Telephony System"),("Development Service","AI Agents"),
            ("Development Service","Transit"),("Development Service","Others"),
            ("Support Services","Maintenance"),("Support Services","AI Agents"),
            ("Support Services","Customer Support"),("Support Services","Others"),
        ])

    # Seed companies if empty
    if c.execute("SELECT COUNT(*) FROM companies").fetchone()[0] == 0:
        c.executemany("INSERT INTO companies (name) VALUES (?)", [("IT Curves",),("AI Dev Lab",)])

    conn.commit()
    conn.close()
    print(f"  Database: {DB_PATH}")


# ══════════════════════════════════════════════════════════════
#  FRONTEND HTML (inline)
# ══════════════════════════════════════════════════════════════

FRONTEND_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Orders Backlog & Billing</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f0f2f5; }
.tab-btn { padding: 10px 24px; cursor: pointer; border: none; background: transparent;
           font-size: 14px; font-weight: 600; color: #666; border-bottom: 3px solid transparent; transition: all 0.2s; }
.tab-btn.active { color: #0f3460; border-bottom-color: #0f3460; }
.tab-btn:hover { color: #0f3460; }
.tab-content { display: none; }
.tab-content.active { display: block; }
.card { background: white; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 16px; overflow: hidden; }
.card-header { background: #f8f9fa; padding: 12px 20px; border-bottom: 1px solid #e9ecef;
               display: flex; align-items: center; justify-content: space-between; }
.card-header h3 { font-size: 15px; color: #333; font-weight: 700; }
.card-body { padding: 16px 20px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { background: #f0f2f5; padding: 8px 12px; text-align: left; font-weight: 600; color: #555; border-bottom: 2px solid #ddd; }
td { padding: 8px 12px; border-bottom: 1px solid #eee; vertical-align: middle; }
tr:hover td { background: #f8f9fa; }
.btn { padding: 6px 14px; border: none; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; transition: opacity 0.2s; }
.btn:hover { opacity: 0.85; }
.btn-primary { background: #0f3460; color: white; }
.btn-danger  { background: #dc3545; color: white; }
.btn-success { background: #28a745; color: white; }
.btn-sm { padding: 4px 10px; font-size: 12px; }
input, select, textarea { padding: 8px 12px; border: 1px solid #ddd; border-radius: 6px; font-size: 13px; width: 100%; transition: border-color 0.2s; }
input:focus, select:focus, textarea:focus { outline: none; border-color: #0f3460; }
label { display: block; font-size: 12px; font-weight: 600; color: #555; margin-bottom: 4px; text-transform: uppercase; }
.fg { margin-bottom: 12px; }
.modal-overlay { display: none; position: fixed; top:0; left:0; width:100%; height:100%;
                 background: rgba(0,0,0,0.5); z-index:1000; align-items:center; justify-content:center; }
.modal-overlay.show { display: flex; }
.modal { background: white; border-radius: 12px; max-width: 860px; width: 95%;
         max-height: 90vh; overflow-y: auto; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }
.modal-header { padding: 16px 24px; border-bottom: 1px solid #eee;
                display: flex; align-items: center; justify-content: space-between; }
.modal-header h2 { font-size: 18px; color: #333; font-weight: 700; }
.modal-close { background: none; border: none; font-size: 24px; cursor: pointer; color: #999; line-height: 1; }
.modal-body { padding: 20px 24px; }
.modal-footer { padding: 12px 24px; border-top: 1px solid #eee; display: flex; justify-content: flex-end; gap: 10px; }
.stat-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 16px; }
.stat-card { background: white; border-radius: 10px; padding: 16px 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
.stat-card .lbl { font-size: 12px; color: #888; text-transform: uppercase; }
.stat-card .val { font-size: 22px; font-weight: 700; color: #0f3460; margin-top: 4px; }
.badge { padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; white-space: nowrap; }
.bg-green { background: #d4edda; color: #155724; }
.bg-yellow { background: #fff3cd; color: #856404; }
.bg-blue { background: #d1ecf1; color: #0c5460; }
.bg-red { background: #f8d7da; color: #721c24; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
.pos { color: #28a745; font-weight: 700; }
.neg { color: #dc3545; font-weight: 700; }
.note-item { padding: 8px 12px; border-left: 3px solid #0f3460; margin-bottom: 8px;
             background: #f8f9fa; border-radius: 0 6px 6px 0; }
.note-meta { font-size: 11px; color: #888; margin-top: 2px; }
.empty { color: #999; text-align: center; padding: 24px; font-size: 13px; }
.amt { text-align: right; }
</style>
</head>
<body>

<div style="background:white;border-bottom:1px solid #ddd;padding:0 24px;display:flex;align-items:center;justify-content:space-between;">
  <div style="display:flex;gap:4px;">
    <button class="tab-btn active" onclick="switchTab('admin')">Admin Setup</button>
    <button class="tab-btn" onclick="switchTab('orders')">Orders</button>
    <button class="tab-btn" onclick="switchTab('invoices')">Invoices &amp; Receipts</button>
    <button class="tab-btn" onclick="switchTab('reports')">Reports</button>
  </div>
  <span style="font-size:13px;color:#888;">Orders Backlog &amp; Billing</span>
</div>

<div style="padding:20px 24px;max-width:1400px;margin:0 auto;">

<!-- ADMIN TAB -->
<div id="tab-admin" class="tab-content active">
  <div class="card">
    <div class="card-header"><h3>Clients</h3>
      <button class="btn btn-primary btn-sm" onclick="openClientForm()">+ Add Client</button></div>
    <div class="card-body">
      <table><thead><tr><th>Client ID</th><th>Name</th><th>Billing Name</th><th>Email</th><th>Actions</th></tr></thead>
      <tbody id="tblClients"></tbody></table>
    </div>
  </div>
  <div class="card">
    <div class="card-header"><h3>Contract Types</h3>
      <button class="btn btn-primary btn-sm" onclick="openCTForm()">+ Add Type</button></div>
    <div class="card-body">
      <table><thead><tr><th>Category</th><th>Subcategory</th><th>Actions</th></tr></thead>
      <tbody id="tblCT"></tbody></table>
    </div>
  </div>
  <div class="card">
    <div class="card-header"><h3>Companies (Performing)</h3>
      <button class="btn btn-primary btn-sm" onclick="openCompanyForm()">+ Add Company</button></div>
    <div class="card-body">
      <table><thead><tr><th>Company Name</th><th>Actions</th></tr></thead>
      <tbody id="tblCompanies"></tbody></table>
    </div>
  </div>
</div>

<!-- ORDERS TAB -->
<div id="tab-orders" class="tab-content">
  <div class="stat-cards" id="orderStats"></div>
  <div class="card">
    <div class="card-header"><h3>Orders</h3>
      <div style="display:flex;gap:10px;align-items:center;">
        <input type="text" placeholder="Search..." id="orderSearch" style="width:220px;" onkeyup="loadOrders()">
        <button class="btn btn-primary" onclick="openOrderForm()">+ New Order</button>
      </div>
    </div>
    <div class="card-body">
      <table><thead><tr><th>Order</th><th>Client</th><th>Type</th><th>Company</th><th>Date</th>
        <th class="amt">Amount</th><th>PO#</th><th>Actions</th></tr></thead>
      <tbody id="tblOrders"></tbody></table>
    </div>
  </div>
</div>

<!-- INVOICES TAB -->
<div id="tab-invoices" class="tab-content">
  <div class="card">
    <div class="card-header"><h3>Invoices &amp; Receipts</h3>
      <div style="display:flex;gap:8px;align-items:center;">
        <label style="margin:0;white-space:nowrap;">Order:</label>
        <select id="invOrderSel" onchange="loadInvoiceTab(this.value)" style="width:260px;"></select>
      </div>
    </div>
    <div class="card-body" id="invContent"><p class="empty">Select an order above to view its invoices.</p></div>
  </div>
</div>

<!-- REPORTS TAB -->
<div id="tab-reports" class="tab-content">
  <div class="card">
    <div class="card-header"><h3>Backlog Report (Unbilled Milestones)</h3>
      <div style="display:flex;gap:8px;align-items:center;">
        <label style="margin:0;">Start Month:</label>
        <input type="month" id="rptMonth" style="width:180px;">
        <button class="btn btn-primary btn-sm" onclick="runBacklog()">Generate</button>
      </div>
    </div>
    <div class="card-body" id="rptBacklog"><p class="empty">Select a month and click Generate.</p></div>
  </div>
  <div class="card">
    <div class="card-header"><h3>Milestone Schedule</h3>
      <div style="display:flex;gap:8px;align-items:center;">
        <label style="margin:0;">From:</label><input type="date" id="rptFrom" style="width:155px;">
        <label style="margin:0;">To:</label><input type="date" id="rptTo" style="width:155px;">
        <button class="btn btn-primary btn-sm" onclick="runSchedule()">Generate</button>
      </div>
    </div>
    <div class="card-body" id="rptSchedule"><p class="empty">Select dates and click Generate.</p></div>
  </div>
</div>

</div><!-- /container -->

<!-- MODALS -->

<!-- Client Modal -->
<div class="modal-overlay" id="mClient">
  <div class="modal" style="max-width:560px;">
    <div class="modal-header"><h2 id="mClientTitle">Add Client</h2>
      <button class="modal-close" onclick="closeModal('mClient')">&times;</button></div>
    <div class="modal-body">
      <input type="hidden" id="cEditId">
      <div class="grid2">
        <div class="fg"><label>Client ID (8 chars)</label><input type="text" id="cId" maxlength="8"></div>
        <div class="fg"><label>Client Name</label><input type="text" id="cName"></div>
      </div>
      <div class="fg"><label>Billing Address</label><textarea id="cAddr" rows="2"></textarea></div>
      <div class="grid2">
        <div class="fg"><label>Contact Names</label><input type="text" id="cContacts"></div>
        <div class="fg"><label>Billing Name</label><input type="text" id="cBillName"></div>
      </div>
      <div class="fg"><label>Billing Email</label><input type="email" id="cEmail"></div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mClient')">Cancel</button>
      <button class="btn btn-primary" onclick="saveClient()">Save</button>
    </div>
  </div>
</div>

<!-- Contract Type Modal -->
<div class="modal-overlay" id="mCT">
  <div class="modal" style="max-width:380px;">
    <div class="modal-header"><h2>Add Contract Type</h2>
      <button class="modal-close" onclick="closeModal('mCT')">&times;</button></div>
    <div class="modal-body">
      <div class="fg"><label>Category</label>
        <select id="ctCat"><option>SaaS</option><option>Development Service</option><option>Support Services</option></select></div>
      <div class="fg"><label>Subcategory</label><input type="text" id="ctSub"></div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mCT')">Cancel</button>
      <button class="btn btn-primary" onclick="saveCT()">Save</button>
    </div>
  </div>
</div>

<!-- Company Modal -->
<div class="modal-overlay" id="mCompany">
  <div class="modal" style="max-width:380px;">
    <div class="modal-header"><h2>Add Company</h2>
      <button class="modal-close" onclick="closeModal('mCompany')">&times;</button></div>
    <div class="modal-body">
      <div class="fg"><label>Company Name</label><input type="text" id="coName"></div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mCompany')">Cancel</button>
      <button class="btn btn-primary" onclick="saveCompany()">Save</button>
    </div>
  </div>
</div>

<!-- Order Modal -->
<div class="modal-overlay" id="mOrder">
  <div class="modal">
    <div class="modal-header"><h2 id="mOrderTitle">New Order</h2>
      <button class="modal-close" onclick="closeModal('mOrder')">&times;</button></div>
    <div class="modal-body">
      <input type="hidden" id="oEditId">
      <div class="grid3">
        <div class="fg"><label>Order Name (12 chars)</label><input type="text" id="oName" maxlength="12"></div>
        <div class="fg"><label>Client</label><select id="oClient"></select></div>
        <div class="fg"><label>Contract Type</label><select id="oCT"></select></div>
      </div>
      <div class="grid3">
        <div class="fg"><label>Company</label><select id="oCompany"></select></div>
        <div class="fg"><label>Date of Order</label><input type="date" id="oDate"></div>
        <div class="fg"><label>PO Number</label><input type="text" id="oPO"></div>
      </div>
      <div class="grid2">
        <div class="fg"><label>Contract Amount ($)</label><input type="number" id="oAmount" step="0.01" min="0"></div>
        <div></div>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mOrder')">Cancel</button>
      <button class="btn btn-primary" onclick="saveOrder()">Save Order</button>
    </div>
  </div>
</div>

<!-- Order Detail Modal -->
<div class="modal-overlay" id="mDetail">
  <div class="modal" style="max-width:1000px;">
    <div class="modal-header"><h2 id="mDetailTitle">Order</h2>
      <button class="modal-close" onclick="closeModal('mDetail')">&times;</button></div>
    <div class="modal-body" id="mDetailBody"></div>
  </div>
</div>

<!-- Milestone Modal -->
<div class="modal-overlay" id="mMS">
  <div class="modal" style="max-width:500px;">
    <div class="modal-header"><h2 id="mMSTitle">Add Milestone</h2>
      <button class="modal-close" onclick="closeModal('mMS')">&times;</button></div>
    <div class="modal-body">
      <input type="hidden" id="msEditId"><input type="hidden" id="msOrderId">
      <div class="grid2">
        <div class="fg"><label>Name (12 chars)</label><input type="text" id="msName" maxlength="12"></div>
        <div class="fg"><label>Scheduled Date</label><input type="date" id="msDate"></div>
      </div>
      <div class="grid2">
        <div class="fg"><label>Amount ($)</label><input type="number" id="msAmt" step="0.01" min="0"></div>
        <div class="fg"><label>Type</label><select id="msType"><option>Estimate</option><option>Confirmed</option></select></div>
      </div>
      <div class="fg"><label>Description (50 chars)</label><input type="text" id="msDesc" maxlength="50"></div>
      <div id="msReschedDiv" style="display:none;">
        <div class="fg"><label>Rescheduled Date</label><input type="date" id="msResDate"></div>
        <div class="fg"><label>Reason</label><input type="text" id="msResReason"></div>
        <div class="fg"><label>Change Reason (audit)</label><input type="text" id="msChangeReason"></div>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mMS')">Cancel</button>
      <button class="btn btn-primary" onclick="saveMS()">Save</button>
    </div>
  </div>
</div>

<!-- Invoice Modal -->
<div class="modal-overlay" id="mInv">
  <div class="modal" style="max-width:460px;">
    <div class="modal-header"><h2>Create Invoice</h2>
      <button class="modal-close" onclick="closeModal('mInv')">&times;</button></div>
    <div class="modal-body">
      <input type="hidden" id="invOrdId">
      <div class="fg"><label>Milestone</label><select id="invMS" onchange="fillInvAmt()"></select></div>
      <div class="grid2">
        <div class="fg"><label>Invoice Number</label><input type="text" id="invNum"></div>
        <div class="fg"><label>Invoice Date</label><input type="date" id="invDate"></div>
      </div>
      <div class="fg"><label>Amount ($)</label><input type="number" id="invAmt" step="0.01" min="0"></div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mInv')">Cancel</button>
      <button class="btn btn-primary" onclick="saveInv()">Save Invoice</button>
    </div>
  </div>
</div>

<!-- Receipt Modal -->
<div class="modal-overlay" id="mRcpt">
  <div class="modal" style="max-width:420px;">
    <div class="modal-header"><h2>Record Receipt</h2>
      <button class="modal-close" onclick="closeModal('mRcpt')">&times;</button></div>
    <div class="modal-body">
      <input type="hidden" id="rcptInvId">
      <div class="grid2">
        <div class="fg"><label>Date</label><input type="date" id="rcptDate"></div>
        <div class="fg"><label>Amount ($)</label><input type="number" id="rcptAmt" step="0.01" min="0"></div>
      </div>
      <div class="fg"><label>Notes</label><input type="text" id="rcptNotes"></div>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('mRcpt')">Cancel</button>
      <button class="btn btn-primary" onclick="saveRcpt()">Save Receipt</button>
    </div>
  </div>
</div>

<script>
const API = '/api';
let clients=[], ctypes=[], companies=[], allMS=[];

// ── Init ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadAdmin();
  loadOrders();
  const now = new Date();
  document.getElementById('rptMonth').value = now.toISOString().slice(0,7);
  document.getElementById('rptFrom').value = now.toISOString().slice(0,10);
  document.getElementById('rptTo').value = new Date(now.getFullYear(),11,31).toISOString().slice(0,10);
});

// ── API helper ────────────────────────────────────────────────
async function api(method, path, body) {
  const opt = {method, headers:{'Content-Type':'application/json'}};
  if (body) opt.body = JSON.stringify(body);
  const r = await fetch(API+path, opt);
  if (!r.ok) { const e=await r.json().catch(()=>({})); throw new Error(e.detail||'Error '+r.status); }
  return r.json();
}

const fmt = n => '$'+Number(n||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});

// ── Tabs ──────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  const map = {admin:0,orders:1,invoices:2,reports:3};
  document.querySelectorAll('.tab-btn')[map[name]].classList.add('active');
  document.getElementById('tab-'+name).classList.add('active');
  if (name==='invoices') fillInvOrderDrop();
  if (name==='orders') loadOrders();
}
function showModal(id) { document.getElementById(id).classList.add('show'); }
function closeModal(id) { document.getElementById(id).classList.remove('show'); }

// ── Admin ─────────────────────────────────────────────────────
async function loadAdmin() {
  clients = await api('GET','/clients');
  ctypes  = await api('GET','/contract-types');
  companies = await api('GET','/companies');
  // Clients table
  document.getElementById('tblClients').innerHTML = clients.map(c=>`<tr>
    <td><b>${c.client_id}</b></td><td>${c.name}</td><td>${c.billing_name||''}</td><td>${c.billing_email||''}</td>
    <td><button class="btn btn-sm btn-primary" onclick="editClient(${c.id})">Edit</button>
        <button class="btn btn-sm btn-danger" onclick="delClient(${c.id})">Del</button></td></tr>`).join('')||'<tr><td colspan="5" class="empty">No clients yet.</td></tr>';
  // CT table
  document.getElementById('tblCT').innerHTML = ctypes.map(ct=>`<tr>
    <td>${ct.category}</td><td>${ct.subcategory}</td>
    <td><button class="btn btn-sm btn-danger" onclick="delCT(${ct.id})">Del</button></td></tr>`).join('');
  // Companies table
  document.getElementById('tblCompanies').innerHTML = companies.map(co=>`<tr>
    <td>${co.name}</td>
    <td><button class="btn btn-sm btn-danger" onclick="delCompany(${co.id})">Del</button></td></tr>`).join('');
}

// Client form
function openClientForm(){
  ['cEditId','cId','cName','cAddr','cContacts','cBillName','cEmail'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('mClientTitle').textContent='Add Client'; showModal('mClient');
}
function editClient(id){
  const c=clients.find(x=>x.id===id); if(!c) return;
  document.getElementById('cEditId').value=c.id; document.getElementById('cId').value=c.client_id;
  document.getElementById('cName').value=c.name; document.getElementById('cAddr').value=c.billing_address||'';
  document.getElementById('cContacts').value=c.contact_names||''; document.getElementById('cBillName').value=c.billing_name||'';
  document.getElementById('cEmail').value=c.billing_email||'';
  document.getElementById('mClientTitle').textContent='Edit Client'; showModal('mClient');
}
async function saveClient(){
  const eid=document.getElementById('cEditId').value;
  const d={client_id:document.getElementById('cId').value.trim(),name:document.getElementById('cName').value.trim(),
    billing_address:document.getElementById('cAddr').value.trim(),contact_names:document.getElementById('cContacts').value.trim(),
    billing_name:document.getElementById('cBillName').value.trim(),billing_email:document.getElementById('cEmail').value.trim()};
  if(!d.client_id||!d.name) return alert('Client ID and Name required');
  try { eid ? await api('PUT',`/clients/${eid}`,d) : await api('POST','/clients',d);
    closeModal('mClient'); loadAdmin(); } catch(e){alert(e.message);}
}
async function delClient(id){
  if(!confirm('Delete this client?')) return;
  try{await api('DELETE',`/clients/${id}`); loadAdmin();}catch(e){alert(e.message);}
}

// CT
function openCTForm(){ showModal('mCT'); }
async function saveCT(){
  const d={category:document.getElementById('ctCat').value,subcategory:document.getElementById('ctSub').value.trim()};
  if(!d.subcategory) return alert('Subcategory required');
  try{await api('POST','/contract-types',d); closeModal('mCT'); loadAdmin();}catch(e){alert(e.message);}
}
async function delCT(id){
  if(!confirm('Delete?')) return;
  try{await api('DELETE',`/contract-types/${id}`); loadAdmin();}catch(e){alert(e.message);}
}

// Company
function openCompanyForm(){ document.getElementById('coName').value=''; showModal('mCompany'); }
async function saveCompany(){
  const name=document.getElementById('coName').value.trim();
  if(!name) return alert('Name required');
  try{await api('POST','/companies',{name}); closeModal('mCompany'); loadAdmin();}catch(e){alert(e.message);}
}
async function delCompany(id){
  if(!confirm('Delete?')) return;
  try{await api('DELETE',`/companies/${id}`); loadAdmin();}catch(e){alert(e.message);}
}

// ── Orders ────────────────────────────────────────────────────
async function loadOrders(){
  const s=document.getElementById('orderSearch')?.value||'';
  const orders=await api('GET',`/orders?search=${encodeURIComponent(s)}`);
  const total=orders.reduce((a,o)=>a+(o.contract_amount||0),0);
  document.getElementById('orderStats').innerHTML=`
    <div class="stat-card"><div class="lbl">Total Orders</div><div class="val">${orders.length}</div></div>
    <div class="stat-card"><div class="lbl">Total Contract Value</div><div class="val">${fmt(total)}</div></div>`;
  document.getElementById('tblOrders').innerHTML=orders.map(o=>`<tr>
    <td><a href="#" onclick="openDetail(${o.id});return false;" style="color:#0f3460;font-weight:700;">${o.order_name}</a></td>
    <td>${o.client_name||''}</td><td style="font-size:12px;">${o.contract_type||''}</td><td>${o.company_name||''}</td>
    <td>${o.date_of_order||''}</td><td class="amt">${fmt(o.contract_amount)}</td><td>${o.po_number||''}</td>
    <td><button class="btn btn-sm btn-primary" onclick="editOrder(${o.id})">Edit</button>
        <button class="btn btn-sm btn-danger" onclick="delOrder(${o.id})">Del</button></td></tr>`)
    .join('')||'<tr><td colspan="8" class="empty">No orders yet. Click "+ New Order".</td></tr>';
}

function fillOrderDrops(){
  document.getElementById('oClient').innerHTML='<option value="">-- Client --</option>'+clients.map(c=>`<option value="${c.id}">${c.client_id} – ${c.name}</option>`).join('');
  document.getElementById('oCT').innerHTML='<option value="">-- Type --</option>'+ctypes.map(ct=>`<option value="${ct.id}">${ct.category} – ${ct.subcategory}</option>`).join('');
  document.getElementById('oCompany').innerHTML='<option value="">-- Company --</option>'+companies.map(co=>`<option value="${co.id}">${co.name}</option>`).join('');
}
function openOrderForm(){
  fillOrderDrops();
  ['oEditId','oName','oPO','oAmount'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('oDate').value=new Date().toISOString().slice(0,10);
  document.getElementById('mOrderTitle').textContent='New Order'; showModal('mOrder');
}
async function editOrder(id){
  fillOrderDrops();
  const o=await api('GET',`/orders/${id}`);
  document.getElementById('oEditId').value=o.id; document.getElementById('oName').value=o.order_name;
  document.getElementById('oClient').value=o.client_id; document.getElementById('oCT').value=o.contract_type_id;
  document.getElementById('oCompany').value=o.company_id; document.getElementById('oDate').value=o.date_of_order;
  document.getElementById('oPO').value=o.po_number||''; document.getElementById('oAmount').value=o.contract_amount||0;
  document.getElementById('mOrderTitle').textContent='Edit Order'; showModal('mOrder');
}
async function saveOrder(){
  const eid=document.getElementById('oEditId').value;
  const d={order_name:document.getElementById('oName').value.trim(),
    client_id:parseInt(document.getElementById('oClient').value),
    contract_type_id:parseInt(document.getElementById('oCT').value),
    company_id:parseInt(document.getElementById('oCompany').value),
    date_of_order:document.getElementById('oDate').value,
    po_number:document.getElementById('oPO').value.trim(),
    contract_amount:parseFloat(document.getElementById('oAmount').value)||0};
  if(!d.order_name||!d.client_id||!d.contract_type_id||!d.company_id||!d.date_of_order) return alert('Fill all required fields');
  try{ eid ? await api('PUT',`/orders/${eid}`,d) : await api('POST','/orders',d);
    closeModal('mOrder'); loadOrders(); }catch(e){alert(e.message);}
}
async function delOrder(id){
  if(!confirm('Delete this order?')) return;
  try{await api('DELETE',`/orders/${id}`); loadOrders();}catch(e){alert(e.message);}
}

// ── Order Detail ──────────────────────────────────────────────
async function openDetail(id){
  const o=await api('GET',`/orders/${id}`);
  document.getElementById('mDetailTitle').textContent=`Order: ${o.order_name}`;
  const dc=o.amount_difference>0.01?'pos':o.amount_difference<-0.01?'neg':'';
  const msRows=o.milestones.map(m=>`<tr>
    <td>${m.milestone_name}</td><td>${m.scheduled_date}</td>
    <td class="amt">${fmt(m.payment_amount)}</td>
    <td><span class="badge ${m.milestone_type==='Confirmed'?'bg-green':'bg-yellow'}">${m.milestone_type}</span></td>
    <td>${m.is_billed?'<span class="badge bg-blue">Billed</span>':''}</td>
    <td style="font-size:12px;">${m.description||''}</td>
    <td><button class="btn btn-sm btn-primary" onclick="editMS(${m.id},${id})">Edit</button>
        <button class="btn btn-sm btn-danger" onclick="delMS(${m.id},${id})">Del</button></td></tr>`).join('');
  const noteRows=o.notes.map(n=>`<div class="note-item"><div>${n.note_text}</div><div class="note-meta">${n.note_date} — ${n.login_name}</div></div>`).join('')||'<p style="color:#888;font-size:13px;">No notes.</p>';
  document.getElementById('mDetailBody').innerHTML=`
    <div class="grid2" style="margin-bottom:14px;">
      <div><p><b>Client:</b> ${o.client_code||''} — ${o.client_name||''}</p>
           <p><b>Type:</b> ${o.contract_type||''}</p><p><b>Company:</b> ${o.company_name||''}</p></div>
      <div><p><b>Date:</b> ${o.date_of_order||''}</p><p><b>PO#:</b> ${o.po_number||'N/A'}</p>
           <p><b>Contract:</b> ${fmt(o.contract_amount)}</p></div>
    </div>
    <div class="stat-cards" style="margin-bottom:14px;">
      <div class="stat-card"><div class="lbl">Contract</div><div class="val">${fmt(o.contract_amount)}</div></div>
      <div class="stat-card"><div class="lbl">Milestones Total</div><div class="val">${fmt(o.milestone_total)}</div></div>
      <div class="stat-card"><div class="lbl">Difference</div><div class="val ${dc}">${fmt(o.amount_difference)}</div></div>
    </div>
    <div class="card" style="box-shadow:none;border:1px solid #eee;">
      <div class="card-header"><h3>Milestones (${o.milestones.length}/15)</h3>
        <button class="btn btn-primary btn-sm" onclick="openMSForm(${id})">+ Add Milestone</button></div>
      <div class="card-body"><table><thead><tr><th>Name</th><th>Date</th><th class="amt">Amount</th>
        <th>Type</th><th>Billed</th><th>Desc</th><th>Actions</th></tr></thead>
        <tbody>${msRows||'<tr><td colspan="7" class="empty">No milestones yet.</td></tr>'}</tbody></table></div>
    </div>
    <div class="card" style="box-shadow:none;border:1px solid #eee;margin-top:12px;">
      <div class="card-header"><h3>Notes</h3>
        <div style="display:flex;gap:8px;">
          <input type="text" id="newNote" maxlength="50" placeholder="Add a note (50 chars max)..." style="width:280px;">
          <button class="btn btn-primary btn-sm" onclick="addNote(${id})">Add</button>
        </div>
      </div>
      <div class="card-body">${noteRows}</div>
    </div>`;
  showModal('mDetail');
}

// ── Milestones ────────────────────────────────────────────────
function openMSForm(orderId){
  document.getElementById('msEditId').value=''; document.getElementById('msOrderId').value=orderId;
  ['msName','msDate','msAmt','msDesc','msResDate','msResReason','msChangeReason'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('msType').value='Estimate'; document.getElementById('msReschedDiv').style.display='none';
  document.getElementById('mMSTitle').textContent='Add Milestone'; showModal('mMS');
}
async function editMS(msId,orderId){
  const msList=await api('GET',`/orders/${orderId}/milestones`);
  const m=msList.find(x=>x.id===msId); if(!m) return;
  document.getElementById('msEditId').value=msId; document.getElementById('msOrderId').value=orderId;
  document.getElementById('msName').value=m.milestone_name; document.getElementById('msDate').value=m.scheduled_date;
  document.getElementById('msAmt').value=m.payment_amount; document.getElementById('msType').value=m.milestone_type;
  document.getElementById('msDesc').value=m.description||''; document.getElementById('msResDate').value=m.rescheduled_date||'';
  document.getElementById('msResReason').value=m.rescheduling_reason||''; document.getElementById('msChangeReason').value='';
  document.getElementById('msReschedDiv').style.display='block';
  document.getElementById('mMSTitle').textContent='Edit Milestone'; showModal('mMS');
}
async function saveMS(){
  const eid=document.getElementById('msEditId').value, oid=document.getElementById('msOrderId').value;
  const d={milestone_name:document.getElementById('msName').value.trim(),
    scheduled_date:document.getElementById('msDate').value,
    payment_amount:parseFloat(document.getElementById('msAmt').value)||0,
    milestone_type:document.getElementById('msType').value,
    description:document.getElementById('msDesc').value.trim(), modified_by:'User'};
  if(!d.milestone_name||!d.scheduled_date) return alert('Name and date required');
  try{
    if(eid){ d.rescheduled_date=document.getElementById('msResDate').value||null;
      d.rescheduling_reason=document.getElementById('msResReason').value||'';
      d.change_reason=document.getElementById('msChangeReason').value||'';
      await api('PUT',`/milestones/${eid}`,d); }
    else { await api('POST',`/orders/${oid}/milestones`,d); }
    closeModal('mMS'); openDetail(parseInt(oid));
  }catch(e){alert(e.message);}
}
async function delMS(msId,orderId){
  if(!confirm('Delete milestone?')) return;
  try{await api('DELETE',`/milestones/${msId}`); openDetail(orderId);}catch(e){alert(e.message);}
}
async function addNote(orderId){
  const t=document.getElementById('newNote').value.trim();
  if(!t) return alert('Enter note text');
  try{await api('POST',`/orders/${orderId}/notes`,{note_text:t,login_name:'User'}); openDetail(orderId);}catch(e){alert(e.message);}
}

// ── Invoices Tab ──────────────────────────────────────────────
async function fillInvOrderDrop(){
  const orders=await api('GET','/orders');
  document.getElementById('invOrderSel').innerHTML='<option value="">Select order...</option>'+
    orders.map(o=>`<option value="${o.id}">${o.order_name} — ${o.client_name||''} (${fmt(o.contract_amount)})</option>`).join('');
}
async function loadInvoiceTab(orderId){
  if(!orderId){document.getElementById('invContent').innerHTML='<p class="empty">Select an order.</p>'; return;}
  const order=await api('GET',`/orders/${orderId}`);
  const invs=await api('GET',`/orders/${orderId}/invoices`);
  let rows='';
  for(const inv of invs){
    const rcpts=await api('GET',`/invoices/${inv.id}/receipts`);
    rows+=`<tr><td><b>${inv.invoice_number}</b></td><td>${inv.milestone_name||''}</td>
      <td>${inv.invoice_date||''}</td><td class="amt">${fmt(inv.invoice_amount)}</td>
      <td class="amt">${fmt(inv.total_received)}</td>
      <td class="amt ${inv.balance>0.01?'neg':''}">${fmt(inv.balance)}</td>
      <td><button class="btn btn-sm btn-success" onclick="openRcpt(${inv.id})">+ Receipt</button></td></tr>`;
    rows+=rcpts.map(r=>`<tr style="background:#f9fafb;">
      <td style="padding-left:36px;color:#888;">↳ Receipt</td><td></td>
      <td>${r.receipt_date||''}</td><td class="amt">${fmt(r.receipt_amount)}</td>
      <td></td><td class="amt ${r.difference>0.01?'pos':r.difference<-0.01?'neg':''}">${fmt(r.difference)}</td>
      <td style="font-size:12px;color:#888;">${r.receipt_notes||''}</td></tr>`).join('');
  }
  document.getElementById('invContent').innerHTML=`
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
      <h3 style="font-weight:700;">${order.order_name} — ${order.client_name||''}</h3>
      <button class="btn btn-primary" onclick="openInvForm(${orderId})">+ New Invoice</button>
    </div>
    <table><thead><tr><th>Invoice #</th><th>Milestone</th><th>Date</th><th class="amt">Amount</th>
      <th class="amt">Received</th><th class="amt">Balance</th><th>Actions</th></tr></thead>
    <tbody>${rows||'<tr><td colspan="7" class="empty">No invoices yet.</td></tr>'}</tbody></table>`;
}
async function openInvForm(orderId){
  document.getElementById('invOrdId').value=orderId;
  allMS=await api('GET',`/orders/${orderId}/milestones`);
  const unbilled=allMS.filter(m=>!m.is_billed);
  if(!unbilled.length){alert('No unbilled milestones for this order.');return;}
  document.getElementById('invMS').innerHTML=unbilled.map(m=>`<option value="${m.id}" data-amt="${m.payment_amount}">${m.milestone_name} — ${fmt(m.payment_amount)}</option>`).join('');
  document.getElementById('invNum').value=''; document.getElementById('invDate').value=new Date().toISOString().slice(0,10);
  fillInvAmt(); showModal('mInv');
}
function fillInvAmt(){
  const sel=document.getElementById('invMS'); const opt=sel.options[sel.selectedIndex];
  if(opt) document.getElementById('invAmt').value=opt.dataset.amt||0;
}
async function saveInv(){
  const oid=document.getElementById('invOrdId').value;
  const d={milestone_id:parseInt(document.getElementById('invMS').value),
    invoice_number:document.getElementById('invNum').value.trim(),
    invoice_date:document.getElementById('invDate').value,
    invoice_amount:parseFloat(document.getElementById('invAmt').value)||0};
  if(!d.invoice_number||!d.invoice_date) return alert('Invoice number and date required');
  try{await api('POST',`/orders/${oid}/invoices`,d); closeModal('mInv'); loadInvoiceTab(oid);}catch(e){alert(e.message);}
}
function openRcpt(invId){
  document.getElementById('rcptInvId').value=invId;
  document.getElementById('rcptDate').value=new Date().toISOString().slice(0,10);
  document.getElementById('rcptAmt').value=''; document.getElementById('rcptNotes').value='';
  showModal('mRcpt');
}
async function saveRcpt(){
  const invId=document.getElementById('rcptInvId').value;
  const d={receipt_date:document.getElementById('rcptDate').value,
    receipt_amount:parseFloat(document.getElementById('rcptAmt').value)||0,
    receipt_notes:document.getElementById('rcptNotes').value.trim()};
  if(!d.receipt_date||!d.receipt_amount) return alert('Date and amount required');
  try{await api('POST',`/invoices/${invId}/receipts`,d); closeModal('mRcpt');
    const oid=document.getElementById('invOrderSel').value; if(oid) loadInvoiceTab(oid);}catch(e){alert(e.message);}
}

// ── Reports ───────────────────────────────────────────────────
async function runBacklog(){
  const m=document.getElementById('rptMonth').value; if(!m) return alert('Select a month');
  const d=await api('GET',`/reports/backlog?start_month=${m}`);
  document.getElementById('rptBacklog').innerHTML=`
    <p style="margin-bottom:10px;"><b>Period:</b> ${d.period} &nbsp;|&nbsp; <b>Total Unbilled:</b> ${fmt(d.total)}</p>
    <table><thead><tr><th>Order</th><th>Client</th><th>Milestone</th><th>Date</th><th class="amt">Amount</th><th>Type</th></tr></thead>
    <tbody>${d.rows.map(r=>`<tr><td>${r.order_name}</td><td>${r.client_name}</td><td>${r.milestone_name}</td>
      <td>${r.scheduled_date}</td><td class="amt">${fmt(r.payment_amount)}</td>
      <td><span class="badge ${r.milestone_type==='Confirmed'?'bg-green':'bg-yellow'}">${r.milestone_type}</span></td></tr>`).join('')
      ||'<tr><td colspan="6" class="empty">No unbilled milestones in this period.</td></tr>'}</tbody></table>`;
}
async function runSchedule(){
  const f=document.getElementById('rptFrom').value, t=document.getElementById('rptTo').value;
  if(!f||!t) return alert('Select both dates');
  const d=await api('GET',`/reports/milestone-schedule?date_from=${f}&date_to=${t}`);
  document.getElementById('rptSchedule').innerHTML=`
    <p style="margin-bottom:10px;"><b>Period:</b> ${f} to ${t} &nbsp;|&nbsp; <b>Total:</b> ${fmt(d.total)}</p>
    <table><thead><tr><th>Order</th><th>Client</th><th>Milestone</th><th>Date</th><th class="amt">Amount</th><th>Type</th><th>Billed</th></tr></thead>
    <tbody>${d.rows.map(r=>`<tr><td>${r.order_name}</td><td>${r.client_name}</td><td>${r.milestone_name}</td>
      <td>${r.scheduled_date}</td><td class="amt">${fmt(r.payment_amount)}</td>
      <td><span class="badge ${r.milestone_type==='Confirmed'?'bg-green':'bg-yellow'}">${r.milestone_type}</span></td>
      <td>${r.is_billed?'<span class="badge bg-blue">Billed</span>':''}</td></tr>`).join('')
      ||'<tr><td colspan="7" class="empty">No milestones in this period.</td></tr>'}</tbody></table>`;
}
</script>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════
#  HTTP HANDLER
# ══════════════════════════════════════════════════════════════

def r2d(row): return dict(row) if row else None
def r2l(rows): return [dict(r) for r in rows]


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]}")

    def _send(self, code, body, ct="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        if isinstance(body, str): body = body.encode()
        self.wfile.write(body)

    def _j(self, code, data): self._send(code, json.dumps(data))
    def _body(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n)) if n else {}

    def do_OPTIONS(self): self._send(200, "")

    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        path = p.path.rstrip("/")
        qs = urllib.parse.parse_qs(p.query)
        if path in ("", "/"): self._send(200, FRONTEND_HTML, "text/html"); return

        db = get_db(); c = db.cursor()
        try:
            if path == "/api/clients":
                self._j(200, r2l(c.execute("SELECT * FROM clients ORDER BY name").fetchall())); return
            if path == "/api/contract-types":
                self._j(200, r2l(c.execute("SELECT * FROM contract_types ORDER BY category,subcategory").fetchall())); return
            if path == "/api/companies":
                self._j(200, r2l(c.execute("SELECT * FROM companies ORDER BY name").fetchall())); return

            if path == "/api/orders":
                s = qs.get("search",[""])[0]
                sql = """SELECT o.*,cl.name client_name,cl.client_id client_code,
                    ct.category||' – '||ct.subcategory contract_type,co.name company_name
                    FROM orders o LEFT JOIN clients cl ON o.client_id=cl.id
                    LEFT JOIN contract_types ct ON o.contract_type_id=ct.id
                    LEFT JOIN companies co ON o.company_id=co.id WHERE o.is_deleted=0"""
                params = []
                if s: sql += " AND o.order_name LIKE ?"; params.append(f"%{s}%")
                sql += " ORDER BY o.date_of_order DESC"
                self._j(200, r2l(c.execute(sql, params).fetchall())); return

            m = re.match(r"^/api/orders/(\d+)$", path)
            if m:
                oid = int(m.group(1))
                o = r2d(c.execute("""SELECT o.*,cl.name client_name,cl.client_id client_code,
                    ct.category||' – '||ct.subcategory contract_type,co.name company_name
                    FROM orders o LEFT JOIN clients cl ON o.client_id=cl.id
                    LEFT JOIN contract_types ct ON o.contract_type_id=ct.id
                    LEFT JOIN companies co ON o.company_id=co.id WHERE o.id=?""", (oid,)).fetchone())
                if not o: self._j(404,{"detail":"Not found"}); return
                mss = r2l(c.execute("SELECT * FROM milestones WHERE order_id=? ORDER BY scheduled_date",(oid,)).fetchall())
                notes = r2l(c.execute("SELECT * FROM order_notes WHERE order_id=? ORDER BY note_date DESC",(oid,)).fetchall())
                mt = sum(x.get("payment_amount",0) or 0 for x in mss)
                o["milestones"]=mss; o["notes"]=notes; o["milestone_total"]=mt
                o["amount_difference"]=(o.get("contract_amount",0) or 0)-mt
                self._j(200,o); return

            m = re.match(r"^/api/orders/(\d+)/milestones$", path)
            if m:
                self._j(200, r2l(c.execute("SELECT * FROM milestones WHERE order_id=? ORDER BY scheduled_date",(int(m.group(1)),)).fetchall())); return

            m = re.match(r"^/api/orders/(\d+)/notes$", path)
            if m:
                self._j(200, r2l(c.execute("SELECT * FROM order_notes WHERE order_id=? ORDER BY note_date DESC",(int(m.group(1)),)).fetchall())); return

            m = re.match(r"^/api/orders/(\d+)/invoices$", path)
            if m:
                oid=int(m.group(1))
                invs=r2l(c.execute("""SELECT i.*,ms.milestone_name FROM invoices i
                    LEFT JOIN milestones ms ON i.milestone_id=ms.id WHERE i.order_id=? ORDER BY i.invoice_date DESC""",(oid,)).fetchall())
                for inv in invs:
                    rcpts=c.execute("SELECT * FROM receipts WHERE invoice_id=?",(inv["id"],)).fetchall()
                    tr=sum(r["receipt_amount"] or 0 for r in rcpts)
                    inv["total_received"]=tr; inv["balance"]=(inv["invoice_amount"] or 0)-tr
                self._j(200,invs); return

            m = re.match(r"^/api/invoices/(\d+)/receipts$", path)
            if m:
                self._j(200, r2l(c.execute("""SELECT r.*,i.invoice_number FROM receipts r
                    LEFT JOIN invoices i ON r.invoice_id=i.id WHERE r.invoice_id=? ORDER BY r.receipt_date DESC""",(int(m.group(1)),)).fetchall())); return

            if path == "/api/reports/backlog":
                sm=qs.get("start_month",[datetime.utcnow().strftime("%Y-%m")])[0]
                sd=sm+"-01"; yr,mo=int(sm[:4]),int(sm[5:7]); ey=yr+1; em=mo
                from calendar import monthrange; _,ld=monthrange(ey,em)
                ed=f"{ey}-{em:02d}-{ld:02d}"
                rows=r2l(c.execute("""SELECT m.*,o.order_name,cl.name client_name FROM milestones m
                    JOIN orders o ON m.order_id=o.id LEFT JOIN clients cl ON o.client_id=cl.id
                    WHERE o.is_deleted=0 AND m.is_billed=0 AND m.scheduled_date>=? AND m.scheduled_date<=?
                    ORDER BY m.scheduled_date""",(sd,ed)).fetchall())
                total=sum(r.get("payment_amount",0) or 0 for r in rows)
                self._j(200,{"rows":[{"order_name":r.get("order_name",""),"client_name":r.get("client_name",""),
                    "milestone_name":r.get("milestone_name",""),"scheduled_date":r.get("scheduled_date",""),
                    "payment_amount":r.get("payment_amount",0),"milestone_type":r.get("milestone_type","")} for r in rows],
                    "total":total,"start_month":sm,"period":f"{sm} — {ey}-{em:02d}"}); return

            if path == "/api/reports/milestone-schedule":
                df=qs.get("date_from",[datetime.utcnow().strftime("%Y-%m-%d")])[0]
                dt=qs.get("date_to",[datetime(datetime.utcnow().year,12,31).strftime("%Y-%m-%d")])[0]
                rows=r2l(c.execute("""SELECT m.*,o.order_name,cl.name client_name FROM milestones m
                    JOIN orders o ON m.order_id=o.id LEFT JOIN clients cl ON o.client_id=cl.id
                    WHERE o.is_deleted=0 AND m.scheduled_date>=? AND m.scheduled_date<=?
                    ORDER BY m.scheduled_date""",(df,dt)).fetchall())
                total=sum(r.get("payment_amount",0) or 0 for r in rows)
                self._j(200,{"rows":[{"order_name":r.get("order_name",""),"client_name":r.get("client_name",""),
                    "milestone_name":r.get("milestone_name",""),"scheduled_date":r.get("scheduled_date",""),
                    "payment_amount":r.get("payment_amount",0),"milestone_type":r.get("milestone_type",""),
                    "is_billed":bool(r.get("is_billed",0))} for r in rows],
                    "total":total,"date_from":df,"date_to":dt}); return

            self._j(404,{"detail":"Not found"})
        except Exception as e:
            traceback.print_exc(); self._j(500,{"detail":str(e)})
        finally: db.close()

    def do_POST(self):
        p = urllib.parse.urlparse(self.path).path.rstrip("/")
        d = self._body(); db = get_db(); c = db.cursor()
        try:
            if p=="/api/clients":
                if c.execute("SELECT id FROM clients WHERE client_id=?",(d["client_id"],)).fetchone():
                    self._j(400,{"detail":f"Client ID '{d['client_id']}' already exists"}); return
                c.execute("INSERT INTO clients (client_id,name,billing_address,contact_names,billing_name,billing_email) VALUES (?,?,?,?,?,?)",
                    (d["client_id"],d["name"],d.get("billing_address",""),d.get("contact_names",""),d.get("billing_name",""),d.get("billing_email","")))
                db.commit(); self._j(200, r2d(c.execute("SELECT * FROM clients WHERE id=?",(c.lastrowid,)).fetchone())); return

            if p=="/api/contract-types":
                c.execute("INSERT INTO contract_types (category,subcategory) VALUES (?,?)",(d["category"],d["subcategory"]))
                db.commit(); self._j(200,r2d(c.execute("SELECT * FROM contract_types WHERE id=?",(c.lastrowid,)).fetchone())); return

            if p=="/api/companies":
                if c.execute("SELECT id FROM companies WHERE name=?",(d["name"],)).fetchone():
                    self._j(400,{"detail":f"Company '{d['name']}' already exists"}); return
                c.execute("INSERT INTO companies (name) VALUES (?)",(d["name"],))
                db.commit(); self._j(200,r2d(c.execute("SELECT * FROM companies WHERE id=?",(c.lastrowid,)).fetchone())); return

            if p=="/api/orders":
                c.execute("INSERT INTO orders (client_id,order_name,contract_type_id,company_id,date_of_order,po_number,contract_amount,created_by) VALUES (?,?,?,?,?,?,?,?)",
                    (d["client_id"],d["order_name"],d["contract_type_id"],d["company_id"],d["date_of_order"],d.get("po_number",""),d.get("contract_amount",0),"User"))
                db.commit()
                row=r2d(c.execute("""SELECT o.*,cl.name client_name,cl.client_id client_code,
                    ct.category||' – '||ct.subcategory contract_type,co.name company_name
                    FROM orders o LEFT JOIN clients cl ON o.client_id=cl.id
                    LEFT JOIN contract_types ct ON o.contract_type_id=ct.id
                    LEFT JOIN companies co ON o.company_id=co.id WHERE o.id=?""",(c.lastrowid,)).fetchone())
                self._j(200,row); return

            m=re.match(r"^/api/orders/(\d+)/milestones$",p)
            if m:
                oid=int(m.group(1))
                cnt=c.execute("SELECT COUNT(*) FROM milestones WHERE order_id=?",(oid,)).fetchone()[0]
                if cnt>=15: self._j(400,{"detail":"Max 15 milestones"}); return
                c.execute("INSERT INTO milestones (order_id,milestone_name,scheduled_date,payment_amount,milestone_type,description,modified_by) VALUES (?,?,?,?,?,?,?)",
                    (oid,d["milestone_name"],d["scheduled_date"],d.get("payment_amount",0),d.get("milestone_type","Estimate"),d.get("description",""),d.get("modified_by","User")))
                db.commit(); self._j(200,r2d(c.execute("SELECT * FROM milestones WHERE id=?",(c.lastrowid,)).fetchone())); return

            m=re.match(r"^/api/orders/(\d+)/notes$",p)
            if m:
                oid=int(m.group(1))
                cnt=c.execute("SELECT COUNT(*) FROM order_notes WHERE order_id=?",(oid,)).fetchone()[0]
                if cnt>=50: self._j(400,{"detail":"Max 50 notes"}); return
                c.execute("INSERT INTO order_notes (order_id,note_text,login_name) VALUES (?,?,?)",(oid,d["note_text"][:50],d.get("login_name","User")))
                db.commit(); self._j(200,r2d(c.execute("SELECT * FROM order_notes WHERE id=?",(c.lastrowid,)).fetchone())); return

            m=re.match(r"^/api/orders/(\d+)/invoices$",p)
            if m:
                oid=int(m.group(1))
                if c.execute("SELECT id FROM invoices WHERE invoice_number=?",(d["invoice_number"],)).fetchone():
                    self._j(400,{"detail":f"Invoice '{d['invoice_number']}' already exists"}); return
                c.execute("INSERT INTO invoices (order_id,milestone_id,invoice_number,invoice_date,invoice_amount,created_by) VALUES (?,?,?,?,?,?)",
                    (oid,d["milestone_id"],d["invoice_number"],d["invoice_date"],d["invoice_amount"],"User"))
                c.execute("UPDATE milestones SET is_billed=1 WHERE id=?",(d["milestone_id"],))
                db.commit()
                inv=r2d(c.execute("SELECT i.*,ms.milestone_name FROM invoices i LEFT JOIN milestones ms ON i.milestone_id=ms.id WHERE i.id=?",(c.lastrowid,)).fetchone())
                inv["total_received"]=0; inv["balance"]=inv["invoice_amount"]
                self._j(200,inv); return

            m=re.match(r"^/api/invoices/(\d+)/receipts$",p)
            if m:
                invid=int(m.group(1))
                inv=c.execute("SELECT * FROM invoices WHERE id=?",(invid,)).fetchone()
                if not inv: self._j(404,{"detail":"Invoice not found"}); return
                tr=c.execute("SELECT COALESCE(SUM(receipt_amount),0) FROM receipts WHERE invoice_id=?",(invid,)).fetchone()[0]
                diff=d["receipt_amount"]-(inv["invoice_amount"]-tr)
                c.execute("INSERT INTO receipts (invoice_id,receipt_date,receipt_amount,difference,receipt_notes,created_by) VALUES (?,?,?,?,?,?)",
                    (invid,d["receipt_date"],d["receipt_amount"],diff,d.get("receipt_notes",""),"User"))
                db.commit()
                row=r2d(c.execute("SELECT r.*,i.invoice_number FROM receipts r LEFT JOIN invoices i ON r.invoice_id=i.id WHERE r.id=?",(c.lastrowid,)).fetchone())
                self._j(200,row); return

            self._j(404,{"detail":"Not found"})
        except Exception as e:
            traceback.print_exc(); db.rollback(); self._j(500,{"detail":str(e)})
        finally: db.close()

    def do_PUT(self):
        p=urllib.parse.urlparse(self.path).path.rstrip("/"); d=self._body(); db=get_db(); c=db.cursor()
        try:
            m=re.match(r"^/api/clients/(\d+)$",p)
            if m:
                cid=int(m.group(1)); sets=[]; vals=[]
                for k in ("client_id","name","billing_address","contact_names","billing_name","billing_email"):
                    if k in d: sets.append(f"{k}=?"); vals.append(d[k])
                if sets: vals.append(cid); c.execute(f"UPDATE clients SET {','.join(sets)} WHERE id=?",vals); db.commit()
                self._j(200,r2d(c.execute("SELECT * FROM clients WHERE id=?",(cid,)).fetchone())); return

            m=re.match(r"^/api/orders/(\d+)$",p)
            if m:
                oid=int(m.group(1)); sets=["last_modified=datetime('now')"]; vals=[]
                for k in ("client_id","order_name","contract_type_id","company_id","date_of_order","po_number","contract_amount"):
                    if k in d: sets.append(f"{k}=?"); vals.append(d[k])
                vals.append(oid); c.execute(f"UPDATE orders SET {','.join(sets)} WHERE id=?",vals); db.commit()
                row=r2d(c.execute("""SELECT o.*,cl.name client_name,cl.client_id client_code,
                    ct.category||' – '||ct.subcategory contract_type,co.name company_name
                    FROM orders o LEFT JOIN clients cl ON o.client_id=cl.id
                    LEFT JOIN contract_types ct ON o.contract_type_id=ct.id
                    LEFT JOIN companies co ON o.company_id=co.id WHERE o.id=?""",(oid,)).fetchone())
                self._j(200,row); return

            m=re.match(r"^/api/milestones/(\d+)$",p)
            if m:
                msid=int(m.group(1)); old=r2d(c.execute("SELECT * FROM milestones WHERE id=?",(msid,)).fetchone())
                if not old: self._j(404,{"detail":"Not found"}); return
                cr=d.pop("change_reason",""); mb=d.pop("modified_by","User")
                sets=["last_modified=datetime('now')","modified_by=?"]; vals=[mb]
                for f in ("milestone_name","scheduled_date","payment_amount","milestone_type","rescheduled_date","rescheduling_reason","description"):
                    if f not in d: continue
                    nv=d[f]; ov=old.get(f,"")
                    if str(ov or "")!=str(nv or ""):
                        c.execute("INSERT INTO milestone_audit (milestone_id,field_changed,old_value,new_value,change_reason,changed_by) VALUES (?,?,?,?,?,?)",
                            (msid,f,str(ov or ""),str(nv or ""),cr,mb))
                    sets.append(f"{f}=?"); vals.append(nv)
                vals.append(msid); c.execute(f"UPDATE milestones SET {','.join(sets)} WHERE id=?",vals); db.commit()
                self._j(200,r2d(c.execute("SELECT * FROM milestones WHERE id=?",(msid,)).fetchone())); return

            self._j(404,{"detail":"Not found"})
        except Exception as e:
            traceback.print_exc(); db.rollback(); self._j(500,{"detail":str(e)})
        finally: db.close()

    def do_DELETE(self):
        p=urllib.parse.urlparse(self.path).path.rstrip("/"); db=get_db(); c=db.cursor()
        try:
            m=re.match(r"^/api/clients/(\d+)$",p)
            if m:
                cid=int(m.group(1))
                if c.execute("SELECT COUNT(*) FROM orders WHERE client_id=? AND is_deleted=0",(cid,)).fetchone()[0]:
                    self._j(400,{"detail":"Cannot delete client with existing orders"}); return
                c.execute("DELETE FROM clients WHERE id=?",(cid,)); db.commit(); self._j(200,{"ok":True}); return

            m=re.match(r"^/api/contract-types/(\d+)$",p)
            if m: c.execute("DELETE FROM contract_types WHERE id=?",(int(m.group(1)),)); db.commit(); self._j(200,{"ok":True}); return

            m=re.match(r"^/api/companies/(\d+)$",p)
            if m: c.execute("DELETE FROM companies WHERE id=?",(int(m.group(1)),)); db.commit(); self._j(200,{"ok":True}); return

            m=re.match(r"^/api/orders/(\d+)$",p)
            if m: c.execute("UPDATE orders SET is_deleted=1,last_modified=datetime('now') WHERE id=?",(int(m.group(1)),)); db.commit(); self._j(200,{"ok":True}); return

            m=re.match(r"^/api/milestones/(\d+)$",p)
            if m:
                msid=int(m.group(1))
                if c.execute("SELECT COUNT(*) FROM invoices WHERE milestone_id=?",(msid,)).fetchone()[0]:
                    self._j(400,{"detail":"Cannot delete milestone with invoices"}); return
                c.execute("DELETE FROM milestone_audit WHERE milestone_id=?",(msid,))
                c.execute("DELETE FROM milestones WHERE id=?",(msid,)); db.commit(); self._j(200,{"ok":True}); return

            self._j(404,{"detail":"Not found"})
        except Exception as e:
            traceback.print_exc(); db.rollback(); self._j(500,{"detail":str(e)})
        finally: db.close()


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  Orders Backlog & Billing")
    print("=" * 60)
    init_db()
    print(f"  Open: http://localhost:{PORT}")
    print(f"  Press Ctrl+C to stop")
    print("=" * 60)
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDone.")

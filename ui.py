"""
Local transaction browser — reads from ledger.db, no Plaid API calls.

Usage:
    source .venv/bin/activate
    python3 ui.py
    # Open http://localhost:5050
"""

import logging
import threading
from datetime import datetime

from flask import Flask, jsonify, request

import db

db.init_db()
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Background sync scheduler
# ---------------------------------------------------------------------------

_sync_available = False
_plaid_sync = None

try:
    from sync import sync_and_store as _plaid_sync_fn
    _plaid_sync = _plaid_sync_fn
    _sync_available = True
except Exception as _sync_import_err:
    logging.getLogger(__name__).warning(
        "[sync] sync module not available: %s", _sync_import_err
    )

_sync_lock = threading.Lock()


def _run_sync_bg() -> None:
    """Run full sync in background; silently skip if already running."""
    if not _sync_available or not _sync_lock.acquire(blocking=False):
        return
    try:
        result = _plaid_sync()
        print(f"[sync] complete: {result}", flush=True)
    except Exception as e:
        print(f"[sync] error: {e}", flush=True)
    finally:
        _sync_lock.release()


if _sync_available:
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _scheduler = BackgroundScheduler(daemon=True)
        # Run immediately on startup, then every 12 hours
        _scheduler.add_job(_run_sync_bg, "interval", hours=12, next_run_time=datetime.now())
        _scheduler.start()
    except Exception as _sched_err:
        logging.getLogger(__name__).warning("[sync] scheduler failed to start: %s", _sched_err)

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Finances</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           background: #f5f5f5; color: #222; padding: 24px; }
    h1 { font-size: 1.4rem; margin-bottom: 20px; }
    .controls { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end;
                background: #fff; padding: 16px; border-radius: 8px;
                box-shadow: 0 1px 3px rgba(0,0,0,.1); margin-bottom: 16px; }
    .field { display: flex; flex-direction: column; gap: 4px; }
    label { font-size: 0.75rem; font-weight: 600; color: #555; }
    input[type=date], select { padding: 7px 10px; border: 1px solid #ccc;
                                border-radius: 6px; font-size: 0.9rem; }
    button { padding: 8px 18px; background: #0070f3; color: #fff; border: none;
             border-radius: 6px; cursor: pointer; font-size: 0.9rem; align-self: flex-end; }
    button:hover { background: #005ed4; }
    .summary { font-size: 0.85rem; color: #555; margin-bottom: 8px; }
    table { width: 100%; border-collapse: collapse; background: #fff;
            border-radius: 8px; overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    thead { background: #f0f0f0; }
    th { text-align: left; padding: 10px 14px; font-size: 0.8rem;
         font-weight: 600; color: #444; cursor: pointer; user-select: none; }
    th:hover { background: #e4e4e4; }
    td { padding: 9px 14px; font-size: 0.875rem; border-top: 1px solid #eee; }
    tr:hover td { background: #fafafa; }
    .amount { text-align: right; font-variant-numeric: tabular-nums; }
    .amount.debit { color: #d32f2f; }
    .amount.credit { color: #388e3c; }
    .pending { font-size: 0.7rem; background: #fff3cd; color: #856404;
               padding: 2px 6px; border-radius: 10px; }
    .cat-badge { font-size: 0.75rem; padding: 2px 8px; border-radius: 10px;
                 background: #e8f0fe; color: #1a56db; cursor: pointer; }
    .cat-badge.custom { background: #dcfce7; color: #166534; }
    .cat-select { font-size: 0.8rem; padding: 2px 4px; border: 1px solid #ccc;
                  border-radius: 4px; }
    .empty { text-align: center; padding: 40px; color: #888; }
    details { margin-bottom: 16px; }
    details > summary { cursor: pointer; font-weight: 600; padding: 8px 0;
                        font-size: 0.9rem; color: #444; list-style: none; }
    details > summary::before { content: '▶ '; font-size: 0.7rem; }
    details[open] > summary::before { content: '▼ '; }
    .cat-panel { background: #fff; padding: 16px; border-radius: 8px;
                 box-shadow: 0 1px 3px rgba(0,0,0,.1); margin-top: 8px; }
    .cat-list { margin-bottom: 14px; columns: 2; column-gap: 24px; }
    .cat-list .parent { font-weight: 600; font-size: 0.85rem; margin-bottom: 2px; }
    .cat-list .child { font-size: 0.8rem; color: #555; padding-left: 14px; }
    .cat-add { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .cat-add input[type=text] { padding: 7px 10px; border: 1px solid #ccc;
                                 border-radius: 6px; font-size: 0.9rem; }
    .cat-add button { align-self: auto; }
  </style>
</head>
<body>
  <h1>Finances</h1>

  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
    <div style="font-size:0.85rem;color:#555">
      <a href="/" style="margin-right:12px;color:#0070f3;text-decoration:none">Transactions</a>
      <a href="/rules" style="margin-right:12px;color:#0070f3;text-decoration:none">Rules</a>
      <a href="/recap" style="color:#0070f3;text-decoration:none">Weekly Recap</a>
    </div>
    <button id="sync-btn" onclick="runSync()"
      style="padding:6px 14px;background:#0070f3;color:#fff;border:none;border-radius:6px;
             font-size:0.8rem;cursor:pointer;white-space:nowrap">Sync</button>
  </div>

  <div class="controls">
    <div class="field">
      <label>From</label>
      <input type="date" id="start">
    </div>
    <div class="field">
      <label>To</label>
      <input type="date" id="end">
    </div>
    <div class="field">
      <label>Account</label>
      <select id="acct-filter"><option value="">All accounts</option></select>
    </div>
    <div class="field">
      <label>Category</label>
      <select id="cat-filter">
        <option value="">All</option>
      </select>
    </div>
    <button onclick="load()">Filter</button>
  </div>

  <details>
    <summary>Manage Categories</summary>
    <div class="cat-panel">
      <div class="cat-list" id="cat-list"></div>
      <div class="cat-add">
        <input type="text" id="new-cat-name" placeholder="New category name">
        <select id="new-cat-parent">
          <option value="">(top-level)</option>
        </select>
        <button onclick="addCategory()">Add</button>
      </div>
    </div>
  </details>

  <details>
    <summary>Budget Tracker</summary>
    <div class="cat-panel">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
        <label style="font-size:0.8rem;font-weight:600;color:#555">Month</label>
        <input type="month" id="budget-month" style="padding:5px 8px;border:1px solid #ccc;border-radius:6px;font-size:0.85rem">
        <button onclick="loadBudgetStatus()">Refresh</button>
      </div>
      <table style="width:100%;border-collapse:collapse;margin-bottom:14px">
        <thead style="background:#f0f0f0">
          <tr>
            <th style="text-align:left;padding:8px 12px;font-size:0.8rem">Category</th>
            <th style="text-align:right;padding:8px 12px;font-size:0.8rem">Budget</th>
            <th style="text-align:right;padding:8px 12px;font-size:0.8rem">Spent</th>
            <th style="text-align:right;padding:8px 12px;font-size:0.8rem">Remaining</th>
            <th style="padding:8px 12px;font-size:0.8rem">Status</th>
          </tr>
        </thead>
        <tbody id="budget-tbody"></tbody>
      </table>
      <div class="cat-add">
        <select id="budget-cat-sel"></select>
        <input type="number" id="budget-amount" placeholder="Monthly budget $" min="0" step="0.01"
               style="width:150px;padding:7px 10px;border:1px solid #ccc;border-radius:6px;font-size:0.9rem">
        <button onclick="setBudget()">Set Budget</button>
      </div>
    </div>
  </details>

  <p class="summary" id="summary"></p>

  <table id="tbl">
    <thead>
      <tr>
        <th onclick="sortBy('date')">Date</th>
        <th onclick="sortBy('name')">Name / Merchant</th>
        <th onclick="sortBy('amount')" style="text-align:right">Amount</th>
        <th onclick="sortBy('account_name')">Account</th>
        <th>Category</th>
        <th>Pending</th>
        <th style="width:40px"></th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>

  <script>
    let CATS = [];
    let ACCOUNTS = [];
    let BUDGETS = {};
    let rows = [];
    let RULE_MODAL = null;
    let RULE_TX = null;
    let sortKey = 'date';
    let sortAsc = false;

    function today() {
      return new Date().toISOString().slice(0, 10);
    }
    function firstOfMonth() {
      const d = new Date();
      return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-01`;
    }

    document.getElementById('start').value = firstOfMonth();
    document.getElementById('end').value = today();

    async function loadCategories() {
      const res = await fetch('/api/categories');
      const data = await res.json();
      CATS = data.map(c => c.name);

      // Populate filter dropdown
      const filter = document.getElementById('cat-filter');
      filter.innerHTML = '<option value="">All</option>';
      data.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.name;
        opt.textContent = c.parent ? '\u2514 ' + c.name : c.name;
        filter.appendChild(opt);
      });

      // Populate parent selector in management panel
      const parentSel = document.getElementById('new-cat-parent');
      parentSel.innerHTML = '<option value="">(top-level)</option>';
      data.filter(c => !c.parent).forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.name; opt.textContent = c.name;
        parentSel.appendChild(opt);
      });

      // Render category list in management panel
      const listEl = document.getElementById('cat-list');
      const toplevel = data.filter(c => !c.parent);
      const deleteBtn = (name) =>
        `<button onclick="removeCategory('${esc(name)}')" style="margin-left:6px;padding:1px 6px;font-size:0.75rem;color:#c00;background:none;border:1px solid #c00;border-radius:4px;cursor:pointer">Delete</button>`;
      listEl.innerHTML = toplevel.map(c => {
        const children = data.filter(ch => ch.parent === c.name);
        const childHtml = children.map(ch =>
          `<div class="child">\u2514 ${esc(ch.name)} ${deleteBtn(ch.name)}</div>`
        ).join('');
        return `<div style="margin-bottom:.5rem">
          <div class="parent">${esc(c.name)} ${deleteBtn(c.name)}</div>
          ${childHtml}
        </div>`;
      }).join('');
    }

    async function addCategory() {
      const name = document.getElementById('new-cat-name').value.trim();
      const parent = document.getElementById('new-cat-parent').value;
      if (!name) return;
      const res = await fetch('/api/categories', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name, parent})
      });
      const data = await res.json();
      if (data.ok) {
        document.getElementById('new-cat-name').value = '';
        await loadCategories();
      } else {
        alert(data.error || 'Failed to add category');
      }
    }

    async function removeCategory(name) {
      if (!confirm(`Delete category "${name}"? Historical transactions will be preserved but the category will no longer be assignable.`)) return;
      const res = await fetch('/api/categories/' + encodeURIComponent(name), {method: 'DELETE'});
      const data = await res.json();
      if (data.ok) {
        await loadCategories();
      } else {
        alert(data.error || 'Failed to remove category');
      }
    }

    async function loadAccounts() {
      const res = await fetch('/api/accounts');
      ACCOUNTS = await res.json();
      const sel = document.getElementById('acct-filter');
      sel.innerHTML = '<option value="">All accounts</option>';
      const nameCounts = {};
      ACCOUNTS.forEach(a => {
        const dn = a.official_name || a.name;
        const key = `${a.institution}|${dn}`;
        nameCounts[key] = (nameCounts[key] || 0) + 1;
      });
      ACCOUNTS.forEach(a => {
        const opt = document.createElement('option');
        opt.value = a.account_id;
        const displayName = a.official_name || a.name;
        const key = `${a.institution}|${displayName}`;
        const label = (nameCounts[key] > 1 && a.mask)
          ? `${displayName} (...${a.mask})`
          : displayName;
        opt.textContent = a.institution ? `${a.institution} \u2014 ${label}` : label;
        sel.appendChild(opt);
      });
    }

    async function load() {
      const start = document.getElementById('start').value;
      const end = document.getElementById('end').value;
      const cat = document.getElementById('cat-filter').value;
      const acct = document.getElementById('acct-filter').value;
      const params = new URLSearchParams({start, end, category: cat, account_id: acct});
      const res = await fetch('/api/transactions?' + params);
      rows = await res.json();
      render();
    }

    function sortBy(key) {
      if (sortKey === key) sortAsc = !sortAsc;
      else { sortKey = key; sortAsc = key === 'date' ? false : true; }
      render();
    }

    function render() {
      const sorted = [...rows].sort((a, b) => {
        let va = a[sortKey], vb = b[sortKey];
        if (typeof va === 'string') va = va.toLowerCase();
        if (typeof vb === 'string') vb = vb.toLowerCase();
        return sortAsc ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1);
      });

      const total = rows.reduce((s, r) => s + r.amount, 0);
      const count = rows.length;
      document.getElementById('summary').textContent =
        `${count} transaction${count !== 1 ? 's' : ''} · Net $${total.toFixed(2)}`;

      const tbody = document.getElementById('tbody');
      if (!sorted.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty">No transactions found.</td></tr>';
        return;
      }

      tbody.innerHTML = sorted.map(r => {
        const isCredit = r.amount < 0;
        const display = (isCredit ? '-' : '') + '$' + Math.abs(r.amount).toFixed(2);
        const amtClass = 'amount ' + (isCredit ? 'credit' : 'debit');
        const cat = r.custom_category || r.plaid_category || '';
        const badgeClass = 'cat-badge' + (r.custom_category ? ' custom' : '');
        const merchant = r.merchant_name && r.merchant_name !== r.name
          ? `<br><span style="color:#888;font-size:0.78rem">${esc(r.merchant_name)}</span>` : '';
        return `<tr>
          <td>${r.date}</td>
          <td>${esc(r.name)}${merchant}</td>
          <td class="${amtClass}">${display}</td>
          <td>${esc(r.account_name || 'Unknown')}</td>
          <td><span class="${badgeClass}" onclick="startEdit(this,'${esc(r.transaction_id)}')">${esc(cat)}</span></td>
          <td>${r.pending ? '<span class="pending">Pending</span>' : ''}</td>
          <td style="text-align:center">
            <button onclick="openRuleModal('${esc(r.transaction_id)}')" title="Create rule"
                    style="padding:0 6px;height:22px;line-height:20px;font-size:1rem;border-radius:999px;border:1px solid #ccc;background:#fff;cursor:pointer;color:#555">
              ⋯
            </button>
          </td>
        </tr>`;
      }).join('');
    }

    function esc(s) {
      return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    function startEdit(badge, txId) {
      const sel = document.createElement('select');
      sel.className = 'cat-select';
      CATS.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c; opt.textContent = c;
        if (c === badge.textContent) opt.selected = true;
        sel.appendChild(opt);
      });
      sel.onchange = async () => {
        await fetch('/api/category', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({transaction_id: txId, category: sel.value})
        });
        const row = rows.find(r => r.transaction_id === txId);
        if (row) row.custom_category = sel.value;
        render();
      };
      sel.onblur = () => render();
      badge.replaceWith(sel);
      sel.focus();
    }

    function openRuleModal(txId) {
      const tx = rows.find(r => r.transaction_id === txId);
      if (!tx) return;
      RULE_TX = tx;
      const overlay = document.createElement('div');
      overlay.style.position = 'fixed';
      overlay.style.inset = '0';
      overlay.style.background = 'rgba(0,0,0,.35)';
      overlay.style.display = 'flex';
      overlay.style.alignItems = 'center';
      overlay.style.justifyContent = 'center';
      overlay.style.zIndex = '1000';

      const modal = document.createElement('div');
      modal.style.background = '#fff';
      modal.style.borderRadius = '10px';
      modal.style.boxShadow = '0 10px 30px rgba(0,0,0,.18)';
      modal.style.width = 'min(900px, 95vw)';
      modal.style.maxHeight = '90vh';
      modal.style.overflow = 'hidden';
      modal.innerHTML = `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid #eee">
          <div style="font-weight:600;font-size:0.95rem">Create rule from transaction</div>
          <button onclick="closeRuleModal()" style="border:none;background:none;font-size:1.1rem;cursor:pointer;color:#666">&times;</button>
        </div>
        <div style="display:flex;gap:0;border-bottom:1px solid #f3f3f3">
          <div style="flex:1;padding:16px 20px;border-right:1px solid #f3f3f3;font-size:0.85rem">
            <div style="font-weight:600;margin-bottom:8px">Transaction</div>
            <div style="margin-bottom:6px"><span style="color:#555">Date:</span> ${tx.date}</div>
            <div style="margin-bottom:6px"><span style="color:#555">Account:</span> ${esc(tx.account_name || 'Unknown')}</div>
            <div style="margin-bottom:6px"><span style="color:#555">Payee:</span> ${esc(tx.name)}</div>
            ${tx.merchant_name && tx.merchant_name !== tx.name ? `<div style="margin-bottom:6px"><span style="color:#555">Merchant:</span> ${esc(tx.merchant_name)}</div>` : ''}
            <div style="margin-bottom:6px"><span style="color:#555">Amount:</span> $${Math.abs(tx.amount).toFixed(2)}</div>
            <div style="margin-bottom:6px"><span style="color:#555">Current category:</span> ${esc(tx.custom_category || tx.plaid_category || '')}</div>
          </div>
          <div style="flex:1.4;padding:16px 20px;font-size:0.85rem">
            <div id="rule-step-1">
              <div style="font-weight:600;margin-bottom:10px">If a transaction matches this...</div>
              <div style="margin-bottom:10px">
                <label style="display:block;font-size:0.75rem;font-weight:600;color:#555;margin-bottom:4px">Statement / merchant name contains</label>
                <input id="rule-pattern" type="text" value="${esc((tx.merchant_name || tx.name || '').split(' ')[0] || '')}" style="width:100%;padding:7px 10px;border:1px solid #ccc;border-radius:6px;font-size:0.85rem">
              </div>
              <div style="margin-bottom:14px">
                <label style="display:block;font-size:0.75rem;font-weight:600;color:#555;margin-bottom:4px">Account</label>
                <select id="rule-account" style="width:100%;padding:7px 10px;border:1px solid #ccc;border-radius:6px;font-size:0.85rem">
                  <option value="">All accounts</option>
                  ${ACCOUNTS.map(a => {
                    const displayName = a.official_name || a.name;
                    const label = a.institution ? `${a.institution} \u2014 ${displayName}` : displayName;
                    const selected = a.account_id === tx.account_id ? 'selected' : '';
                    return `<option value="${esc(a.account_id)}" ${selected}>${esc(label)}</option>`;
                  }).join('')}
                </select>
              </div>
              <div style="font-weight:600;margin-bottom:8px">Then make these changes</div>
              <div style="margin-bottom:10px">
                <label style="display:block;font-size:0.75rem;font-weight:600;color:#555;margin-bottom:4px">Rename payee to (optional)</label>
                <input id="rule-new-payee" type="text" value="${esc(tx.name)}" style="width:100%;padding:7px 10px;border:1px solid #ccc;border-radius:6px;font-size:0.85rem">
              </div>
              <div style="margin-bottom:16px">
                <label style="display:block;font-size:0.75rem;font-weight:600;color:#555;margin-bottom:4px">Update category to</label>
                <select id="rule-category" style="width:100%;padding:7px 10px;border:1px solid #ccc;border-radius:6px;font-size:0.85rem">
                  ${CATS.map(c => `<option value="${esc(c)}" ${c === (tx.custom_category || tx.plaid_category || '') ? 'selected' : ''}>${esc(c)}</option>`).join('')}
                </select>
              </div>
              <div style="display:flex;justify-content:flex-end;gap:8px">
                <button onclick="closeRuleModal()" style="padding:7px 14px;border-radius:6px;border:1px solid #ccc;background:#fff;color:#444;font-size:0.85rem;cursor:pointer">Cancel</button>
                <button onclick="previewRule()" style="padding:7px 16px;border-radius:6px;border:none;background:#0070f3;color:#fff;font-size:0.85rem;cursor:pointer">Next</button>
              </div>
            </div>
            <div id="rule-step-2" style="display:none">
              <div style="font-weight:600;margin-bottom:10px">Review matches</div>
              <div id="rule-preview-body" style="max-height:260px;overflow:auto;border:1px solid #eee;border-radius:6px;background:#fff"></div>
              <div style="margin-top:12px;display:flex;justify-content:space-between;align-items:center">
                <button onclick="backToRuleEdit()" style="padding:7px 14px;border-radius:6px;border:1px solid #ccc;background:#fff;color:#444;font-size:0.85rem;cursor:pointer">Back</button>
                <div style="display:flex;gap:8px">
                  <button onclick="closeRuleModal()" style="padding:7px 14px;border-radius:6px;border:1px solid #ccc;background:#fff;color:#444;font-size:0.85rem;cursor:pointer">Cancel</button>
                  <button onclick="applyRule()" style="padding:7px 16px;border-radius:6px;border:none;background:#0070f3;color:#fff;font-size:0.85rem;cursor:pointer">Apply rule</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      `;

      overlay.appendChild(modal);
      document.body.appendChild(overlay);
      RULE_MODAL = overlay;
    }

    function closeRuleModal() {
      if (RULE_MODAL) {
        RULE_MODAL.remove();
        RULE_MODAL = null;
        RULE_TX = null;
      }
    }

    function backToRuleEdit() {
      document.getElementById('rule-step-1').style.display = '';
      document.getElementById('rule-step-2').style.display = 'none';
    }

    async function previewRule() {
      const pattern = document.getElementById('rule-pattern').value.trim();
      const accountId = document.getElementById('rule-account').value || '';
      const category = document.getElementById('rule-category').value;
      const newPayee = document.getElementById('rule-new-payee').value.trim();
      if (!pattern) {
        alert('Pattern is required.');
        return;
      }
      const res = await fetch('/api/rules/preview', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({pattern, account_id: accountId || null, category, new_payee: newPayee || null})
      });
      const data = await res.json();
      const box = document.getElementById('rule-preview-body');
      if (!Array.isArray(data) || !data.length) {
        box.innerHTML = '<div style="padding:12px;font-size:0.85rem;color:#777">No matching transactions found for this rule.</div>';
      } else {
        box.innerHTML = `
          <table style="width:100%;border-collapse:collapse;font-size:0.8rem">
            <thead style="background:#fafafa">
              <tr>
                <th style="text-align:left;padding:6px 8px">Date</th>
                <th style="text-align:left;padding:6px 8px">Account</th>
                <th style="text-align:left;padding:6px 8px">Payee</th>
                <th style="text-align:left;padding:6px 8px">Category</th>
                <th style="text-align:right;padding:6px 8px">Amount</th>
              </tr>
            </thead>
            <tbody>
              ${data.map(r => `
                <tr>
                  <td style="padding:6px 8px;border-top:1px solid #eee">${r.date}</td>
                  <td style="padding:6px 8px;border-top:1px solid #eee">${esc(r.account_name || '')}</td>
                  <td style="padding:6px 8px;border-top:1px solid #eee">${esc(r.name)}</td>
                  <td style="padding:6px 8px;border-top:1px solid #eee">${esc(r.custom_category || r.plaid_category || '')}</td>
                  <td style="padding:6px 8px;border-top:1px solid #eee;text-align:right">$${Math.abs(r.amount).toFixed(2)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
      }
      document.getElementById('rule-step-1').style.display = 'none';
      document.getElementById('rule-step-2').style.display = '';
    }

    async function applyRule() {
      const pattern = document.getElementById('rule-pattern').value.trim();
      const accountId = document.getElementById('rule-account').value || '';
      const category = document.getElementById('rule-category').value;
      const newPayee = document.getElementById('rule-new-payee').value.trim();
      const oldPayee = RULE_TX ? RULE_TX.name : '';
      const oldCategory = RULE_TX ? (RULE_TX.custom_category || RULE_TX.plaid_category || '') : '';
      const res = await fetch('/api/rules', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          pattern,
          account_id: accountId || null,
          category,
          new_payee: newPayee || null,
          old_payee: oldPayee || null,
          old_category: oldCategory || null
        })
      });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || 'Failed to create rule');
        return;
      }
      closeRuleModal();
      await load();
    }

    async function loadBudgetStatus() {
      const month = document.getElementById('budget-month').value;
      if (!month) return;
      const res = await fetch('/api/budget-status?month=' + month);
      const data = await res.json();
      BUDGETS = {};
      data.forEach(r => { BUDGETS[r.category] = r.budgeted; });

      // Populate category selector
      const sel = document.getElementById('budget-cat-sel');
      sel.innerHTML = '';
      CATS.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c; opt.textContent = c;
        sel.appendChild(opt);
      });

      const tbody = document.getElementById('budget-tbody');
      if (!data.length) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:16px;color:#888">No data. Set a budget or sync transactions first.</td></tr>';
        return;
      }
      tbody.innerHTML = data.map(r => {
        const rowStyle = r.over_budget ? 'background:#fff5f5' : '';
        const remColor = r.remaining < 0 ? '#d32f2f' : (r.budgeted > 0 ? '#388e3c' : '#888');
        const status = r.over_budget
          ? '<span style="font-size:0.75rem;background:#fde8e8;color:#c00;padding:2px 8px;border-radius:10px">\u26A0 Over</span>'
          : (r.budgeted > 0
            ? `<span style="font-size:0.75rem;background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px">$${r.remaining.toFixed(2)} left</span>`
            : '<span style="font-size:0.75rem;color:#888">no budget</span>');
        return `<tr style="${rowStyle}">
          <td style="padding:8px 12px;border-top:1px solid #eee;font-size:0.875rem">${esc(r.category)}</td>
          <td style="padding:8px 12px;border-top:1px solid #eee;text-align:right;font-size:0.875rem;font-variant-numeric:tabular-nums">${r.budgeted > 0 ? '$' + r.budgeted.toFixed(2) : '\u2014'}</td>
          <td style="padding:8px 12px;border-top:1px solid #eee;text-align:right;font-size:0.875rem;font-variant-numeric:tabular-nums;color:#d32f2f">$${r.spent.toFixed(2)}</td>
          <td style="padding:8px 12px;border-top:1px solid #eee;text-align:right;font-size:0.875rem;font-variant-numeric:tabular-nums;color:${remColor}">${r.budgeted > 0 ? (r.remaining < 0 ? '-' : '') + '$' + Math.abs(r.remaining).toFixed(2) : '\u2014'}</td>
          <td style="padding:8px 12px;border-top:1px solid #eee">${status}</td>
        </tr>`;
      }).join('');
    }

    async function setBudget() {
      const category = document.getElementById('budget-cat-sel').value;
      const amount = parseFloat(document.getElementById('budget-amount').value);
      if (!category || isNaN(amount) || amount < 0) return;
      await fetch('/api/budgets', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({category, monthly_amount: amount})
      });
      document.getElementById('budget-amount').value = '';
      await loadBudgetStatus();
    }

    async function runSync() {
      const btn = document.getElementById('sync-btn');
      btn.disabled = true;
      btn.textContent = 'Syncing\u2026';
      try {
        const res = await fetch('/api/sync', {method: 'POST'});
        const data = await res.json();
        if (data.ok) {
          btn.textContent = `+${data.added ?? 0} synced`;
          await Promise.all([loadAccounts(), load()]);
        } else {
          btn.textContent = (data.error || '').includes('progress') ? 'Busy\u2026' : 'Failed';
        }
      } catch {
        btn.textContent = 'Error';
      } finally {
        setTimeout(() => { btn.disabled = false; btn.textContent = 'Sync'; }, 3000);
      }
    }

    (async () => {
      // Set budget month default to current month
      const now = new Date();
      document.getElementById('budget-month').value =
        `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;

      await Promise.all([loadCategories(), loadAccounts()]);
      loadBudgetStatus();
      load();
    })();
  </script>
</body>
</html>
"""


@app.route("/")
def index():
    return HTML


@app.route("/api/transactions")
def api_transactions():
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    category = request.args.get("category", "")
    account_id = request.args.get("account_id", "")
    if not start or not end:
        return jsonify({"error": "start and end are required"}), 400
    rows = db.query_transactions(start, end, category, account_id)
    return jsonify(rows)


@app.route("/api/accounts")
def api_accounts():
    return jsonify(db.get_all_accounts())


@app.route("/api/budgets")
def api_get_budgets():
    return jsonify(db.get_all_budgets())


@app.route("/api/budgets", methods=["POST"])
def api_set_budget():
    data = request.get_json(force=True)
    category = (data.get("category") or "").strip()
    amount = data.get("monthly_amount")
    if not category or amount is None:
        return jsonify({"ok": False, "error": "category and monthly_amount required"}), 400
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "monthly_amount must be a number"}), 400
    db.set_budget(category, amount)
    return jsonify({"ok": True, "category": category, "monthly_amount": amount})


@app.route("/api/budget-status")
def api_budget_status():
    month = request.args.get("month", "")
    if not month:
        return jsonify({"error": "month is required"}), 400
    return jsonify(db.get_budget_status(month))


@app.route("/api/category", methods=["POST"])
def api_category():
    data = request.get_json(force=True)
    tx_id = data.get("transaction_id", "")
    category = data.get("category", "")
    ok = db.set_custom_category(tx_id, category)
    return jsonify({"ok": ok})


@app.route("/api/categories")
def api_categories():
    return jsonify(db.get_all_categories())


@app.route("/api/categories/<path:name>", methods=["DELETE"])
def api_remove_category(name: str):
    try:
        db.remove_category(name)
        return jsonify({"ok": True})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/categories", methods=["POST"])
def api_add_category():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    parent = (data.get("parent") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "name is required"}), 400
    try:
        ok = db.add_category(name, parent)
        return jsonify({"ok": ok, "name": name, "parent": parent})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/rules", methods=["GET"])
def api_get_rules():
    return jsonify(db.get_all_rules())


@app.route("/api/rules/preview", methods=["POST"])
def api_preview_rule():
    data = request.get_json(force=True)
    pattern = (data.get("pattern") or "").strip()
    account_id = (data.get("account_id") or "").strip() or None
    category = (data.get("category") or "").strip()
    if not pattern or not category:
        return jsonify({"ok": False, "error": "pattern and category are required"}), 400
    matches = db.find_rule_matches(pattern, account_id)
    return jsonify(matches)


@app.route("/api/rules", methods=["POST"])
def api_create_rule():
    data = request.get_json(force=True)
    pattern = (data.get("pattern") or "").strip()
    account_id = (data.get("account_id") or "").strip() or None
    category = (data.get("category") or "").strip()
    new_payee = (data.get("new_payee") or "").strip() or None
    old_payee = (data.get("old_payee") or "").strip() or None
    old_category = (data.get("old_category") or "").strip() or None
    if not pattern or not category:
        return jsonify({"ok": False, "error": "pattern and category are required"}), 400
    rule_id = db.insert_rule(pattern, account_id, category, new_payee, old_payee, old_category)
    affected = db.apply_rule(rule_id)
    return jsonify({"ok": True, "rule_id": rule_id, "affected_count": affected})


@app.route("/api/rules/<int:rule_id>", methods=["DELETE"])
def api_delete_rule(rule_id: int):
    db.delete_rule(rule_id)
    return jsonify({"ok": True})


@app.route("/api/sync", methods=["POST"])
def api_sync():
    """Trigger an incremental Plaid sync synchronously. Returns sync result counts."""
    if not _sync_available:
        return jsonify({"ok": False, "error": "Sync not configured — check Plaid env vars"}), 503
    if not _sync_lock.acquire(blocking=False):
        return jsonify({"ok": False, "error": "Sync already in progress"}), 409
    try:
        result = _plaid_sync()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _sync_lock.release()


@app.route("/rules")
def rules_page():
    rules = db.get_all_rules()
    rows = "".join(
        f"<tr>"
        f"<td>{r['id']}</td>"
        f"<td>{r['pattern']}</td>"
        f"<td>{(r.get('account_id') or '')}</td>"
        f"<td>{(r.get('old_category') or '')}</td>"
        f"<td>{r['category']}</td>"
        f"<td>{(r.get('old_payee') or '')}</td>"
        f"<td>{(r.get('new_payee') or '')}</td>"
        f"<td>{(r.get('created_at') or '')}</td>"
        f"<td><button onclick=\"deleteRule({r['id']})\" style='padding:2px 8px;font-size:0.75rem;border-radius:4px;border:1px solid #ccc;background:#fff;cursor:pointer'>Delete</button></td>"
        f"</tr>"
        for r in rules
    )
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>Finances – Rules</title>
      <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:#f5f5f5; color:#222; padding:24px; }}
        h1 {{ font-size:1.3rem; margin-bottom:16px; }}
        a {{ color:#0070f3; text-decoration:none; }}
        table {{ width:100%; border-collapse:collapse; background:#fff; border-radius:8px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,.1); }}
        th, td {{ padding:8px 12px; font-size:0.85rem; border-top:1px solid #eee; }}
        thead th {{ background:#f0f0f0; text-align:left; }}
      </style>
    </head>
    <body>
      <div style="margin-bottom:12px"><a href="/">&larr; Back to transactions</a></div>
      <h1>Rules</h1>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Pattern</th>
            <th>Account</th>
            <th>Old category</th>
            <th>New category</th>
            <th>Old payee</th>
            <th>New payee</th>
            <th>Created</th>
            <th></th>
          </tr>
        </thead>
        <tbody id="rules-tbody">
          {rows or "<tr><td colspan='9' style='text-align:center;padding:24px;color:#777'>No rules yet.</td></tr>"}
        </tbody>
      </table>
      <script>
        async function deleteRule(id) {{
          if (!confirm('Delete this rule?')) return;
          const res = await fetch('/api/rules/' + id, {{method: 'DELETE'}});
          const data = await res.json();
          if (!data.ok) {{
            alert(data.error || 'Failed to delete rule');
            return;
          }}
          location.reload();
        }}
      </script>
    </body>
    </html>
    """
    return html


@app.route("/api/weekly-recap")
def api_weekly_recap():
    from datetime import date, timedelta
    week = request.args.get("week", "").strip()
    if not week:
        today = date.today()
        monday = today - timedelta(days=today.weekday())
        week = monday.isoformat()
    try:
        import recap
        data = recap.build_weekly_recap(week)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/recap")
def recap_page():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Finances – Weekly Recap</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           background: #f5f5f5; color: #222; padding: 24px; }
    h1 { font-size: 1.4rem; margin-bottom: 8px; }
    .nav { display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; }
    .nav a { color:#0070f3; text-decoration:none; font-size:0.85rem; margin-right:12px; }
    .week-nav { display:flex; align-items:center; gap:12px; margin-bottom:20px; }
    .week-nav button { padding:6px 14px; background:#fff; border:1px solid #ccc;
                       border-radius:6px; cursor:pointer; font-size:0.85rem; }
    .week-nav button:hover { background:#f0f0f0; }
    .week-label { font-size:0.9rem; color:#555; min-width:160px; text-align:center; }
    .card { background:#fff; border-radius:12px; box-shadow:0 1px 4px rgba(0,0,0,.1);
            padding:24px; margin-bottom:16px; }
    .card-label { font-size:0.7rem; font-weight:700; color:#888; letter-spacing:.06em;
                  text-transform:uppercase; margin-bottom:4px; }
    .card-title { font-size:1.5rem; font-weight:700; margin-bottom:4px; }
    .card-sub { font-size:0.85rem; color:#555; margin-bottom:16px; }
    .change-up { color:#e53935; }
    .change-down { color:#43a047; }
    .narrative { font-size:0.95rem; line-height:1.55; color:#333; margin-bottom:20px;
                 padding:16px; background:#f0f4ff; border-radius:8px; border-left:3px solid #0070f3; }
    .day-bars { display:flex; gap:6px; align-items:flex-end; height:60px; margin-bottom:16px; }
    .day-bar-wrap { display:flex; flex-direction:column; align-items:center; flex:1; }
    .day-bar { width:100%; background:#0070f3; border-radius:3px 3px 0 0; min-height:2px; }
    .day-bar-label { font-size:0.65rem; color:#888; margin-top:4px; }
    .cat-list { display:flex; flex-direction:column; gap:10px; }
    .cat-row { display:flex; align-items:baseline; justify-content:space-between; gap:8px; }
    .cat-name { font-size:0.9rem; font-weight:600; }
    .cat-merchants { font-size:0.75rem; color:#888; }
    .cat-amount { font-size:0.9rem; font-weight:600; white-space:nowrap; }
    .nw-number { font-size:2rem; font-weight:700; }
    .nw-change { font-size:0.95rem; margin-left:8px; }
    .breakdown { display:flex; gap:16px; flex-wrap:wrap; margin-top:12px; }
    .breakdown-item { font-size:0.8rem; }
    .breakdown-item span { font-weight:700; }
    .loading { color:#888; font-size:0.9rem; }
    .error { color:#e53935; font-size:0.85rem; }
  </style>
</head>
<body>
  <div class="nav">
    <div>
      <a href="/">Transactions</a>
      <a href="/rules">Rules</a>
      <a href="/recap">Weekly Recap</a>
    </div>
  </div>
  <h1>Weekly Recap</h1>

  <div class="week-nav">
    <button onclick="changeWeek(-7)">&larr; Prev</button>
    <div class="week-label" id="week-label"></div>
    <button onclick="changeWeek(7)">Next &rarr;</button>
  </div>

  <div id="recap-container"><p class="loading">Loading...</p></div>

  <script>
    function getMondayISO(d) {
      const day = d.getDay();
      const diff = (day === 0 ? -6 : 1 - day);
      const mon = new Date(d);
      mon.setDate(d.getDate() + diff);
      return mon.toISOString().slice(0, 10);
    }

    let currentWeek = getMondayISO(new Date());

    function changeWeek(delta) {
      const d = new Date(currentWeek + 'T00:00:00');
      d.setDate(d.getDate() + delta);
      currentWeek = d.toISOString().slice(0, 10);
      loadRecap();
    }

    function fmt(n) {
      if (n == null) return 'N/A';
      return '$' + Math.abs(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    }
    function fmtSigned(n) {
      if (n == null) return 'N/A';
      const sign = n >= 0 ? '+' : '-';
      return sign + '$' + Math.abs(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    }
    function shortDate(iso) {
      const d = new Date(iso + 'T00:00:00');
      return ['Su','Mo','Tu','We','Th','Fr','Sa'][d.getDay()];
    }

    async function loadRecap() {
      document.getElementById('week-label').textContent = 'Loading...';
      document.getElementById('recap-container').innerHTML = '<p class="loading">Loading...</p>';
      try {
        const res = await fetch('/api/weekly-recap?week=' + currentWeek);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        render(data);
      } catch(e) {
        document.getElementById('recap-container').innerHTML =
          '<p class="error">Error: ' + e.message + '</p>';
        document.getElementById('week-label').textContent = currentWeek;
      }
    }

    function render(data) {
      const s = data.spending;
      const nw = data.net_worth;
      document.getElementById('week-label').textContent =
        data.week_start + ' – ' + data.week_end;

      // Day bars
      const dayEntries = Object.entries(s.by_day).sort();
      const maxAmt = Math.max(...dayEntries.map(e => e[1]), 1);
      const barsHtml = dayEntries.map(([d, amt]) => {
        const pct = Math.round((amt / maxAmt) * 100);
        return '<div class="day-bar-wrap">' +
          '<div class="day-bar" style="height:' + Math.max(pct, 2) + '%"></div>' +
          '<div class="day-bar-label">' + shortDate(d) + '</div>' +
          '</div>';
      }).join('');

      // Categories
      const catHtml = (s.by_category || []).slice(0, 5).map(c => {
        const merchants = (c.transactions || []).slice(0,3).map(t => t.name).filter(Boolean).join(', ');
        return '<div class="cat-row">' +
          '<div><div class="cat-name">' + c.category + '</div>' +
          '<div class="cat-merchants">' + (merchants || '') + '</div></div>' +
          '<div class="cat-amount">' + fmt(c.total) + '</div>' +
          '</div>';
      }).join('');

      // Net worth
      const nwChangeClass = nw.change_amount == null ? '' :
        (nw.change_amount >= 0 ? 'change-up' : 'change-down');
      const nwChangePct = nw.change_pct != null ?
        ' (' + (nw.change_pct >= 0 ? '+' : '') + nw.change_pct.toFixed(2) + '%)' : '';

      // Breakdown
      const bd = nw.breakdown || {};
      const breakdownHtml = Object.entries(bd).filter(([,v]) => v !== 0).map(([k,v]) =>
        '<div class="breakdown-item">' + k + ': <span>' + fmt(v) + '</span></div>'
      ).join('');

      const changeClass = s.direction === 'up' ? 'change-up' : (s.direction === 'down' ? 'change-down' : '');

      document.getElementById('recap-container').innerHTML =
        (data.narrative ? '<div class="narrative">' + data.narrative + '</div>' : '') +

        '<div class="card">' +
          '<div class="card-label">Spending</div>' +
          '<div class="card-title">' + fmt(s.total) + '</div>' +
          '<div class="card-sub ' + changeClass + '">' + s.description + ' vs last week</div>' +
          '<div class="day-bars">' + barsHtml + '</div>' +
          '<div class="cat-list">' + (catHtml || '<p style="color:#888;font-size:0.85rem">No transactions this week.</p>') + '</div>' +
          (s.income_total > 0 ? '<div style="margin-top:12px;font-size:0.8rem;color:#888">Deposits/income: ' + fmt(s.income_total) + '</div>' : '') +
        '</div>' +

        '<div class="card">' +
          '<div class="card-label">Net Worth</div>' +
          (nw.current != null ?
            '<div style="display:flex;align-items:baseline">' +
              '<div class="nw-number">' + fmt(nw.current) + '</div>' +
              (nw.change_amount != null ?
                '<div class="nw-change ' + nwChangeClass + '">' + fmtSigned(nw.change_amount) + nwChangePct + '</div>'
                : '') +
            '</div>'
            : '<div style="color:#888;font-size:0.9rem">No balance snapshot data yet. Trigger a sync to start tracking net worth.</div>') +
          (breakdownHtml ? '<div class="breakdown">' + breakdownHtml + '</div>' : '') +
        '</div>';
    }

    loadRecap();
  </script>
</body>
</html>"""


if __name__ == "__main__":
    print("Transaction browser running at http://localhost:5050")
    app.run(port=5050, debug=True)

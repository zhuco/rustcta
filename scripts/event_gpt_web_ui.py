#!/usr/bin/env python3
"""Small read-only web UI for the event GPT simulator SQLite database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Event GPT Monitor</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #637083;
      --border: #d9dee7;
      --good: #0f8b4c;
      --bad: #c13f36;
      --warn: #a66a00;
      --accent: #2457d6;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, "Microsoft YaHei", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      padding: 18px 22px;
      background: #111827;
      color: #fff;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      flex-wrap: wrap;
    }
    h1 { font-size: 20px; margin: 0; font-weight: 700; }
    .status { color: #cbd5e1; font-size: 13px; }
    main { padding: 18px 22px 28px; max-width: 1500px; margin: 0 auto; }
    .grid { display: grid; gap: 14px; }
    .cards { grid-template-columns: repeat(6, minmax(130px, 1fr)); }
    .split { grid-template-columns: minmax(360px, 1fr) minmax(460px, 1.4fr); }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
      overflow: hidden;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 13px 14px;
      min-height: 82px;
    }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 8px; }
    .value { font-size: 24px; font-weight: 700; line-height: 1.1; }
    .sub { color: var(--muted); font-size: 12px; margin-top: 6px; }
    h2 { font-size: 16px; margin: 0 0 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td {
      text-align: left;
      border-bottom: 1px solid var(--border);
      padding: 8px 7px;
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      background: #fafbfc;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .table-wrap { overflow: auto; max-height: 520px; border: 1px solid var(--border); border-radius: 6px; }
    .badge {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 12px;
      border: 1px solid var(--border);
    }
    .up { color: var(--good); }
    .down { color: var(--bad); }
    .open { color: var(--warn); }
    .settled { color: var(--muted); }
    .win { color: var(--good); }
    .loss { color: var(--bad); }
    .muted { color: var(--muted); }
    .tabs { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
    .tab {
      border: 1px solid var(--border);
      background: #fff;
      border-radius: 6px;
      padding: 7px 10px;
      cursor: pointer;
      font-weight: 600;
    }
    .tab.active { color: #fff; background: var(--accent); border-color: var(--accent); }
    @media (max-width: 1100px) {
      .cards { grid-template-columns: repeat(3, minmax(130px, 1fr)); }
      .split { grid-template-columns: 1fr; }
    }
    @media (max-width: 650px) {
      main { padding: 12px; }
      .cards { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
      header { padding: 14px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Event GPT Monitor</h1>
    <div class="status" id="status">加载中...</div>
  </header>
  <main>
    <section class="grid cards" id="cards"></section>
    <section class="grid split" style="margin-top:14px;">
      <div class="panel">
        <h2>档位成功率</h2>
        <div class="table-wrap"><table id="thresholds"></table></div>
      </div>
      <div class="panel">
        <h2>交易对 / 周期 / 档位</h2>
        <div class="table-wrap"><table id="buckets"></table></div>
      </div>
    </section>
    <section class="panel" style="margin-top:14px;">
      <div class="tabs">
        <button class="tab active" data-tab="trades">模拟开仓</button>
        <button class="tab" data-tab="predictions">GPT 预测</button>
      </div>
      <div class="table-wrap"><table id="detail"></table></div>
    </section>
  </main>
  <script>
    let activeTab = 'trades';
    document.querySelectorAll('.tab').forEach(btn => {
      btn.addEventListener('click', () => {
        activeTab = btn.dataset.tab;
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        load();
      });
    });

    function fmtPct(v) {
      if (v === null || v === undefined) return '-';
      return (Number(v) * 100).toFixed(2) + '%';
    }
    function fmtNum(v, digits=2) {
      if (v === null || v === undefined || v === '') return '-';
      return Number(v).toFixed(digits);
    }
    function esc(v) {
      return String(v ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[c]));
    }
    function card(label, value, sub='') {
      return `<div class="card"><div class="label">${label}</div><div class="value">${value}</div><div class="sub">${sub}</div></div>`;
    }
    function renderTable(id, headers, rows) {
      const table = document.getElementById(id);
      const head = `<thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
      const body = `<tbody>${rows.join('')}</tbody>`;
      table.innerHTML = head + body;
    }
    function renderSummary(data) {
      const s = data.summary;
      document.getElementById('cards').innerHTML = [
        card('预测记录', s.predictions_total, '全部 GPT 返回'),
        card('模拟开仓', s.trades_total, `未结算 ${s.open_trades}`),
        card('已结算', s.settled_trades, `胜 ${s.wins} / 负 ${s.losses}`),
        card('总胜率', fmtPct(s.win_rate), '全部档位'),
        card('累计 PnL', fmtNum(s.pnl_units, 2), '单位下注收益'),
        card('最新预测', s.latest_prediction_time || '-', data.db_path),
      ].join('');

      renderTable('thresholds',
        ['档位', '总数', '已结算', '胜', '负', '胜率', 'PnL', '平均概率'],
        data.by_threshold.map(r => `<tr>
          <td>${fmtNum(r.signal_threshold, 2)}</td><td>${r.total}</td><td>${r.settled}</td>
          <td class="win">${r.wins}</td><td class="loss">${r.losses}</td>
          <td>${fmtPct(r.win_rate)}</td><td>${fmtNum(r.pnl_units, 2)}</td><td>${fmtPct(r.avg_probability)}</td>
        </tr>`)
      );

      renderTable('buckets',
        ['交易对', '周期', '档位', '总数', '已结算', '胜率', 'PnL'],
        data.by_bucket.map(r => `<tr>
          <td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td><td>${fmtNum(r.signal_threshold, 2)}</td>
          <td>${r.total}</td><td>${r.settled}</td><td>${fmtPct(r.win_rate)}</td><td>${fmtNum(r.pnl_units, 2)}</td>
        </tr>`)
      );
    }
    function renderTrades(rows) {
      renderTable('detail',
        ['入场时间', '交易对', '周期', '方向', '概率', '档位', '入场价', '到期价', '状态', '结果', 'PnL'],
        rows.map(r => `<tr>
          <td>${esc(r.entry_time_utc)}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td>
          <td class="${r.direction === 'UP' ? 'up' : 'down'}">${esc(r.direction)}</td>
          <td>${fmtPct(r.probability)}</td><td>${fmtNum(r.signal_threshold, 2)}</td>
          <td>${fmtNum(r.entry_price, 4)}</td><td>${fmtNum(r.expiry_price, 4)}</td>
          <td class="${r.status === 'OPEN' ? 'open' : 'settled'}">${esc(r.status)}</td>
          <td class="${r.success === 'true' ? 'win' : (r.success === 'false' ? 'loss' : 'muted')}">${r.success || '-'}</td>
          <td>${fmtNum(r.pnl_units, 2)}</td>
        </tr>`)
      );
    }
    function renderPredictions(rows) {
      renderTable('detail',
        ['预测时间', '交易对', '周期', '方向', '概率', '开仓', '档位', '入场价', '价格源'],
        rows.map(r => `<tr>
          <td>${esc(r.prediction_time_utc)}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td>
          <td class="${r.direction === 'UP' ? 'up' : 'down'}">${esc(r.direction)}</td>
          <td>${fmtPct(r.probability)}</td><td>${esc(r.accepted)}</td>
          <td>${esc(r.accepted_thresholds)}</td><td>${fmtNum(r.entry_price, 4)}</td>
          <td>${esc(r.entry_price_source)}</td>
        </tr>`)
      );
    }
    function renderTrades(rows) {
      renderTable('detail',
        ['#', 'Entry Time', 'Symbol', 'Horizon', 'Direction', 'Prob', 'Band', 'Entry', 'Expiry', 'Status', 'Result', 'PnL'],
        rows.map(r => `<tr>
          <td>${r.row_no ?? '-'}</td>
          <td>${esc(r.entry_time_utc)}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td>
          <td class="${r.direction === 'UP' ? 'up' : 'down'}">${esc(r.direction)}</td>
          <td>${fmtPct(r.probability)}</td><td>${fmtNum(r.signal_threshold, 2)}</td>
          <td>${fmtNum(r.entry_price, 4)}</td><td>${fmtNum(r.expiry_price, 4)}</td>
          <td class="${r.status === 'OPEN' ? 'open' : 'settled'}">${esc(r.status)}</td>
          <td class="${r.success === 'true' ? 'win' : (r.success === 'false' ? 'loss' : 'muted')}">${r.success || '-'}</td>
          <td>${fmtNum(r.pnl_units, 2)}</td>
        </tr>`)
      );
    }
    function renderPredictions(rows) {
      renderTable('detail',
        ['#', 'Prediction Time', 'Symbol', 'Horizon', 'Decision', 'Direction', 'Prob', 'Accepted', 'Bands', 'Entry', 'Source', 'Setup', 'Reject Reason'],
        rows.map(r => `<tr>
          <td>${r.row_no ?? '-'}</td>
          <td>${esc(r.prediction_time_utc)}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td>
          <td>${esc(r.decision || '')}</td>
          <td class="${r.direction === 'UP' ? 'up' : (r.direction === 'DOWN' ? 'down' : 'muted')}">${esc(r.direction)}</td>
          <td>${fmtPct(r.probability)}</td><td>${esc(r.accepted)}</td>
          <td>${esc(r.accepted_thresholds)}</td><td>${fmtNum(r.entry_price, 4)}</td>
          <td>${esc(r.entry_price_source)}</td><td>${esc(r.setup || '')}</td><td>${esc(r.reject_reason || '')}</td>
        </tr>`)
      );
    }
    async function load() {
      try {
        const res = await fetch('/api/state?limit=120', {cache: 'no-store'});
        const data = await res.json();
        renderSummary(data);
        if (activeTab === 'trades') renderTrades(data.trades);
        else renderPredictions(data.predictions);
        document.getElementById('status').textContent =
          `已刷新 ${new Date().toLocaleTimeString()} | DB ${data.db_exists ? 'OK' : 'missing'}`;
      } catch (err) {
        document.getElementById('status').textContent = '加载失败: ' + err;
      }
    }
    load();
    setInterval(load, 5000);
  </script>
</body>
</html>"""


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Event GPT Monitor</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #637083;
      --border: #d9dee7;
      --good: #0f8b4c;
      --bad: #c13f36;
      --warn: #a66a00;
      --accent: #2457d6;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, "Microsoft YaHei", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      padding: 18px 22px;
      background: #111827;
      color: #fff;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      flex-wrap: wrap;
    }
    h1 { font-size: 20px; margin: 0; font-weight: 700; }
    .status { color: #cbd5e1; font-size: 13px; }
    main { padding: 18px 22px 28px; max-width: 1500px; margin: 0 auto; }
    .grid { display: grid; gap: 14px; }
    .cards { grid-template-columns: repeat(6, minmax(130px, 1fr)); }
    .split { grid-template-columns: minmax(360px, 1fr) minmax(460px, 1.4fr); }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
      overflow: hidden;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 13px 14px;
      min-height: 82px;
    }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 8px; }
    .value { font-size: 24px; font-weight: 700; line-height: 1.1; }
    .sub { color: var(--muted); font-size: 12px; margin-top: 6px; overflow: hidden; text-overflow: ellipsis; }
    h2 { font-size: 16px; margin: 0 0 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td {
      text-align: left;
      border-bottom: 1px solid var(--border);
      padding: 8px 7px;
      white-space: nowrap;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      background: #fafbfc;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .table-wrap { overflow: auto; max-height: 520px; border: 1px solid var(--border); border-radius: 6px; }
    .up { color: var(--good); font-weight: 700; }
    .down { color: var(--bad); font-weight: 700; }
    .open { color: var(--warn); font-weight: 700; }
    .settled { color: var(--muted); }
    .win { color: var(--good); font-weight: 700; }
    .loss { color: var(--bad); font-weight: 700; }
    .muted { color: var(--muted); }
    .tabs { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
    .tab {
      border: 1px solid var(--border);
      background: #fff;
      border-radius: 6px;
      padding: 7px 10px;
      cursor: pointer;
      font-weight: 600;
    }
    .tab.active { color: #fff; background: var(--accent); border-color: var(--accent); }
    .wide { max-width: 360px; white-space: normal; line-height: 1.35; }
    @media (max-width: 1100px) {
      .cards { grid-template-columns: repeat(3, minmax(130px, 1fr)); }
      .split { grid-template-columns: 1fr; }
    }
    @media (max-width: 650px) {
      main { padding: 12px; }
      .cards { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
      header { padding: 14px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Event GPT Monitor</h1>
    <div class="status" id="status">Loading...</div>
  </header>
  <main>
    <section class="grid cards" id="cards"></section>
    <section class="grid split" style="margin-top:14px;">
      <div class="panel">
        <h2>Threshold Results</h2>
        <div class="table-wrap"><table id="thresholds"></table></div>
      </div>
      <div class="panel">
        <h2>Symbol / Horizon / Threshold</h2>
        <div class="table-wrap"><table id="buckets"></table></div>
      </div>
    </section>
    <section class="panel" style="margin-top:14px;">
      <div class="tabs">
        <button class="tab active" data-tab="trades">Trades</button>
        <button class="tab" data-tab="predictions">GPT Predictions</button>
      </div>
      <div class="table-wrap"><table id="detail"></table></div>
    </section>
  </main>
  <script>
    let activeTab = 'trades';
    document.querySelectorAll('.tab').forEach(btn => {
      btn.addEventListener('click', () => {
        activeTab = btn.dataset.tab;
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        load();
      });
    });

    function fmtPct(v) {
      if (v === null || v === undefined || v === '') return '-';
      return (Number(v) * 100).toFixed(2) + '%';
    }
    function fmtNum(v, digits=2) {
      if (v === null || v === undefined || v === '') return '-';
      const n = Number(v);
      return Number.isFinite(n) ? n.toFixed(digits) : '-';
    }
    function esc(v) {
      return String(v ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[c]));
    }
    function card(label, value, sub='') {
      return `<div class="card"><div class="label">${label}</div><div class="value">${value}</div><div class="sub">${esc(sub)}</div></div>`;
    }
    function renderTable(id, headers, rows) {
      const table = document.getElementById(id);
      const head = `<thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
      const body = `<tbody>${rows.join('')}</tbody>`;
      table.innerHTML = head + body;
    }
    function renderSummary(data) {
      const s = data.summary;
      document.getElementById('cards').innerHTML = [
        card('Predictions', s.predictions_total, 'All GPT responses'),
        card('Trades', s.trades_total, `Open ${s.open_trades}`),
        card('Settled', s.settled_trades, `Win ${s.wins} / Loss ${s.losses}`),
        card('Win Rate', fmtPct(s.win_rate), 'All thresholds'),
        card('PnL Units', fmtNum(s.pnl_units, 2), 'Unit stake accounting'),
        card('Latest Prediction', s.latest_prediction_time || '-', data.db_path),
      ].join('');

      renderTable('thresholds',
        ['#', 'Threshold', 'Total', 'Settled', 'Wins', 'Losses', 'Win Rate', 'PnL', 'Avg Prob'],
        data.by_threshold.map(r => `<tr>
          <td>${r.row_no ?? '-'}</td><td>${fmtNum(r.signal_threshold, 2)}</td><td>${r.total}</td><td>${r.settled}</td>
          <td class="win">${r.wins}</td><td class="loss">${r.losses}</td>
          <td>${fmtPct(r.win_rate)}</td><td>${fmtNum(r.pnl_units, 2)}</td><td>${fmtPct(r.avg_probability)}</td>
        </tr>`)
      );

      renderTable('buckets',
        ['#', 'Symbol', 'Horizon', 'Threshold', 'Total', 'Settled', 'Win Rate', 'PnL'],
        data.by_bucket.map(r => `<tr>
          <td>${r.row_no ?? '-'}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td><td>${fmtNum(r.signal_threshold, 2)}</td>
          <td>${r.total}</td><td>${r.settled}</td><td>${fmtPct(r.win_rate)}</td><td>${fmtNum(r.pnl_units, 2)}</td>
        </tr>`)
      );
    }
    function renderTrades(rows) {
      renderTable('detail',
        ['#', 'Entry Time', 'Symbol', 'Horizon', 'Direction', 'Prob', 'Band', 'Entry', 'Expiry', 'Status', 'Result', 'PnL'],
        rows.map(r => `<tr>
          <td>${r.row_no ?? '-'}</td>
          <td>${esc(r.entry_time_utc)}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td>
          <td class="${r.direction === 'UP' ? 'up' : 'down'}">${esc(r.direction)}</td>
          <td>${fmtPct(r.probability)}</td><td>${fmtNum(r.signal_threshold, 2)}</td>
          <td>${fmtNum(r.entry_price, 4)}</td><td>${fmtNum(r.expiry_price, 4)}</td>
          <td class="${r.status === 'OPEN' ? 'open' : 'settled'}">${esc(r.status)}</td>
          <td class="${r.success === 'true' ? 'win' : (r.success === 'false' ? 'loss' : 'muted')}">${r.success || '-'}</td>
          <td>${fmtNum(r.pnl_units, 2)}</td>
        </tr>`)
      );
    }
    function renderPredictions(rows) {
      renderTable('detail',
        ['#', 'Prediction Time', 'Symbol', 'Horizon', 'Decision', 'Direction', 'Prob', 'Accepted', 'Bands', 'Entry', 'Source', 'Setup', 'Reject Reason'],
        rows.map(r => `<tr>
          <td>${r.row_no ?? '-'}</td>
          <td>${esc(r.prediction_time_utc)}</td><td>${esc(r.symbol)}</td><td>${r.horizon_minutes}m</td>
          <td>${esc(r.decision || '')}</td>
          <td class="${r.direction === 'UP' ? 'up' : (r.direction === 'DOWN' ? 'down' : 'muted')}">${esc(r.direction)}</td>
          <td>${fmtPct(r.probability)}</td><td>${esc(r.accepted)}</td>
          <td>${esc(r.accepted_thresholds)}</td><td>${fmtNum(r.entry_price, 4)}</td>
          <td>${esc(r.entry_price_source)}</td><td class="wide">${esc(r.setup || '')}</td><td class="wide">${esc(r.reject_reason || '')}</td>
        </tr>`)
      );
    }
    async function load() {
      try {
        const res = await fetch('/api/state?limit=120', {cache: 'no-store'});
        const data = await res.json();
        renderSummary(data);
        if (activeTab === 'trades') renderTrades(data.trades);
        else renderPredictions(data.predictions);
        document.getElementById('status').textContent =
          `Updated ${new Date().toLocaleTimeString()} | DB ${data.db_exists ? 'OK' : 'missing'}`;
      } catch (err) {
        document.getElementById('status').textContent = 'Load failed: ' + err;
      }
    }
    load();
    setInterval(load, 5000);
  </script>
</body>
</html>"""


class UiHandler(BaseHTTPRequestHandler):
    db_path: Path = Path("logs/event_gpt.sqlite3")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_text(HTML, "text/html; charset=utf-8")
            return
        if parsed.path == "/api/state":
            query = parse_qs(parsed.query)
            limit = int(query.get("limit", ["100"])[0])
            self.send_json(read_state(self.db_path, limit=limit))
            return
        self.send_error(404, "not found")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def send_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, text: str, content_type: str) -> None:
        body = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def read_state(db_path: Path, limit: int) -> dict[str, Any]:
    if not db_path.exists():
        return empty_state(db_path)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        return {
            "db_path": str(db_path),
            "db_exists": True,
            "server_time_ms": int(time.time() * 1000),
            "summary": read_summary(conn),
            "by_threshold": rows(conn, THRESHOLD_SQL),
            "by_bucket": rows(conn, BUCKET_SQL),
            "trades": rows(conn, LATEST_TRADES_SQL, {"limit": limit}),
            "predictions": rows(conn, LATEST_PREDICTIONS_SQL, {"limit": limit}),
        }
    finally:
        conn.close()


def empty_state(db_path: Path) -> dict[str, Any]:
    return {
        "db_path": str(db_path),
        "db_exists": False,
        "server_time_ms": int(time.time() * 1000),
        "summary": {
            "predictions_total": 0,
            "trades_total": 0,
            "open_trades": 0,
            "settled_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "pnl_units": 0,
            "latest_prediction_time": None,
        },
        "by_threshold": [],
        "by_bucket": [],
        "trades": [],
        "predictions": [],
    }


def read_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(SUMMARY_SQL).fetchone()
    result = dict(row) if row else {}
    settled = result.get("settled_trades") or 0
    wins = result.get("wins") or 0
    result["win_rate"] = wins / settled if settled else None
    return result


def rows(
    conn: sqlite3.Connection,
    sql: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    result = []
    for index, row in enumerate(conn.execute(sql, params or {}), start=1):
        item = dict(row)
        item.setdefault("row_no", index)
        result.append(item)
    return result


SUMMARY_SQL = """
SELECT
  (SELECT COUNT(*) FROM predictions) AS predictions_total,
  COUNT(*) AS trades_total,
  SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_trades,
  SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) AS settled_trades,
  SUM(CASE WHEN success='true' THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN success='false' THEN 1 ELSE 0 END) AS losses,
  COALESCE(SUM(CAST(NULLIF(pnl_units, '') AS REAL)), 0) AS pnl_units,
  (SELECT MAX(prediction_time_utc) FROM predictions) AS latest_prediction_time
FROM trades
"""

THRESHOLD_SQL = """
SELECT
  signal_threshold,
  COUNT(*) AS total,
  SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) AS settled,
  SUM(CASE WHEN success='true' THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN success='false' THEN 1 ELSE 0 END) AS losses,
  CASE
    WHEN SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) > 0
    THEN 1.0 * SUM(CASE WHEN success='true' THEN 1 ELSE 0 END)
      / SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END)
    ELSE NULL
  END AS win_rate,
  COALESCE(SUM(CAST(NULLIF(pnl_units, '') AS REAL)), 0) AS pnl_units,
  AVG(probability) AS avg_probability
FROM trades
GROUP BY signal_threshold
ORDER BY signal_threshold
"""

BUCKET_SQL = """
SELECT
  symbol,
  horizon_minutes,
  signal_threshold,
  COUNT(*) AS total,
  SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) AS settled,
  SUM(CASE WHEN success='true' THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN success='false' THEN 1 ELSE 0 END) AS losses,
  CASE
    WHEN SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) > 0
    THEN 1.0 * SUM(CASE WHEN success='true' THEN 1 ELSE 0 END)
      / SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END)
    ELSE NULL
  END AS win_rate,
  COALESCE(SUM(CAST(NULLIF(pnl_units, '') AS REAL)), 0) AS pnl_units
FROM trades
GROUP BY symbol, horizon_minutes, signal_threshold
ORDER BY symbol, horizon_minutes, signal_threshold
"""

LATEST_TRADES_SQL = """
SELECT *
FROM trades
ORDER BY entry_epoch_ms DESC, trade_id DESC
LIMIT :limit
"""

LATEST_PREDICTIONS_SQL = """
SELECT *
FROM predictions
ORDER BY prediction_epoch_ms DESC, id DESC
LIMIT :limit
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve Event GPT simulator dashboard.")
    parser.add_argument("--db-path", default="logs/event_gpt.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    UiHandler.db_path = Path(args.db_path)
    server = ThreadingHTTPServer((args.host, args.port), UiHandler)
    print(f"Event GPT web UI: http://{args.host}:{args.port}")
    print(f"SQLite DB: {UiHandler.db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 130
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

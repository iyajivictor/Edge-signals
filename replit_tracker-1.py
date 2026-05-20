"""
EDGE Replit Tracker — main.py
==============================
Real-time TP1/SL monitoring for Sweep+FVG trades.
Price data via Finnhub (free tier, 60 calls/min) — no Twelve Data credits used.
Kept alive on Render via UptimeRobot pinging /health every 5min.

Setup:
  1. Add environment variables on Render:
       TG_TOKEN, TG_CHAT_ID, FINNHUB_KEY, SHEET_ID, GOOGLE_CREDENTIALS
  2. Deploy — copy the URL shown
  3. Add URL to UptimeRobot (free) as HTTP monitor, every 5 minutes

How it works:
  · Flask server at port 8080 — responds to /health pings (keeps Render alive)
  · Background thread polls prices every 60 seconds, staggered across pairs
  · Second thread syncs new ACTIVE trades from Google Sheet every 60 seconds
  · On restart: reloads ACTIVE trades from sheet automatically

Signal lifecycle this tracker handles (full Phase 1 + Phase 2):
  · Entry triggered (status=ACTIVE) → monitor TP1 and SL
  · TP1 hit → Telegram alert, update sheet, continue monitoring Phase 2
  · TP2 hit → FULL WIN alert, write final outcome, close trade
  · BE hit  → TP1_BE alert, write final outcome, close trade
  · SL hit  → LOSS alert, write final outcome, close trade

Outcome tracker (GitHub Actions) handles:
  · PENDING_ENTRY monitoring (entry trigger detection)
  · Fallback for Phase 1 + Phase 2 if Render is down

Sheet columns read/written (SweepFVG tab):
  1 fired_at  5 sl    9 rr_tp2   13 sweep_time  17 tp2_outcome
  2 pair       6 tp1  10 sl_pips  14 status       18 pnl_r
  3 direction  7 tp2  11 zone_src 15 entry_time
  4 entry      8 rr_tp1 12 session 16 tp1_outcome
"""

import os, json, time, threading, requests
from datetime import datetime, timezone
from flask import Flask, jsonify
from google.oauth2.service_account import Credentials
import gspread

# ── CONFIG ────────────────────────────────────────────────────────────────────
TG_TOKEN      = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TG_CHAT_ID", "")
FINNHUB_KEY   = os.environ.get("FINNHUB_KEY", "")
SHEET_ID      = os.environ.get("SHEET_ID", "")
GOOGLE_CREDS  = os.environ.get("GOOGLE_CREDENTIALS", "")

POLL_INTERVAL = 60   # seconds between price checks
SYNC_INTERVAL = 60   # seconds between sheet syncs

# Finnhub forex symbols use OANDA format
FH_SYMBOLS = {
    "EURUSD": "OANDA:EUR_USD", "GBPUSD": "OANDA:GBP_USD",
    "USDJPY": "OANDA:USD_JPY", "AUDJPY": "OANDA:AUD_JPY",
    "CADJPY": "OANDA:CAD_JPY", "USDCAD": "OANDA:USD_CAD",
    "EURJPY": "OANDA:EUR_JPY", "GBPJPY": "OANDA:GBP_JPY",
    "GBPAUD": "OANDA:GBP_AUD", "XAUUSD": "OANDA:XAU_USD",
}
DP = {
    "EURUSD":5,"GBPUSD":5,"USDCAD":5,"GBPAUD":5,
    "USDJPY":3,"AUDJPY":3,"CADJPY":3,"EURJPY":3,"GBPJPY":3,"XAUUSD":2,
}

# ── FLASK ─────────────────────────────────────────────────────────────────────
app           = Flask(__name__)
active_trades = {}       # key → trade dict
trades_lock   = threading.Lock()
stats         = {"started":"","last_poll":"","polls":0,
                 "tp1_hits":0,"full_wins":0,"tp1_be":0,"losses":0,"active":0}

@app.route("/health")
def health():
    with trades_lock: n = len(active_trades)
    return jsonify({**stats, "active": n, "status": "ok",
                    "price_source": "finnhub"})

@app.route("/trades")
def trades_view():
    with trades_lock:
        return jsonify({"trades": list(active_trades.values()), "count": len(active_trades)})

@app.route("/")
def index():
    return "<h2>EDGE Tracker 🟢</h2><p><a href='/health'>/health</a> | <a href='/trades'>/trades</a></p>"


# ── SHEETS ────────────────────────────────────────────────────────────────────
_sweep_sheet = None

def get_sheet():
    global _sweep_sheet
    if _sweep_sheet: return _sweep_sheet
    try:
        creds        = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        _sweep_sheet = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("SweepFVG")
        print("[SHEET] Connected ✓")
        return _sweep_sheet
    except Exception as e:
        print(f"[SHEET] Error: {e}"); return None

def find_row(sheet, sig):
    try:
        for i, row in enumerate(sheet.get_all_values()[1:], start=2):
            if len(row)>=3 and row[0]==sig['fired_at'] and row[1]==sig['pair'] \
               and row[2].upper()==sig['direction'].upper():
                return i
    except Exception as e: print(f"[SHEET] Row search: {e}")
    return None

def sheet_tp1(sig):
    sheet = get_sheet()
    if not sheet: return
    try:
        row = find_row(sheet, sig)
        if row: sheet.update_cell(row, 16, 'WIN'); print(f"[SHEET] TP1 row {row} ✓")
    except Exception as e: print(f"[SHEET] TP1 err: {e}")

def sheet_final(sig, outcome):
    sheet = get_sheet()
    if not sheet: return
    try:
        row = find_row(sheet, sig)
        if not row: return
        r1 = float(sig.get('rr_tp1',1.5)); r2 = float(sig.get('rr_tp2',3.0))
        if outcome=='LOSS':
            pnl=-1.0; sheet.update_cell(row,16,'LOSS'); sheet.update_cell(row,17,'LOSS')
        elif outcome=='TP1_BE':
            pnl=round(r1*.5,2); sheet.update_cell(row,17,'TP1_BE')
        else:
            pnl=round(r1*.5+r2*.5,2); sheet.update_cell(row,17,'FULL_WIN')
        sheet.update_cell(row,14,'CLOSED'); sheet.update_cell(row,18,pnl)
        print(f"[SHEET] Final row {row} — {outcome} {pnl:+.2f}R ✓")
    except Exception as e: print(f"[SHEET] Final err: {e}")

def load_from_sheet():
    sheet = get_sheet()
    if not sheet: return {}
    try:
        rows = sheet.get_all_values()
        out  = {}
        for row in rows[1:]:
            if len(row)<14 or row[13].strip() != 'ACTIVE': continue
            try:
                tp1_hit = (row[15].strip() == 'WIN') if len(row)>15 else False
                pair    = row[1].strip().upper()
                direct  = row[2].strip().lower()
                key     = f"{pair}_{direct}_{row[0].strip()}"
                out[key] = {
                    'fired_at':  row[0].strip(), 'pair': pair,
                    'direction': direct,
                    'entry': float(row[3]), 'sl': float(row[4]),
                    'tp1':   float(row[5]), 'tp2': float(row[6]),
                    'rr_tp1': float(row[7]) if row[7] else 1.5,
                    'rr_tp2': float(row[8]) if row[8] else 3.0,
                    'entry_time': row[14].strip() if len(row)>14 else '',
                    'tp1_hit': tp1_hit, 'status': 'ACTIVE',
                }
                print(f"[LOAD] {pair} {direct.upper()} tp1_hit={tp1_hit}")
            except Exception as e: print(f"[LOAD] Row err: {e}")
        print(f"[LOAD] {len(out)} trade(s) loaded from sheet")
        return out
    except Exception as e: print(f"[LOAD] {e}"); return {}


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def _tg(msg):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id":TG_CHAT_ID,"text":msg,"parse_mode":"Markdown"},
                      timeout=10)
    except Exception as e: print(f"[TG] {e}")

def alert_tp1(sig):
    pair=sig['pair']; dp=DP.get(pair,5)
    side='SELL' if sig['direction']=='short' else 'BUY'
    _tg(f"🎯 *{pair} — TP1 Hit!*\n\n"
        f"Direction : `{side}`\n"
        f"TP1 : `{float(sig['tp1']):.{dp}f}` ✅  50% closed\n"
        f"SL → BE : `{float(sig['entry']):.{dp}f}` ← move stop now\n"
        f"TP2 active : `{float(sig['tp2']):.{dp}f}`  RR `1:{sig.get('rr_tp2','?')}`\n\n"
        f"_Remainder running to TP2._")
    print(f"[ALERT] TP1 — {pair}")

def alert_final(sig, outcome, price):
    pair=sig['pair']; dp=DP.get(pair,5)
    side='SELL' if sig['direction']=='short' else 'BUY'
    r1=float(sig.get('rr_tp1',1.5)); r2=float(sig.get('rr_tp2',3.0))
    if outcome=='FULL_WIN': emoji='✅✅'; label='FULL WIN'; pnl=f"+{r1*.5+r2*.5:.2f}R"
    elif outcome=='TP1_BE': emoji='✅';  label='TP1 + Breakeven'; pnl=f"+{r1*.5:.2f}R"
    else:                   emoji='❌';  label='LOSS'; pnl="-1.00R"
    _tg(f"{emoji} *SWEEP RESULT — {side} {pair}*\n\n"
        f"Outcome : *{label}*\n"
        f"Entry : `{float(sig['entry']):.{dp}f}`\n"
        f"Close : `{price:.{dp}f}`\n"
        f"P&L : `{pnl}`\n\n"
        f"_Logged → EDGE Journal SweepFVG tab_")
    print(f"[ALERT] {outcome} — {pair}")


# ── PRICE FETCH (Finnhub — zero Twelve Data credits) ─────────────────────────
_price_cache = {}; _price_ts = {}
CACHE_TTL = 55   # just under POLL_INTERVAL so cache is always fresh per poll

def fetch_price(pair):
    now = time.time()
    if pair in _price_cache and now - _price_ts.get(pair, 0) < CACHE_TTL:
        return _price_cache[pair]
    sym = FH_SYMBOLS.get(pair)
    if not sym: return None
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}",
            timeout=8).json()
        # Finnhub returns 'c' for current price
        p = r.get("c")
        if p and float(p) > 0:
            _price_cache[pair] = float(p)
            _price_ts[pair]    = now
            return float(p)
    except Exception as e:
        print(f"[PRICE] {pair}: {e}")
    return None


# ── MONITOR THREAD ────────────────────────────────────────────────────────────
def monitor_loop():
    time.sleep(5)
    print(f"[MONITOR] Running — poll every {POLL_INTERVAL}s")
    while True:
        try:
            _poll()
        except Exception as e:
            print(f"[MONITOR] Cycle error: {e}")
        time.sleep(POLL_INTERVAL)

def _poll():
    stats["polls"] += 1
    stats["last_poll"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    with trades_lock:
        if not active_trades: return
        keys = list(active_trades.keys())

    resolved = []
    for i, key in enumerate(keys):
        with trades_lock:
            sig = active_trades.get(key)
        if not sig: continue

        # Stagger API calls across the poll window to stay under rate limit
        if i > 0: time.sleep(60 / max(len(keys), 1))

        pair      = sig['pair']; direction = sig['direction']
        entry     = float(sig['entry']); sl = float(sig['sl'])
        tp1       = float(sig['tp1']); tp2 = float(sig['tp2'])
        tp1_hit   = sig.get('tp1_hit', False)

        price = fetch_price(pair)
        if price is None: continue

        dp = DP.get(pair, 5)

        if not tp1_hit:
            # Phase 1 — entry to TP1 or SL
            tp1_reached = price <= tp1 if direction == 'short' else price >= tp1
            sl_reached  = price >= sl  if direction == 'short' else price <= sl

            if tp1_reached:
                print(f"[MONITOR] {pair} TP1 @ {price:.{dp}f}")
                stats["tp1_hits"] += 1
                with trades_lock: active_trades[key]['tp1_hit'] = True
                alert_tp1(sig); sheet_tp1(sig)
            elif sl_reached:
                print(f"[MONITOR] {pair} LOSS @ {price:.{dp}f}")
                stats["losses"] += 1
                alert_final(sig, 'LOSS', price); sheet_final(sig, 'LOSS')
                resolved.append(key)
        else:
            # Phase 2 — TP1 already hit, tracking to TP2 or BE
            be          = entry
            tp2_reached = price <= tp2 if direction == 'short' else price >= tp2
            be_reached  = price >= be  if direction == 'short' else price <= be

            if tp2_reached:
                print(f"[MONITOR] {pair} FULL WIN @ {price:.{dp}f}")
                stats["full_wins"] += 1
                alert_final(sig, 'FULL_WIN', price); sheet_final(sig, 'FULL_WIN')
                resolved.append(key)
            elif be_reached:
                print(f"[MONITOR] {pair} TP1_BE @ {price:.{dp}f}")
                stats["tp1_be"] += 1
                alert_final(sig, 'TP1_BE', price); sheet_final(sig, 'TP1_BE')
                resolved.append(key)

    with trades_lock:
        for key in resolved: active_trades.pop(key, None)
        stats["active"] = len(active_trades)


# ── SYNC THREAD ───────────────────────────────────────────────────────────────
def sync_loop():
    time.sleep(30)
    while True:
        try:
            _sync()
        except Exception as e:
            print(f"[SYNC] Error: {e}")
        time.sleep(SYNC_INTERVAL)

def _sync():
    sheet = get_sheet()
    if not sheet: return
    try:
        added = 0
        for row in sheet.get_all_values()[1:]:
            if len(row)<14 or row[13].strip()!='ACTIVE': continue
            try:
                pair   = row[1].strip().upper()
                direct = row[2].strip().lower()
                key    = f"{pair}_{direct}_{row[0].strip()}"
                with trades_lock:
                    if key in active_trades: continue
                tp1_hit = (row[15].strip()=='WIN') if len(row)>15 else False
                sig = {
                    'fired_at': row[0].strip(), 'pair': pair, 'direction': direct,
                    'entry': float(row[3]), 'sl': float(row[4]),
                    'tp1': float(row[5]),   'tp2': float(row[6]),
                    'rr_tp1': float(row[7]) if row[7] else 1.5,
                    'rr_tp2': float(row[8]) if row[8] else 3.0,
                    'entry_time': row[14].strip() if len(row)>14 else '',
                    'tp1_hit': tp1_hit, 'status': 'ACTIVE',
                }
                with trades_lock:
                    active_trades[key] = sig
                    stats["active"]    = len(active_trades)
                print(f"[SYNC] Added {pair} {direct.upper()} tp1_hit={tp1_hit}")
                added += 1
            except Exception as e: print(f"[SYNC] Row err: {e}")
        if added: print(f"[SYNC] +{added} trade(s). Total: {stats['active']}")
    except Exception as e: print(f"[SYNC] Read err: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    stats["started"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*50}")
    print(f"  EDGE Replit Tracker — {stats['started']}")
    print(f"{'='*50}\n")

    # Load existing active trades from sheet on startup
    loaded = load_from_sheet()
    with trades_lock:
        active_trades.update(loaded)
        stats["active"] = len(active_trades)

    # Start background threads
    threading.Thread(target=monitor_loop, daemon=True, name="monitor").start()
    threading.Thread(target=sync_loop,    daemon=True, name="sync").start()
    print(f"[OK] Watching {stats['active']} trade(s). Threads started.\n")

    # Flask — port 8080, publicly accessible on Replit
    app.run(host="0.0.0.0", port=8080, debug=False)

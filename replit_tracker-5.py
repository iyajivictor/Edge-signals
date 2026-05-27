"""
EDGE Replit Tracker — v4
=========================
Real-time price monitoring for B&R and Sweep+FVG trades.
Runs on Render, kept alive via UptimeRobot pinging /health every 5min.
Price data via Finnhub (free tier, 60 calls/min).

Setup:
  1. Add environment variables on Render:
       TG_TOKEN, TG_CHAT_ID, FINNHUB_KEY, SHEET_ID, GOOGLE_CREDENTIALS
  2. Deploy — copy the URL shown
  3. Add URL to UptimeRobot as HTTP monitor, every 5 minutes

Architecture:
  Two background threads:
    monitor_loop — polls prices every 60s, runs _poll()
    sync_loop    — reads sheet every 60s, runs _sync()

  Three trade pools (all in memory, synced from sheet):
    sweep_pending{}  ← PENDING_ENTRY rows from SweepFVG tab
    sweep_active{}   ← ACTIVE rows from SweepFVG tab
    br_active{}      ← rows from Sheet1 with no outcome yet

  _poll() lifecycle:

    Sweep Phase 0 — pending → retrace → entry
      SHORT: price retraces UP into FVG (price >= fvg_bottom)
      LONG:  price retraces DOWN into FVG (price <= fvg_top)
      On trigger:
        → Telegram entry alert
        → Sheet: status ACTIVE, entry_time filled
        → Move to sweep_active{}

    Sweep Phase 1 — entry → TP1 or SL
      On TP1: alert, sheet col 16, continue Phase 2
      On SL:  alert, sheet final, analytics, remove

    Sweep Phase 2 — TP1 hit → TP2 or BE
      On TP2: FULL WIN alert, sheet final, analytics, remove
      On BE:  TP1_BE alert, sheet final, analytics, remove

    B&R Phase — entry → TP or SL (single TP, no partials)
      On TP: WIN alert, sheet cols I-P, remove
      On SL: LOSS alert, sheet cols I-P, remove

Sheet columns (SweepFVG tab):
  1  fired_at     9  rr_tp2    17 tp2_outcome
  2  pair        10  sl_pips   18 pnl_r
  3  direction   11  zone_src  19 fvg_top    ← Phase 0 retrace
  4  entry       12  session   20 fvg_bottom ← Phase 0 retrace
  5  sl          13  sweep_time 21 tp_mode
  6  tp1         14  status    22 quality
  7  tp2         15  entry_time
  8  rr_tp1      16  tp1_outcome

Sheet columns (Sheet1 — B&R tab):
  A signal_time   E sl          I  outcome      M entry_time
  B pair          F tp          J  close_price   N bars_to_outcome
  C side          G rr          K  close_time    O mae_pips
  D entry         H sl_pips     L  trend         P mfe_pips
                                                  Q session

Changes vs v3:
  · B&R monitoring added (Phase — single TP/SL)
  · Sweep Phase 0 added (retrace monitoring)
  · outcome_tracker.py fully replaced
  · SINGLE TP mode handled (tp2 may be None)
  · MAE/MFE tracked for both strategies
  · Analytics tab updated for both strategies
  · Distinct Telegram signatures per strategy
  · Sheet1 cols I-Q written by Replit on B&R resolution
"""

import os
import json
import time
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, jsonify
from google.oauth2.service_account import Credentials
import gspread

# ── CONFIG ────────────────────────────────────────────────────────────────────
TG_TOKEN     = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID   = os.environ.get("TG_CHAT_ID", "")
FINNHUB_KEY  = os.environ.get("FINNHUB_KEY", "")
SHEET_ID     = os.environ.get("SHEET_ID", "")
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDENTIALS", "")

POLL_INTERVAL = 60   # seconds between price checks
SYNC_INTERVAL = 60   # seconds between sheet syncs

# ── PAIR MAPS ─────────────────────────────────────────────────────────────────
FH_SYMBOLS = {
    "EURUSD": "OANDA:EUR_USD", "GBPUSD": "OANDA:GBP_USD",
    "USDJPY": "OANDA:USD_JPY", "AUDJPY": "OANDA:AUD_JPY",
    "CADJPY": "OANDA:CAD_JPY", "USDCAD": "OANDA:USD_CAD",
    "EURJPY": "OANDA:EUR_JPY", "GBPJPY": "OANDA:GBP_JPY",
    "GBPAUD": "OANDA:GBP_AUD", "XAUUSD": "OANDA:XAU_USD",
}

PIP_SIZE = {
    "EURUSD": 0.0001, "GBPUSD": 0.0001,
    "USDCAD": 0.0001, "GBPAUD": 0.0001,
    "USDJPY": 0.01,   "AUDJPY": 0.01,
    "CADJPY": 0.01,   "EURJPY": 0.01,
    "GBPJPY": 0.01,   "XAUUSD": 0.10,
}

DP = {
    "EURUSD": 5, "GBPUSD": 5,
    "USDCAD": 5, "GBPAUD": 5,
    "USDJPY": 3, "AUDJPY": 3,
    "CADJPY": 3, "EURJPY": 3,
    "GBPJPY": 3, "XAUUSD": 2,
}

# Pending TTL — matches signal_engine
PENDING_TTL_MINS = 60

# Invalidation buffer beyond FVG in pips
INVALIDATION_PIPS = 20

# MAE/MFE persistence file
EXCURSION_FILE = 'state/excursion.json'


# ── MAE/MFE PERSISTENCE ───────────────────────────────────────────────────────
def _load_excursion() -> dict:
    """
    Load persisted MAE/MFE state from disk.
    Survives Replit restarts — ensures excursion
    tracking continues accurately after reconnect.
    """
    if os.path.exists(EXCURSION_FILE):
        try:
            with open(EXCURSION_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_excursion(data: dict):
    """Persist MAE/MFE state to disk."""
    os.makedirs(os.path.dirname(EXCURSION_FILE), exist_ok=True)
    with open(EXCURSION_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def _update_excursion(key: str, mae: float, mfe: float,
                      entry_time: str = ''):
    """
    Update persisted excursion for a single trade key.
    Called every poll cycle for active trades.
    """
    data = _load_excursion()
    data[key] = {
        'mae'       : mae,
        'mfe'       : mfe,
        'entry_time': entry_time,
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }
    _save_excursion(data)

def _remove_excursion(key: str):
    """Remove excursion entry when trade resolves."""
    data = _load_excursion()
    if key in data:
        data.pop(key)
        _save_excursion(data)

def _restore_excursion(key: str, sig: dict) -> dict:
    """
    On sync, restore persisted MAE/MFE into active sig dict.
    If no persisted data, initialize anchors to entry price.
    Returns sig with mae/mfe populated.
    """
    data    = _load_excursion()
    entry   = float(sig.get('entry', 0))
    stored  = data.get(key)

    if stored:
        sig['mae']        = stored['mae']
        sig['mfe']        = stored['mfe']
        sig['entry_time'] = stored.get('entry_time') or \
                            sig.get('entry_time', '')
        print(
            f"[EXCURSION] Restored {key} | "
            f"mae={stored['mae']} mfe={stored['mfe']}"
        )
    else:
        # No persisted data — initialize to entry
        # (trade was active before excursion tracking existed)
        sig['mae'] = entry
        sig['mfe'] = entry

    return sig


# ── FLASK ─────────────────────────────────────────────────────────────────────
app          = Flask(__name__)
trades_lock  = threading.Lock()

# Trade pools
sweep_pending = {}   # key → pending signal dict (Phase 0)
sweep_active  = {}   # key → active signal dict  (Phase 1+2)
br_active     = {}   # key → B&R signal dict

stats = {
    "started"      : "",
    "last_poll"    : "",
    "polls"        : 0,
    # Sweep counters
    "sweep_entries": 0,
    "sweep_tp1"    : 0,
    "sweep_wins"   : 0,
    "sweep_tp1_be" : 0,
    "sweep_losses" : 0,
    # B&R counters
    "br_wins"      : 0,
    "br_losses"    : 0,
}

@app.route("/health")
def health():
    with trades_lock:
        pending = len(sweep_pending)
        active  = len(sweep_active)
        br      = len(br_active)
    return jsonify({
        **stats,
        "sweep_pending": pending,
        "sweep_active" : active,
        "br_active"    : br,
        "status"       : "ok",
        "price_source" : "finnhub",
    })

@app.route("/trades")
def trades_view():
    with trades_lock:
        return jsonify({
            "sweep_pending": list(sweep_pending.values()),
            "sweep_active" : list(sweep_active.values()),
            "br_active"    : list(br_active.values()),
        })

@app.route("/")
def index():
    return (
        "<h2>EDGE Tracker 🟢</h2>"
        "<p>"
        "<a href='/health'>/health</a> | "
        "<a href='/trades'>/trades</a>"
        "</p>"
    )


# ── SHEET CONNECTIONS ─────────────────────────────────────────────────────────
_sweep_sheet     = None
_br_sheet        = None
_analytics_sheet = None

def _auth():
    """Return authenticated gspread workbook."""
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds).open_by_key(SHEET_ID)

def get_sweep_sheet():
    global _sweep_sheet
    if _sweep_sheet:
        return _sweep_sheet
    try:
        _sweep_sheet = _auth().worksheet("SweepFVG")
        print("[SHEET] SweepFVG connected ✓")
        return _sweep_sheet
    except Exception as e:
        print(f"[SHEET] SweepFVG error: {e}")
        return None

def get_br_sheet():
    global _br_sheet
    if _br_sheet:
        return _br_sheet
    try:
        _br_sheet = _auth().sheet1
        print("[SHEET] Sheet1 (B&R) connected ✓")
        return _br_sheet
    except Exception as e:
        print(f"[SHEET] B&R error: {e}")
        return None

def get_analytics_sheet():
    global _analytics_sheet
    if _analytics_sheet:
        return _analytics_sheet
    try:
        wb = _auth()
        try:
            _analytics_sheet = wb.worksheet("Analytics")
        except gspread.exceptions.WorksheetNotFound:
            _analytics_sheet = wb.add_worksheet(
                "Analytics", rows=5000, cols=35
            )
            _analytics_sheet.append_row([
                # Identity (7)
                'strategy', 'pair', 'direction', 'session',
                'zone_src', 'quality', 'tp_mode',
                # Levels (7)
                'entry', 'sl', 'tp1', 'tp2',
                'rr_tp1', 'rr_tp2', 'sl_pips',
                # Timestamps (4)
                'fired_at', 'entry_time',
                'tp1_time', 'outcome_time',
                # Outcome (3)
                'outcome', 'final_price', 'pnl_r',
                # MAE/MFE (3)
                'mae_pips', 'mfe_pips', 'mae_mfe_ratio',
                # Timing in minutes (4)
                'fired_to_entry_min',
                'entry_to_tp1_min',
                'tp1_to_outcome_min',
                'total_trade_min',
                # New analytics fields (10)
                'htf_trend',
                'sweep_body_pct',
                'sweep_wick_ratio',
                'n_candles_in_zone',
                'zone_age_h4_bars',
                'distance_to_cluster_pips',
                'fvg_size_atr_mult',
                'sweep_to_fvg_bars',
                'mss_level',
                'mss_candle_time',
            ])
            print("[ANALYTICS] Tab created with headers ✓")
        print("[ANALYTICS] Connected ✓")
        return _analytics_sheet
    except Exception as e:
        print(f"[ANALYTICS] Error: {e}")
        return None


# ── SHEET HELPERS ─────────────────────────────────────────────────────────────
def _find_sweep_row(sheet, sig: dict) -> int | None:
    """Find SweepFVG row by fired_at + pair + direction."""
    try:
        for i, row in enumerate(sheet.get_all_values()[1:], start=2):
            if (len(row) >= 3 and
                    row[0] == sig.get('fired_at', '') and
                    row[1] == sig.get('pair', '') and
                    row[2].upper() == sig.get('direction', '').upper()):
                return i
    except Exception as e:
        print(f"[SHEET] Row search error: {e}")
    return None

def _find_br_row(sheet, sig: dict) -> int | None:
    """Find Sheet1 row by signal_time + pair + side."""
    try:
        for i, row in enumerate(sheet.get_all_values()[1:], start=2):
            if (len(row) >= 3 and
                    row[0] == sig.get('signal_time', '') and
                    row[1] == sig.get('pair', '') and
                    row[2].upper() == sig.get('side', '').upper()):
                return i
    except Exception as e:
        print(f"[SHEET] B&R row search error: {e}")
    return None

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def _parse_ts(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(
            s.replace(' UTC', '+00:00')
        )
    except Exception:
        return None

def _mins(a: datetime, b: datetime) -> str:
    if a and b:
        return str(round((b - a).total_seconds() / 60, 1))
    return ''


# ── SHEET WRITES — SWEEP ──────────────────────────────────────────────────────
def sheet_sweep_entry(sig: dict, entry_time: str):
    """
    Phase 0 → Phase 1 transition.
    Update SweepFVG row: status=ACTIVE, entry_time filled.
    """
    sheet = get_sweep_sheet()
    if not sheet:
        return
    try:
        row = _find_sweep_row(sheet, sig)
        if not row:
            print(f"[SHEET] Entry row not found: {sig.get('pair')}")
            return
        sheet.update_cell(row, 14, 'ACTIVE')
        sheet.update_cell(row, 15, entry_time)
        print(f"[SHEET] Sweep entry — row {row} ACTIVE ✓")
    except Exception as e:
        print(f"[SHEET] Sweep entry error: {e}")

def sheet_sweep_tp1(sig: dict):
    """Phase 1 → Phase 2. Update col 16 = WIN."""
    sheet = get_sweep_sheet()
    if not sheet:
        return
    try:
        row = _find_sweep_row(sheet, sig)
        if row:
            sheet.update_cell(row, 16, 'WIN')
            print(f"[SHEET] Sweep TP1 — row {row} ✓")
    except Exception as e:
        print(f"[SHEET] Sweep TP1 error: {e}")

def sheet_sweep_final(sig: dict, outcome: str):
    """
    Phase 2 final. Update:
      col 14 → CLOSED
      col 16 → tp1_outcome
      col 17 → tp2_outcome
      col 18 → pnl_r
    """
    sheet = get_sweep_sheet()
    if not sheet:
        return
    try:
        row = _find_sweep_row(sheet, sig)
        if not row:
            return

        r1  = float(sig.get('rr_tp1') or 2.0)
        r2  = float(sig.get('rr_tp2') or 0)
        tp_mode = sig.get('tp_mode', 'DUAL')

        if outcome == 'FULL_WIN':
            pnl = round(r1 * 0.5 + r2 * 0.5, 2) \
                  if tp_mode == 'DUAL' else round(r1, 2)
            tp1_out = 'WIN'
            tp2_out = 'WIN'
        elif outcome == 'TP1_BE':
            pnl     = round(r1 * 0.5, 2)
            tp1_out = 'WIN'
            tp2_out = 'BE'
        else:  # LOSS
            pnl     = -1.0
            tp1_out = 'LOSS'
            tp2_out = 'LOSS'

        sheet.update_cell(row, 14, 'CLOSED')
        sheet.update_cell(row, 16, tp1_out)
        sheet.update_cell(row, 17, tp2_out)
        sheet.update_cell(row, 18, pnl)
        print(
            f"[SHEET] Sweep final — row {row} "
            f"{outcome} {pnl:+.2f}R ✓"
        )
    except Exception as e:
        print(f"[SHEET] Sweep final error: {e}")


# ── SHEET WRITES — B&R ────────────────────────────────────────────────────────
def sheet_br_final(sig: dict, outcome: str,
                   close_price: float, close_time: str,
                   mae_pips: float, mfe_pips: float):
    """
    B&R outcome resolution.
    Fills Sheet1 cols I-P.
    """
    sheet = get_br_sheet()
    if not sheet:
        return
    try:
        row = _find_br_row(sheet, sig)
        if not row:
            print(f"[SHEET] B&R row not found: {sig.get('pair')}")
            return

        pnl = float(sig.get('rr', 2.0)) if outcome == 'WIN' else -1.0

        sheet.update_cell(row, 9,  outcome)       # I outcome
        sheet.update_cell(row, 10, close_price)   # J close_price
        sheet.update_cell(row, 11, close_time)    # K close_time
        # col L (trend) already written by signal_engine
        sheet.update_cell(row, 13, sig.get('signal_time', ''))  # M entry_time
        sheet.update_cell(row, 14, '')            # N bars_to_outcome (not tracked live)
        sheet.update_cell(row, 15, mae_pips)      # O mae_pips
        sheet.update_cell(row, 16, mfe_pips)      # P mfe_pips
        # col Q (session) already written by signal_engine

        print(
            f"[SHEET] B&R final — row {row} "
            f"{outcome} {pnl:+.2f}R ✓"
        )
    except Exception as e:
        print(f"[SHEET] B&R final error: {e}")


# ── ANALYTICS ─────────────────────────────────────────────────────────────────
def log_analytics_sweep(sig: dict, outcome: str,
                         final_price: float,
                         entry_time: str, tp1_time: str,
                         outcome_time: str):
    """Log completed Sweep+FVG trade to Analytics tab — 38 data points."""
    try:
        fired_dt = _parse_ts(sig.get('fired_at', ''))
        entry_dt = _parse_ts(entry_time)
        tp1_dt   = _parse_ts(tp1_time)
        out_dt   = _parse_ts(outcome_time)

        r1      = float(sig.get('rr_tp1') or 2.0)
        r2      = float(sig.get('rr_tp2') or 0)
        tp_mode = sig.get('tp_mode', 'DUAL')

        if outcome == 'FULL_WIN':
            pnl = round(r1 * 0.5 + r2 * 0.5, 2) \
                  if tp_mode == 'DUAL' else round(r1, 2)
        elif outcome == 'TP1_BE':
            pnl = round(r1 * 0.5, 2)
        else:
            pnl = -1.0

        mae   = sig.get('mae')
        mfe   = sig.get('mfe')
        pip   = PIP_SIZE.get(sig.get('pair', ''), 0.0001)
        entry = float(sig.get('entry', 0))

        if mae is not None:
            direction = sig.get('direction', '')
            mae_pips  = round(
                abs(mae - entry) / pip, 1
            ) if direction == 'short' else round(
                abs(entry - mae) / pip, 1
            )
            mfe_pips  = round(
                abs(entry - mfe) / pip, 1
            ) if direction == 'short' else round(
                abs(mfe - entry) / pip, 1
            )
            ratio = round(mae_pips / mfe_pips, 2) \
                    if mfe_pips else ''
        else:
            mae_pips = mfe_pips = ratio = ''

        row = [
            # ── Identity (7) ──────────────────────────────────
            'Sweep+FVG',
            sig.get('pair', ''),
            sig.get('direction', ''),
            sig.get('session', ''),
            sig.get('zone_src', ''),
            sig.get('quality', ''),
            tp_mode,
            # ── Levels (7) ────────────────────────────────────
            sig.get('entry', ''),
            sig.get('sl', ''),
            sig.get('tp1', ''),
            sig.get('tp2', ''),
            sig.get('rr_tp1', ''),
            sig.get('rr_tp2', ''),
            sig.get('sl_pips', ''),
            # ── Timestamps (4) ────────────────────────────────
            sig.get('fired_at', ''),
            entry_time,
            tp1_time,
            outcome_time,
            # ── Outcome (3) ───────────────────────────────────
            outcome,
            final_price,
            pnl,
            # ── MAE/MFE (3) ───────────────────────────────────
            mae_pips,
            mfe_pips,
            ratio,
            # ── Timing in minutes (4) ─────────────────────────
            _mins(fired_dt, entry_dt),
            _mins(entry_dt, tp1_dt),
            _mins(tp1_dt, out_dt) if tp1_time else '',
            _mins(entry_dt, out_dt),
            # ── New analytics fields (10) ─────────────────────
            sig.get('htf_trend', ''),
            sig.get('sweep_body_pct', ''),
            sig.get('sweep_wick_ratio', ''),
            sig.get('n_candles_in_zone', ''),
            sig.get('zone_age_h4_bars', ''),
            sig.get('distance_to_cluster_pips', ''),
            sig.get('fvg_size_atr_mult', ''),
            sig.get('sweep_to_fvg_bars', ''),
            sig.get('mss_level', ''),
            sig.get('mss_candle_time', ''),
        ]

        sheet = get_analytics_sheet()
        if sheet:
            sheet.append_row(row)
            print(
                f"[ANALYTICS] Sweep {sig.get('pair')} "
                f"{outcome} → logged ✓"
            )
    except Exception as e:
        print(f"[ANALYTICS] Sweep log error: {e}")

def log_analytics_br(sig: dict, outcome: str,
                     close_price: float, close_time: str,
                     mae_pips: float, mfe_pips: float):
    """Log completed B&R trade to Analytics tab."""
    try:
        signal_dt = _parse_ts(sig.get('signal_time', ''))
        close_dt  = _parse_ts(close_time)
        rr        = float(sig.get('rr', 2.0))
        pnl       = rr if outcome == 'WIN' else -1.0
        ratio     = round(mae_pips / mfe_pips, 2) \
                    if mfe_pips else ''

        row = [
            'B&R',
            sig.get('pair', ''),
            sig.get('side', ''),
            sig.get('session', ''),
            '',            # zone_src — N/A for B&R
            '',            # quality  — N/A for B&R
            'SINGLE',      # tp_mode  — B&R always single
            sig.get('entry', ''),
            sig.get('sl', ''),
            sig.get('tp', ''),
            '',            # tp2 — N/A
            rr,
            '',            # rr_tp2 — N/A
            sig.get('sl_pips', ''),
            sig.get('signal_time', ''),
            sig.get('signal_time', ''),  # entry_time = signal_time for B&R
            '',            # tp1_time — N/A
            close_time,
            outcome,
            close_price,
            pnl,
            mae_pips,
            mfe_pips,
            ratio,
            '0',           # fired_to_entry — instant for B&R
            '',            # entry_to_tp1 — N/A
            '',            # tp1_to_outcome — N/A
            _mins(signal_dt, close_dt),
        ]

        sheet = get_analytics_sheet()
        if sheet:
            sheet.append_row(row)
            print(
                f"[ANALYTICS] B&R {sig.get('pair')} "
                f"{outcome} → logged ✓"
            )
    except Exception as e:
        print(f"[ANALYTICS] B&R log error: {e}")


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def _tg(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={
                "chat_id"   : TG_CHAT_ID,
                "text"      : msg,
                "parse_mode": "Markdown",
            },
            timeout=10,
        )
    except Exception as e:
        print(f"[TG] Error: {e}")

# ── Sweep alerts ──────────────────────────────────────────────────────────────
def alert_sweep_entry(sig: dict, entry_time: str):
    pair = sig['pair']
    dp   = DP.get(pair, 5)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'
    tp_mode = sig.get('tp_mode', 'DUAL')

    if tp_mode == 'DUAL':
        tp_lines = (
            f"TP1 (50%) : `{float(sig['tp1']):.{dp}f}`\n"
            f"TP2 (50%) : `{float(sig['tp2']):.{dp}f}`"
        )
    else:
        tp_lines = f"TP1       : `{float(sig['tp1']):.{dp}f}`"

    _tg(
        f"⚡ *{pair} — Entry Triggered!*\n\n"
        f"Direction : `{side}`\n"
        f"Entry     : `{float(sig['entry']):.{dp}f}` ✅\n"
        f"Stop Loss : `{float(sig['sl']):.{dp}f}`\n"
        f"{tp_lines}\n\n"
        f"_🟢 Trade ACTIVE. Monitoring started._\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )
    print(f"[ALERT] Sweep entry — {pair} {side}")

def alert_sweep_tp1(sig: dict):
    pair = sig['pair']
    dp   = DP.get(pair, 5)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'
    _tg(
        f"🎯 *{pair} — TP1 Hit!*\n\n"
        f"Direction : `{side}`\n"
        f"TP1       : `{float(sig['tp1']):.{dp}f}` ✅  50% closed\n"
        f"SL → BE   : `{float(sig['entry']):.{dp}f}` ← move stop now\n"
        f"TP2 active: `{float(sig['tp2']):.{dp}f}`  "
        f"RR `1:{sig.get('rr_tp2','?')}`\n\n"
        f"_Remainder running to TP2._\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )
    print(f"[ALERT] Sweep TP1 — {pair}")

def alert_sweep_final(sig: dict, outcome: str, price: float):
    pair    = sig['pair']
    dp      = DP.get(pair, 5)
    side    = 'SELL' if sig['direction'] == 'short' else 'BUY'
    r1      = float(sig.get('rr_tp1') or 2.0)
    r2      = float(sig.get('rr_tp2') or 0)
    tp_mode = sig.get('tp_mode', 'DUAL')

    if outcome == 'FULL_WIN':
        emoji = '✅✅'
        label = 'FULL WIN'
        pnl   = f"+{r1*0.5 + r2*0.5:.2f}R" \
                if tp_mode == 'DUAL' else f"+{r1:.2f}R"
    elif outcome == 'TP1_BE':
        emoji = '✅'
        label = 'TP1 + Breakeven'
        pnl   = f"+{r1*0.5:.2f}R"
    else:
        emoji = '❌'
        label = 'LOSS'
        pnl   = '-1.00R'

    _tg(
        f"{emoji} *SWEEP RESULT — {side} {pair}*\n\n"
        f"Outcome : *{label}*\n"
        f"Entry   : `{float(sig['entry']):.{dp}f}`\n"
        f"Close   : `{price:.{dp}f}`\n"
        f"P&L     : `{pnl}`\n\n"
        f"_Logged → EDGE Journal SweepFVG tab_\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )
    print(f"[ALERT] Sweep {outcome} — {pair}")

# ── B&R alerts ────────────────────────────────────────────────────────────────
def alert_br_final(sig: dict, outcome: str, price: float):
    pair  = sig['pair']
    dp    = DP.get(pair, 5)
    side  = sig.get('side', '')
    rr    = float(sig.get('rr', 2.0))
    emoji = '✅' if outcome == 'WIN' else '❌'
    pnl   = f"+{rr:.2f}R" if outcome == 'WIN' else '-1.00R'

    _tg(
        f"{emoji} *B&R RESULT — {side} {pair}*\n\n"
        f"Outcome : *{outcome}*\n"
        f"Entry   : `{float(sig['entry']):.{dp}f}`\n"
        f"Close   : `{price:.{dp}f}`\n"
        f"P&L     : `{pnl}`\n\n"
        f"_Logged → EDGE Journal Sheet1_\n"
        f"_⚡ EDGE Replit Tracker — Break & Retest_"
    )
    print(f"[ALERT] B&R {outcome} — {pair} {side}")


# ── PRICE FETCH ───────────────────────────────────────────────────────────────
_price_cache = {}
_price_ts    = {}
CACHE_TTL    = 55  # just under POLL_INTERVAL

def fetch_price(pair: str) -> float | None:
    """
    Fetch current price from Finnhub.
    Cached for CACHE_TTL seconds to avoid redundant calls
    when multiple trades share the same pair.
    """
    now = time.time()
    if (pair in _price_cache and
            now - _price_ts.get(pair, 0) < CACHE_TTL):
        return _price_cache[pair]

    sym = FH_SYMBOLS.get(pair)
    if not sym:
        return None

    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/quote"
            f"?symbol={sym}&token={FINNHUB_KEY}",
            timeout=8,
        ).json()
        p = r.get("c")
        if p and float(p) > 0:
            _price_cache[pair] = float(p)
            _price_ts[pair]    = now
            return float(p)
    except Exception as e:
        print(f"[PRICE] {pair}: {e}")

    return None


# ── POLL ──────────────────────────────────────────────────────────────────────
def _poll():
    """
    Main monitoring cycle — runs every POLL_INTERVAL seconds.

    Order of operations:
      1. Sweep Phase 0 — pending retrace checks
      2. Sweep Phase 1+2 — active trade monitoring
      3. B&R — active trade monitoring
    """
    stats["polls"]     += 1
    stats["last_poll"]  = _now_str()
    now_utc             = datetime.now(timezone.utc)

    # ── Collect all pairs needing price ───────────────────────
    with trades_lock:
        pending_keys = list(sweep_pending.keys())
        active_keys  = list(sweep_active.keys())
        br_keys      = list(br_active.keys())

    all_pairs = set()
    for k in pending_keys + active_keys + br_keys:
        sig = None
        with trades_lock:
            sig = (
                sweep_pending.get(k) or
                sweep_active.get(k)  or
                br_active.get(k)
            )
        if sig:
            all_pairs.add(sig.get('pair', ''))

    if not all_pairs:
        return

    # ── Stagger API calls ─────────────────────────────────────
    # Pre-fetch prices for all pairs with stagger
    pair_list = list(all_pairs)
    for i, pair in enumerate(pair_list):
        fetch_price(pair)
        if i < len(pair_list) - 1:
            time.sleep(60 / max(len(pair_list), 1))

    # ── Phase 0 — Sweep pending retrace ───────────────────────
    sweep_to_activate = []   # (key, sig, entry_time)
    sweep_to_invalidate = []

    for key in pending_keys:
        with trades_lock:
            sig = sweep_pending.get(key)
        if not sig:
            continue

        pair      = sig['pair']
        direction = sig['direction']
        fvg_top   = float(sig['fvg_top'])
        fvg_bottom= float(sig['fvg_bottom'])
        pip       = PIP_SIZE.get(pair, 0.0001)
        price     = fetch_price(pair)

        if price is None:
            continue

        # TTL check
        try:
            created  = _parse_ts(sig.get('created_at', ''))
            age_mins = (
                now_utc - created
            ).total_seconds() / 60 if created else 9999
            if age_mins > PENDING_TTL_MINS:
                sweep_to_invalidate.append(key)
                print(
                    f"[PHASE0] {pair} pending expired "
                    f"({age_mins:.0f}m) — removed"
                )
                continue
        except Exception:
            pass

        # Directional retrace check
        if direction == 'short':
            # Bearish FVG above price — retrace UP into zone
            triggered   = price >= fvg_bottom
            invalidated = price < fvg_bottom - INVALIDATION_PIPS * pip
        else:
            # Bullish FVG below price — retrace DOWN into zone
            triggered   = price <= fvg_top
            invalidated = price > fvg_top + INVALIDATION_PIPS * pip

        if invalidated:
            sweep_to_invalidate.append(key)
            print(
                f"[PHASE0] {pair} {direction.upper()} "
                f"invalidated — price broke through FVG"
            )
            continue

        if triggered:
            entry_time = _now_str()
            sweep_to_activate.append((key, sig, entry_time))
            stats["sweep_entries"] += 1
            print(
                f"[PHASE0] {pair} {direction.upper()} "
                f"entry triggered @ {price}"
            )

    # Apply Phase 0 transitions
    for key in sweep_to_invalidate:
        with trades_lock:
            sweep_pending.pop(key, None)

    for key, sig, entry_time in sweep_to_activate:
        # Build active sig with entry context
        entry_price = float(sig.get('entry', 0))
        active_sig  = {
            **sig,
            'entry_time': entry_time,
            'tp1_hit'   : False,
            'tp1_hit_at': '',
            'status'    : 'ACTIVE',
            'mae'       : entry_price,   # initialize at entry
            'mfe'       : entry_price,   # initialize at entry
        }
        with trades_lock:
            sweep_pending.pop(key, None)
            sweep_active[key] = active_sig

        # Initialize excursion persistence at entry
        _update_excursion(key, entry_price, entry_price, entry_time)

        # Alert + sheet update
        alert_sweep_entry(sig, entry_time)
        sheet_sweep_entry(sig, entry_time)

    # ── Phase 1+2 — Sweep active ──────────────────────────────
    sweep_resolved = []

    for key in active_keys:
        with trades_lock:
            sig = sweep_active.get(key)
        if not sig:
            continue

        pair      = sig['pair']
        direction = sig['direction']
        entry     = float(sig['entry'])
        sl        = float(sig['sl'])
        tp1       = float(sig['tp1'])
        tp2_raw   = sig.get('tp2')
        tp2       = float(tp2_raw) if tp2_raw else None
        tp1_hit   = sig.get('tp1_hit', False)
        tp_mode   = sig.get('tp_mode', 'DUAL')
        price     = fetch_price(pair)

        if price is None:
            continue

        dp = DP.get(pair, 5)

        # Update MAE / MFE
        with trades_lock:
            if direction == 'short':
                mae = max(sig.get('mae') or price, price)
                mfe = min(sig.get('mfe') or price, price)
            else:
                mae = min(sig.get('mae') or price, price)
                mfe = max(sig.get('mfe') or price, price)
            sweep_active[key]['mae'] = mae
            sweep_active[key]['mfe'] = mfe

        # Persist to disk — survives restarts
        _update_excursion(
            key, mae, mfe,
            sig.get('entry_time', '')
        )

        if not tp1_hit:
            # Phase 1 — entry to TP1 or SL
            tp1_reached = (
                price <= tp1 if direction == 'short'
                else price >= tp1
            )
            sl_reached = (
                price >= sl if direction == 'short'
                else price <= sl
            )

            if tp1_reached and tp_mode == 'DUAL':
                tp1_time = _now_str()
                print(f"[PHASE1] {pair} TP1 @ {price:.{dp}f}")
                stats["sweep_tp1"] += 1
                with trades_lock:
                    sweep_active[key]['tp1_hit']    = True
                    sweep_active[key]['tp1_hit_at'] = tp1_time
                alert_sweep_tp1(sig)
                sheet_sweep_tp1(sig)

            elif tp1_reached and tp_mode == 'SINGLE':
                # Single TP — full close at TP1
                outcome_time = _now_str()
                print(
                    f"[PHASE1] {pair} FULL WIN (SINGLE) "
                    f"@ {price:.{dp}f}"
                )
                stats["sweep_wins"] += 1
                with trades_lock:
                    sig = sweep_active[key]
                alert_sweep_final(sig, 'FULL_WIN', price)
                sheet_sweep_final(sig, 'FULL_WIN')
                log_analytics_sweep(
                    sig, 'FULL_WIN', price,
                    sig.get('entry_time', ''), '',
                    outcome_time
                )
                _remove_excursion(key)
                sweep_resolved.append(key)

            elif sl_reached:
                outcome_time = _now_str()
                print(f"[PHASE1] {pair} LOSS @ {price:.{dp}f}")
                stats["sweep_losses"] += 1
                with trades_lock:
                    sig = sweep_active[key]
                alert_sweep_final(sig, 'LOSS', price)
                sheet_sweep_final(sig, 'LOSS')
                log_analytics_sweep(
                    sig, 'LOSS', price,
                    sig.get('entry_time', ''), '',
                    outcome_time
                )
                _remove_excursion(key)
                sweep_resolved.append(key)

        else:
            # Phase 2 — TP1 hit, monitoring TP2 or BE
            be          = entry
            tp2_reached = (
                price <= tp2 if direction == 'short'
                else price >= tp2
            ) if tp2 else False
            be_reached = (
                price >= be if direction == 'short'
                else price <= be
            )

            if tp2_reached:
                outcome_time = _now_str()
                print(f"[PHASE2] {pair} FULL WIN @ {price:.{dp}f}")
                stats["sweep_wins"] += 1
                with trades_lock:
                    sig = sweep_active[key]
                alert_sweep_final(sig, 'FULL_WIN', price)
                sheet_sweep_final(sig, 'FULL_WIN')
                log_analytics_sweep(
                    sig, 'FULL_WIN', price,
                    sig.get('entry_time', ''),
                    sig.get('tp1_hit_at', ''),
                    outcome_time
                )
                _remove_excursion(key)
                sweep_resolved.append(key)

            elif be_reached:
                outcome_time = _now_str()
                print(f"[PHASE2] {pair} TP1_BE @ {price:.{dp}f}")
                stats["sweep_tp1_be"] += 1
                with trades_lock:
                    sig = sweep_active[key]
                alert_sweep_final(sig, 'TP1_BE', price)
                sheet_sweep_final(sig, 'TP1_BE')
                log_analytics_sweep(
                    sig, 'TP1_BE', price,
                    sig.get('entry_time', ''),
                    sig.get('tp1_hit_at', ''),
                    outcome_time
                )
                _remove_excursion(key)
                sweep_resolved.append(key)

    with trades_lock:
        for key in sweep_resolved:
            sweep_active.pop(key, None)

    # ── B&R Phase — single TP/SL ──────────────────────────────
    br_resolved = []

    for key in br_keys:
        with trades_lock:
            sig = br_active.get(key)
        if not sig:
            continue

        pair  = sig['pair']
        side  = sig['side']
        entry = float(sig['entry'])
        sl    = float(sig['sl'])
        tp    = float(sig['tp'])
        price = fetch_price(pair)

        if price is None:
            continue

        dp = DP.get(pair, 5)

        # Update MAE / MFE
        with trades_lock:
            if side == 'SELL':
                mae = max(sig.get('mae') or price, price)
                mfe = min(sig.get('mfe') or price, price)
            else:
                mae = min(sig.get('mae') or price, price)
                mfe = max(sig.get('mfe') or price, price)
            br_active[key]['mae'] = mae
            br_active[key]['mfe'] = mfe

        # Persist to disk — survives restarts
        _update_excursion(
            key, mae, mfe,
            sig.get('signal_time', '')
        )

        pip      = PIP_SIZE.get(pair, 0.0001)
        tp_hit   = price >= tp if side == 'BUY' else price <= tp
        sl_hit   = price <= sl if side == 'BUY' else price >= sl

        if tp_hit or sl_hit:
            outcome      = 'WIN' if tp_hit else 'LOSS'
            close_price  = tp if tp_hit else sl
            close_time   = _now_str()

            with trades_lock:
                sig = br_active[key]

            # Compute MAE/MFE in pips
            if sig.get('mae') is not None:
                if side == 'SELL':
                    mae_pips = round(
                        abs(sig['mae'] - entry) / pip, 1
                    )
                    mfe_pips = round(
                        abs(entry - sig['mfe']) / pip, 1
                    )
                else:
                    mae_pips = round(
                        abs(entry - sig['mae']) / pip, 1
                    )
                    mfe_pips = round(
                        abs(sig['mfe'] - entry) / pip, 1
                    )
            else:
                mae_pips = mfe_pips = 0

            print(
                f"[B&R] {pair} {side} {outcome} "
                f"@ {close_price:.{dp}f}"
            )
            stats[f"br_{'wins' if outcome == 'WIN' else 'losses'}"] += 1

            alert_br_final(sig, outcome, close_price)
            sheet_br_final(
                sig, outcome, close_price,
                close_time, mae_pips, mfe_pips
            )
            log_analytics_br(
                sig, outcome, close_price,
                close_time, mae_pips, mfe_pips
            )
            _remove_excursion(key)
            br_resolved.append(key)

    with trades_lock:
        for key in br_resolved:
            br_active.pop(key, None)


# ── SYNC ──────────────────────────────────────────────────────────────────────
def _sync():
    """
    Read both sheets and populate trade pools.
    Runs every SYNC_INTERVAL seconds.

    SweepFVG tab:
      PENDING_ENTRY rows → sweep_pending{}
      ACTIVE rows        → sweep_active{}

    Sheet1 (B&R):
      Rows with no outcome (col I empty) → br_active{}
    """
    _sync_sweep()
    _sync_br()

def _sync_sweep():
    """Sync SweepFVG tab → sweep_pending + sweep_active."""
    sheet = get_sweep_sheet()
    if not sheet:
        return
    try:
        added_pending = 0
        added_active  = 0

        for row in sheet.get_all_values()[1:]:
            if len(row) < 14:
                continue

            status = row[13].strip()
            if status not in ('PENDING_ENTRY', 'ACTIVE'):
                continue

            try:
                pair      = row[1].strip().upper()
                direction = row[2].strip().lower()
                fired_at  = row[0].strip()
                key       = f"{pair}_{direction}_{fired_at}"

                with trades_lock:
                    already = (
                        key in sweep_pending or
                        key in sweep_active
                    )
                if already:
                    continue

                # fvg_top and fvg_bottom in cols 19-20
                fvg_top    = float(row[18]) if len(row) > 18 and row[18] else None
                fvg_bottom = float(row[19]) if len(row) > 19 and row[19] else None

                sig = {
                    'fired_at'   : fired_at,
                    'created_at' : fired_at,  # proxy for TTL
                    'pair'       : pair,
                    'direction'  : direction,
                    'entry'      : float(row[3]),
                    'sl'         : float(row[4]),
                    'tp1'        : float(row[5]),
                    'tp2'        : float(row[6]) if row[6] else None,
                    'rr_tp1'     : float(row[7]) if row[7] else 2.0,
                    'rr_tp2'     : float(row[8]) if row[8] else None,
                    'sl_pips'    : float(row[9]) if row[9] else None,
                    'zone_src'   : row[10].strip() if len(row) > 10 else '',
                    'session'    : row[11].strip() if len(row) > 11 else '',
                    'sweep_time' : row[12].strip() if len(row) > 12 else '',
                    'entry_time' : row[14].strip() if len(row) > 14 else '',
                    'fvg_top'    : fvg_top,
                    'fvg_bottom' : fvg_bottom,
                    'tp_mode'    : row[20].strip() if len(row) > 20 else 'DUAL',
                    'quality'    : row[21].strip() if len(row) > 21 else '',
                    'tp1_hit'    : row[15].strip() == 'WIN' if len(row) > 15 else False,
                    'tp1_hit_at' : row[15].strip() if len(row) > 15 else '',
                    'status'     : status,
                    'mae'        : None,
                    'mfe'        : None,
                }

                with trades_lock:
                    if status == 'PENDING_ENTRY':
                        if fvg_top is None or fvg_bottom is None:
                            print(
                                f"[SYNC] {pair} PENDING missing "
                                f"fvg bounds — skipping"
                            )
                            continue
                        sweep_pending[key] = sig
                        added_pending += 1
                        print(
                            f"[SYNC] Pending: {pair} "
                            f"{direction.upper()}"
                        )
                    else:
                        # Restore persisted MAE/MFE on reconnect
                        sig = _restore_excursion(key, sig)
                        sweep_active[key] = sig
                        added_active += 1
                        print(
                            f"[SYNC] Active: {pair} "
                            f"{direction.upper()} "
                            f"tp1_hit={sig['tp1_hit']}"
                        )

            except Exception as e:
                print(f"[SYNC] Sweep row error: {e}")

        if added_pending or added_active:
            print(
                f"[SYNC] Sweep +{added_pending} pending "
                f"+{added_active} active"
            )

    except Exception as e:
        print(f"[SYNC] Sweep read error: {e}")

def _sync_br():
    """Sync Sheet1 → br_active. Loads rows with no outcome."""
    sheet = get_br_sheet()
    if not sheet:
        return
    try:
        added = 0
        for row in sheet.get_all_values()[1:]:
            if len(row) < 8:
                continue

            # Skip if outcome already filled (col I = index 8)
            outcome_col = row[8].strip() if len(row) > 8 else ''
            if outcome_col:
                continue

            try:
                pair        = row[1].strip().upper()
                side        = row[2].strip().upper()
                signal_time = row[0].strip()
                key         = f"BR_{pair}_{side}_{signal_time}"

                with trades_lock:
                    if key in br_active:
                        continue

                sig = {
                    'signal_time': signal_time,
                    'pair'       : pair,
                    'side'       : side,
                    'entry'      : float(row[3]),
                    'sl'         : float(row[4]),
                    'tp'         : float(row[5]),
                    'rr'         : float(row[6]) if row[6] else 2.0,
                    'sl_pips'    : float(row[7]) if row[7] else None,
                    'trend'      : row[11].strip() if len(row) > 11 else '',
                    'session'    : row[16].strip() if len(row) > 16 else '',
                    'mae'        : None,
                    'mfe'        : None,
                }

                with trades_lock:
                    # Restore persisted MAE/MFE on reconnect
                    sig = _restore_excursion(key, sig)
                    br_active[key] = sig
                    added += 1
                    print(
                        f"[SYNC] B&R: {pair} {side} "
                        f"@ {sig['entry']}"
                    )

            except Exception as e:
                print(f"[SYNC] B&R row error: {e}")

        if added:
            print(f"[SYNC] B&R +{added} trade(s)")

    except Exception as e:
        print(f"[SYNC] B&R read error: {e}")


# ── BACKGROUND THREADS ────────────────────────────────────────────────────────
def monitor_loop():
    time.sleep(5)
    print(f"[MONITOR] Running — poll every {POLL_INTERVAL}s")
    while True:
        try:
            _poll()
        except Exception as e:
            print(f"[MONITOR] Cycle error: {e}")
        time.sleep(POLL_INTERVAL)

def sync_loop():
    time.sleep(30)
    print(f"[SYNC] Running — sync every {SYNC_INTERVAL}s")
    while True:
        try:
            _sync()
        except Exception as e:
            print(f"[SYNC] Error: {e}")
        time.sleep(SYNC_INTERVAL)


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    stats["started"] = _now_str()

    print(f"\n{'='*55}")
    print(f"  EDGE Replit Tracker v4 — {stats['started']}")
    print(f"{'='*55}\n")

    # Initial sync on startup — load all open trades
    print("[STARTUP] Loading open trades from sheets...")
    _sync()

    with trades_lock:
        print(
            f"[STARTUP] Loaded: "
            f"{len(sweep_pending)} sweep pending | "
            f"{len(sweep_active)} sweep active | "
            f"{len(br_active)} B&R active"
        )

    # Start background threads
    threading.Thread(
        target=monitor_loop, daemon=True, name="monitor"
    ).start()
    threading.Thread(
        target=sync_loop, daemon=True, name="sync"
    ).start()

    print(f"[OK] Threads started. Monitoring active.\n")

    # Flask — port 8080
    app.run(host="0.0.0.0", port=8080, debug=False)

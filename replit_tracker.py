"""
EDGE Replit Tracker — v5
=========================
Real-time price monitoring for Sweep+FVG trades only.
B&R fully removed.

Runs on Render, kept alive via UptimeRobot pinging /health every 5min.
Price data via Finnhub (free tier, 60 calls/min).

Changes vs v4:
  · B&R monitoring removed entirely
    (br_active, _sync_br, get_br_sheet, sheet_br_final,
     alert_br_final, log_analytics_br, _find_br_row all gone)
  · Sheet1 / B&R tab reference removed
  · Analytics tab expanded to 42 columns (was 38):
      39 d1_trend_state
      40 distance_to_d1_level_r
      41 weekly_range_pct
      42 day_of_week
      43 mins_from_session_open
      (entry_candle_type col 44, mae_1r_hit col 45 — Replit computed)
  · entry_candle_type logged at Phase 0 activation
      'wick'       — price wicked into FVG, closed outside
      'body_close' — price closed inside FVG
  · mae_1r_hit timestamp logged when MAE first crosses 1× SL distance
    during Phase 1 — distinguishes fast losses from slow deaths

Architecture (unchanged):
  Two background threads:
    monitor_loop — polls prices every 60s, runs _poll()
    sync_loop    — reads sheet every 60s, runs _sync()

  Two trade pools:
    sweep_pending{}  ← PENDING_ENTRY rows from SweepFVG tab
    sweep_active{}   ← ACTIVE rows from SweepFVG tab

Sheet columns (SweepFVG tab — 37 cols, engine writes):
  1  fired_at        14 status          27 distance_to_cluster_pips
  2  pair            15 entry_time      28 htf_trend
  3  direction       16 tp1_outcome     29 fvg_size_atr_mult
  4  entry           17 tp2_outcome     30 sweep_to_fvg_bars
  5  sl              18 pnl_r           31 mss_level
  6  tp1             19 fvg_top         32 mss_candle_time
  7  tp2             20 fvg_bottom      33 d1_trend_state
  8  rr_tp1          21 tp_mode         34 distance_to_d1_level_r
  9  rr_tp2          22 quality         35 weekly_range_pct
  10 sl_pips         23 sweep_body_pct  36 day_of_week
  11 zone_src        24 sweep_wick_ratio 37 mins_from_session_open
  12 session         25 n_candles_in_zone
  13 sweep_time      26 zone_age_h4_bars

Analytics tab — 45 columns:
  Identity (7): strategy pair direction session zone_src quality tp_mode
  Levels (7):   entry sl tp1 tp2 rr_tp1 rr_tp2 sl_pips
  Timestamps (4): fired_at entry_time tp1_time outcome_time
  Outcome (3):  outcome final_price pnl_r
  MAE/MFE (3):  mae_pips mfe_pips mae_mfe_ratio
  Timing (4):   fired_to_entry_min entry_to_tp1_min tp1_to_outcome_min total_trade_min
  Sweep analytics (10): htf_trend sweep_body_pct sweep_wick_ratio
                        n_candles_in_zone zone_age_h4_bars
                        distance_to_cluster_pips fvg_size_atr_mult
                        sweep_to_fvg_bars mss_level mss_candle_time
  New context (5): d1_trend_state distance_to_d1_level_r weekly_range_pct
                   day_of_week mins_from_session_open
  Entry quality (2): entry_candle_type mae_1r_hit_time
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

POLL_INTERVAL = 60
SYNC_INTERVAL = 60

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

PENDING_TTL_MINS   = 60
INVALIDATION_PIPS  = 20
EXCURSION_FILE     = 'state/excursion.json'


# ── MAE/MFE PERSISTENCE ───────────────────────────────────────────────────────
def _load_excursion() -> dict:
    if os.path.exists(EXCURSION_FILE):
        try:
            with open(EXCURSION_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_excursion(data: dict):
    os.makedirs(os.path.dirname(EXCURSION_FILE), exist_ok=True)
    with open(EXCURSION_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def _update_excursion(key: str, mae: float, mfe: float,
                      entry_time: str = '',
                      mae_1r_hit_time: str = ''):
    data = _load_excursion()
    data[key] = {
        'mae'            : mae,
        'mfe'            : mfe,
        'entry_time'     : entry_time,
        'mae_1r_hit_time': mae_1r_hit_time,
        'updated_at'     : datetime.now(timezone.utc).isoformat(),
    }
    _save_excursion(data)

def _remove_excursion(key: str):
    data = _load_excursion()
    if key in data:
        data.pop(key)
        _save_excursion(data)

def _restore_excursion(key: str, sig: dict) -> dict:
    data   = _load_excursion()
    entry  = float(sig.get('entry', 0))
    stored = data.get(key)
    if stored:
        sig['mae']             = stored['mae']
        sig['mfe']             = stored['mfe']
        sig['entry_time']      = stored.get('entry_time') or sig.get('entry_time', '')
        sig['mae_1r_hit_time'] = stored.get('mae_1r_hit_time', '')
        print(f"[EXCURSION] Restored {key} | mae={stored['mae']} mfe={stored['mfe']}")
    else:
        sig['mae']             = entry
        sig['mfe']             = entry
        sig['mae_1r_hit_time'] = ''
    return sig


# ── FLASK ─────────────────────────────────────────────────────────────────────
app         = Flask(__name__)
trades_lock = threading.Lock()

sweep_pending = {}
sweep_active  = {}

stats = {
    "started"      : "",
    "last_poll"    : "",
    "polls"        : 0,
    "sweep_entries": 0,
    "sweep_tp1"    : 0,
    "sweep_wins"   : 0,
    "sweep_tp1_be" : 0,
    "sweep_losses" : 0,
}

@app.route("/health")
def health():
    with trades_lock:
        pending = len(sweep_pending)
        active  = len(sweep_active)
    return jsonify({
        **stats,
        "sweep_pending": pending,
        "sweep_active" : active,
        "status"       : "ok",
        "price_source" : "finnhub",
    })

@app.route("/trades")
def trades_view():
    with trades_lock:
        return jsonify({
            "sweep_pending": list(sweep_pending.values()),
            "sweep_active" : list(sweep_active.values()),
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
_analytics_sheet = None

def _auth():
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

def get_analytics_sheet():
    global _analytics_sheet
    if _analytics_sheet:
        return _analytics_sheet
    try:
        wb = _auth()
        try:
            _analytics_sheet = wb.worksheet("Analytics")
        except gspread.exceptions.WorksheetNotFound:
            _analytics_sheet = wb.add_worksheet("Analytics", rows=5000, cols=50)
            _analytics_sheet.append_row([
                # Identity (7)
                'strategy','pair','direction','session',
                'zone_src','quality','tp_mode',
                # Levels (7)
                'entry','sl','tp1','tp2',
                'rr_tp1','rr_tp2','sl_pips',
                # Timestamps (4)
                'fired_at','entry_time','tp1_time','outcome_time',
                # Outcome (3)
                'outcome','final_price','pnl_r',
                # MAE/MFE (3)
                'mae_pips','mfe_pips','mae_mfe_ratio',
                # Timing (4)
                'fired_to_entry_min','entry_to_tp1_min',
                'tp1_to_outcome_min','total_trade_min',
                # Sweep analytics (10)
                'htf_trend','sweep_body_pct','sweep_wick_ratio',
                'n_candles_in_zone','zone_age_h4_bars',
                'distance_to_cluster_pips','fvg_size_atr_mult',
                'sweep_to_fvg_bars','mss_level','mss_candle_time',
                # New context (5)
                'd1_trend_state','distance_to_d1_level_r',
                'weekly_range_pct','day_of_week','mins_from_session_open',
                # Entry quality (2)
                'entry_candle_type','mae_1r_hit_time',
            ])
            print("[ANALYTICS] Tab created with 45-col headers ✓")
        print("[ANALYTICS] Connected ✓")
        return _analytics_sheet
    except Exception as e:
        print(f"[ANALYTICS] Error: {e}")
        return None


# ── SHEET HELPERS ─────────────────────────────────────────────────────────────
def _find_sweep_row(sheet, sig: dict) -> int | None:
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

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def _parse_ts(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace(' UTC', '+00:00'))
    except Exception:
        return None

def _mins(a: datetime, b: datetime) -> str:
    if a and b:
        return str(round((b - a).total_seconds() / 60, 1))
    return ''


# ── SHEET WRITES ──────────────────────────────────────────────────────────────
def sheet_sweep_missed(sig: dict):
    sheet = get_sweep_sheet()
    if not sheet: return
    try:
        row = _find_sweep_row(sheet, sig)
        if row:
            sheet.update_cell(row, 14, 'MISSED')
            print(f"[SHEET] Sweep MISSED — row {row} ✓")
    except Exception as e:
        print(f"[SHEET] Sweep MISSED error: {e}")

def sheet_sweep_entry(sig: dict, entry_time: str):
    sheet = get_sweep_sheet()
    if not sheet: return
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
    sheet = get_sweep_sheet()
    if not sheet: return
    try:
        row = _find_sweep_row(sheet, sig)
        if row:
            sheet.update_cell(row, 16, 'WIN')
            print(f"[SHEET] Sweep TP1 — row {row} ✓")
    except Exception as e:
        print(f"[SHEET] Sweep TP1 error: {e}")

def sheet_sweep_final(sig: dict, outcome: str):
    sheet = get_sweep_sheet()
    if not sheet: return
    try:
        row = _find_sweep_row(sheet, sig)
        if not row: return
        r1  = float(sig.get('rr_tp1') or 2.0)
        r2  = float(sig.get('rr_tp2') or 0)
        tp_mode = sig.get('tp_mode', 'DUAL')
        if outcome == 'FULL_WIN':
            pnl     = round(r1*0.5 + r2*0.5, 2) if tp_mode == 'DUAL' else round(r1, 2)
            tp1_out = 'WIN'; tp2_out = 'WIN'
        elif outcome == 'TP1_BE':
            pnl     = round(r1*0.5, 2)
            tp1_out = 'WIN'; tp2_out = 'BE'
        else:
            pnl     = -1.0
            tp1_out = 'LOSS'; tp2_out = 'LOSS'
        sheet.update_cell(row, 14, 'CLOSED')
        sheet.update_cell(row, 16, tp1_out)
        sheet.update_cell(row, 17, tp2_out)
        sheet.update_cell(row, 18, pnl)
        print(f"[SHEET] Sweep final — row {row} {outcome} {pnl:+.2f}R ✓")
    except Exception as e:
        print(f"[SHEET] Sweep final error: {e}")


# ── ANALYTICS ─────────────────────────────────────────────────────────────────
ANALYTICS_CSV = 'state/analytics_fallback.csv'

ANALYTICS_HEADERS = (
    'strategy,pair,direction,session,zone_src,quality,tp_mode,'
    'entry,sl,tp1,tp2,rr_tp1,rr_tp2,sl_pips,'
    'fired_at,entry_time,tp1_time,outcome_time,'
    'outcome,final_price,pnl_r,'
    'mae_pips,mfe_pips,mae_mfe_ratio,'
    'fired_to_entry_min,entry_to_tp1_min,tp1_to_outcome_min,total_trade_min,'
    'htf_trend,sweep_body_pct,sweep_wick_ratio,n_candles_in_zone,'
    'zone_age_h4_bars,distance_to_cluster_pips,fvg_size_atr_mult,'
    'sweep_to_fvg_bars,mss_level,mss_candle_time,'
    'd1_trend_state,distance_to_d1_level_r,weekly_range_pct,'
    'day_of_week,mins_from_session_open,'
    'entry_candle_type,mae_1r_hit_time'
)

def _write_analytics_csv(row: list):
    """
    Fallback local CSV write — called every time log_analytics_sweep()
    runs, regardless of whether Sheets succeeded.
    Survives Render restarts only until next redeploy, but gives a
    manual recovery path if Sheets fails silently.
    """
    try:
        os.makedirs(os.path.dirname(ANALYTICS_CSV), exist_ok=True)
        write_header = not os.path.exists(ANALYTICS_CSV)
        with open(ANALYTICS_CSV, 'a') as f:
            if write_header:
                f.write(ANALYTICS_HEADERS + '\n')
            # Escape any commas inside values
            safe = [
                f'"{str(v)}"' if ',' in str(v) else str(v)
                for v in row
            ]
            f.write(','.join(safe) + '\n')
        print(f"[ANALYTICS] CSV fallback written ✓")
    except Exception as e:
        print(f"[ANALYTICS] CSV fallback error: {e}")


def log_analytics_sweep(sig: dict, outcome: str,
                         final_price: float,
                         entry_time: str, tp1_time: str,
                         outcome_time: str):
    """
    Log completed Sweep+FVG trade — 45 columns.
    Writes to TWO places:
      1. Google Sheets Analytics tab (primary)
      2. state/analytics_fallback.csv (local safety net)
    If Sheets fails, CSV preserves the row for manual recovery.
    """
    fired_dt = _parse_ts(sig.get('fired_at', ''))
    entry_dt = _parse_ts(entry_time)
    tp1_dt   = _parse_ts(tp1_time)
    out_dt   = _parse_ts(outcome_time)

    r1      = float(sig.get('rr_tp1') or 2.0)
    r2      = float(sig.get('rr_tp2') or 0)
    tp_mode = sig.get('tp_mode', 'DUAL')

    if outcome == 'FULL_WIN':
        pnl = round(r1*0.5 + r2*0.5, 2) if tp_mode == 'DUAL' else round(r1, 2)
    elif outcome == 'TP1_BE':
        pnl = round(r1*0.5, 2)
    else:
        pnl = -1.0

    mae       = sig.get('mae')
    mfe       = sig.get('mfe')
    pip       = PIP_SIZE.get(sig.get('pair', ''), 0.0001)
    entry     = float(sig.get('entry', 0))
    direction = sig.get('direction', '')

    if mae is not None:
        mae_pips = round(
            abs(mae - entry) / pip, 1
        ) if direction == 'short' else round(abs(entry - mae) / pip, 1)
        mfe_pips = round(
            abs(entry - mfe) / pip, 1
        ) if direction == 'short' else round(abs(mfe - entry) / pip, 1)
        ratio = round(mae_pips / mfe_pips, 2) if mfe_pips else ''
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
        entry_time, tp1_time, outcome_time,
        # ── Outcome (3) ───────────────────────────────────
        outcome, final_price, pnl,
        # ── MAE/MFE (3) ───────────────────────────────────
        mae_pips, mfe_pips, ratio,
        # ── Timing (4) ────────────────────────────────────
        _mins(fired_dt, entry_dt),
        _mins(entry_dt, tp1_dt),
        _mins(tp1_dt, out_dt) if tp1_time else '',
        _mins(entry_dt, out_dt),
        # ── Sweep analytics (10) ──────────────────────────
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
        # ── New context (5) ───────────────────────────────
        sig.get('d1_trend_state', ''),
        sig.get('distance_to_d1_level_r', ''),
        sig.get('weekly_range_pct', ''),
        sig.get('day_of_week', ''),
        sig.get('mins_from_session_open', ''),
        # ── Entry quality (2) ─────────────────────────────
        sig.get('entry_candle_type', ''),
        sig.get('mae_1r_hit_time', ''),
    ]

    # ── Always write CSV first (zero-dependency fallback) ─────
    _write_analytics_csv(row)

    # ── Then attempt Sheets ───────────────────────────────────
    try:
        sheet = get_analytics_sheet()
        if sheet:
            sheet.append_row(row)
            print(f"[ANALYTICS] Sweep {sig.get('pair')} {outcome} → Sheets ✓")
        else:
            print(f"[ANALYTICS] Sheets unavailable — CSV fallback is source of truth")
    except Exception as e:
        print(f"[ANALYTICS] Sheets error (CSV preserved): {e}")


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def _tg(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        print(f"[TG] Error: {e}")

def alert_sweep_missed(sig: dict, price: float):
    pair = sig['pair']; dp = DP.get(pair, 5)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'
    _tg(
        f"⚠️ *{pair} — Setup MISSED*\n\n"
        f"Direction : `{side}`\n"
        f"FVG Entry : `{float(sig['entry']):.{dp}f}` ← never filled\n"
        f"TP target : `{float(sig['tp1']):.{dp}f}`\n"
        f"Price now : `{price:.{dp}f}`\n\n"
        f"_Move played out without retrace. Setup killed._\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )
    print(f"[ALERT] Sweep MISSED — {pair} {side}")

def alert_sweep_entry(sig: dict, entry_time: str):
    pair = sig['pair']; dp = DP.get(pair, 5)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'
    tp_mode = sig.get('tp_mode', 'DUAL')
    tp_lines = (
        f"TP1 (50%) : `{float(sig['tp1']):.{dp}f}`\n"
        f"TP2 (50%) : `{float(sig['tp2']):.{dp}f}`"
        if tp_mode == 'DUAL' else
        f"TP1       : `{float(sig['tp1']):.{dp}f}`"
    )
    ec = sig.get('entry_candle_type', '')
    ec_tag = f"\nEntry type: `{ec}`" if ec else ''
    _tg(
        f"⚡ *{pair} — Entry Triggered!*\n\n"
        f"Direction : `{side}`\n"
        f"Entry     : `{float(sig['entry']):.{dp}f}` ✅\n"
        f"Stop Loss : `{float(sig['sl']):.{dp}f}`\n"
        f"{tp_lines}{ec_tag}\n\n"
        f"_🟢 Trade ACTIVE. Monitoring started._\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )
    print(f"[ALERT] Sweep entry — {pair} {side}")

def alert_sweep_tp1(sig: dict):
    pair = sig['pair']; dp = DP.get(pair, 5)
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
    pair    = sig['pair']; dp = DP.get(pair, 5)
    side    = 'SELL' if sig['direction'] == 'short' else 'BUY'
    r1      = float(sig.get('rr_tp1') or 2.0)
    r2      = float(sig.get('rr_tp2') or 0)
    tp_mode = sig.get('tp_mode', 'DUAL')
    if outcome == 'FULL_WIN':
        emoji = '✅✅'; label = 'FULL WIN'
        pnl   = f"+{r1*0.5+r2*0.5:.2f}R" if tp_mode == 'DUAL' else f"+{r1:.2f}R"
    elif outcome == 'TP1_BE':
        emoji = '✅'; label = 'TP1 + Breakeven'; pnl = f"+{r1*0.5:.2f}R"
    else:
        emoji = '❌'; label = 'LOSS'; pnl = '-1.00R'
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


# ── PRICE FETCH ───────────────────────────────────────────────────────────────
_price_cache = {}
_price_ts    = {}
CACHE_TTL    = 55

def fetch_price(pair: str) -> float | None:
    now = time.time()
    if pair in _price_cache and now - _price_ts.get(pair, 0) < CACHE_TTL:
        return _price_cache[pair]
    sym = FH_SYMBOLS.get(pair)
    if not sym: return None
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}",
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


# ── ENTRY CANDLE TYPE ─────────────────────────────────────────────────────────
def _classify_entry_candle(price: float, sig: dict) -> str:
    """
    At Phase 0 trigger, classify how price entered the FVG.
    'wick'       — price touched FVG but previous close was outside
                   (inferred: current price inside FVG bounds)
    'body_close' — price closed inside FVG (we can only approximate
                   this from the live quote, so we use fvg midpoint)
    Heuristic: if price is within the inner 50% of the FVG → body_close
               if price is near the boundary → wick
    """
    try:
        fvg_top    = float(sig['fvg_top'])
        fvg_bottom = float(sig['fvg_bottom'])
        fvg_mid    = (fvg_top + fvg_bottom) / 2
        fvg_range  = fvg_top - fvg_bottom
        if fvg_range <= 0:
            return 'wick'
        dist_from_mid = abs(price - fvg_mid) / fvg_range
        return 'body_close' if dist_from_mid < 0.25 else 'wick'
    except Exception:
        return ''


# ── POLL ──────────────────────────────────────────────────────────────────────
def _poll():
    stats["polls"]    += 1
    stats["last_poll"] = _now_str()
    now_utc            = datetime.now(timezone.utc)

    with trades_lock:
        pending_keys = list(sweep_pending.keys())
        active_keys  = list(sweep_active.keys())

    all_pairs = set()
    for k in pending_keys + active_keys:
        with trades_lock:
            sig = sweep_pending.get(k) or sweep_active.get(k)
        if sig:
            all_pairs.add(sig.get('pair', ''))

    if not all_pairs:
        return

    pair_list = list(all_pairs)
    for i, pair in enumerate(pair_list):
        fetch_price(pair)
        if i < len(pair_list) - 1:
            time.sleep(60 / max(len(pair_list), 1))

    # ── Phase 0 — Sweep pending retrace ───────────────────────
    sweep_to_activate   = []
    sweep_to_invalidate = []
    sweep_to_missed     = []

    for key in pending_keys:
        with trades_lock:
            sig = sweep_pending.get(key)
        if not sig: continue

        pair       = sig['pair']
        direction  = sig['direction']
        fvg_top    = float(sig['fvg_top'])
        fvg_bottom = float(sig['fvg_bottom'])
        pip        = PIP_SIZE.get(pair, 0.0001)
        price      = fetch_price(pair)
        if price is None: continue

        # TTL check
        try:
            created  = _parse_ts(sig.get('created_at', ''))
            age_mins = (now_utc - created).total_seconds() / 60 if created else 9999
            if age_mins > PENDING_TTL_MINS:
                sweep_to_invalidate.append(key)
                print(f"[PHASE0] {pair} pending expired ({age_mins:.0f}m) — removed")
                continue
        except Exception:
            pass

        if direction == 'short':
            triggered   = price >= fvg_bottom
            invalidated = price < fvg_bottom - INVALIDATION_PIPS * pip
            tp_blown    = price <= float(sig.get('tp1', 0))
        else:
            triggered   = price <= fvg_top
            invalidated = price > fvg_top + INVALIDATION_PIPS * pip
            tp_blown    = price >= float(sig.get('tp1', 0))

        if tp_blown:
            sweep_to_missed.append((key, sig, price))
            print(f"[PHASE0] {pair} {direction.upper()} MISSED — TP blown @ {price}")
            continue

        if invalidated:
            sweep_to_invalidate.append(key)
            print(f"[PHASE0] {pair} {direction.upper()} invalidated")
            continue

        if triggered:
            entry_time = _now_str()
            # Classify entry candle type at trigger moment
            ec_type = _classify_entry_candle(price, sig)
            sweep_to_activate.append((key, sig, entry_time, ec_type))
            stats["sweep_entries"] += 1
            print(f"[PHASE0] {pair} {direction.upper()} entry triggered @ {price} [{ec_type}]")

    for key in sweep_to_invalidate:
        with trades_lock:
            sweep_pending.pop(key, None)

    for key, sig, price in sweep_to_missed:
        with trades_lock:
            sweep_pending.pop(key, None)
        alert_sweep_missed(sig, price)
        sheet_sweep_missed(sig)

    for key, sig, entry_time, ec_type in sweep_to_activate:
        entry_price = float(sig.get('entry', 0))
        active_sig  = {
            **sig,
            'entry_time'      : entry_time,
            'tp1_hit'         : False,
            'tp1_hit_at'      : '',
            'status'          : 'ACTIVE',
            'mae'             : entry_price,
            'mfe'             : entry_price,
            'mae_1r_hit_time' : '',
            'entry_candle_type': ec_type,
        }
        with trades_lock:
            sweep_pending.pop(key, None)
            sweep_active[key] = active_sig

        _update_excursion(key, entry_price, entry_price, entry_time)
        alert_sweep_entry(active_sig, entry_time)
        sheet_sweep_entry(sig, entry_time)

    # ── Phase 1+2 — Sweep active ──────────────────────────────
    sweep_resolved = []

    for key in active_keys:
        with trades_lock:
            sig = sweep_active.get(key)
        if not sig: continue

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
        if price is None: continue

        dp  = DP.get(pair, 5)
        pip = PIP_SIZE.get(pair, 0.0001)
        sl_dist = abs(entry - sl)

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

            # MAE 1R hit — log timestamp once
            mae_1r_hit_time = sig.get('mae_1r_hit_time', '')
            if not mae_1r_hit_time and sl_dist > 0:
                mae_dist = abs(mae - entry)
                if mae_dist >= sl_dist:
                    mae_1r_hit_time = _now_str()
                    sweep_active[key]['mae_1r_hit_time'] = mae_1r_hit_time
                    print(f"[PHASE1] {pair} MAE crossed 1R @ {_now_str()}")

        _update_excursion(
            key, mae, mfe,
            sig.get('entry_time', ''),
            sweep_active[key].get('mae_1r_hit_time', '')
        )

        if not tp1_hit:
            tp1_reached = (price <= tp1 if direction == 'short' else price >= tp1)
            sl_reached  = (price >= sl  if direction == 'short' else price <= sl)

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
                outcome_time = _now_str()
                print(f"[PHASE1] {pair} FULL WIN (SINGLE) @ {price:.{dp}f}")
                stats["sweep_wins"] += 1
                with trades_lock:
                    sig = sweep_active[key]
                alert_sweep_final(sig, 'FULL_WIN', price)
                sheet_sweep_final(sig, 'FULL_WIN')
                log_analytics_sweep(
                    sig, 'FULL_WIN', price,
                    sig.get('entry_time', ''), '', outcome_time
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
                    sig.get('entry_time', ''), '', outcome_time
                )
                _remove_excursion(key)
                sweep_resolved.append(key)

        else:
            be          = entry
            tp2_reached = (
                (price <= tp2 if direction == 'short' else price >= tp2)
                if tp2 else False
            )
            be_reached  = (price >= be if direction == 'short' else price <= be)

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
                    sig.get('entry_time', ''), sig.get('tp1_hit_at', ''), outcome_time
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
                    sig.get('entry_time', ''), sig.get('tp1_hit_at', ''), outcome_time
                )
                _remove_excursion(key)
                sweep_resolved.append(key)

    with trades_lock:
        for key in sweep_resolved:
            sweep_active.pop(key, None)


# ── SYNC ──────────────────────────────────────────────────────────────────────
def _sync():
    sheet = get_sweep_sheet()
    if not sheet: return
    try:
        added_pending = added_active = 0
        for row in sheet.get_all_values()[1:]:
            if len(row) < 14: continue
            status = row[13].strip()
            if status not in ('PENDING_ENTRY', 'ACTIVE'): continue
            try:
                pair      = row[1].strip().upper()
                direction = row[2].strip().lower()
                fired_at  = row[0].strip()
                key       = f"{pair}_{direction}_{fired_at}"

                with trades_lock:
                    already = key in sweep_pending or key in sweep_active
                if already: continue

                fvg_top    = float(row[18]) if len(row) > 18 and row[18] else None
                fvg_bottom = float(row[19]) if len(row) > 19 and row[19] else None

                sig = {
                    'fired_at'   : fired_at,
                    'created_at' : fired_at,
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
                    'mae_1r_hit_time': '',
                    'entry_candle_type': '',
                    # Restore new analytics fields from sheet cols 23–37
                    'sweep_body_pct'          : row[22] if len(row) > 22 else '',
                    'sweep_wick_ratio'         : row[23] if len(row) > 23 else '',
                    'n_candles_in_zone'        : row[24] if len(row) > 24 else '',
                    'zone_age_h4_bars'         : row[25] if len(row) > 25 else '',
                    'distance_to_cluster_pips' : row[26] if len(row) > 26 else '',
                    'htf_trend'                : row[27] if len(row) > 27 else '',
                    'fvg_size_atr_mult'        : row[28] if len(row) > 28 else '',
                    'sweep_to_fvg_bars'        : row[29] if len(row) > 29 else '',
                    'mss_level'                : row[30] if len(row) > 30 else '',
                    'mss_candle_time'          : row[31] if len(row) > 31 else '',
                    'd1_trend_state'           : row[32] if len(row) > 32 else '',
                    'distance_to_d1_level_r'   : row[33] if len(row) > 33 else '',
                    'weekly_range_pct'         : row[34] if len(row) > 34 else '',
                    'day_of_week'              : row[35] if len(row) > 35 else '',
                    'mins_from_session_open'   : row[36] if len(row) > 36 else '',
                }

                with trades_lock:
                    if status == 'PENDING_ENTRY':
                        if fvg_top is None or fvg_bottom is None:
                            print(f"[SYNC] {pair} PENDING missing fvg bounds — skipping")
                            continue
                        sweep_pending[key] = sig
                        added_pending += 1
                        print(f"[SYNC] Pending: {pair} {direction.upper()}")
                    else:
                        sig = _restore_excursion(key, sig)
                        sweep_active[key] = sig
                        added_active += 1
                        print(f"[SYNC] Active: {pair} {direction.upper()} tp1_hit={sig['tp1_hit']}")

            except Exception as e:
                print(f"[SYNC] Row error: {e}")

        if added_pending or added_active:
            print(f"[SYNC] +{added_pending} pending +{added_active} active")
    except Exception as e:
        print(f"[SYNC] Read error: {e}")


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
    print(f"  EDGE Replit Tracker v5 — {stats['started']}")
    print(f"{'='*55}\n")

    print("[STARTUP] Loading open trades from SweepFVG sheet...")
    _sync()

    with trades_lock:
        print(
            f"[STARTUP] Loaded: "
            f"{len(sweep_pending)} sweep pending | "
            f"{len(sweep_active)} sweep active"
        )

    threading.Thread(target=monitor_loop, daemon=True, name="monitor").start()
    threading.Thread(target=sync_loop,    daemon=True, name="sync").start()

    print(f"[OK] Threads started. Monitoring active.\n")
    app.run(host="0.0.0.0", port=8080, debug=False)

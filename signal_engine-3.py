"""
EDGE Signal Engine — v5
========================
Strategies:
  1. Break & Retest (B&R) — 5 pairs, M15, unchanged
  2. H4 Zone Sweep + M15 FVG — 10 pairs, two-stage signal lifecycle
  3. Prop Firm LOLG — EURUSD/GBPUSD only, unchanged

Two-stage Sweep lifecycle this file manages:
  Stage 1 — scan() detects H4 sweep + FVG formation
    · Telegram: "Set pending order at X"
    · Sheet row appended: status=PENDING_ENTRY
  Stage 2 — check_pending_entries() detects retrace
    · Telegram: "Entry triggered"
    · Sheet: status → ACTIVE, entry_time filled
  Stages 3+4 — handled by Replit tracker (real-time price monitoring)

Changes vs v4:
  · SWEEP_PAIRS expanded to 10
  · H4 cache fetched for all SWEEP_PAIRS
  · check_pending_entries() called per sweep pair each run
  · cleanup_expired_pending() called once per run
  · log_sweep_fvg_to_sheet() uses 18-column layout
  · _update_sweep_entry_in_sheet() updates status+entry_time on stage 2
  · sleep(8) between H4 fetches to respect API rate limits
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

try:
    from sweep_fvg import (scan as sweep_fvg_scan,
                           check_pending_entries,
                           cleanup_expired_pending,
                           format_tp1_alert)
    SWEEP_FVG_ENABLED = True
except ImportError:
    print("[WARN] sweep_fvg.py not found — Sweep+FVG disabled")
    SWEEP_FVG_ENABLED = False

try:
    from prop_engine import scan as prop_scan
    PROP_ENABLED = True
except ImportError:
    PROP_ENABLED = False

# ── CONFIG ────────────────────────────────────────────────────────────────────
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN        = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID      = os.environ.get("TG_CHAT_ID", "")

STRATEGY_PAIRS = ["USDJPY", "GBPUSD", "AUDJPY", "XAUUSD", "EURUSD"]
PAIRS          = STRATEGY_PAIRS

SWEEP_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDJPY", "XAUUSD",
    "CADJPY", "USDCAD", "EURJPY", "GBPJPY", "GBPAUD",
]

TD_SYMBOLS = {
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "AUDJPY": "AUD/JPY", "XAUUSD": "XAU/USD", "CADJPY": "CAD/JPY",
    "USDCAD": "USD/CAD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "GBPAUD": "GBP/AUD",
}
PIP_SIZE = {
    "EURUSD": 0.0001, "GBPUSD": 0.0001, "USDCAD": 0.0001, "GBPAUD": 0.0001,
    "USDJPY": 0.01,   "AUDJPY": 0.01,   "CADJPY": 0.01,
    "EURJPY": 0.01,   "GBPJPY": 0.01,   "XAUUSD": 0.10,
}
DP = {
    "EURUSD": 5, "GBPUSD": 5, "USDCAD": 5, "GBPAUD": 5,
    "USDJPY": 3, "AUDJPY": 3, "CADJPY": 3,
    "EURJPY": 3, "GBPJPY": 3, "XAUUSD": 2,
}
RR_MAP = {"USDJPY": 2.0, "GBPUSD": 2.0, "AUDJPY": 2.0, "XAUUSD": 3.0, "EURUSD": 2.0}

SWING_LB       = 5
SWING_MIN_DIST = 5
RETEST_TOL     = 0.0008
RETEST_WINDOW  = 5
TREND_N        = 3
BREAK_WINDOW   = 20
CANDLES_NEEDED = 200
MAX_HISTORY    = 1344
ATR_PERIOD     = 14
ATR_MULT       = {"USDJPY": 0.25, "GBPUSD": 0.25, "AUDJPY": 0.25,
                  "XAUUSD": 0.75, "EURUSD": 0.25}

STATE_FILE        = Path("state/price_history.json")
LIVE_SIGNALS_FILE = Path("state/live_signals.json")
SIGNALS_LOG       = Path("state/signals_log.csv")
SWEEP_FVG_LOG     = Path("state/sweep_fvg_log.csv")
PROP_LOG          = Path("state/prop_log.csv")
SWEEP_LIVE_FILE   = Path("state/sweep_fvg_live.json")
PROP_PAIRS        = ["EURUSD", "GBPUSD"]

H1_FETCH_SIZE = 50
H4_FETCH_SIZE = 80   # increased from 40 — covers zone lookback on first deploy
H4_HOURS      = {0, 4, 8, 12, 16, 20}


# ── FETCH CANDLES ─────────────────────────────────────────────────────────────
def fetch_candles(pair, outputsize=50):
    symbol = TD_SYMBOLS[pair]
    url = (f"https://api.twelvedata.com/time_series"
           f"?symbol={symbol}&interval=15min"
           f"&outputsize={outputsize}&apikey={TWELVE_DATA_KEY}")
    try:
        res  = requests.get(url, timeout=15)
        data = res.json()
        if data.get("status") == "error":
            print(f"  [{pair}] API error: {data.get('message')}"); return []
        return [{"time": c["datetime"], "open": float(c["open"]),
                 "high": float(c["high"]), "low": float(c["low"]),
                 "close": float(c["close"])}
                for c in reversed(data.get("values", []))]
    except Exception as e:
        print(f"  [{pair}] Fetch failed: {e}"); return []

def fetch_htf_candles(pair, interval, outputsize):
    symbol = TD_SYMBOLS[pair]
    url = (f"https://api.twelvedata.com/time_series"
           f"?symbol={symbol}&interval={interval}"
           f"&outputsize={outputsize}&apikey={TWELVE_DATA_KEY}")
    try:
        res  = requests.get(url, timeout=15)
        data = res.json()
        if data.get("status") == "error":
            print(f"  [{pair}] HTF {interval} error: {data.get('message')}"); return []
        return [{"time": c["datetime"], "open": float(c["open"]),
                 "high": float(c["high"]), "low": float(c["low"]),
                 "close": float(c["close"])}
                for c in reversed(data.get("values", []))]
    except Exception as e:
        print(f"  [{pair}] HTF fetch failed: {e}"); return []


# ── HTF CACHE ─────────────────────────────────────────────────────────────────
def load_htf_cache(state): return state.get("htf_cache", {})
def save_htf_cache(state, cache): state["htf_cache"] = cache

def _ensure(pair, cache):
    if pair not in cache:
        cache[pair] = {"h1": {"candles": [], "fetched_hour": -1},
                       "h4": {"candles": [], "fetched_hour": -1}}

def should_refresh_h1(now, entry): return entry.get("fetched_hour") != now.hour
def should_refresh_h4(now, entry):
    return now.hour in H4_HOURS and entry.get("fetched_hour") != now.hour

def refresh_h1_cache(cache, now):
    needs = [p for p in STRATEGY_PAIRS
             if should_refresh_h1(now, cache.get(p, {}).get("h1", {"fetched_hour": -1}))]
    if not needs: return
    print(f"  Fetching H1 for: {needs}")
    for i, pair in enumerate(needs):
        _ensure(pair, cache)
        raw = fetch_htf_candles(pair, "1h", H1_FETCH_SIZE)
        if raw:
            cache[pair]["h1"]["candles"]      = raw
            cache[pair]["h1"]["fetched_hour"] = now.hour
        if i < len(needs) - 1: time.sleep(8)

def refresh_h4_cache(cache, now):
    needs = [p for p in SWEEP_PAIRS
             if should_refresh_h4(now, cache.get(p, {}).get("h4", {"fetched_hour": -1}))]
    if not needs: return
    print(f"  Fetching H4 for: {needs}")
    for i, pair in enumerate(needs):
        _ensure(pair, cache)
        raw = fetch_htf_candles(pair, "4h", H4_FETCH_SIZE)
        if raw:
            cache[pair]["h4"]["candles"]      = raw
            cache[pair]["h4"]["fetched_hour"] = now.hour
            print(f"  [{pair}] H4: {len(raw)} candles")
        else:
            print(f"  [{pair}] H4 fetch failed — using cache")
        if i < len(needs) - 1: time.sleep(8)


# ── STATE ─────────────────────────────────────────────────────────────────────
def load_state():
    STATE_FILE.parent.mkdir(exist_ok=True)
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text())
        except Exception: pass
    return {"candle_history": {p: [] for p in PAIRS},
            "active_setups": {}, "fired_signals": [], "htf_cache": {}}

def save_state(state):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def load_live_signals():
    if LIVE_SIGNALS_FILE.exists():
        try: return json.loads(LIVE_SIGNALS_FILE.read_text())
        except: return {}
    return {}

def save_live_signals(live):
    LIVE_SIGNALS_FILE.parent.mkdir(exist_ok=True)
    LIVE_SIGNALS_FILE.write_text(json.dumps(live, indent=2))

def merge_candles(stored, fetched, max_size=MAX_HISTORY):
    times = {c["time"] for c in stored}
    for c in fetched:
        if c["time"] not in times:
            stored.append(c); times.add(c["time"])
    stored.sort(key=lambda c: c["time"])
    return stored[-max_size:]


# ── ATR ───────────────────────────────────────────────────────────────────────
def calc_atr(candles, idx, period=ATR_PERIOD):
    start = max(1, idx - period + 1); trs = []
    for j in range(start, idx + 1):
        h, l, pc = candles[j]["high"], candles[j]["low"], candles[j-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(sum(trs) / len(trs)) if trs else 0.0


# ── B&R STRATEGY (unchanged) ──────────────────────────────────────────────────
def find_swings(closes, candles, lb=SWING_LB, pair=None):
    sh, sl = [], []
    pip = PIP_SIZE.get(pair, 0.0001) if pair else 0.0001
    min_dist = SWING_MIN_DIST * pip
    for i in range(lb, len(closes) - lb):
        win = closes[i-lb:i+lb+1]
        if closes[i] == max(win):
            if sh and abs(closes[i] - closes[sh[-1]]) < min_dist: continue
            sh.append(i)
        if closes[i] == min(win):
            if sl and abs(closes[i] - closes[sl[-1]]) < min_dist: continue
            sl.append(i)
    return sh, sl

def detect_trend(closes, sh, sl, n=TREND_N):
    psh = sh[-n:]; psl = sl[-n:]
    if len(psh) < 2 or len(psl) < 2: return "neutral"
    shv = [closes[i] for i in psh]; slv = [closes[i] for i in psl]
    bull = all(shv[i] < shv[i+1] for i in range(len(shv)-1)) and \
           all(slv[i] < slv[i+1] for i in range(len(slv)-1))
    bear = all(shv[i] > shv[i+1] for i in range(len(shv)-1)) and \
           all(slv[i] > slv[i+1] for i in range(len(slv)-1))
    return "bullish" if bull else "bearish" if bear else "neutral"

def run_signal_check(pair, candles, active_setups):
    if len(candles) < CANDLES_NEEDED: return None
    pip = PIP_SIZE.get(pair, 0.0001); rr = RR_MAP.get(pair, 2.0)
    closes = [c["close"] for c in candles]; n = len(closes); cur = closes[-1]
    tol = cur * RETEST_TOL; sh, sl = find_swings(closes, candles, SWING_LB, pair=pair)
    setup = active_setups.get(pair)
    if setup and n - setup.get("breakout_idx", 0) > RETEST_WINDOW:
        active_setups.pop(pair, None); return None
    if setup:
        if setup.get("breakout_time") and candles[-1]["time"] <= setup["breakout_time"]:
            return None
        lv = setup["level"]
        if setup["dir"] == "long":
            if candles[-1]["low"] <= lv + tol and cur > lv:
                sl_p = setup["sl"]; risk = cur - sl_p
                if risk > pip * 3:
                    tp = cur + risk * rr; active_setups.pop(pair, None)
                    live = load_live_signals()
                    live[pair] = {"side": "BUY", "entry": cur, "sl": sl_p, "tp": tp,
                                  "time": datetime.now(timezone.utc).isoformat()}
                    save_live_signals(live)
                    return {"pair": pair, "side": "BUY", "entry": cur, "sl": sl_p,
                            "tp": tp, "level": lv, "sl_pips": round(risk/pip, 1),
                            "rr": rr, "trend": setup.get("trend", ""),
                            "time": datetime.now(timezone.utc).isoformat()}
        else:
            if candles[-1]["high"] >= lv - tol and cur < lv:
                sl_p = setup["sl"]; risk = sl_p - cur
                if risk > pip * 3:
                    tp = cur - risk * rr; active_setups.pop(pair, None)
                    live = load_live_signals()
                    live[pair] = {"side": "SELL", "entry": cur, "sl": sl_p, "tp": tp,
                                  "time": datetime.now(timezone.utc).isoformat()}
                    save_live_signals(live)
                    return {"pair": pair, "side": "SELL", "entry": cur, "sl": sl_p,
                            "tp": tp, "level": lv, "sl_pips": round(risk/pip, 1),
                            "rr": rr, "trend": setup.get("trend", ""),
                            "time": datetime.now(timezone.utc).isoformat()}
    if not setup:
        trend = detect_trend(closes, sh, sl)
        if trend == "bullish" and sh:
            valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
            if valid_sh:
                last_sh = valid_sh[-1]
                if (n-1)-(last_sh+SWING_LB) <= BREAK_WINDOW and cur > closes[last_sh]:
                    valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
                    if valid_sl:
                        lsi = valid_sl[-1]; atr = calc_atr(candles, lsi)
                        sl_p = closes[lsi] - ATR_MULT[pair] * atr
                        active_setups[pair] = {"dir": "long", "level": closes[last_sh],
                            "sl": sl_p, "breakout_idx": n-1,
                            "breakout_time": candles[-1]["time"], "trend": trend,
                            "created": datetime.now(timezone.utc).isoformat()}
        elif trend == "bearish" and sl:
            valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
            if valid_sl:
                last_sl = valid_sl[-1]
                if (n-1)-(last_sl+SWING_LB) <= BREAK_WINDOW and cur < closes[last_sl]:
                    valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
                    if valid_sh:
                        shi = valid_sh[-1]; atr = calc_atr(candles, shi)
                        sl_p = closes[shi] + ATR_MULT[pair] * atr
                        active_setups[pair] = {"dir": "short", "level": closes[last_sl],
                            "sl": sl_p, "breakout_idx": n-1,
                            "breakout_time": candles[-1]["time"], "trend": trend,
                            "created": datetime.now(timezone.utc).isoformat()}
    return None


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def _tg(msg, parse_mode="Markdown"):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT_ID, "text": msg,
                            "parse_mode": parse_mode}, timeout=10)
    except Exception as e:
        print(f"  [TG] Error: {e}")

def send_telegram(sig):
    pair  = sig["pair"]; dp = DP.get(pair, 5)
    arrow = "🟢" if sig["side"] == "BUY" else "🔴"
    t_icon = "↗️" if sig.get("trend") == "bullish" else "↘️" if sig.get("trend") == "bearish" else "➡️"
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = (f"{arrow} *EDGE SIGNAL — {sig['side']} {pair}*\n\n"
           f"📍 Entry: `{sig['entry']:.{dp}f}`\n"
           f"🛑 SL: `{sig['sl']:.{dp}f}`\n"
           f"🎯 TP: `{sig['tp']:.{dp}f}`\n\n"
           f"RR: 1:{sig['rr']}  |  Risk: {sig['sl_pips']:.1f} pips\n"
           f"{t_icon} Trend: {sig.get('trend','').capitalize()}\n"
           f"🕐 {now}\n\n_Break & Retest — M15_")
    _tg(msg)
    print(f"  [B&R] Alert sent — {pair} {sig['side']}")

def send_telegram_sweep_stage1(sig):
    _tg(sig["message"])
    print(f"  [SWEEP S1] Alert sent — {sig['direction'].upper()} {sig['pair']}")

def send_telegram_sweep_stage2(sig):
    _tg(sig["entry_message"])
    print(f"  [SWEEP S2] Entry alert sent — {sig['direction'].upper()} {sig['pair']}")

def send_telegram_prop(sig):
    _tg(sig.get("message", ""))
    print(f"  [PROP] Alert sent — {sig.get('side')} {sig.get('pair')}")


# ── SHEET HELPERS ─────────────────────────────────────────────────────────────
def _get_sheet(tab=None):
    raw   = os.environ.get("GOOGLE_CREDENTIALS", "")
    creds = Credentials.from_service_account_info(
        json.loads(raw), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    wb = gspread.authorize(creds).open_by_key(os.environ.get("SHEET_ID", ""))
    return wb.worksheet(tab) if tab else wb.sheet1

def log_signal(sig):
    SIGNALS_LOG.parent.mkdir(exist_ok=True)
    hdr = not SIGNALS_LOG.exists()
    with open(SIGNALS_LOG, "a") as f:
        if hdr: f.write("time,pair,side,entry,sl,tp,sl_pips,rr,level,trend\n")
        f.write(f"{sig['time']},{sig['pair']},{sig['side']},{sig['entry']},"
                f"{sig['sl']},{sig['tp']},{sig['sl_pips']},{sig['rr']},"
                f"{sig['level']},{sig.get('trend','')}\n")

def log_signal_to_sheet(sig):
    try:
        _get_sheet().append_row([
            sig["time"], sig["pair"], sig["side"],
            sig["entry"], sig["sl"], sig["tp"],
            sig["rr"], sig["sl_pips"], "PENDING", "", "",
            sig.get("trend", ""),
        ])
        print(f"  [SHEET] B&R logged — {sig['pair']}")
    except Exception as e:
        print(f"  [SHEET] B&R error: {e}")

def log_sweep_fvg_signal(sig):
    """Stage 1 — append row with status=PENDING_ENTRY."""
    SWEEP_FVG_LOG.parent.mkdir(exist_ok=True)
    hdr = not SWEEP_FVG_LOG.exists()
    with open(SWEEP_FVG_LOG, "a") as f:
        if hdr:
            f.write("fired_at,pair,direction,entry,sl,tp1,tp2,"
                    "rr_tp1,rr_tp2,sl_pips,zone_src,session,sweep_time\n")
        f.write(f"{sig['fired_at']},{sig['pair']},{sig['direction']},"
                f"{sig['entry']},{sig['sl']},{sig['tp1']},{sig['tp2']},"
                f"{sig['rr_tp1']},{sig['rr_tp2']},{sig['sl_pips']},"
                f"{sig.get('zone_src','')},{sig['session']},{sig['sweep_time']}\n")

def log_sweep_fvg_to_sheet(sig):
    """
    Stage 1 — append 18-column row to SweepFVG tab.
    Columns:
      fired_at|pair|direction|entry|sl|tp1|tp2|rr_tp1|rr_tp2|
      sl_pips|zone_src|session|sweep_time|status|entry_time|
      tp1_outcome|tp2_outcome|pnl_r
    """
    try:
        sheet = _get_sheet("SweepFVG")
        sheet.append_row([
            sig['fired_at'],        # 1
            sig['pair'],            # 2
            sig['direction'].upper(),# 3
            sig['entry'],           # 4
            sig['sl'],              # 5
            sig['tp1'],             # 6
            sig['tp2'],             # 7
            sig['rr_tp1'],          # 8
            sig['rr_tp2'],          # 9
            sig['sl_pips'],         # 10
            sig.get('zone_src',''), # 11
            sig['session'],         # 12
            sig['sweep_time'],      # 13
            'PENDING_ENTRY',        # 14 status
            '',                     # 15 entry_time
            '',                     # 16 tp1_outcome
            '',                     # 17 tp2_outcome
            '',                     # 18 pnl_r
        ])
        print(f"  [SWEEP] Sheet S1 logged — {sig['pair']}")
    except Exception as e:
        print(f"  [SWEEP] Sheet S1 error: {e}")

def update_sweep_entry_in_sheet(sig):
    """Stage 2 — update status → ACTIVE and fill entry_time."""
    try:
        sheet    = _get_sheet("SweepFVG")
        all_rows = sheet.get_all_values()
        for row_i, row in enumerate(all_rows[1:], start=2):
            if (len(row) >= 3 and
                    row[0] == sig.get('fired_at', '') and
                    row[1] == sig.get('pair', '') and
                    row[2].upper() == sig.get('direction', '').upper()):
                sheet.update_cell(row_i, 14, 'ACTIVE')
                sheet.update_cell(row_i, 15, sig.get('entry_time', ''))
                print(f"  [SWEEP] Sheet S2 updated — row {row_i} ACTIVE")
                return
        print(f"  [SWEEP] S2 row not found for {sig.get('pair')}")
    except Exception as e:
        print(f"  [SWEEP] S2 sheet error: {e}")

def log_prop_signal(sig):
    PROP_LOG.parent.mkdir(exist_ok=True)
    hdr = not PROP_LOG.exists()
    with open(PROP_LOG, "a") as f:
        if hdr:
            f.write("fired_at,pair,direction,entry,sl,tp,rr,sl_pips,"
                    "asian_high,asian_low,sweep_src,session\n")
        f.write(f"{sig['fired_at']},{sig['pair']},{sig['direction']},"
                f"{sig['entry']},{sig['sl']},{sig['tp']},{sig['rr']},"
                f"{sig['sl_pips']},{sig['asian_high']},{sig['asian_low']},"
                f"{sig['sweep_src']},{sig['session']}\n")

def log_prop_to_sheet(sig):
    try:
        _get_sheet("PropFirm").append_row([
            sig["fired_at"], sig["pair"], sig["direction"].upper(),
            sig["entry"], sig["sl"], sig["tp"], sig["rr"],
            sig["asian_high"], sig["asian_low"],
            sig["sweep_src"], sig["session"], "", "", "",
        ])
    except Exception as e:
        print(f"  [PROP] Sheet error: {e}")


# ── DUPLICATE GUARD (B&R) ─────────────────────────────────────────────────────
def is_duplicate(sig, fired):
    sig_id = f"{sig['pair']}_{sig['side']}_{round(sig['level'], 4)}"
    now    = datetime.now(timezone.utc)
    recent = [s for s in fired
              if (now - datetime.fromisoformat(s["time"])).total_seconds() < 14400]
    fired.clear(); fired.extend(recent)
    if any(s.get("id") == sig_id for s in recent):
        return True
    fired.append({"id": sig_id, "time": now.isoformat()})
    return False


# ── CANDLE PREP ───────────────────────────────────────────────────────────────
def prepare_candles(raw, now_utc=None):
    now      = now_utc or datetime.now(timezone.utc)
    prepared = []
    for c in raw:
        try:
            t = datetime.strptime(c["time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if t > now: continue
            prepared.append({"time": t, "open": float(c["open"]),
                             "high": float(c["high"]), "low": float(c["low"]),
                             "close": float(c["close"])})
        except Exception: continue
    return prepared


# ── SWEEP LIVE STATE ──────────────────────────────────────────────────────────
def load_sweep_live():
    if SWEEP_LIVE_FILE.exists():
        try: return json.loads(SWEEP_LIVE_FILE.read_text())
        except: return {}
    return {}


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now_utc = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"  EDGE Signal Engine — {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*55}")

    if not TWELVE_DATA_KEY:
        print("[ERROR] TWELVE_DATA_KEY not set"); return

    state          = load_state()
    candle_history = state["candle_history"]
    active_setups  = state["active_setups"]
    fired_signals  = state["fired_signals"]
    htf_cache      = load_htf_cache(state)

    # ── Backfill guard ────────────────────────────────────────────────────
    # If state is fresh (no candle history at all), this is a cold start.
    # Fetch candles to populate cache but skip all scanning and alerting.
    # This prevents a state clear from triggering a backfill signal dump.
    is_cold_start = not any(candle_history.get(p) for p in PAIRS + SWEEP_PAIRS)
    if is_cold_start:
        print("\n[COLD START] Fresh state detected — initialising candle cache, no signals this run.")
        all_pairs = list(set(PAIRS + SWEEP_PAIRS))
        for i, pair in enumerate(all_pairs):
            candles = fetch_candles(pair, outputsize=50)
            if i < len(all_pairs) - 1: time.sleep(8)
            if not candles: continue
            candle_history.setdefault(pair, [])
            candle_history[pair] = merge_candles(candle_history[pair], candles)
            print(f"  {pair}: {len(candle_history[pair])} candles cached")
        state["candle_history"] = candle_history
        save_htf_cache(state, htf_cache)
        save_state(state)
        print("\n[COLD START] Cache initialised. Signals will fire from next run.")
        print(f"{'='*55}\n")
        return

    # ── CSV self-heal ─────────────────────────────────────────────────────
    # If sweep_fvg_log.csv exists but has wrong column count (legacy rows),
    # archive it and let it be recreated cleanly on next signal.
    if SWEEP_FVG_LOG.exists():
        with open(SWEEP_FVG_LOG) as f:
            first_line = f.readline().strip()
        expected_cols = 13
        if first_line.count(',') + 1 != expected_cols:
            archive = SWEEP_FVG_LOG.with_suffix('.csv.bak')
            SWEEP_FVG_LOG.rename(archive)
            print(f"[WARN] sweep_fvg_log.csv had wrong schema — archived to {archive.name}, will recreate.")

    # ── [1] Fetch M15 — all pairs ─────────────────────────────────────────
    print("\n[1] Fetching M15 candles...")
    all_pairs = list(set(PAIRS + SWEEP_PAIRS))
    for i, pair in enumerate(all_pairs):
        candles = fetch_candles(pair, outputsize=50)
        time.sleep(8)
        if not candles: continue
        candle_history.setdefault(pair, [])
        candle_history[pair] = merge_candles(candle_history[pair], candles)
        latest = candle_history[pair][-1]
        print(f"  {pair}: {len(candle_history[pair])} candles | "
              f"close={latest['close']:.{DP[pair]}f}")

    # ── [1b] HTF cache refresh ────────────────────────────────────────────
    h1_needed = any(should_refresh_h1(now_utc,
                    htf_cache.get(p, {}).get("h1", {"fetched_hour": -1}))
                    for p in STRATEGY_PAIRS)
    h4_needed = now_utc.hour in H4_HOURS and any(
        should_refresh_h4(now_utc, htf_cache.get(p, {}).get("h4", {"fetched_hour": -1}))
        for p in SWEEP_PAIRS)

    def _ensure(pair, cache):
        if pair not in cache:
            cache[pair] = {"h1": {"candles": [], "fetched_hour": -1},
                           "h4": {"candles": [], "fetched_hour": -1}}

    if h1_needed or h4_needed:
        print(f"\n[1b] HTF refresh — H1={h1_needed} H4={h4_needed}")
        for p in STRATEGY_PAIRS: _ensure(p, htf_cache)
        for p in SWEEP_PAIRS:    _ensure(p, htf_cache)
        if h1_needed: refresh_h1_cache(htf_cache, now_utc)
        if h4_needed: refresh_h4_cache(htf_cache, now_utc)
    else:
        print("\n[1b] HTF cache OK")

    # ── [2] B&R scan ──────────────────────────────────────────────────────
    print("\n[2] B&R scan...")
    br_signals = []
    for pair in STRATEGY_PAIRS:
        if pair not in candle_history or not candle_history[pair]: continue
        sig = run_signal_check(pair, candle_history[pair], active_setups)
        if sig and not is_duplicate(sig, fired_signals):
            br_signals.append(sig)
            print(f"  [{pair}] 🔔 B&R {sig['side']} @ {sig['entry']:.{DP[pair]}f}")

    # ── [3] Sweep+FVG scan — Stage 1 + Stage 2 ───────────────────────────
    sweep_s1 = []   # FVG formation signals
    sweep_s2 = []   # Entry triggered signals

    if SWEEP_FVG_ENABLED:
        print(f"\n[3] Sweep+FVG scan ({len(SWEEP_PAIRS)} pairs)...")
        cleanup_expired_pending()

        for pair in SWEEP_PAIRS:
            if pair not in candle_history or not candle_history[pair]: continue

            m15 = prepare_candles(candle_history[pair], now_utc)
            if len(m15) < 30:
                print(f"  [{pair}] Insufficient M15 ({len(m15)})"); continue

            h4_raw = htf_cache.get(pair, {}).get("h4", {}).get("candles", [])
            h1_raw = htf_cache.get(pair, {}).get("h1", {}).get("candles", [])
            h4     = prepare_candles(h4_raw, now_utc) if h4_raw else []
            h1     = prepare_candles(h1_raw, now_utc) if h1_raw else []

            print(f"  [{pair}] M15={len(m15)} H4={len(h4)}")

            # Stage 1 — FVG formation
            s1 = sweep_fvg_scan(m15_candles=m15, h1_candles=h1 or None,
                                 h4_candles=h4 or None, pair=pair)
            if s1:
                print(f"  [{pair}] 🔔 {len(s1)} stage-1 signal(s)")
                sweep_s1.extend(s1)

            # Stage 2 — Entry trigger check
            s2 = check_pending_entries(m15, pair)
            if s2:
                print(f"  [{pair}] ⚡ {len(s2)} entry trigger(s)")
                sweep_s2.extend(s2)

    # ── [4] Prop scan ─────────────────────────────────────────────────────
    prop_signals = []
    if PROP_ENABLED:
        print(f"\n[4] Prop scan...")
        for pair in PROP_PAIRS:
            if pair not in candle_history or not candle_history[pair]: continue
            m15 = prepare_candles(candle_history[pair], now_utc)
            if len(m15) < 50: continue
            h1_raw = htf_cache.get(pair, {}).get("h1", {}).get("candles", [])
            h1     = prepare_candles(h1_raw, now_utc) if h1_raw else []
            sigs   = prop_scan(m15_candles=m15, h1_candles=h1 or None, pair=pair)
            if sigs: prop_signals.extend(sigs)

    # ── [5] Send alerts + log ─────────────────────────────────────────────
    print("\n[5] Sending alerts...")

    for sig in br_signals:
        send_telegram(sig); log_signal(sig); log_signal_to_sheet(sig)

    for sig in sweep_s1:
        send_telegram_sweep_stage1(sig)
        log_sweep_fvg_signal(sig)
        log_sweep_fvg_to_sheet(sig)

    for sig in sweep_s2:
        send_telegram_sweep_stage2(sig)
        update_sweep_entry_in_sheet(sig)

    for sig in prop_signals:
        send_telegram_prop(sig); log_prop_signal(sig); log_prop_to_sheet(sig)

    if not (br_signals or sweep_s1 or sweep_s2 or prop_signals):
        print("  No new signals this run")

    # ── Persist ───────────────────────────────────────────────────────────
    state["candle_history"] = candle_history
    state["active_setups"]  = active_setups
    state["fired_signals"]  = fired_signals
    save_htf_cache(state, htf_cache)
    save_state(state)

    print(f"\n{'='*55}")
    print(f"  B&R signals    : {len(br_signals)}")
    print(f"  Sweep S1 (FVG) : {len(sweep_s1)}")
    print(f"  Sweep S2 (Entry): {len(sweep_s2)}")
    print(f"  Prop signals   : {len(prop_signals)}")
    print(f"  Live sweep     : {list(load_sweep_live().keys())}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()

"""
EDGE Signal Engine
==================
Break & Retest signal detector for:
  USDJPY (primary), GBPUSD, AUDJPY  — active B&R strategy
  XAUUSD, EURUSD                    — data collection only

  M15 timeframe | 1:2 RR (XAUUSD 1:3) | Trend-filtered

Runs every 15 minutes via GitHub Actions + cron-job.org
Sends Telegram alerts when a signal fires.
Logs all signals to state/signals_log.csv

Changes vs previous version:
  - TREND_N reduced 3 → 2 (less strict trend confirmation)
  - SL placement uses swing close + 0.25x ATR14 buffer (not wicks)
  - Swing detection remains close-based (unchanged)
  - RETEST_WINDOW unchanged at 30 candles
"""

import os
import json
import requests
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

# ══════════════════════════════════════════════
#  CONFIG  — set these as GitHub Secrets
# ══════════════════════════════════════════════
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN        = os.environ.get("TG_TOKEN",        "")
TG_CHAT_ID      = os.environ.get("TG_CHAT_ID",      "")

# ── Active strategy pairs ──
STRATEGY_PAIRS = ["USDJPY", "GBPUSD", "AUDJPY", "XAUUSD"]

# ── Data collection only (no signals) ──
DATA_ONLY_PAIRS = ["EURUSD"]

# ── All pairs combined ──
PAIRS = STRATEGY_PAIRS + DATA_ONLY_PAIRS

# ── Twelve Data symbol map ──
TD_SYMBOLS = {
    "USDJPY": "USD/JPY",
    "GBPUSD": "GBP/USD",
    "AUDJPY": "AUD/JPY",
    "XAUUSD": "XAU/USD",
    "EURUSD": "EUR/USD",
}

# ── Pip config ──
PIP_SIZE = {
    "USDJPY": 0.01,
    "GBPUSD": 0.0001,
    "AUDJPY": 0.01,
    "XAUUSD": 0.10,
    "EURUSD": 0.0001,
}
DP = {
    "USDJPY": 3,
    "GBPUSD": 5,
    "AUDJPY": 3,
    "XAUUSD": 2,
    "EURUSD": 5,
}

# ── Strategy params ──
RR_MAP = {
    "USDJPY": 2.0,
    "GBPUSD": 2.0,
    "AUDJPY": 2.0,
    "XAUUSD": 3.0,
}
SWING_LB       = 5
RETEST_TOL     = 0.0008
RETEST_WINDOW  = 30
TREND_N        = 2        # changed: 3 → 2
BREAK_WINDOW   = 20
CANDLES_NEEDED = 200
MAX_HISTORY    = 1344     # ~2 weeks of M15 candles

# ── ATR-based SL buffer ──
ATR_PERIOD     = 14
ATR_MULT       = 0.25     # SL = swing close ± 0.25 × ATR14

# ── State files ──
STATE_FILE  = Path("state/price_history.json")
SIGNALS_LOG = Path("state/signals_log.csv")

# ══════════════════════════════════════════════
#  1. FETCH M15 OHLC CANDLES
# ══════════════════════════════════════════════
def fetch_candles(pair, outputsize=50):
    """Fetch last N M15 OHLC candles from Twelve Data."""
    symbol = TD_SYMBOLS[pair]
    url = (
        f"https://api.twelvedata.com/time_series"
        f"?symbol={symbol}"
        f"&interval=15min"
        f"&outputsize={outputsize}"
        f"&apikey={TWELVE_DATA_KEY}"
    )
    try:
        res  = requests.get(url, timeout=15)
        data = res.json()

        if data.get("status") == "error":
            print(f"  [{pair}] API error: {data.get('message')}")
            return []

        candles = []
        for c in reversed(data.get("values", [])):  # oldest first
            try:
                candles.append({
                    "time":  c["datetime"],
                    "open":  float(c["open"]),
                    "high":  float(c["high"]),
                    "low":   float(c["low"]),
                    "close": float(c["close"]),
                })
            except Exception as e:
                print(f"  [{pair}] Candle parse error: {e}")
                continue
        return candles

    except Exception as e:
        print(f"  [{pair}] Fetch failed: {e}")
        return []

# ══════════════════════════════════════════════
#  2. STATE MANAGEMENT
# ══════════════════════════════════════════════
def load_state():
    STATE_FILE.parent.mkdir(exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception as e:
            print(f"[WARN] State load failed: {e}")
    return {
        "candle_history": {p: [] for p in PAIRS},
        "active_setups":  {},
        "fired_signals":  [],
    }

def save_state(state):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ══════════════════════════════════════════════
#  3. MERGE CANDLES
# ══════════════════════════════════════════════
def merge_candles(stored, fetched, max_size=MAX_HISTORY):
    existing_times = {c["time"] for c in stored}
    for c in fetched:
        if c["time"] not in existing_times:
            stored.append(c)
            existing_times.add(c["time"])
    stored.sort(key=lambda c: c["time"])
    return stored[-max_size:]

# ══════════════════════════════════════════════
#  4. ATR CALCULATION
# ══════════════════════════════════════════════
def calc_atr(candles, idx, period=ATR_PERIOD):
    """Calculate ATR14 at a given candle index."""
    start = max(1, idx - period + 1)
    trs = []
    for j in range(start, idx + 1):
        high  = candles[j]["high"]
        low   = candles[j]["low"]
        prev_close = candles[j - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return float(np.mean(trs)) if trs else 0.0

# ══════════════════════════════════════════════
#  5. STRATEGY LOGIC
# ══════════════════════════════════════════════
def find_swings(closes, lb=SWING_LB):
    """Detect swing highs/lows using candle closes only (no wicks)."""
    sh, sl = [], []
    for i in range(lb, len(closes) - lb):
        win = closes[i-lb:i+lb+1]
        if closes[i] == max(win): sh.append(i)
        if closes[i] == min(win): sl.append(i)
    return sh, sl

def detect_trend(closes, sh, sl, n=TREND_N):
    """
    Trend detection using last N swing highs and lows.
    TREND_N=2: only requires 2 consecutive HH+HL or LH+LL.
    """
    psh = sh[-n:]
    psl = sl[-n:]
    if len(psh) < 2 or len(psl) < 2:
        return "neutral"
    shv = [closes[i] for i in psh]
    slv = [closes[i] for i in psl]
    bull = all(shv[i] < shv[i+1] for i in range(len(shv)-1)) and \
           all(slv[i] < slv[i+1] for i in range(len(slv)-1))
    bear = all(shv[i] > shv[i+1] for i in range(len(shv)-1)) and \
           all(slv[i] > slv[i+1] for i in range(len(slv)-1))
    if bull: return "bullish"
    if bear: return "bearish"
    return "neutral"

def run_signal_check(pair, candles, active_setups):
    if len(candles) < CANDLES_NEEDED:
        print(f"  [{pair}] Only {len(candles)} candles — need {CANDLES_NEEDED} minimum")
        return None

    pip    = PIP_SIZE[pair]
    rr     = RR_MAP[pair]
    closes = [c["close"] for c in candles]
    n      = len(closes)
    cur    = closes[-1]
    tol    = cur * RETEST_TOL

    sh, sl = find_swings(closes, SWING_LB)
    setup  = active_setups.get(pair)

    # ── Expire stale setup ──
    if setup and n - setup.get("breakout_idx", 0) > RETEST_WINDOW:
        print(f"  [{pair}] Setup expired — resetting")
        active_setups.pop(pair, None)
        setup = None

    # ── Check retest entry ──
    if setup:
        candles_since_break = (n - 1) - setup.get("breakout_idx", 0)
        if candles_since_break < 1:
            print(f"  [{pair}] Waiting — breakout candle not yet closed")
            return None

        lv = setup["level"]

        if setup["dir"] == "long":
            # Close must be near the level and above it (body confirmation)
            pulled_back = cur <= lv + tol
            body_above  = cur > lv
            if pulled_back and body_above:
                sl_p = setup["sl"]   # swing low close - 0.25*ATR buffer
                risk = cur - sl_p
                if risk > pip * 3:
                    tp = cur + risk * rr
                    active_setups.pop(pair, None)
                    return {
                        "pair":    pair,
                        "side":    "BUY",
                        "entry":   cur,
                        "sl":      sl_p,
                        "tp":      tp,
                        "level":   lv,
                        "sl_pips": round(risk / pip, 1),
                        "rr":      rr,
                        "trend":   setup.get("trend", "unknown"),
                        "time":    datetime.now(timezone.utc).isoformat(),
                    }

        else:  # short
            pulled_back = cur >= lv - tol
            body_below  = cur < lv
            if pulled_back and body_below:
                sl_p = setup["sl"]   # swing high close + 0.25*ATR buffer
                risk = sl_p - cur
                if risk > pip * 3:
                    tp = cur - risk * rr
                    active_setups.pop(pair, None)
                    return {
                        "pair":    pair,
                        "side":    "SELL",
                        "entry":   cur,
                        "sl":      sl_p,
                        "tp":      tp,
                        "level":   lv,
                        "sl_pips": round(risk / pip, 1),
                        "rr":      rr,
                        "trend":   setup.get("trend", "unknown"),
                        "time":    datetime.now(timezone.utc).isoformat(),
                    }

    # ── Look for new breakout ──
    if not setup:
        trend = detect_trend(closes, sh, sl)
        if trend == "bullish" and sh:
            valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
            if valid_sh:
                last_sh = valid_sh[-1]
                if (n - 1) - (last_sh + SWING_LB) <= BREAK_WINDOW and cur > closes[last_sh]:
                    valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
                    if valid_sl:
                        last_sl_idx = valid_sl[-1]
                        sl_close    = closes[last_sl_idx]
                        atr         = calc_atr(candles, last_sl_idx)
                        sl_price    = sl_close - ATR_MULT * atr   # buffer below swing low close
                        print(f"  [{pair}] 🔍 Bullish breakout above {closes[last_sh]:.{DP[pair]}f} — waiting for retest | SL: {sl_price:.{DP[pair]}f} (close {sl_close:.{DP[pair]}f} - {ATR_MULT}×ATR {atr:.{DP[pair]}f})")
                        active_setups[pair] = {
                            "dir":          "long",
                            "level":        closes[last_sh],
                            "sl":           sl_price,
                            "breakout_idx": n - 1,
                            "trend":        trend,
                            "created":      datetime.now(timezone.utc).isoformat(),
                        }
        elif trend == "bearish" and sl:
            valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
            if valid_sl:
                last_sl = valid_sl[-1]
                if (n - 1) - (last_sl + SWING_LB) <= BREAK_WINDOW and cur < closes[last_sl]:
                    valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
                    if valid_sh:
                        last_sh_idx = valid_sh[-1]
                        sl_close    = closes[last_sh_idx]
                        atr         = calc_atr(candles, last_sh_idx)
                        sl_price    = sl_close + ATR_MULT * atr   # buffer above swing high close
                        print(f"  [{pair}] 🔍 Bearish breakout below {closes[last_sl]:.{DP[pair]}f} — waiting for retest | SL: {sl_price:.{DP[pair]}f} (close {sl_close:.{DP[pair]}f} + {ATR_MULT}×ATR {atr:.{DP[pair]}f})")
                        active_setups[pair] = {
                            "dir":          "short",
                            "level":        closes[last_sl],
                            "sl":           sl_price,
                            "breakout_idx": n - 1,
                            "trend":        trend,
                            "created":      datetime.now(timezone.utc).isoformat(),
                        }
    return None

# ══════════════════════════════════════════════
#  6. TELEGRAM ALERT
# ══════════════════════════════════════════════
def send_telegram(signal):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("  [ALERT] Telegram not configured — skipping")
        return

    pair   = signal["pair"]
    side   = signal["side"]
    dp     = DP[pair]
    arrow  = "🟢" if side == "BUY" else "🔴"
    trend  = signal.get("trend", "unknown")
    t_icon = "↗️" if trend == "bullish" else "↘️" if trend == "bearish" else "➡️"
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    msg = (
        f"{arrow} *EDGE SIGNAL — {side} {pair}*\n\n"
        f"📍 Entry:       `{signal['entry']:.{dp}f}`\n"
        f"🛑 Stop Loss:   `{signal['sl']:.{dp}f}`\n"
        f"🎯 Take Profit: `{signal['tp']:.{dp}f}`\n\n"
        f"📊 RR: 1:{signal['rr']}  |  Risk: {signal['sl_pips']:.1f} pips\n"
        f"{t_icon} Trend: {trend.capitalize()}\n"
        f"🕐 {now}\n\n"
        f"_Break & Retest setup — M15 | Log it in EDGE Journal_"
    )

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        res = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       msg,
            "parse_mode": "Markdown",
        }, timeout=10)
        if res.json().get("ok"):
            print(f"  [ALERT] ✓ Telegram sent for {pair} {side}")
        else:
            print(f"  [ALERT] ✗ Telegram failed: {res.json()}")
    except Exception as e:
        print(f"  [ALERT] ✗ Telegram error: {e}")

# ══════════════════════════════════════════════
#  7. SIGNAL LOG
# ══════════════════════════════════════════════
def log_signal(signal):
    SIGNALS_LOG.parent.mkdir(exist_ok=True)
    header = not SIGNALS_LOG.exists()
    with open(SIGNALS_LOG, "a") as f:
        if header:
            f.write("time,pair,side,entry,sl,tp,sl_pips,rr,level,trend\n")
        f.write(
            f"{signal['time']},{signal['pair']},{signal['side']},"
            f"{signal['entry']},{signal['sl']},{signal['tp']},"
            f"{signal['sl_pips']},{signal['rr']},{signal['level']},"
            f"{signal.get('trend','')}\n"
        )

# ══════════════════════════════════════════════
#  8. SHEET LOGGER
# ══════════════════════════════════════════════
def log_signal_to_sheet(signal):
    """Write new signal to Google Sheet as PENDING."""
    try:
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(os.environ.get("SHEET_ID", "")).sheet1

        sheet.append_row([
            signal["time"],
            signal["pair"],
            signal["side"],
            signal["entry"],
            signal["sl"],
            signal["tp"],
            signal["rr"],
            signal["sl_pips"],
            "PENDING",
            "",
            "",
            signal.get("trend", ""),
        ])
        print(f"  [SHEET] ✓ Signal logged for {signal['pair']}")
    except Exception as e:
        print(f"  [SHEET] ✗ Sheet log error: {e}")

# ══════════════════════════════════════════════
#  9. DUPLICATE GUARD
# ══════════════════════════════════════════════
def is_duplicate(signal, fired_signals):
    sig_id = f"{signal['pair']}_{signal['side']}_{round(signal['level'], 4)}"
    now    = datetime.now(timezone.utc)
    recent = []
    for s in fired_signals:
        try:
            t = datetime.fromisoformat(s["time"])
            if (now - t).total_seconds() < 14400:
                recent.append(s)
        except Exception as e:
            print(f"[WARN] Duplicate check error: {e}")
    fired_signals.clear()
    fired_signals.extend(recent)
    for s in recent:
        if s.get("id") == sig_id:
            return True
    fired_signals.append({"id": sig_id, "time": now.isoformat()})
    return False

# ══════════════════════════════════════════════
#  10. MAIN
# ══════════════════════════════════════════════
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*55}")
    print(f"  EDGE Signal Engine — {now}")
    print(f"{'='*55}")

    if not TWELVE_DATA_KEY:
        print("[ERROR] TWELVE_DATA_KEY not set — aborting")
        return

    state           = load_state()
    candle_history  = state["candle_history"]
    active_setups   = state["active_setups"]
    fired_signals   = state["fired_signals"]

    # ── Fetch & merge candles for all pairs ──
    print("\n[1/3] Fetching M15 candles...")
    for pair in PAIRS:
        candles = fetch_candles(pair, outputsize=50)
        if not candles:
            print(f"  [{pair}] No candles returned — skipping")
            continue
        if pair not in candle_history:
            candle_history[pair] = []
        candle_history[pair] = merge_candles(candle_history[pair], candles)
        latest = candle_history[pair][-1]
        print(f"  {pair}: {len(candle_history[pair])} candles stored | Latest close: {latest['close']:.{DP[pair]}f}")

    # ── Run signal scan on strategy pairs only ──
    print("\n[2/3] Running signal scan...")
    signals_fired = []

    for pair in STRATEGY_PAIRS:
        if pair not in candle_history or not candle_history[pair]:
            print(f"  [{pair}] No history — skipping")
            continue

        hist_len  = len(candle_history[pair])
        setup     = active_setups.get(pair)
        setup_str = f"({setup['dir']} setup active)" if setup else "(no setup)"
        print(f"  [{pair}] {hist_len} candles  {setup_str}")

        signal = run_signal_check(pair, candle_history[pair], active_setups)

        if signal:
            if not is_duplicate(signal, fired_signals):
                signals_fired.append(signal)
                print(f"  [{pair}] 🔔 SIGNAL: {signal['side']}  Entry: {signal['entry']:.{DP[pair]}f}  SL: {signal['sl']:.{DP[pair]}f}  TP: {signal['tp']:.{DP[pair]}f}  Trend: {signal.get('trend')}")
            else:
                print(f"  [{pair}] Signal already fired recently — skipping duplicate")

    for pair in DATA_ONLY_PAIRS:
        stored = len(candle_history.get(pair, []))
        print(f"  [{pair}] Data collection only — {stored} candles stored")

    # ── Send alerts & log ──
    print(f"\n[3/3] Sending alerts...")
    if signals_fired:
        for sig in signals_fired:
            send_telegram(sig)
            log_signal(sig)
            log_signal_to_sheet(sig)
            print(f"  ✓ {sig['side']} {sig['pair']} logged")
    else:
        print("  No new signals this run")

    # ── Save state ──
    state["candle_history"] = candle_history
    state["active_setups"]  = active_setups
    state["fired_signals"]  = fired_signals
    save_state(state)

    # ── Summary ──
    print(f"\n{'='*55}")
    print(f"  Strategy pairs:    {STRATEGY_PAIRS}")
    print(f"  Data-only pairs:   {DATA_ONLY_PAIRS}")
    print(f"  Active setups:     {len(active_setups)}")
    print(f"  Signals this run:  {len(signals_fired)}")
    total = sum(len(v) for v in candle_history.values())
    print(f"  Total candles:     {total}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()

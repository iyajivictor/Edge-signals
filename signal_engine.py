"""
EDGE Signal Engine
==================
Three-strategy signal detector:

  1. Break & Retest (B&R)
     Pairs: USDJPY, GBPUSD, AUDJPY, XAUUSD, EURUSD
     M15 timeframe | 1:2 RR (XAUUSD 1:3) | Trend-filtered

  2. Sweep + FVG
     Pairs: all 5 strategy pairs
     M15 entry | H1 TP targets | Session-filtered
     Sources: PDH, AsianH, NewYorkH | RR 1:3–1:5

  3. Prop Firm — London Open Liquidity Grab (LOLG)
     Pairs: EURUSD, GBPUSD only
     H1 Asian range | M15 sweep detection | 07:00–09:00 UTC
     RR 1:1.5–1:3 | Risk 0.5% | Max 2 trades/day
     Telegram label: 🏆 PROP | Logged to PropFirm Sheets tab

Runs every 15 minutes via GitHub Actions + cron-job.org
Sends Telegram alerts when a signal fires.
Logs all signals to state/ CSV files and Google Sheets.

Changes vs previous version:
  - prop_engine.py integrated as [2c/3] scan step
  - PROP_PAIRS constant added (EURUSD, GBPUSD)
  - PROP_LOG path added (state/prop_log.csv)
  - send_telegram_prop(), log_prop_signal(), log_prop_to_sheet() added
  - Summary block now shows Prop signals count
  - SweepFVG sheet logger: h1_bias removed, sweep_time added, outcome pre-filled empty
  - TREND_N reverted 2 → 3 (stricter trend confirmation)
  - SWING_MIN_DIST added: minimum pip distance between swing points
  - SL placement uses swing close + 0.25x ATR14 buffer (not wicks)
  - Swing detection remains close-based (unchanged)
  - RETEST_WINDOW = 5 candles (75 minutes max retest window)
  - FIX: return None after setup expiry (prevents same-run rebuild)
  - FIX: live_signals now in separate file (no race condition)
  - FIX: Hybrid retest detection (wick touch + close confirmation)
  - EURUSD moved to active strategy pairs
  - FIX: breakout_time stored as timestamp (fixes stale index bug)
  - XAUUSD ATR_MULT increased to 0.75 (wider SL buffer for gold volatility)
  - FIX: 1s delay between API calls (avoids Twelve Data rate limiting)
  - HTF cache added: H1 fetched on the hour, H4 at 00/04/08/12/16/20
  - Sweep+FVG scan now runs on all 5 pairs with H1 + H4 candles
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

# ── Sweep + FVG strategy module ──
try:
    from sweep_fvg import scan as sweep_fvg_scan
    SWEEP_FVG_ENABLED = True
except ImportError:
    print("[WARN] sweep_fvg.py not found — Sweep+FVG strategy disabled")
    SWEEP_FVG_ENABLED = False

# ── Prop Firm strategy module ──
try:
    from prop_engine import scan as prop_scan
    PROP_ENABLED = True
except ImportError:
    print("[WARN] prop_engine.py not found — Prop Firm strategy disabled")
    PROP_ENABLED = False

# ══════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN        = os.environ.get("TG_TOKEN",        "")
TG_CHAT_ID      = os.environ.get("TG_CHAT_ID",      "")

STRATEGY_PAIRS  = ["USDJPY", "GBPUSD", "AUDJPY", "XAUUSD", "EURUSD"]
DATA_ONLY_PAIRS = []
PAIRS           = STRATEGY_PAIRS

TD_SYMBOLS = {
    "USDJPY": "USD/JPY",
    "GBPUSD": "GBP/USD",
    "AUDJPY": "AUD/JPY",
    "XAUUSD": "XAU/USD",
    "EURUSD": "EUR/USD",
}

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

RR_MAP = {
    "USDJPY": 2.0,
    "GBPUSD": 2.0,
    "AUDJPY": 2.0,
    "XAUUSD": 3.0,
    "EURUSD": 2.0,
}

SWING_LB       = 5
SWING_MIN_DIST = 5
RETEST_TOL     = 0.0008
RETEST_WINDOW  = 5
TREND_N        = 3
BREAK_WINDOW   = 20
CANDLES_NEEDED = 200
MAX_HISTORY    = 1344

ATR_PERIOD = 14
ATR_MULT   = {
    "USDJPY": 0.25,
    "GBPUSD": 0.25,
    "AUDJPY": 0.25,
    "XAUUSD": 0.75,
    "EURUSD": 0.25,
}

STATE_FILE        = Path("state/price_history.json")
LIVE_SIGNALS_FILE = Path("state/live_signals.json")
SIGNALS_LOG       = Path("state/signals_log.csv")
SWEEP_FVG_LOG     = Path("state/sweep_fvg_log.csv")
PROP_LOG          = Path("state/prop_log.csv")

# Prop firm pairs — London Open Liquidity Grab runs on these only
PROP_PAIRS = ["EURUSD", "GBPUSD"]

# HTF candle counts to fetch when cache is stale
H1_FETCH_SIZE = 50   # ~50 hours of H1
H4_FETCH_SIZE = 30   # ~5 days of H4

# H4 boundary hours (UTC) — re-fetch at these hours only
H4_HOURS = {0, 4, 8, 12, 16, 20}


# ══════════════════════════════════════════════
#  1a. FETCH M15 OHLC CANDLES
# ══════════════════════════════════════════════
def fetch_candles(pair, outputsize=50):
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
        for c in reversed(data.get("values", [])):
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
#  1b. HTF CACHE — H1 / H4 FETCH + CACHE
# ══════════════════════════════════════════════
def fetch_htf_candles(pair: str, interval: str, outputsize: int) -> list:
    """
    Fetch H1 or H4 candles from Twelve Data.
    interval: '1h' or '4h'
    Returns list of candle dicts with 'time' as string.
    """
    symbol = TD_SYMBOLS[pair]
    url = (
        f"https://api.twelvedata.com/time_series"
        f"?symbol={symbol}"
        f"&interval={interval}"
        f"&outputsize={outputsize}"
        f"&apikey={TWELVE_DATA_KEY}"
    )
    try:
        res  = requests.get(url, timeout=15)
        data = res.json()
        if data.get("status") == "error":
            print(f"  [{pair}] HTF {interval} API error: {data.get('message')}")
            return []
        candles = []
        for c in reversed(data.get("values", [])):
            try:
                candles.append({
                    "time":  c["datetime"],
                    "open":  float(c["open"]),
                    "high":  float(c["high"]),
                    "low":   float(c["low"]),
                    "close": float(c["close"]),
                })
            except Exception as e:
                print(f"  [{pair}] HTF candle parse error: {e}")
                continue
        return candles
    except Exception as e:
        print(f"  [{pair}] HTF fetch failed: {e}")
        return []


def should_refresh_h1(now_utc: datetime, cache_entry: dict) -> bool:
    """H1 refreshes once per hour — when current minute is 0-14 (first cron of the hour)."""
    last_fetched = cache_entry.get("fetched_hour")
    return last_fetched != now_utc.hour


def should_refresh_h4(now_utc: datetime, cache_entry: dict) -> bool:
    """H4 refreshes only at 00, 04, 08, 12, 16, 20 UTC — first cron of that 4hr block."""
    if now_utc.hour not in H4_HOURS:
        return False
    last_fetched_hour = cache_entry.get("fetched_hour")
    return last_fetched_hour != now_utc.hour


def load_htf_cache(state: dict) -> dict:
    """
    Returns htf_cache from state.
    Structure: { pair: { 'h1': { 'candles': [...], 'fetched_hour': int },
                         'h4': { 'candles': [...], 'fetched_hour': int } } }
    """
    return state.get("htf_cache", {})


def save_htf_cache(state: dict, htf_cache: dict):
    state["htf_cache"] = htf_cache


def _ensure_pair_cache(pair: str, htf_cache: dict):
    """Initialise cache entry for a pair if missing."""
    if pair not in htf_cache:
        htf_cache[pair] = {
            "h1": {"candles": [], "fetched_hour": -1},
            "h4": {"candles": [], "fetched_hour": -1},
        }


def refresh_h1_cache(htf_cache: dict, now_utc: datetime):
    """
    Fetch H1 for every pair that needs it, with 8s between calls.
    All 5 H1 fetches stay within one 60-second window (5 x 8s = 40s).
    """
    needs_refresh = []
    for pair in STRATEGY_PAIRS:
        _ensure_pair_cache(pair, htf_cache)
        if should_refresh_h1(now_utc, htf_cache[pair]["h1"]):
            needs_refresh.append(pair)
        else:
            print(f"  [{pair}] H1 cache hit (fetched hour={htf_cache[pair]['h1']['fetched_hour']})")

    if not needs_refresh:
        return

    print(f"  Fetching H1 for: {needs_refresh} (8s between calls)")
    for i, pair in enumerate(needs_refresh):
        print(f"  [{pair}] Fetching H1 candles (hour={now_utc.hour})...")
        h1_raw = fetch_htf_candles(pair, "1h", H1_FETCH_SIZE)
        if h1_raw:
            htf_cache[pair]["h1"]["candles"]      = h1_raw
            htf_cache[pair]["h1"]["fetched_hour"] = now_utc.hour
            print(f"  [{pair}] H1 cache updated — {len(h1_raw)} candles")
        else:
            print(f"  [{pair}] H1 fetch failed — using cached data")
        if i < len(needs_refresh) - 1:
            time.sleep(12)   # 12s gap keeps all 5 calls under 8 credits/min


def refresh_h4_cache(htf_cache: dict, now_utc: datetime):
    """
    Fetch H4 for every pair that needs it, with 8s between calls.
    Only runs at H4 boundary hours (00, 04, 08, 12, 16, 20 UTC).
    """
    needs_refresh = []
    for pair in STRATEGY_PAIRS:
        _ensure_pair_cache(pair, htf_cache)
        if should_refresh_h4(now_utc, htf_cache[pair]["h4"]):
            needs_refresh.append(pair)
        else:
            print(f"  [{pair}] H4 cache hit (fetched hour={htf_cache[pair]['h4']['fetched_hour']})")

    if not needs_refresh:
        return

    print(f"  Fetching H4 for: {needs_refresh} (8s between calls)")
    for i, pair in enumerate(needs_refresh):
        print(f"  [{pair}] Fetching H4 candles (hour={now_utc.hour})...")
        h4_raw = fetch_htf_candles(pair, "4h", H4_FETCH_SIZE)
        if h4_raw:
            htf_cache[pair]["h4"]["candles"]      = h4_raw
            htf_cache[pair]["h4"]["fetched_hour"] = now_utc.hour
            print(f"  [{pair}] H4 cache updated — {len(h4_raw)} candles")
        else:
            print(f"  [{pair}] H4 fetch failed — using cached data")
        if i < len(needs_refresh) - 1:
            time.sleep(12)   # 12s gap keeps all 5 calls under 8 credits/min





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
        "htf_cache":      {},
    }

def save_state(state):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def load_live_signals():
    if LIVE_SIGNALS_FILE.exists():
        try:
            return json.loads(LIVE_SIGNALS_FILE.read_text())
        except:
            return {}
    return {}

def save_live_signals(live):
    LIVE_SIGNALS_FILE.parent.mkdir(exist_ok=True)
    LIVE_SIGNALS_FILE.write_text(json.dumps(live, indent=2))

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
    start = max(1, idx - period + 1)
    trs = []
    for j in range(start, idx + 1):
        high       = candles[j]["high"]
        low        = candles[j]["low"]
        prev_close = candles[j - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return float(sum(trs) / len(trs)) if trs else 0.0

# ══════════════════════════════════════════════
#  5. STRATEGY LOGIC
# ══════════════════════════════════════════════
def find_swings(closes, candles, lb=SWING_LB, pair=None):
    sh, sl = [], []
    pip = PIP_SIZE.get(pair, 0.0001) if pair else 0.0001
    min_dist = SWING_MIN_DIST * pip

    for i in range(lb, len(closes) - lb):
        win = closes[i-lb:i+lb+1]

        if closes[i] == max(win):
            if sh:
                prev_high = closes[sh[-1]]
                if abs(closes[i] - prev_high) < min_dist:
                    continue
            sh.append(i)

        if closes[i] == min(win):
            if sl:
                prev_low = closes[sl[-1]]
                if abs(closes[i] - prev_low) < min_dist:
                    continue
            sl.append(i)

    return sh, sl

def detect_trend(closes, sh, sl, n=TREND_N):
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

    # ── Block if live signal already active for this pair ──
    live_signals = load_live_signals()
    if pair in live_signals:
        fired_at = live_signals[pair].get("time", "unknown")
        print(f"  [{pair}] Live signal active since {fired_at} — skipping")
        return None

    pip    = PIP_SIZE[pair]
    rr     = RR_MAP[pair]
    closes = [c["close"] for c in candles]
    n      = len(closes)
    cur    = closes[-1]
    tol    = cur * RETEST_TOL

    sh, sl = find_swings(closes, candles, SWING_LB, pair=pair)
    setup  = active_setups.get(pair)

    # ── Expire stale setup ──
    if setup and n - setup.get("breakout_idx", 0) > RETEST_WINDOW:
        print(f"  [{pair}] Setup expired — resetting")
        active_setups.pop(pair, None)
        return None

    # ── Check retest entry ──
    if setup:
        breakout_time = setup.get("breakout_time")
        if breakout_time and candles[-1]["time"] <= breakout_time:
            print(f"  [{pair}] Waiting — breakout candle not yet closed")
            return None

        lv = setup["level"]

        if setup["dir"] == "long":
            candle_low  = candles[-1]["low"]
            pulled_back = candle_low <= lv + tol
            body_above  = cur > lv
            if pulled_back and body_above:
                sl_p = setup["sl"]
                risk = cur - sl_p
                if risk > pip * 3:
                    tp = cur + risk * rr
                    active_setups.pop(pair, None)
                    live = load_live_signals()
                    live[pair] = {
                        "side":  "BUY",
                        "entry": cur,
                        "sl":    sl_p,
                        "tp":    tp,
                        "time":  datetime.now(timezone.utc).isoformat(),
                    }
                    save_live_signals(live)
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

        else:
            candle_high = candles[-1]["high"]
            pulled_back = candle_high >= lv - tol
            body_below  = cur < lv
            if pulled_back and body_below:
                sl_p = setup["sl"]
                risk = sl_p - cur
                if risk > pip * 3:
                    tp = cur - risk * rr
                    active_setups.pop(pair, None)
                    live = load_live_signals()
                    live[pair] = {
                        "side":  "SELL",
                        "entry": cur,
                        "sl":    sl_p,
                        "tp":    tp,
                        "time":  datetime.now(timezone.utc).isoformat(),
                    }
                    save_live_signals(live)
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
                        sl_price    = sl_close - ATR_MULT[pair] * atr
                        print(f"  [{pair}] 🔍 Bullish breakout above {closes[last_sh]:.{DP[pair]}f} — waiting for retest | SL: {sl_price:.{DP[pair]}f}")
                        active_setups[pair] = {
                            "dir":           "long",
                            "level":         closes[last_sh],
                            "sl":            sl_price,
                            "breakout_idx":  n - 1,
                            "breakout_time": candles[-1]["time"],
                            "trend":         trend,
                            "created":       datetime.now(timezone.utc).isoformat(),
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
                        sl_price    = sl_close + ATR_MULT[pair] * atr
                        print(f"  [{pair}] 🔍 Bearish breakout below {closes[last_sl]:.{DP[pair]}f} — waiting for retest | SL: {sl_price:.{DP[pair]}f}")
                        active_setups[pair] = {
                            "dir":           "short",
                            "level":         closes[last_sl],
                            "sl":            sl_price,
                            "breakout_idx":  n - 1,
                            "breakout_time": candles[-1]["time"],
                            "trend":         trend,
                            "created":       datetime.now(timezone.utc).isoformat(),
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
            f"{signal.get('trend','')}\\n"
        )

# ══════════════════════════════════════════════
#  8. SHEET LOGGER
# ══════════════════════════════════════════════
def log_signal_to_sheet(signal):
    try:
        raw = os.environ.get("GOOGLE_CREDENTIALS", "")
        creds_dict = json.loads(raw)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(os.environ.get("SHEET_ID", "")).sheet1
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
#  10. SWEEP+FVG HELPERS
# ══════════════════════════════════════════════
def prepare_candles_for_sweep_fvg(raw_candles: list, now_utc: datetime = None) -> list:
    """
    Convert signal_engine candle format -> sweep_fvg format (datetime objects).
    Filters out any candle whose timestamp is ahead of now_utc -- these are
    stale future-dated candles carried over in state from earlier runs and
    would produce sweep signals with impossible future timestamps.
    """
    now = now_utc or datetime.now(timezone.utc)
    prepared = []
    skipped  = 0
    for c in raw_candles:
        try:
            candle_time = datetime.strptime(c["time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if candle_time > now:
                skipped += 1
                continue
            prepared.append({
                "time":   candle_time,
                "open":   float(c["open"]),
                "high":   float(c["high"]),
                "low":    float(c["low"]),
                "close":  float(c["close"]),
                "volume": 0,
            })
        except Exception as e:
            print(f"  [SWEEP] Candle prep error: {e}")
            continue
    if skipped:
        print(f"  [SWEEP] Filtered {skipped} future-dated candle(s) from history")
    return prepared


def send_telegram_sweep_fvg(signal: dict):
    """Send Telegram alert for a Sweep+FVG signal."""
    if not TG_TOKEN or not TG_CHAT_ID:
        print("  [SWEEP] Telegram not configured — skipping")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        res = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       signal["message"],
            "parse_mode": "Markdown",
        }, timeout=10)
        if res.json().get("ok"):
            print(f"  [SWEEP] ✓ Telegram sent — {signal['direction'].upper()} {signal['pair']}")
        else:
            print(f"  [SWEEP] ✗ Telegram failed: {res.json()}")
    except Exception as e:
        print(f"  [SWEEP] ✗ Telegram error: {e}")


def log_sweep_fvg_signal(signal: dict):
    """Append Sweep+FVG signal to its own CSV log.

    Header (auto-filled columns only — outcome/pnl_pips/notes are manual):
    fired_at | pair | direction | entry | sl | tp | rr | session |
    lv_source | tp_source | sweep_time
    """
    SWEEP_FVG_LOG.parent.mkdir(exist_ok=True)
    write_header = not SWEEP_FVG_LOG.exists()
    with open(SWEEP_FVG_LOG, "a") as f:
        if write_header:
            f.write("fired_at,pair,direction,entry,sl,tp,rr,session,lv_source,tp_source,sweep_time\n")
        f.write(
            f"{signal['fired_at']},{signal['pair']},{signal['direction']},"
            f"{signal['entry']},{signal['sl']},{signal['tp']},"
            f"{signal['rr']},{signal['session']},"
            f"{signal['lv_source']},{signal.get('tp_source','')},"
            f"{signal['sweep_time']}\n"
        )


def log_sweep_fvg_to_sheet(signal: dict):
    """Log Sweep+FVG signal to Google Sheets (SweepFVG tab).

    Columns written (must match SweepFVG tab header exactly):
    fired_at | pair | direction | entry | sl | tp | rr | session |
    lv_source | tp_source | sweep_time | outcome | pnl_pips | notes
    outcome/pnl_pips/notes pre-filled as empty strings for manual entry.
    """
    try:
        raw        = os.environ.get("GOOGLE_CREDENTIALS", "")
        creds_dict = json.loads(raw)
        creds      = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client   = gspread.authorize(creds)
        workbook = client.open_by_key(os.environ.get("SHEET_ID", ""))

        all_tabs = [ws.title for ws in workbook.worksheets()]
        print(f"  [SWEEP] Available tabs: {all_tabs}")

        try:
            sheet = workbook.worksheet("SweepFVG")
            print(f"  [SWEEP] Found SweepFVG tab ✓")
        except Exception as tab_err:
            print(f"  [SWEEP] ✗ SweepFVG tab not found: {tab_err}")
            print(f"  [SWEEP] Available tabs were: {all_tabs}")
            return

        row = [
            signal["fired_at"],
            signal["pair"],
            signal["direction"].upper(),
            signal["entry"],
            signal["sl"],
            signal["tp"],
            signal["rr"],
            signal["session"],
            signal["lv_source"],
            signal.get("tp_source", ""),
            signal["sweep_time"],
            "",   # outcome   — manual
            "",   # pnl_pips  — manual
            "",   # notes     — manual
        ]
        print(f"  [SWEEP] Writing row: {row}")
        sheet.append_row(row)
        print(f"  [SWEEP] ✓ Sheet logged")
    except Exception as e:
        print(f"  [SWEEP] ✗ Sheet log error: {e}")


# ══════════════════════════════════════════════
#  11. PROP FIRM HELPERS
# ══════════════════════════════════════════════
def send_telegram_prop(signal: dict):
    """Send Telegram alert for a Prop Firm signal (🏆 PROP label)."""
    if not TG_TOKEN or not TG_CHAT_ID:
        print("  [PROP] Telegram not configured — skipping")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        res = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       signal["message"],
            "parse_mode": "Markdown",
        }, timeout=10)
        if res.json().get("ok"):
            print(f"  [PROP] ✓ Telegram sent — {signal['side']} {signal['pair']}")
        else:
            print(f"  [PROP] ✗ Telegram failed: {res.json()}")
    except Exception as e:
        print(f"  [PROP] ✗ Telegram error: {e}")


def log_prop_signal(signal: dict):
    """Append Prop Firm signal to prop_log.csv.

    Header (auto-filled — outcome/pnl_pips/notes are manual):
    fired_at | pair | direction | entry | sl | tp | rr | sl_pips |
    asian_high | asian_low | sweep_src | sweep_type | risk_pct | session
    """
    PROP_LOG.parent.mkdir(exist_ok=True)
    write_header = not PROP_LOG.exists()
    with open(PROP_LOG, "a") as f:
        if write_header:
            f.write(
                "fired_at,pair,direction,entry,sl,tp,rr,sl_pips,"
                "asian_high,asian_low,sweep_src,sweep_type,risk_pct,session\n"
            )
        f.write(
            f"{signal['fired_at']},{signal['pair']},{signal['direction']},"
            f"{signal['entry']},{signal['sl']},{signal['tp']},"
            f"{signal['rr']},{signal['sl_pips']},"
            f"{signal['asian_high']},{signal['asian_low']},"
            f"{signal['sweep_src']},{signal['sweep_type']},"
            f"{signal['risk_pct']},{signal['session']}\n"
        )


def log_prop_to_sheet(signal: dict):
    """Log Prop Firm signal to Google Sheets (PropFirm tab).

    Columns written (must match PropFirm tab header exactly):
    fired_at | pair | direction | entry | sl | tp | rr |
    asian_high | asian_low | sweep_src | session |
    outcome | pnl_pips | notes
    outcome/pnl_pips/notes pre-filled empty for manual entry.
    """
    try:
        raw        = os.environ.get("GOOGLE_CREDENTIALS", "")
        creds_dict = json.loads(raw)
        creds      = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client   = gspread.authorize(creds)
        workbook = client.open_by_key(os.environ.get("SHEET_ID", ""))

        all_tabs = [ws.title for ws in workbook.worksheets()]
        print(f"  [PROP] Available tabs: {all_tabs}")

        try:
            sheet = workbook.worksheet("PropFirm")
            print(f"  [PROP] Found PropFirm tab ✓")
        except Exception as tab_err:
            print(f"  [PROP] ✗ PropFirm tab not found: {tab_err}")
            print(f"  [PROP] Available tabs were: {all_tabs}")
            return

        row = [
            signal["fired_at"],
            signal["pair"],
            signal["direction"].upper(),
            signal["entry"],
            signal["sl"],
            signal["tp"],
            signal["rr"],
            signal["asian_high"],
            signal["asian_low"],
            signal["sweep_src"],
            signal["session"],
            "",   # outcome   — manual
            "",   # pnl_pips  — manual
            "",   # notes     — manual
        ]
        print(f"  [PROP] Writing row: {row}")
        sheet.append_row(row)
        print(f"  [PROP] ✓ PropFirm sheet logged")
    except Exception as e:
        print(f"  [PROP] ✗ Sheet log error: {e}")

# ══════════════════════════════════════════════
#  11. MAIN
# ══════════════════════════════════════════════
def main():
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*55}")
    print(f"  EDGE Signal Engine — {now_str}")
    print(f"{'='*55}")

    if not TWELVE_DATA_KEY:
        print("[ERROR] TWELVE_DATA_KEY not set — aborting")
        return

    state          = load_state()
    candle_history = state["candle_history"]
    active_setups  = state["active_setups"]
    fired_signals  = state["fired_signals"]
    htf_cache      = load_htf_cache(state)

    # ── [1/3] Fetch M15 candles ──────────────────────────────────────────
    print("\n[1/3] Fetching M15 candles...")
    for pair in PAIRS:
        candles = fetch_candles(pair, outputsize=50)
        time.sleep(1)
        if not candles:
            print(f"  [{pair}] No candles returned — skipping")
            continue
        if pair not in candle_history:
            candle_history[pair] = []
        candle_history[pair] = merge_candles(candle_history[pair], candles)
        latest = candle_history[pair][-1]
        print(f"  {pair}: {len(candle_history[pair])} candles stored | Latest close: {latest['close']:.{DP[pair]}f}")

    # ── [1b/3] Refresh HTF cache ───────────────────────────────────────────
    # H1 fetches on the hour, H4 at boundary hours (00,04,08,12,16,20).
    # Each pass uses 12s between calls (5 calls x 12s = 60s) so all 5
    # fetches stay under 8 credits/min with no extra waiting needed.
    h1_needed = any(
        should_refresh_h1(now_utc, htf_cache.get(p, {}).get("h1", {"fetched_hour": -1}))
        for p in STRATEGY_PAIRS
    )
    h4_needed = now_utc.hour in H4_HOURS and any(
        should_refresh_h4(now_utc, htf_cache.get(p, {}).get("h4", {"fetched_hour": -1}))
        for p in STRATEGY_PAIRS
    )

    if h1_needed or h4_needed:
        print(f"\n[1b/3] HTF refresh needed -- H1={h1_needed} H4={h4_needed}")
        if h1_needed:
            print(f"\n  [H1 pass] Fetching H1 candles...")
            refresh_h1_cache(htf_cache, now_utc)

        if h4_needed:
            print(f"\n  [H4 pass] Fetching H4 candles (boundary hour={now_utc.hour})...")
            refresh_h4_cache(htf_cache, now_utc)
    else:
        print(f"\n[1b/3] HTF cache OK -- all pairs current, no fetches needed")

    # ── [2/3] Run B&R signal scan ─────────────────────────────────────────
    print("\n[2/3] Running signal scan...")
    signals_fired = []
    live = load_live_signals()

    for pair in STRATEGY_PAIRS:
        if pair not in candle_history or not candle_history[pair]:
            print(f"  [{pair}] No history — skipping")
            continue

        hist_len  = len(candle_history[pair])
        setup     = active_setups.get(pair)
        live_str  = f"(LIVE {live[pair]['side']} active)" if pair in live else ""
        setup_str = f"({setup['dir']} setup active)" if setup else "(no setup)"
        status    = live_str if live_str else setup_str
        print(f"  [{pair}] {hist_len} candles  {status}")

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

    # ── [2b/3] Sweep+FVG scan — all 5 pairs ──────────────────────────────
    sweep_signals = []
    if SWEEP_FVG_ENABLED:
        print(f"\n[2b/3] Running Sweep+FVG scan on all pairs...")
        for pair in STRATEGY_PAIRS:
            if pair not in candle_history or not candle_history[pair]:
                print(f"  [{pair}] No M15 history — skipping Sweep+FVG")
                continue

            m15_prepared = prepare_candles_for_sweep_fvg(candle_history[pair], now_utc)
            if len(m15_prepared) < 100:
                print(f"  [{pair}] Not enough M15 candles ({len(m15_prepared)}) — need 100 minimum")
                continue

            # Pull HTF from cache (already refreshed in [1b/3])
            h1_raw, h4_raw = htf_cache.get(pair, {}).get("h1", {}).get("candles", []), \
                             htf_cache.get(pair, {}).get("h4", {}).get("candles", [])

            h1_prepared = prepare_candles_for_sweep_fvg(h1_raw, now_utc) if h1_raw else []
            h4_prepared = prepare_candles_for_sweep_fvg(h4_raw, now_utc) if h4_raw else []

            print(f"  [{pair}] Sweep+FVG scan | M15={len(m15_prepared)} H1={len(h1_prepared)} H4={len(h4_prepared)}")

            pair_sweep_signals = sweep_fvg_scan(
                m15_candles=m15_prepared,
                h1_candles=h1_prepared if h1_prepared else None,
                h4_candles=h4_prepared if h4_prepared else None,
                pair=pair,
            )

            if pair_sweep_signals:
                print(f"  [{pair}] 🔔 {len(pair_sweep_signals)} Sweep+FVG signal(s) found")
                sweep_signals.extend(pair_sweep_signals)
            else:
                print(f"  [{pair}] No Sweep+FVG signals this run")

    # ── [2c/3] Prop Firm scan — EURUSD + GBPUSD only ─────────────────────
    prop_signals = []
    if PROP_ENABLED:
        print(f"\n[2c/3] Running Prop Firm scan (EURUSD, GBPUSD)...")
        for pair in PROP_PAIRS:
            if pair not in candle_history or not candle_history[pair]:
                print(f"  [{pair}] No M15 history — skipping Prop scan")
                continue

            m15_prepared = prepare_candles_for_sweep_fvg(candle_history[pair], now_utc)
            if len(m15_prepared) < 50:
                print(f"  [{pair}] Not enough M15 candles ({len(m15_prepared)}) — need 50 minimum")
                continue

            # H1 from existing HTF cache — no extra API calls
            h1_raw      = htf_cache.get(pair, {}).get("h1", {}).get("candles", [])
            h1_prepared = prepare_candles_for_sweep_fvg(h1_raw, now_utc) if h1_raw else []

            print(f"  [{pair}] Prop scan | M15={len(m15_prepared)} H1={len(h1_prepared)}")

            pair_prop_signals = prop_scan(
                m15_candles=m15_prepared,
                h1_candles=h1_prepared if h1_prepared else None,
                pair=pair,
            )

            if pair_prop_signals:
                print(f"  [{pair}] 🏆 {len(pair_prop_signals)} Prop signal(s) found")
                prop_signals.extend(pair_prop_signals)
            else:
                print(f"  [{pair}] No Prop signals this run")
    else:
        prop_signals = []

    # ── [3/3] Send alerts ─────────────────────────────────────────────────
    print(f"\n[3/3] Sending alerts...")

    # B&R alerts
    if signals_fired:
        for sig in signals_fired:
            send_telegram(sig)
            log_signal(sig)
            log_signal_to_sheet(sig)
            print(f"  ✓ {sig['side']} {sig['pair']} logged")
    else:
        print("  No new B&R signals this run")

    # Sweep+FVG alerts
    if sweep_signals:
        for sig in sweep_signals:
            send_telegram_sweep_fvg(sig)
            log_sweep_fvg_signal(sig)
            log_sweep_fvg_to_sheet(sig)
    else:
        print("  No new Sweep+FVG signals this run")

    # Prop Firm alerts
    if prop_signals:
        for sig in prop_signals:
            send_telegram_prop(sig)
            log_prop_signal(sig)
            log_prop_to_sheet(sig)
            print(f"  ✓ 🏆 {sig['side']} {sig['pair']} prop signal logged")
    else:
        print("  No new Prop signals this run")

    # ── Persist state ──────────────────────────────────────────────────────
    state["candle_history"] = candle_history
    state["active_setups"]  = active_setups
    state["fired_signals"]  = fired_signals
    save_htf_cache(state, htf_cache)
    save_state(state)

    print(f"\n{'='*55}")
    print(f"  Strategy pairs:    {STRATEGY_PAIRS}")
    print(f"  Data-only pairs:   {DATA_ONLY_PAIRS}")
    print(f"  Active setups:     {len([k for k in active_setups if not k.startswith('_')])}")
    print(f"  Live signals:      {list(load_live_signals().keys())}")
    print(f"  B&R signals:       {len(signals_fired)}")
    print(f"  Sweep+FVG signals: {len(sweep_signals)}")
    print(f"  Prop signals:      {len(prop_signals)}")
    total = sum(len(v) for v in candle_history.values())
    print(f"  Total candles:     {total}")
    h1_cached = sum(1 for p in STRATEGY_PAIRS if htf_cache.get(p, {}).get("h1", {}).get("candles"))
    h4_cached = sum(1 for p in STRATEGY_PAIRS if htf_cache.get(p, {}).get("h4", {}).get("candles"))
    print(f"  H1 cache:          {h1_cached}/{len(STRATEGY_PAIRS)} pairs")
    print(f"  H4 cache:          {h4_cached}/{len(STRATEGY_PAIRS)} pairs")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()



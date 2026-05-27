"""
EDGE Signal Engine — v7
========================
Strategies:
  1. Break & Retest (B&R) — 5 pairs, M15
  2. H4 Zone Sweep + M15 FVG — 10 pairs, Stage 1 only

Lifecycle:
  B&R:
    signal_engine → detects retest → Telegram + Sheet1
    Replit        → monitors TP/SL → resolves outcome

  Sweep+FVG:
    signal_engine → Stage 1 only → Telegram + SweepFVG sheet
    Replit        → Phase 0,1,2 → full lifecycle

Changes vs v6:
  · Batch API fetch — all pairs in 2 calls (was 1 per pair)
    cuts daily usage from ~1020 to ~204 calls → 24/7 viable
  · Session filters removed — engine runs all hours, all pairs
    session logged to analytics only
  · MSS filter added — sweep_fvg.py validates structure shift
  · H4 EMA20 computed after fetch, stored in htf_cache
    passed to scan() as htf_trend ('bullish'/'bearish'/'neutral')
  · log_sweep_fvg_to_sheet() expanded to 32 columns
    (added 10 new analytics fields)
  · sweep_fvg_scan() receives htf_trend argument
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# ── Sweep+FVG import ──────────────────────────────────────────────────────────
try:
    from sweep_fvg import (
        scan as sweep_fvg_scan,
        cleanup_expired_pending,
    )
    SWEEP_FVG_ENABLED = True
except ImportError:
    logger.warning("sweep_fvg.py not found — Sweep+FVG disabled")
    SWEEP_FVG_ENABLED = False


# ── CONFIG ────────────────────────────────────────────────────────────────────
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN        = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID      = os.environ.get("TG_CHAT_ID", "")
SHEET_ID        = os.environ.get("SHEET_ID", "")
GOOGLE_CREDS    = os.environ.get("GOOGLE_CREDENTIALS", "")

# ── Pairs ─────────────────────────────────────────────────────────────────────
BR_PAIRS = ["USDJPY", "GBPUSD", "AUDJPY", "XAUUSD", "EURUSD"]

SWEEP_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDJPY", "XAUUSD",
    "CADJPY", "USDCAD", "EURJPY", "GBPJPY", "GBPAUD",
]

ALL_PAIRS = list(set(BR_PAIRS + SWEEP_PAIRS))

TD_SYMBOLS = {
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY", "AUDJPY": "AUD/JPY",
    "XAUUSD": "XAU/USD", "CADJPY": "CAD/JPY",
    "USDCAD": "USD/CAD", "EURJPY": "EUR/JPY",
    "GBPJPY": "GBP/JPY", "GBPAUD": "GBP/AUD",
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

# ── B&R Config ────────────────────────────────────────────────────────────────
RR_MAP = {
    "USDJPY": 2.0, "GBPUSD": 2.0,
    "AUDJPY": 2.0, "XAUUSD": 3.0,
    "EURUSD": 2.0,
}

SWING_LB        = 5
SWING_MIN_DIST  = 5
RETEST_TOL      = 0.0008
RETEST_WINDOW   = 5
TREND_N         = 3
BREAK_WINDOW    = 20
CANDLES_NEEDED  = 200
MAX_HISTORY     = 1344
ATR_PERIOD      = 14
ATR_MULT = {
    "USDJPY": 0.25, "GBPUSD": 0.25,
    "AUDJPY": 0.25, "XAUUSD": 0.75,
    "EURUSD": 0.25,
}

# ── HTF Config ────────────────────────────────────────────────────────────────
H4_FETCH_SIZE  = 130       # 120 lookback + 10 buffer
H4_HOURS       = {0, 4, 8, 12, 16, 20}
H4_EMA_PERIOD  = 20        # EMA period for HTF trend detection
BATCH_SIZE     = 8         # Twelve Data max symbols per batch call

# ── State Files ───────────────────────────────────────────────────────────────
STATE_FILE        = Path("state/price_history.json")
LIVE_SIGNALS_FILE = Path("state/live_signals.json")
SIGNALS_LOG       = Path("state/signals_log.csv")
SWEEP_FVG_LOG     = Path("state/sweep_fvg_log.csv")


# ── STATE ─────────────────────────────────────────────────────────────────────
def load_state() -> dict:
    STATE_FILE.parent.mkdir(exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "candle_history": {p: [] for p in ALL_PAIRS},
        "active_setups" : {},
        "fired_signals" : [],
        "htf_cache"     : {},
    }

def save_state(state: dict):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def load_live_signals() -> dict:
    if LIVE_SIGNALS_FILE.exists():
        try:
            return json.loads(LIVE_SIGNALS_FILE.read_text())
        except Exception:
            return {}
    return {}

def save_live_signals(live: dict):
    LIVE_SIGNALS_FILE.parent.mkdir(exist_ok=True)
    LIVE_SIGNALS_FILE.write_text(json.dumps(live, indent=2))

def merge_candles(stored: list, fetched: list,
                  max_size: int = MAX_HISTORY) -> list:
    times = {c["time"] for c in stored}
    for c in fetched:
        if c["time"] not in times:
            stored.append(c)
            times.add(c["time"])
    stored.sort(key=lambda c: c["time"])
    return stored[-max_size:]


# ── FETCH ─────────────────────────────────────────────────────────────────────
def _parse_td_response(data: dict, pair: str) -> list:
    """Parse a single pair's response from Twelve Data into candle list."""
    if data.get("status") == "error":
        logger.error(f"[{pair}] API error: {data.get('message')}")
        return []
    return [
        {
            "time" : c["datetime"],
            "open" : float(c["open"]),
            "high" : float(c["high"]),
            "low"  : float(c["low"]),
            "close": float(c["close"]),
        }
        for c in reversed(data.get("values", []))
    ]

def fetch_candles_batch(pairs: list,
                        outputsize: int = 50) -> dict:
    """
    Fetch M15 candles for multiple pairs in one API call.
    Twelve Data supports up to 8 symbols per batch request.

    Returns dict: {pair: [candles]}
    Processes pairs in chunks of BATCH_SIZE to stay within limits.
    """
    results = {}
    chunks  = [
        pairs[i:i + BATCH_SIZE]
        for i in range(0, len(pairs), BATCH_SIZE)
    ]

    for chunk in chunks:
        symbols = ','.join(TD_SYMBOLS[p] for p in chunk)
        url     = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol={symbols}&interval=15min"
            f"&outputsize={outputsize}&apikey={TWELVE_DATA_KEY}"
        )
        try:
            res  = requests.get(url, timeout=20)
            data = res.json()

            # Single pair response — data IS the pair response
            if len(chunk) == 1:
                pair = chunk[0]
                results[pair] = _parse_td_response(data, pair)
            else:
                # Multi-pair response — data is keyed by symbol
                for pair in chunk:
                    sym        = TD_SYMBOLS[pair]
                    pair_data  = data.get(sym, {})
                    results[pair] = _parse_td_response(pair_data, pair)

            logger.info(
                f"[BATCH M15] {chunk} — "
                f"{sum(len(v) for v in results.values() if isinstance(v, list))} candles"
            )

        except Exception as e:
            logger.error(f"[BATCH M15] Fetch error {chunk}: {e}")
            for pair in chunk:
                results[pair] = []

        # Stagger between chunks
        if len(chunks) > 1:
            time.sleep(5)

    return results

def fetch_h4_batch(pairs: list) -> dict:
    """
    Fetch H4 candles for multiple pairs in one API call.
    Returns dict: {pair: [candles]}
    """
    results = {}
    chunks  = [
        pairs[i:i + BATCH_SIZE]
        for i in range(0, len(pairs), BATCH_SIZE)
    ]

    for chunk in chunks:
        symbols = ','.join(TD_SYMBOLS[p] for p in chunk)
        url     = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol={symbols}&interval=4h"
            f"&outputsize={H4_FETCH_SIZE}&apikey={TWELVE_DATA_KEY}"
        )
        try:
            res  = requests.get(url, timeout=20)
            data = res.json()

            if len(chunk) == 1:
                pair = chunk[0]
                results[pair] = _parse_td_response(data, pair)
            else:
                for pair in chunk:
                    sym        = TD_SYMBOLS[pair]
                    pair_data  = data.get(sym, {})
                    results[pair] = _parse_td_response(pair_data, pair)

            logger.info(
                f"[BATCH H4] {chunk} — "
                f"{sum(len(v) for v in results.values() if isinstance(v, list))} candles"
            )

        except Exception as e:
            logger.error(f"[BATCH H4] Fetch error {chunk}: {e}")
            for pair in chunk:
                results[pair] = []

        if len(chunks) > 1:
            time.sleep(5)

    return results


# ── HTF CACHE ─────────────────────────────────────────────────────────────────
def _ensure_cache(pair: str, cache: dict):
    if pair not in cache:
        cache[pair] = {
            "h4": {
                "candles"     : [],
                "fetched_hour": -1,
                "ema20"       : None,
            }
        }

def _compute_ema(candles: list, period: int = 20) -> float | None:
    """
    Compute EMA of closing prices.
    Uses standard EMA formula with SMA as seed.
    Returns last EMA value or None if insufficient data.
    """
    closes = [c['close'] for c in candles]
    if len(closes) < period:
        return None
    k   = 2 / (period + 1)
    ema = sum(closes[:period]) / period   # SMA seed
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 5)

def _derive_htf_trend(candles: list, ema20: float | None) -> str:
    """
    Derive H4 trend from last close vs EMA20.
    Returns 'bullish', 'bearish', or 'neutral'.
    """
    if not candles or ema20 is None:
        return 'neutral'
    last_close = candles[-1]['close']
    if last_close > ema20:
        return 'bullish'
    elif last_close < ema20:
        return 'bearish'
    return 'neutral'

def should_refresh_h4(now: datetime, entry: dict) -> bool:
    return (
        now.hour in H4_HOURS and
        entry.get("fetched_hour") != now.hour
    )

def refresh_h4_cache(cache: dict, now: datetime):
    """
    Refresh H4 cache for all sweep pairs that need it.
    Uses batch fetch — all pairs in one or two API calls.
    Computes EMA20 and stores htf_trend after fetch.
    """
    needs = [
        p for p in SWEEP_PAIRS
        if should_refresh_h4(
            now,
            cache.get(p, {}).get("h4", {"fetched_hour": -1})
        )
    ]
    if not needs:
        return

    logger.info(f"[H4 BATCH] Refreshing: {needs}")

    batch = fetch_h4_batch(needs)

    for pair in needs:
        _ensure_cache(pair, cache)
        raw = batch.get(pair, [])
        if raw:
            ema20 = _compute_ema(raw, H4_EMA_PERIOD)
            trend = _derive_htf_trend(raw, ema20)
            cache[pair]["h4"]["candles"]      = raw
            cache[pair]["h4"]["fetched_hour"] = now.hour
            cache[pair]["h4"]["ema20"]        = ema20
            cache[pair]["h4"]["htf_trend"]    = trend
            logger.info(
                f"  [{pair}] H4={len(raw)} "
                f"EMA20={ema20} trend={trend}"
            )
        else:
            logger.warning(f"  [{pair}] H4 batch failed — using cache")


# ── CANDLE PREP ───────────────────────────────────────────────────────────────
def prepare_candles(raw: list,
                    now_utc: datetime = None) -> list:
    """
    Convert raw candle dicts to typed, time-filtered list.
    Filters out future candles (incomplete candle guard).
    """
    now      = now_utc or datetime.now(timezone.utc)
    prepared = []
    for c in raw:
        try:
            t = datetime.strptime(
                c["time"], "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
            if t > now:
                continue
            prepared.append({
                "time" : t,
                "open" : float(c["open"]),
                "high" : float(c["high"]),
                "low"  : float(c["low"]),
                "close": float(c["close"]),
            })
        except Exception:
            continue
    return prepared


# ── ATR (B&R) ─────────────────────────────────────────────────────────────────
def calc_atr(candles: list, idx: int,
             period: int = ATR_PERIOD) -> float:
    start = max(1, idx - period + 1)
    trs   = []
    for j in range(start, idx + 1):
        h  = candles[j]["high"]
        l  = candles[j]["low"]
        pc = candles[j-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(sum(trs) / len(trs)) if trs else 0.0


# ── B&R STRATEGY ──────────────────────────────────────────────────────────────
def _parse_breakout_time(bt) -> datetime:
    """
    Safely parse breakout_time to datetime.
    State is persisted as JSON (strings) and loaded back,
    so breakout_time may be a str or datetime depending on
    whether it came from a fresh run or loaded state.
    """
    if isinstance(bt, datetime):
        return bt
    try:
        # Candle time format from prepare_candles: already UTC-aware
        # State stores raw string from candles e.g. "2026-05-27 09:00:00"
        dt = datetime.strptime(bt, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        try:
            return datetime.fromisoformat(bt).replace(tzinfo=timezone.utc)
        except Exception:
            # Fallback — return epoch so comparison never blocks
            return datetime.fromtimestamp(0, tz=timezone.utc)
def find_swings(closes: list, candles: list,
                lb: int = SWING_LB,
                pair: str = None) -> tuple:
    sh, sl  = [], []
    pip     = PIP_SIZE.get(pair, 0.0001) if pair else 0.0001
    min_dist= SWING_MIN_DIST * pip
    for i in range(lb, len(closes) - lb):
        win = closes[i-lb:i+lb+1]
        if closes[i] == max(win):
            if sh and abs(closes[i] - closes[sh[-1]]) < min_dist:
                continue
            sh.append(i)
        if closes[i] == min(win):
            if sl and abs(closes[i] - closes[sl[-1]]) < min_dist:
                continue
            sl.append(i)
    return sh, sl

def detect_trend(closes: list, sh: list,
                 sl: list, n: int = TREND_N) -> str:
    psh = sh[-n:]; psl = sl[-n:]
    if len(psh) < 2 or len(psl) < 2:
        return "neutral"
    shv  = [closes[i] for i in psh]
    slv  = [closes[i] for i in psl]
    bull = (
        all(shv[i] < shv[i+1] for i in range(len(shv)-1)) and
        all(slv[i] < slv[i+1] for i in range(len(slv)-1))
    )
    bear = (
        all(shv[i] > shv[i+1] for i in range(len(shv)-1)) and
        all(slv[i] > slv[i+1] for i in range(len(slv)-1))
    )
    return "bullish" if bull else "bearish" if bear else "neutral"

def run_signal_check(pair: str, candles: list,
                     active_setups: dict) -> dict | None:
    """
    B&R signal detection.
    Returns signal dict if retest confirmed, else None.
    """
    if len(candles) < CANDLES_NEEDED:
        return None

    pip    = PIP_SIZE.get(pair, 0.0001)
    rr     = RR_MAP.get(pair, 2.0)
    closes = [c["close"] for c in candles]
    n      = len(closes)
    cur    = closes[-1]
    tol    = cur * RETEST_TOL
    sh, sl = find_swings(closes, candles, SWING_LB, pair=pair)
    setup  = active_setups.get(pair)
    now    = datetime.now(timezone.utc)

    # ── Active setup — check for retest ───────────────────────
    if setup:
        if n - setup.get("breakout_idx", 0) > RETEST_WINDOW:
            active_setups.pop(pair, None)
            return None

        if (setup.get("breakout_time") and
                candles[-1]["time"] <= _parse_breakout_time(setup["breakout_time"])):
            return None

        lv = setup["level"]

        if setup["dir"] == "long":
            if candles[-1]["low"] <= lv + tol and cur > lv:
                sl_p = setup["sl"]
                risk = cur - sl_p
                if risk > pip * 3:
                    tp   = cur + risk * rr
                    active_setups.pop(pair, None)
                    live = load_live_signals()
                    live[pair] = {
                        "side"   : "BUY",
                        "entry"  : cur,
                        "sl"     : sl_p,
                        "tp"     : tp,
                        "rr"     : rr,
                        "sl_pips": round(risk / pip, 1),
                        "trend"  : setup.get("trend", ""),
                        "time"   : now.isoformat(),
                    }
                    save_live_signals(live)
                    return {
                        "pair"   : pair, "side": "BUY",
                        "entry"  : cur,  "sl"  : sl_p,
                        "tp"     : tp,   "level": lv,
                        "sl_pips": round(risk / pip, 1),
                        "rr"     : rr,
                        "trend"  : setup.get("trend", ""),
                        "time"   : now.isoformat(),
                    }

        else:  # short
            if candles[-1]["high"] >= lv - tol and cur < lv:
                sl_p = setup["sl"]
                risk = sl_p - cur
                if risk > pip * 3:
                    tp   = cur - risk * rr
                    active_setups.pop(pair, None)
                    live = load_live_signals()
                    live[pair] = {
                        "side"   : "SELL",
                        "entry"  : cur,
                        "sl"     : sl_p,
                        "tp"     : tp,
                        "rr"     : rr,
                        "sl_pips": round(risk / pip, 1),
                        "trend"  : setup.get("trend", ""),
                        "time"   : now.isoformat(),
                    }
                    save_live_signals(live)
                    return {
                        "pair"   : pair, "side": "SELL",
                        "entry"  : cur,  "sl"  : sl_p,
                        "tp"     : tp,   "level": lv,
                        "sl_pips": round(risk / pip, 1),
                        "rr"     : rr,
                        "trend"  : setup.get("trend", ""),
                        "time"   : now.isoformat(),
                    }

    # ── No active setup — look for breakout ───────────────────
    if not setup:
        trend = detect_trend(closes, sh, sl)
        if trend == "bullish" and sh:
            valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
            if valid_sh:
                last_sh = valid_sh[-1]
                if ((n-1) - (last_sh + SWING_LB) <= BREAK_WINDOW
                        and cur > closes[last_sh]):
                    valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
                    if valid_sl:
                        lsi  = valid_sl[-1]
                        atr  = calc_atr(candles, lsi)
                        sl_p = closes[lsi] - ATR_MULT[pair] * atr
                        active_setups[pair] = {
                            "dir"          : "long",
                            "level"        : closes[last_sh],
                            "sl"           : sl_p,
                            "breakout_idx" : n - 1,
                            "breakout_time": candles[-1]["time"].isoformat()
                                             if isinstance(candles[-1]["time"], datetime)
                                             else candles[-1]["time"],
                            "trend"        : trend,
                            "created"      : now.isoformat(),
                        }

        elif trend == "bearish" and sl:
            valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
            if valid_sl:
                last_sl = valid_sl[-1]
                if ((n-1) - (last_sl + SWING_LB) <= BREAK_WINDOW
                        and cur < closes[last_sl]):
                    valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
                    if valid_sh:
                        shi  = valid_sh[-1]
                        atr  = calc_atr(candles, shi)
                        sl_p = closes[shi] + ATR_MULT[pair] * atr
                        active_setups[pair] = {
                            "dir"          : "short",
                            "level"        : closes[last_sl],
                            "sl"           : sl_p,
                            "breakout_idx" : n - 1,
                            "breakout_time": candles[-1]["time"].isoformat()
                                             if isinstance(candles[-1]["time"], datetime)
                                             else candles[-1]["time"],
                            "trend"        : trend,
                            "created"      : now.isoformat(),
                        }

    return None


# ── DUPLICATE GUARD (B&R) ─────────────────────────────────────────────────────
def is_duplicate(sig: dict, fired: list) -> bool:
    """4-hour dedup window for B&R signals."""
    sig_id = (
        f"{sig['pair']}_{sig['side']}_"
        f"{round(sig['level'], 4)}"
    )
    now    = datetime.now(timezone.utc)
    recent = [
        s for s in fired
        if (now - datetime.fromisoformat(s["time"])
            ).total_seconds() < 14400
    ]
    fired.clear()
    fired.extend(recent)
    if any(s.get("id") == sig_id for s in recent):
        return True
    fired.append({"id": sig_id, "time": now.isoformat()})
    return False


# ── SESSION HELPER ────────────────────────────────────────────────────────────
def _get_session(dt_str: str) -> str:
    try:
        dt = datetime.fromisoformat(
            dt_str.replace(" UTC", "+00:00")
        )
        h  = dt.hour
        if 0  <= h < 6:  return 'Asian'
        if 6  <= h < 12: return 'London'
        if 12 <= h < 17: return 'NewYork'
        return 'London'
    except Exception:
        return 'Unknown'


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def _tg(msg: str, parse_mode: str = "Markdown"):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={
                "chat_id"   : TG_CHAT_ID,
                "text"      : msg,
                "parse_mode": parse_mode,
            },
            timeout=10,
        )
    except Exception as e:
        logger.error(f"[TG] Error: {e}")

def send_telegram_br(sig: dict):
    """B&R signal alert — distinct signature."""
    pair  = sig["pair"]
    dp    = DP.get(pair, 5)
    arrow = "🟢" if sig["side"] == "BUY" else "🔴"
    t_icon= (
        "↗️" if sig.get("trend") == "bullish"
        else "↘️" if sig.get("trend") == "bearish"
        else "➡️"
    )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    _tg(
        f"{arrow} *EDGE SIGNAL — {sig['side']} {pair}*\n\n"
        f"📍 Entry : `{sig['entry']:.{dp}f}`\n"
        f"🛑 SL    : `{sig['sl']:.{dp}f}`\n"
        f"🎯 TP    : `{sig['tp']:.{dp}f}`\n\n"
        f"RR: `1:{sig['rr']}`  |  "
        f"Risk: `{sig['sl_pips']:.1f} pips`\n"
        f"{t_icon} Trend: {sig.get('trend','').capitalize()}\n"
        f"🕐 {now}\n\n"
        f"_⚡ EDGE Signal Engine — Break & Retest_"
    )
    logger.info(f"[B&R] Alert sent — {pair} {sig['side']}")

def send_telegram_sweep_s1(sig: dict):
    """Sweep+FVG Stage 1 alert — message pre-built by SignalBuilder."""
    _tg(sig["message"])
    logger.info(
        f"[SWEEP S1] Alert sent — "
        f"{sig['direction'].upper()} {sig['pair']}"
    )


# ── SHEET ─────────────────────────────────────────────────────────────────────
def _get_sheet(tab: str = None):
    """Authenticate and return worksheet."""
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    wb = gspread.authorize(creds).open_by_key(SHEET_ID)
    return wb.worksheet(tab) if tab else wb.sheet1

def log_br_to_sheet(sig: dict):
    """
    Write B&R signal to Sheet1 cols A-Q.
    Replit fills cols I-P on resolution.
    """
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        _get_sheet().append_row([
            sig["time"],               # A signal_time
            sig["pair"],               # B pair
            sig["side"],               # C side
            sig["entry"],              # D entry
            sig["sl"],                 # E sl
            sig["tp"],                 # F tp
            sig["rr"],                 # G rr
            sig["sl_pips"],            # H sl_pips
            "",                        # I outcome      ← Replit fills
            "",                        # J close_price  ← Replit fills
            "",                        # K close_time   ← Replit fills
            sig.get("trend", ""),      # L trend
            "",                        # M entry_time   ← Replit fills
            "",                        # N bars_to_outcome
            "",                        # O mae_pips     ← Replit fills
            "",                        # P mfe_pips     ← Replit fills
            _get_session(sig["time"]), # Q session
        ])
        logger.info(f"[SHEET] B&R logged — {sig['pair']}")
    except Exception as e:
        logger.error(f"[SHEET] B&R error: {e}")

def log_sweep_fvg_to_sheet(sig: dict):
    """
    Write Sweep+FVG Stage 1 to SweepFVG tab.
    32 columns — includes all analytics fields.

    Cols 1-22 : existing fields
    Cols 23-32: new analytics fields
      23 sweep_body_pct
      24 sweep_wick_ratio
      25 n_candles_in_zone
      26 zone_age_h4_bars
      27 distance_to_cluster_pips
      28 htf_trend
      29 fvg_size_atr_mult
      30 sweep_to_fvg_bars
      31 mss_level
      32 mss_candle_time
    """
    try:
        sheet = _get_sheet("SweepFVG")
        sheet.append_row([
            sig['fired_at'],                      # 1  fired_at
            sig['pair'],                          # 2  pair
            sig['direction'].upper(),             # 3  direction
            sig['entry'],                         # 4  entry
            sig['sl'],                            # 5  sl
            sig['tp1'],                           # 6  tp1
            sig.get('tp2', ''),                   # 7  tp2
            sig['rr_tp1'],                        # 8  rr_tp1
            sig.get('rr_tp2', ''),                # 9  rr_tp2
            sig['sl_pips'],                       # 10 sl_pips
            sig.get('zone_src', ''),              # 11 zone_src
            sig['session'],                       # 12 session
            sig['sweep_time'],                    # 13 sweep_time
            'PENDING_ENTRY',                      # 14 status
            '',                                   # 15 entry_time  ← Replit
            '',                                   # 16 tp1_outcome ← Replit
            '',                                   # 17 tp2_outcome ← Replit
            '',                                   # 18 pnl_r       ← Replit
            sig['fvg_top'],                       # 19 fvg_top
            sig['fvg_bottom'],                    # 20 fvg_bottom
            sig.get('tp_mode', ''),               # 21 tp_mode
            sig.get('quality', ''),               # 22 quality
            sig.get('sweep_body_pct', ''),        # 23 sweep_body_pct
            sig.get('sweep_wick_ratio', ''),      # 24 sweep_wick_ratio
            sig.get('n_candles_in_zone', ''),     # 25 n_candles_in_zone
            sig.get('zone_age_h4_bars', ''),      # 26 zone_age_h4_bars
            sig.get('distance_to_cluster_pips',''),# 27 distance_to_cluster_pips
            sig.get('htf_trend', ''),             # 28 htf_trend
            sig.get('fvg_size_atr_mult', ''),     # 29 fvg_size_atr_mult
            sig.get('sweep_to_fvg_bars', ''),     # 30 sweep_to_fvg_bars
            sig.get('mss_level', ''),             # 31 mss_level
            sig.get('mss_candle_time', ''),       # 32 mss_candle_time
        ])
        logger.info(f"[SWEEP] Sheet logged — {sig['pair']}")
    except Exception as e:
        logger.error(f"[SWEEP] Sheet error: {e}")

def log_br_signal_csv(sig: dict):
    """Append B&R signal to local CSV log."""
    SIGNALS_LOG.parent.mkdir(exist_ok=True)
    hdr = not SIGNALS_LOG.exists()
    with open(SIGNALS_LOG, "a") as f:
        if hdr:
            f.write(
                "time,pair,side,entry,sl,tp,"
                "sl_pips,rr,level,trend\n"
            )
        f.write(
            f"{sig['time']},{sig['pair']},{sig['side']},"
            f"{sig['entry']},{sig['sl']},{sig['tp']},"
            f"{sig['sl_pips']},{sig['rr']},"
            f"{sig['level']},{sig.get('trend','')}\n"
        )

def log_sweep_fvg_csv(sig: dict):
    """Append Sweep+FVG signal to local CSV log."""
    SWEEP_FVG_LOG.parent.mkdir(exist_ok=True)
    hdr = not SWEEP_FVG_LOG.exists()
    with open(SWEEP_FVG_LOG, "a") as f:
        if hdr:
            f.write(
                "fired_at,pair,direction,entry,sl,tp1,tp2,"
                "rr_tp1,rr_tp2,sl_pips,zone_src,session,"
                "sweep_time,fvg_top,fvg_bottom,tp_mode,quality\n"
            )
        f.write(
            f"{sig['fired_at']},{sig['pair']},{sig['direction']},"
            f"{sig['entry']},{sig['sl']},{sig['tp1']},"
            f"{sig.get('tp2','')},"
            f"{sig['rr_tp1']},{sig.get('rr_tp2','')},"
            f"{sig['sl_pips']},{sig.get('zone_src','')},"
            f"{sig['session']},{sig['sweep_time']},"
            f"{sig['fvg_top']},{sig['fvg_bottom']},"
            f"{sig.get('tp_mode','')},"
            f"{sig.get('quality','')}\n"
        )


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now_utc = datetime.now(timezone.utc)
    logger.info(f"\n{'='*55}")
    logger.info(
        f"  EDGE Signal Engine v7 — "
        f"{now_utc.strftime('%Y-%m-%d %H:%M UTC')}"
    )
    logger.info(f"{'='*55}")

    if not TWELVE_DATA_KEY:
        logger.error("TWELVE_DATA_KEY not set")
        return

    state          = load_state()
    candle_history = state["candle_history"]
    active_setups  = state["active_setups"]
    fired_signals  = state["fired_signals"]
    htf_cache      = state.get("htf_cache", {})

    # ── Cold start guard ──────────────────────────────────────
    is_cold = not any(
        candle_history.get(p) for p in ALL_PAIRS
    )
    if is_cold:
        logger.info(
            "[COLD START] Fresh state — "
            "initialising cache via batch fetch"
        )
        # Batch fetch on cold start
        batch = fetch_candles_batch(ALL_PAIRS, outputsize=50)
        for pair, raw in batch.items():
            if raw:
                candle_history.setdefault(pair, [])
                candle_history[pair] = merge_candles(
                    candle_history[pair], raw
                )
                logger.info(
                    f"  {pair}: "
                    f"{len(candle_history[pair])} candles cached"
                )
        state["candle_history"] = candle_history
        state["htf_cache"]      = htf_cache
        save_state(state)
        logger.info("[COLD START] Done. Signals fire from next run.")
        return

    # ── [1] Batch fetch M15 — all pairs ──────────────────────
    logger.info("\n[1] Batch fetching M15 candles...")
    batch = fetch_candles_batch(ALL_PAIRS, outputsize=50)

    for pair, raw in batch.items():
        if not raw:
            continue
        candle_history.setdefault(pair, [])
        candle_history[pair] = merge_candles(
            candle_history[pair], raw
        )
        latest = candle_history[pair][-1]
        logger.info(
            f"  {pair}: {len(candle_history[pair])} candles | "
            f"close={latest['close']:.{DP[pair]}f}"
        )

    # ── [2] H4 batch refresh ──────────────────────────────────
    h4_needed = now_utc.hour in H4_HOURS and any(
        should_refresh_h4(
            now_utc,
            htf_cache.get(p, {}).get("h4", {"fetched_hour": -1})
        )
        for p in SWEEP_PAIRS
    )

    if h4_needed:
        logger.info("\n[2] H4 batch refresh...")
        for p in SWEEP_PAIRS:
            _ensure_cache(p, htf_cache)
        refresh_h4_cache(htf_cache, now_utc)
    else:
        logger.info("\n[2] H4 cache OK")

    # ── [3] B&R scan ──────────────────────────────────────────
    logger.info("\n[3] B&R scan...")
    br_signals = []
    for pair in BR_PAIRS:
        raw = candle_history.get(pair, [])
        if not raw:
            continue
        m15 = prepare_candles(raw, now_utc)
        if len(m15) < CANDLES_NEEDED:
            logger.warning(
                f"  [{pair}] Insufficient M15 ({len(m15)})"
            )
            continue
        sig = run_signal_check(pair, m15, active_setups)
        if sig and not is_duplicate(sig, fired_signals):
            br_signals.append(sig)
            logger.info(
                f"  [{pair}] 🔔 B&R {sig['side']} "
                f"@ {sig['entry']:.{DP[pair]}f}"
            )

    # ── [4] Sweep+FVG scan — Stage 1 only ────────────────────
    sweep_signals = []
    if SWEEP_FVG_ENABLED:
        logger.info(
            f"\n[4] Sweep+FVG scan ({len(SWEEP_PAIRS)} pairs)..."
        )

        cleanup_expired_pending()

        for pair in SWEEP_PAIRS:
            raw = candle_history.get(pair, [])
            if not raw:
                continue

            m15 = prepare_candles(raw, now_utc)
            if len(m15) < 30:
                logger.warning(
                    f"  [{pair}] Insufficient M15 ({len(m15)})"
                )
                continue

            h4_raw = (
                htf_cache.get(pair, {})
                         .get("h4", {})
                         .get("candles", [])
            )
            h4 = prepare_candles(h4_raw, now_utc) if h4_raw else []

            if len(h4) < 40:
                logger.warning(
                    f"  [{pair}] Insufficient H4 ({len(h4)}) — skipping"
                )
                continue

            # Read HTF trend from cache
            htf_trend = (
                htf_cache.get(pair, {})
                         .get("h4", {})
                         .get("htf_trend", 'neutral')
            )

            logger.info(
                f"  [{pair}] M15={len(m15)} H4={len(h4)} "
                f"trend={htf_trend}"
            )

            sigs = sweep_fvg_scan(
                m15_candles=m15,
                h4_candles=h4,
                pair=pair,
                htf_trend=htf_trend,
            )
            if sigs:
                logger.info(
                    f"  [{pair}] 🔔 {len(sigs)} signal(s)"
                )
                sweep_signals.extend(sigs)

    # ── [5] Send alerts + log ─────────────────────────────────
    logger.info("\n[5] Sending alerts...")

    for sig in br_signals:
        send_telegram_br(sig)
        log_br_to_sheet(sig)
        log_br_signal_csv(sig)

    for sig in sweep_signals:
        send_telegram_sweep_s1(sig)
        log_sweep_fvg_to_sheet(sig)
        log_sweep_fvg_csv(sig)

    if not br_signals and not sweep_signals:
        logger.info("  No new signals this run")

    # ── [6] Persist state ─────────────────────────────────────
    state["candle_history"] = candle_history
    state["active_setups"]  = active_setups
    state["fired_signals"]  = fired_signals
    state["htf_cache"]      = htf_cache
    save_state(state)

    logger.info(f"\n{'='*55}")
    logger.info(f"  B&R signals      : {len(br_signals)}")
    logger.info(f"  Sweep+FVG signals: {len(sweep_signals)}")
    logger.info(f"{'='*55}\n")


if __name__ == "__main__":
    main()

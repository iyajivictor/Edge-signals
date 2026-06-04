"""
EDGE Signal Engine — v8
========================
Strategy:
  H4 Zone Sweep + M15 FVG — 10 pairs, Stage 1 only

B&R removed entirely.

Lifecycle:
  signal_engine → Stage 1 only → Telegram + SweepFVG sheet
  Replit        → Phase 0,1,2 → full lifecycle

Changes vs v7:
  · B&R strategy fully removed
  · D1 cache added — fetched once/day at 00:00 UTC
    provides d1_trend_state, distance_to_d1_level_r,
    weekly_range_position_pct for each signal
  · 5 new analytics fields computed at signal time:
      d1_trend_state          — D1 EMA20 bias (bullish/bearish/ranging)
      distance_to_d1_level_r  — nearest D1 swing distance in R-multiples
      weekly_range_pct        — price position in current weekly range (0-100)
      day_of_week             — 1=Mon … 5=Fri
      mins_from_session_open  — minutes since London or NY open
  · SweepFVG sheet expanded to 37 cols (was 32)
  · ALL_PAIRS = SWEEP_PAIRS (B&R pairs were a subset)
  · state: active_setups + fired_signals keys removed
  · API budget: +2 calls/day (D1 batch) — negligible
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
SWEEP_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDJPY", "XAUUSD",
    "CADJPY", "USDCAD", "EURJPY", "GBPJPY", "GBPAUD",
]

ALL_PAIRS = SWEEP_PAIRS

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

# ── HTF Config ────────────────────────────────────────────────────────────────
H4_FETCH_SIZE  = 130
H4_HOURS       = {0, 4, 8, 12, 16, 20}
H4_EMA_PERIOD  = 20
D1_FETCH_SIZE  = 30        # 30 D1 candles — enough for EMA20 + weekly range
D1_REFRESH_HOUR = 0        # refresh once per day at 00:00 UTC
BATCH_SIZE     = 8

# ── State Files ───────────────────────────────────────────────────────────────
STATE_FILE     = Path("state/price_history.json")
SWEEP_FVG_LOG  = Path("state/sweep_fvg_log.csv")

MAX_HISTORY    = 1344


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
        "htf_cache"     : {},
    }

def save_state(state: dict):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

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

def _batch_fetch(pairs: list, interval: str,
                 outputsize: int) -> dict:
    """Generic batch fetcher — M15, H4, or D1."""
    results = {}
    chunks  = [pairs[i:i+BATCH_SIZE] for i in range(0, len(pairs), BATCH_SIZE)]

    for chunk in chunks:
        symbols = ','.join(TD_SYMBOLS[p] for p in chunk)
        url = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol={symbols}&interval={interval}"
            f"&outputsize={outputsize}&apikey={TWELVE_DATA_KEY}"
        )
        try:
            res  = requests.get(url, timeout=20)
            data = res.json()
            if len(chunk) == 1:
                results[chunk[0]] = _parse_td_response(data, chunk[0])
            else:
                for pair in chunk:
                    results[pair] = _parse_td_response(
                        data.get(TD_SYMBOLS[pair], {}), pair
                    )
            logger.info(
                f"[BATCH {interval.upper()}] {chunk} — "
                f"{sum(len(v) for v in results.values() if isinstance(v, list))} candles"
            )
        except Exception as e:
            logger.error(f"[BATCH {interval.upper()}] {chunk}: {e}")
            for pair in chunk:
                results[pair] = []
        if len(chunks) > 1:
            time.sleep(5)

    return results

def fetch_candles_batch(pairs: list, outputsize: int = 50) -> dict:
    return _batch_fetch(pairs, "15min", outputsize)

def fetch_h4_batch(pairs: list) -> dict:
    return _batch_fetch(pairs, "4h", H4_FETCH_SIZE)

def fetch_d1_batch(pairs: list) -> dict:
    return _batch_fetch(pairs, "1day", D1_FETCH_SIZE)


# ── HTF CACHE ─────────────────────────────────────────────────────────────────
def _ensure_cache(pair: str, cache: dict):
    if pair not in cache:
        cache[pair] = {
            "h4": {"candles": [], "fetched_hour": -1, "ema20": None},
            "d1": {"candles": [], "fetched_day":  -1, "ema20": None},
        }
    # Back-fill d1 key for older state files
    if "d1" not in cache[pair]:
        cache[pair]["d1"] = {"candles": [], "fetched_day": -1, "ema20": None}

def _compute_ema(candles: list, period: int = 20) -> float | None:
    closes = [c['close'] for c in candles]
    if len(closes) < period:
        return None
    k   = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 5)

def _derive_htf_trend(candles: list, ema20: float | None) -> str:
    if not candles or ema20 is None:
        return 'neutral'
    last_close = candles[-1]['close']
    if last_close > ema20:   return 'bullish'
    elif last_close < ema20: return 'bearish'
    return 'neutral'

def should_refresh_h4(now: datetime, entry: dict) -> bool:
    return now.hour in H4_HOURS and entry.get("fetched_hour") != now.hour

def should_refresh_d1(now: datetime, entry: dict) -> bool:
    return (
        now.hour == D1_REFRESH_HOUR and
        entry.get("fetched_day") != now.day
    )

def refresh_h4_cache(cache: dict, now: datetime):
    needs = [
        p for p in SWEEP_PAIRS
        if should_refresh_h4(now, cache.get(p, {}).get("h4", {"fetched_hour": -1}))
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
            cache[pair]["h4"].update({
                "candles"     : raw,
                "fetched_hour": now.hour,
                "ema20"       : ema20,
                "htf_trend"   : trend,
            })
            logger.info(f"  [{pair}] H4={len(raw)} EMA20={ema20} trend={trend}")
        else:
            logger.warning(f"  [{pair}] H4 batch failed — using cache")

def refresh_d1_cache(cache: dict, now: datetime):
    """
    Refresh D1 cache once per day at 00:00 UTC.
    Provides: d1_trend_state, weekly range, D1 swing levels.
    Cost: 2 API calls/day for 10 pairs (same batch structure as H4).
    """
    needs = [
        p for p in SWEEP_PAIRS
        if should_refresh_d1(now, cache.get(p, {}).get("d1", {"fetched_day": -1}))
    ]
    if not needs:
        return
    logger.info(f"[D1 BATCH] Refreshing: {needs}")
    batch = fetch_d1_batch(needs)
    for pair in needs:
        _ensure_cache(pair, cache)
        raw = batch.get(pair, [])
        if raw:
            ema20 = _compute_ema(raw, 20)
            trend = _derive_htf_trend(raw, ema20)
            # Compute D1 swing highs/lows (last 10 D1 bars)
            swing_highs = sorted(
                [c['high'] for c in raw[-10:]], reverse=True
            )
            swing_lows  = sorted(
                [c['low']  for c in raw[-10:]]
            )
            # Weekly range from last 7 D1 candles
            week = raw[-7:]
            weekly_high = max(c['high'] for c in week) if week else None
            weekly_low  = min(c['low']  for c in week) if week else None
            cache[pair]["d1"].update({
                "candles"     : raw,
                "fetched_day" : now.day,
                "ema20"       : ema20,
                "d1_trend"    : trend,
                "swing_highs" : swing_highs,
                "swing_lows"  : swing_lows,
                "weekly_high" : weekly_high,
                "weekly_low"  : weekly_low,
            })
            logger.info(
                f"  [{pair}] D1={len(raw)} EMA20={ema20} "
                f"trend={trend} wk={weekly_low}-{weekly_high}"
            )
        else:
            logger.warning(f"  [{pair}] D1 batch failed — using cache")


# ── SIGNAL ENRICHMENT ─────────────────────────────────────────────────────────
def _compute_signal_context(sig: dict, pair: str,
                             d1_cache: dict,
                             now_utc: datetime) -> dict:
    """
    Compute the 5 new analytics fields at signal fire time.
    All derived from D1 cache + datetime — zero extra API calls.

    Fields added:
      d1_trend_state         — bullish / bearish / ranging
      distance_to_d1_level_r — nearest D1 swing in R-multiples
      weekly_range_pct       — price position in weekly range (0-100)
      day_of_week            — 1=Mon … 5=Fri (0=Sun, 6=Sat)
      mins_from_session_open — minutes since London (06:00) or NY (12:00) open
    """
    d1  = d1_cache.get(pair, {})
    ctx = {}

    # ── D1 trend state ────────────────────────────────────────
    ctx['d1_trend_state'] = d1.get('d1_trend', '')

    # ── Distance to nearest D1 level in R-multiples ───────────
    entry    = float(sig.get('entry', 0))
    sl       = float(sig.get('sl', 0))
    sl_dist  = abs(entry - sl)
    direction = sig.get('direction', '')

    nearest_dist_r = ''
    if sl_dist > 0:
        if direction == 'long':
            # Nearest D1 swing high above entry (resistance)
            highs_above = [h for h in d1.get('swing_highs', []) if h > entry]
            if highs_above:
                nearest = min(highs_above)
                nearest_dist_r = round(abs(nearest - entry) / sl_dist, 2)
        else:
            # Nearest D1 swing low below entry (support)
            lows_below = [l for l in d1.get('swing_lows', []) if l < entry]
            if lows_below:
                nearest = max(lows_below)
                nearest_dist_r = round(abs(entry - nearest) / sl_dist, 2)
    ctx['distance_to_d1_level_r'] = nearest_dist_r

    # ── Weekly range position (0-100%) ───────────────────────
    wh = d1.get('weekly_high')
    wl = d1.get('weekly_low')
    if wh and wl and wh != wl:
        price = float(sig.get('entry', 0))
        ctx['weekly_range_pct'] = round(
            (price - wl) / (wh - wl) * 100, 1
        )
    else:
        ctx['weekly_range_pct'] = ''

    # ── Day of week (1=Mon … 5=Fri) ──────────────────────────
    ctx['day_of_week'] = now_utc.isoweekday()  # 1=Mon, 7=Sun

    # ── Minutes from session open ─────────────────────────────
    h = now_utc.hour
    m = now_utc.minute
    mins_now = h * 60 + m
    london_open = 6 * 60    # 06:00 UTC
    ny_open     = 12 * 60   # 12:00 UTC
    if mins_now >= ny_open:
        ctx['mins_from_session_open'] = mins_now - ny_open
    elif mins_now >= london_open:
        ctx['mins_from_session_open'] = mins_now - london_open
    else:
        ctx['mins_from_session_open'] = ''   # Asian session — no major open

    return ctx


# ── CANDLE PREP ───────────────────────────────────────────────────────────────
def prepare_candles(raw: list,
                    now_utc: datetime = None) -> list:
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


# ── SESSION HELPER ────────────────────────────────────────────────────────────
def _get_session(dt_str: str) -> str:
    try:
        dt = datetime.fromisoformat(dt_str.replace(" UTC", "+00:00"))
        h  = dt.hour
        if 0  <= h < 6:  return 'Asian'
        if 6  <= h < 12: return 'London'
        if 12 <= h < 17: return 'NewYork'
        return 'Other'
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

def send_telegram_sweep_s1(sig: dict):
    _tg(sig["message"])
    logger.info(
        f"[SWEEP S1] Alert sent — "
        f"{sig['direction'].upper()} {sig['pair']}"
    )


# ── SHEET ─────────────────────────────────────────────────────────────────────
def _get_sheet(tab: str = None):
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    wb = gspread.authorize(creds).open_by_key(SHEET_ID)
    return wb.worksheet(tab) if tab else wb.sheet1

def log_sweep_fvg_to_sheet(sig: dict):
    """
    Write Sweep+FVG Stage 1 to SweepFVG tab.
    37 columns total.

    Cols 1–22 : existing fields (unchanged)
    Cols 23–32: sweep analytics (existing, now populated)
    Cols 33–37: new context fields
      33 d1_trend_state
      34 distance_to_d1_level_r
      35 weekly_range_pct
      36 day_of_week
      37 mins_from_session_open
    """
    try:
        sheet = _get_sheet("SweepFVG")
        sheet.append_row([
            sig['fired_at'],                        # 1  fired_at
            sig['pair'],                            # 2  pair
            sig['direction'].upper(),               # 3  direction
            sig['entry'],                           # 4  entry
            sig['sl'],                              # 5  sl
            sig['tp1'],                             # 6  tp1
            sig.get('tp2', ''),                     # 7  tp2
            sig['rr_tp1'],                          # 8  rr_tp1
            sig.get('rr_tp2', ''),                  # 9  rr_tp2
            sig['sl_pips'],                         # 10 sl_pips
            sig.get('zone_src', ''),                # 11 zone_src
            sig['session'],                         # 12 session
            sig['sweep_time'],                      # 13 sweep_time
            'PENDING_ENTRY',                        # 14 status
            '',                                     # 15 entry_time       ← Replit
            '',                                     # 16 tp1_outcome      ← Replit
            '',                                     # 17 tp2_outcome      ← Replit
            '',                                     # 18 pnl_r            ← Replit
            sig['fvg_top'],                         # 19 fvg_top
            sig['fvg_bottom'],                      # 20 fvg_bottom
            sig.get('tp_mode', ''),                 # 21 tp_mode
            sig.get('quality', ''),                 # 22 quality
            # ── Sweep analytics ───────────────────────────────
            sig.get('sweep_body_pct', ''),          # 23 sweep_body_pct
            sig.get('sweep_wick_ratio', ''),        # 24 sweep_wick_ratio
            sig.get('n_candles_in_zone', ''),       # 25 n_candles_in_zone
            sig.get('zone_age_h4_bars', ''),        # 26 zone_age_h4_bars
            sig.get('distance_to_cluster_pips',''), # 27 distance_to_cluster_pips
            sig.get('htf_trend', ''),               # 28 htf_trend
            sig.get('fvg_size_atr_mult', ''),       # 29 fvg_size_atr_mult
            sig.get('sweep_to_fvg_bars', ''),       # 30 sweep_to_fvg_bars
            sig.get('mss_level', ''),               # 31 mss_level
            sig.get('mss_candle_time', ''),         # 32 mss_candle_time
            # ── New context fields ────────────────────────────
            sig.get('d1_trend_state', ''),          # 33 d1_trend_state
            sig.get('distance_to_d1_level_r', ''), # 34 distance_to_d1_level_r
            sig.get('weekly_range_pct', ''),        # 35 weekly_range_pct
            sig.get('day_of_week', ''),             # 36 day_of_week
            sig.get('mins_from_session_open', ''), # 37 mins_from_session_open
        ])
        logger.info(f"[SWEEP] Sheet logged — {sig['pair']}")
    except Exception as e:
        logger.error(f"[SWEEP] Sheet error: {e}")

def log_sweep_fvg_csv(sig: dict):
    """Append Sweep+FVG signal to local CSV log."""
    SWEEP_FVG_LOG.parent.mkdir(exist_ok=True)
    hdr = not SWEEP_FVG_LOG.exists()
    with open(SWEEP_FVG_LOG, "a") as f:
        if hdr:
            f.write(
                "fired_at,pair,direction,entry,sl,tp1,tp2,"
                "rr_tp1,rr_tp2,sl_pips,zone_src,session,"
                "sweep_time,fvg_top,fvg_bottom,tp_mode,quality,"
                "sweep_body_pct,sweep_wick_ratio,n_candles_in_zone,"
                "zone_age_h4_bars,distance_to_cluster_pips,htf_trend,"
                "fvg_size_atr_mult,sweep_to_fvg_bars,mss_level,"
                "d1_trend_state,distance_to_d1_level_r,"
                "weekly_range_pct,day_of_week,mins_from_session_open\n"
            )
        f.write(
            f"{sig['fired_at']},{sig['pair']},{sig['direction']},"
            f"{sig['entry']},{sig['sl']},{sig['tp1']},"
            f"{sig.get('tp2','')},"
            f"{sig['rr_tp1']},{sig.get('rr_tp2','')},"
            f"{sig['sl_pips']},{sig.get('zone_src','')},"
            f"{sig['session']},{sig['sweep_time']},"
            f"{sig['fvg_top']},{sig['fvg_bottom']},"
            f"{sig.get('tp_mode','')},{sig.get('quality','')},"
            f"{sig.get('sweep_body_pct','')},"
            f"{sig.get('sweep_wick_ratio','')},"
            f"{sig.get('n_candles_in_zone','')},"
            f"{sig.get('zone_age_h4_bars','')},"
            f"{sig.get('distance_to_cluster_pips','')},"
            f"{sig.get('htf_trend','')},"
            f"{sig.get('fvg_size_atr_mult','')},"
            f"{sig.get('sweep_to_fvg_bars','')},"
            f"{sig.get('mss_level','')},"
            f"{sig.get('d1_trend_state','')},"
            f"{sig.get('distance_to_d1_level_r','')},"
            f"{sig.get('weekly_range_pct','')},"
            f"{sig.get('day_of_week','')},"
            f"{sig.get('mins_from_session_open','')}\n"
        )


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now_utc = datetime.now(timezone.utc)
    logger.info(f"\n{'='*55}")
    logger.info(
        f"  EDGE Signal Engine v8 — "
        f"{now_utc.strftime('%Y-%m-%d %H:%M UTC')}"
    )
    logger.info(f"{'='*55}")

    if not TWELVE_DATA_KEY:
        logger.error("TWELVE_DATA_KEY not set")
        return

    state          = load_state()
    candle_history = state["candle_history"]
    htf_cache      = state.get("htf_cache", {})

    # ── Cold start guard ──────────────────────────────────────
    is_cold = not any(candle_history.get(p) for p in ALL_PAIRS)
    if is_cold:
        logger.info("[COLD START] Fresh state — initialising via batch fetch")
        batch = fetch_candles_batch(ALL_PAIRS, outputsize=50)
        for pair, raw in batch.items():
            if raw:
                candle_history.setdefault(pair, [])
                candle_history[pair] = merge_candles(candle_history[pair], raw)
                logger.info(f"  {pair}: {len(candle_history[pair])} candles cached")
        state["candle_history"] = candle_history
        state["htf_cache"]      = htf_cache
        save_state(state)
        logger.info("[COLD START] Done. Signals fire from next run.")
        return

    # ── [1] Batch fetch M15 ───────────────────────────────────
    logger.info("\n[1] Batch fetching M15 candles...")
    batch = fetch_candles_batch(ALL_PAIRS, outputsize=50)
    for pair, raw in batch.items():
        if not raw:
            continue
        candle_history.setdefault(pair, [])
        candle_history[pair] = merge_candles(candle_history[pair], raw)
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

    # ── [3] D1 batch refresh (once/day at 00:00 UTC) ─────────
    d1_needed = now_utc.hour == D1_REFRESH_HOUR and any(
        should_refresh_d1(
            now_utc,
            htf_cache.get(p, {}).get("d1", {"fetched_day": -1})
        )
        for p in SWEEP_PAIRS
    )
    if d1_needed:
        logger.info("\n[3] D1 batch refresh...")
        for p in SWEEP_PAIRS:
            _ensure_cache(p, htf_cache)
        refresh_d1_cache(htf_cache, now_utc)
    else:
        logger.info("\n[3] D1 cache OK")

    # ── [4] Sweep+FVG scan ────────────────────────────────────
    sweep_signals = []
    if SWEEP_FVG_ENABLED:
        logger.info(f"\n[4] Sweep+FVG scan ({len(SWEEP_PAIRS)} pairs)...")
        cleanup_expired_pending()

        for pair in SWEEP_PAIRS:
            raw = candle_history.get(pair, [])
            if not raw:
                continue

            m15 = prepare_candles(raw, now_utc)
            if len(m15) < 30:
                logger.warning(f"  [{pair}] Insufficient M15 ({len(m15)})")
                continue

            h4_raw = (
                htf_cache.get(pair, {})
                         .get("h4", {})
                         .get("candles", [])
            )
            h4 = prepare_candles(h4_raw, now_utc) if h4_raw else []
            if len(h4) < 40:
                logger.warning(f"  [{pair}] Insufficient H4 ({len(h4)}) — skipping")
                continue

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
                logger.info(f"  [{pair}] 🔔 {len(sigs)} signal(s)")
                # Enrich each signal with D1 context + datetime fields
                for sig in sigs:
                    ctx = _compute_signal_context(
                        sig, pair,
                        {p: htf_cache.get(p, {}).get("d1", {}) for p in SWEEP_PAIRS},
                        now_utc
                    )
                    sig.update(ctx)
                sweep_signals.extend(sigs)

    # ── [5] Send alerts + log ─────────────────────────────────
    logger.info("\n[5] Sending alerts...")
    for sig in sweep_signals:
        send_telegram_sweep_s1(sig)
        log_sweep_fvg_to_sheet(sig)
        log_sweep_fvg_csv(sig)

    if not sweep_signals:
        logger.info("  No new signals this run")

    # ── [6] Persist state ─────────────────────────────────────
    state["candle_history"] = candle_history
    state["htf_cache"]      = htf_cache
    save_state(state)

    logger.info(f"\n{'='*55}")
    logger.info(f"  Sweep+FVG signals: {len(sweep_signals)}")
    logger.info(f"{'='*55}\n")


if __name__ == "__main__":
    main()

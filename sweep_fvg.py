"""
sweep_fvg.py — Sweep + FVG Fill Strategy Module for EDGE Signal Engine
=======================================================================
Detects liquidity sweeps followed by Fair Value Gap retracements.
Runs across all 5 active EDGE pairs on M15, with H1 bias filter and
H4 pool-based TP targets.

Strategy spec (backtested 2022–2026, EURUSD M15):
  - Win rate      : 45.1%
  - Avg win       : 32.1 pips
  - Avg loss      : -9.7 pips
  - Total pips    : +3,428
  - Avg RR        : 4.17
  - Expectancy    : +9.14 pips/trade
  - Trades/month  : ~7.7

HTF upgrade (2025-04):
  - H1 strict bias filter — HH+HL (bullish) or LH+LL (bearish) from last 2 swings
  - H4 swing pool detection — same N=3 logic applied to H4 candles
  - H4 pool used as TP target with 1:5 minimum RR; signal suppressed if no pool qualifies
  - Scan expanded to all 5 pairs (USDJPY, GBPUSD, AUDJPY, XAUUSD, EURUSD)
"""

import json
import os
import logging
from datetime import datetime, timezone
from collections import defaultdict
import numpy as np

logger = logging.getLogger(__name__)

# ── STRATEGY PARAMETERS ────────────────────────────────────────────────────
SWING_N       = 3          # N candles each side to confirm a swing point
EQ_TOL_PIPS   = 5          # tolerance for equal highs/lows clustering
MIN_FVG_PIPS  = 2          # minimum FVG size in pips
SL_BUF_PIPS   = 2          # buffer beyond sweep wick for stop loss
MIN_RR        = 1.5        # minimum risk/reward for FVG-based TP fallback
MIN_RR_H4     = 5.0        # minimum RR when using H4 pool as TP
LOOKBACK      = 50         # candles before equal H/L levels expire
FVG_WINDOW    = 10         # candles before sweep to search for FVG
RETRACE_WIN   = 30         # candles after sweep to wait for retrace
FVG_MAX_AGE   = 10         # FVG expires if retrace doesn't happen within N candles

# Per-pair pip sizes (matching signal_engine)
PIP_SIZE = {
    "USDJPY": 0.01,
    "GBPUSD": 0.0001,
    "AUDJPY": 0.01,
    "XAUUSD": 0.10,
    "EURUSD": 0.0001,
}

# Excluded liquidity sources (underperforming in backtest)
WEAK_SOURCES  = {'NewYorkL', 'PDL'}

# Active sessions (UTC hours)
SESSIONS = {
    'Asian':   (0,  6),
    'London':  (7,  12),
    'NewYork': (13, 17),
}

# State files — separate from Break & Retest to avoid conflicts
STATE_FILE          = 'state/sweep_fvg_state.json'
FIRED_SIGNALS_FILE  = 'state/sweep_fvg_fired.json'
SIGNAL_TTL_HOURS    = 4   # how long a fired signal is remembered (prevents refiring)


# ── SESSION HELPERS ────────────────────────────────────────────────────────
def get_session(hour_utc: int) -> str | None:
    for name, (start, end) in SESSIONS.items():
        if start <= hour_utc < end:
            return name
    return None  # off-hours


def is_trading_session(dt: datetime) -> bool:
    return get_session(dt.hour) is not None


def get_pip(pair: str) -> float:
    return PIP_SIZE.get(pair, 0.0001)


# ── SWING DETECTION ────────────────────────────────────────────────────────
def find_swing_highs(candles: list[dict], N: int = SWING_N) -> list[int]:
    """Return indices of swing highs (high > N candles on each side)."""
    highs = []
    for i in range(N, len(candles) - N):
        h = candles[i]['high']
        if all(h > candles[i-j]['high'] for j in range(1, N+1)) and \
           all(h > candles[i+j]['high'] for j in range(1, N+1)):
            highs.append(i)
    return highs


def find_swing_lows(candles: list[dict], N: int = SWING_N) -> list[int]:
    """Return indices of swing lows (low < N candles on each side)."""
    lows = []
    for i in range(N, len(candles) - N):
        l = candles[i]['low']
        if all(l < candles[i-j]['low'] for j in range(1, N+1)) and \
           all(l < candles[i+j]['low'] for j in range(1, N+1)):
            lows.append(i)
    return lows


# ── H1 BIAS FILTER ─────────────────────────────────────────────────────────
def get_h1_bias(h1_candles: list[dict]) -> str:
    """
    Determine directional bias from H1 structure.

    Bullish  : last 2 swing highs are HH and last 2 swing lows are HL (both rising)
    Bearish  : last 2 swing highs are LH and last 2 swing lows are LL (both falling)
    Neutral  : anything else (mixed structure)

    Returns: 'bullish' | 'bearish' | 'neutral'
    """
    if not h1_candles or len(h1_candles) < SWING_N * 2 + 2:
        logger.info("sweep_fvg: Not enough H1 candles for bias — defaulting neutral")
        return 'neutral'

    sh_idx = find_swing_highs(h1_candles, N=SWING_N)
    sl_idx = find_swing_lows(h1_candles,  N=SWING_N)

    if len(sh_idx) < 2 or len(sl_idx) < 2:
        return 'neutral'

    last_2_sh = sh_idx[-2:]
    last_2_sl = sl_idx[-2:]

    sh_vals = [h1_candles[i]['high'] for i in last_2_sh]
    sl_vals = [h1_candles[i]['low']  for i in last_2_sl]

    hh_hl = sh_vals[1] > sh_vals[0] and sl_vals[1] > sl_vals[0]  # HH + HL
    lh_ll = sh_vals[1] < sh_vals[0] and sl_vals[1] < sl_vals[0]  # LH + LL

    if hh_hl:
        return 'bullish'
    if lh_ll:
        return 'bearish'
    return 'neutral'


# ── H4 POOL DETECTION ──────────────────────────────────────────────────────
def get_h4_pools(h4_candles: list[dict]) -> dict:
    """
    Detect H4 swing highs and lows using the same N=3 logic.

    Returns dict with:
        'highs': list of H4 swing high prices (descending — nearest first)
        'lows':  list of H4 swing low prices  (ascending  — nearest first)
    """
    if not h4_candles or len(h4_candles) < SWING_N * 2 + 2:
        logger.info("sweep_fvg: Not enough H4 candles for pool detection")
        return {'highs': [], 'lows': []}

    sh_idx = find_swing_highs(h4_candles, N=SWING_N)
    sl_idx = find_swing_lows(h4_candles,  N=SWING_N)

    h4_highs = sorted([h4_candles[i]['high'] for i in sh_idx], reverse=True)
    h4_lows  = sorted([h4_candles[i]['low']  for i in sl_idx])

    return {'highs': h4_highs, 'lows': h4_lows}


def find_h4_tp(direction: str, entry: float, sl: float, h4_pools: dict) -> float | None:
    """
    Find nearest qualifying H4 pool level as TP target.

    For short: nearest H4 swing low below entry with RR >= MIN_RR_H4
    For long:  nearest H4 swing high above entry with RR >= MIN_RR_H4

    Returns the TP price, or None if no qualifying pool exists.
    """
    sl_dist = abs(entry - sl)
    if sl_dist == 0:
        return None

    if direction == 'short':
        candidates = [p for p in h4_pools.get('lows', []) if p < entry]
        candidates.sort(reverse=True)  # nearest first
        for pool_price in candidates:
            rr = abs(entry - pool_price) / sl_dist
            if rr >= MIN_RR_H4:
                return round(pool_price, 5)

    else:  # long
        candidates = [p for p in h4_pools.get('highs', []) if p > entry]
        candidates.sort()  # nearest first
        for pool_price in candidates:
            rr = abs(pool_price - entry) / sl_dist
            if rr >= MIN_RR_H4:
                return round(pool_price, 5)

    return None  # no qualifying H4 pool


# ── EQUAL HIGHS / LOWS ─────────────────────────────────────────────────────
def find_equal_levels(candles: list[dict],
                      swing_indices: list[int],
                      col: str,
                      side: str,
                      tol_pips: int = EQ_TOL_PIPS,
                      pair: str = 'EURUSD') -> list[dict]:
    """
    Cluster swing points within tolerance into equal high/low levels.
    Returns list of level dicts with price, confirmed_at index, side, source.
    """
    pip = get_pip(pair)
    tol = tol_pips * pip
    levels, used = [], set()
    prices = [(i, candles[i][col]) for i in swing_indices]

    for a in range(len(prices)):
        if a in used:
            continue
        group = [prices[a]]
        for b in range(a + 1, len(prices)):
            if b in used:
                continue
            if abs(prices[a][1] - prices[b][1]) <= tol:
                group.append(prices[b])
                used.add(b)
        if len(group) >= 2:
            levels.append({
                'price':        round(np.mean([p for _, p in group]), 5),
                'confirmed_at': max(i for i, _ in group),
                'side':         side,
                'source':       'equal_hl',
                'swept':        False,
            })
        used.add(a)

    return levels


# ── PREVIOUS DAY / SESSION LEVELS ──────────────────────────────────────────
def build_external_levels(candles: list[dict]) -> dict[int, list[dict]]:
    """
    Build previous day H/L and previous session H/L levels.
    Returns dict: candle_index → list of levels that activate at that candle.
    """
    levels_by_idx = defaultdict(list)

    # Group by date
    by_date = defaultdict(list)
    for idx, c in enumerate(candles):
        date = c['time'].date()
        by_date[date].append((idx, c))

    sorted_dates = sorted(by_date.keys())

    # Previous Day High / Low
    for d_idx in range(len(sorted_dates) - 1):
        today      = sorted_dates[d_idx]
        next_day   = sorted_dates[d_idx + 1]
        today_data = by_date[today]
        pdh = max(c['high'] for _, c in today_data)
        pdl = min(c['low']  for _, c in today_data)
        activate_at = by_date[next_day][0][0]  # first candle of next day

        levels_by_idx[activate_at].append({
            'price': round(pdh, 5), 'side': 'BSL',
            'source': 'PDH', 'swept': False
        })
        levels_by_idx[activate_at].append({
            'price': round(pdl, 5), 'side': 'SSL',
            'source': 'PDL', 'swept': False
        })

    # Previous Session High / Low
    by_date_session = defaultdict(lambda: defaultdict(list))
    for idx, c in enumerate(candles):
        sess = get_session(c['time'].hour)
        if sess:
            by_date_session[c['time'].date()][sess].append((idx, c))

    session_order = ['Asian', 'London', 'NewYork']
    all_session_keys = []
    for date in sorted_dates:
        for sess in session_order:
            if sess in by_date_session[date]:
                all_session_keys.append((date, sess))

    for s_idx in range(len(all_session_keys) - 1):
        date, sess  = all_session_keys[s_idx]
        next_date, next_sess = all_session_keys[s_idx + 1]
        sess_data   = by_date_session[date][sess]
        sh = max(c['high'] for _, c in sess_data)
        sl = min(c['low']  for _, c in sess_data)
        activate_at = by_date_session[next_date][next_sess][0][0]

        levels_by_idx[activate_at].append({
            'price': round(sh, 5), 'side': 'BSL',
            'source': f'{sess}H', 'swept': False
        })
        levels_by_idx[activate_at].append({
            'price': round(sl, 5), 'side': 'SSL',
            'source': f'{sess}L', 'swept': False
        })

    return levels_by_idx


# ── FVG DETECTION ──────────────────────────────────────────────────────────
def build_fvg_index(candles: list[dict],
                    pair: str = 'EURUSD') -> dict[int, list[dict]]:
    """
    Build index of FVGs by the candle index they form on.
    Bullish FVG: gap DOWN — c3.high < c1.low (fill = price moves UP)
    Bearish FVG: gap UP   — c3.low > c1.high (fill = price moves DOWN)
    """
    pip     = get_pip(pair)
    min_gap = MIN_FVG_PIPS * pip
    fvg_index = defaultdict(list)

    for i in range(1, len(candles) - 1):
        c1, c3 = candles[i-1], candles[i+1]

        # Bullish FVG
        if c3['high'] < c1['low']:
            gap = c1['low'] - c3['high']
            if gap >= min_gap:
                fvg_index[i+1].append({
                    'type':     'bullish',
                    'top':      round(c1['low'],   5),
                    'bottom':   round(c3['high'],  5),
                    'formed_i': i + 1,
                })

        # Bearish FVG
        if c3['low'] > c1['high']:
            gap = c3['low'] - c1['high']
            if gap >= min_gap:
                fvg_index[i+1].append({
                    'type':     'bearish',
                    'top':      round(c3['low'],   5),
                    'bottom':   round(c1['high'],  5),
                    'formed_i': i + 1,
                })

    return fvg_index


# ── SWEEP DETECTION ────────────────────────────────────────────────────────
def is_sweep(candle: dict, level_price: float, side: str) -> bool:
    """
    BSL sweep: wick above level, close back below.
    SSL sweep: wick below level, close back above.
    """
    if side == 'BSL':
        return candle['high'] > level_price and candle['close'] < level_price
    else:
        return candle['low'] < level_price and candle['close'] > level_price


# ── STATE MANAGEMENT ───────────────────────────────────────────────────────
def load_state() -> dict:
    """Load persisted state — pending setups waiting for retrace."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {'pending_setups': [], 'active_levels': []}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


# ── SIGNAL FORMATTER ───────────────────────────────────────────────────────
def format_signal(setup: dict) -> dict:
    """Format a confirmed setup into a signal dict matching EDGE conventions."""
    direction  = setup['direction']
    pair       = setup['pair']
    entry      = setup['entry']
    sl         = setup['sl']
    tp         = setup['tp']
    rr         = setup['rr']
    session    = setup['session']
    lv_source  = setup['lv_source']
    sweep_time = setup['sweep_time']
    h1_bias    = setup.get('h1_bias', 'neutral')
    tp_source  = setup.get('tp_source', 'fvg_fallback')

    emoji = '🔻' if direction == 'short' else '🔺'
    side  = 'SELL' if direction == 'short' else 'BUY'

    return {
        'strategy':   'Sweep+FVG',
        'pair':       pair,
        'direction':  direction,
        'side':       side,
        'entry':      entry,
        'sl':         sl,
        'tp':         tp,
        'rr':         round(rr, 2),
        'session':    session,
        'lv_source':  lv_source,
        'h1_bias':    h1_bias,
        'tp_source':  tp_source,
        'sweep_time': sweep_time,
        'fired_at':   setup.get('fired_at', ''),
        'message': (
            f"{emoji} *{pair} — Sweep+FVG Signal*\n"
            f"Direction  : `{side}`\n"
            f"Entry      : `{entry}`\n"
            f"Stop Loss  : `{sl}`\n"
            f"Take Profit: `{tp}`\n"
            f"RR         : `1:{rr:.1f}`\n"
            f"Session    : `{session}`\n"
            f"H1 Bias    : `{h1_bias.capitalize()}`\n"
            f"TP Source  : `{tp_source}`\n"
            f"Source     : `{lv_source}`\n"
            f"Swept at   : `{sweep_time}`"
        )
    }


# ── PERSISTENT DEDUP ──────────────────────────────────────────────────────
def load_fired_signals() -> dict:
    """Load previously fired signals from disk."""
    path = FIRED_SIGNALS_FILE
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_fired_signals(fired: dict):
    """Persist fired signals to disk."""
    os.makedirs(os.path.dirname(FIRED_SIGNALS_FILE), exist_ok=True)
    with open(FIRED_SIGNALS_FILE, 'w') as f:
        json.dump(fired, f, indent=2)


def is_already_fired(dedup_key: str, fired: dict) -> bool:
    """Return True if this signal was fired within TTL window."""
    if dedup_key not in fired:
        return False
    try:
        fired_at = datetime.fromisoformat(fired[dedup_key])
        age_hours = (datetime.now(timezone.utc) - fired_at).total_seconds() / 3600
        return age_hours < SIGNAL_TTL_HOURS
    except Exception:
        return False


def mark_as_fired(dedup_key: str, fired: dict):
    """Record that a signal was fired right now."""
    now = datetime.now(timezone.utc)
    # Prune old entries beyond TTL to keep file small
    pruned = {}
    for k, v in fired.items():
        try:
            age = (now - datetime.fromisoformat(v)).total_seconds() / 3600
            if age < SIGNAL_TTL_HOURS * 2:
                pruned[k] = v
        except Exception:
            pass
    fired.clear()
    fired.update(pruned)
    fired[dedup_key] = now.isoformat()


# ── CORE SCAN FUNCTION ─────────────────────────────────────────────────────
def scan(m15_candles: list[dict],
         h1_candles:  list[dict] | None = None,
         h4_candles:  list[dict] | None = None,
         pair:        str = 'EURUSD',
         send_signal_fn=None) -> list[dict]:
    """
    Main entry point. Called each cron run with the latest candles.

    Args:
        m15_candles    : List of M15 candle dicts (time, open, high, low, close, volume)
        h1_candles     : List of H1 candle dicts (same format) — for bias filter
        h4_candles     : List of H4 candle dicts (same format) — for pool TP targets
        pair           : Instrument symbol, e.g. 'EURUSD', 'USDJPY'
        send_signal_fn : Optional callable(signal_dict) to fire Telegram/Sheets

    Returns:
        List of signal dicts fired this run.
    """
    candles = m15_candles  # alias for readability below

    if len(candles) < SWING_N * 2 + FVG_WINDOW + RETRACE_WIN + 5:
        logger.warning(f"sweep_fvg [{pair}]: Not enough M15 candles to scan.")
        return []

    signals_fired  = []
    fired          = load_fired_signals()   # persistent dedup across runs
    latest         = candles[-1]
    latest_time    = latest['time']

    # Session check
    if not is_trading_session(latest_time):
        logger.info(f"sweep_fvg [{pair}]: Off-hours ({latest_time.hour}:xx UTC), skipping.")
        return []

    current_session = get_session(latest_time.hour)

    # ── H1 Bias Filter ─────────────────────────────────────────────────────
    h1_bias = get_h1_bias(h1_candles) if h1_candles else 'neutral'
    logger.info(f"sweep_fvg [{pair}]: H1 bias = {h1_bias}")

    # ── H4 Pool Detection ──────────────────────────────────────────────────
    h4_pools = get_h4_pools(h4_candles) if h4_candles else {'highs': [], 'lows': []}
    logger.info(
        f"sweep_fvg [{pair}]: H4 pools — "
        f"{len(h4_pools['highs'])} highs, {len(h4_pools['lows'])} lows"
    )

    # ── Build M15 detection structures ────────────────────────────────────
    swing_highs = find_swing_highs(candles)
    swing_lows  = find_swing_lows(candles)

    bsl_eq = find_equal_levels(candles, swing_highs, 'high', 'BSL', pair=pair)
    ssl_eq = find_equal_levels(candles, swing_lows,  'low',  'SSL', pair=pair)
    eq_levels = sorted(bsl_eq + ssl_eq, key=lambda x: x['confirmed_at'])

    ext_levels = build_external_levels(candles)
    fvg_index  = build_fvg_index(candles, pair=pair)

    # ── Build active levels pool ───────────────────────────────────────────
    n = len(candles)
    active_lvls = []

    for lv in eq_levels:
        age = (n - 1) - lv['confirmed_at']
        if age <= LOOKBACK:
            active_lvls.append({**lv, 'swept': False})

    for idx, lvs in ext_levels.items():
        if idx <= n - 1:
            for lv in lvs:
                if lv['source'] not in WEAK_SOURCES:
                    active_lvls.append({**lv, 'swept': False})

    # ── Scan last FVG_WINDOW + RETRACE_WIN candles for setups ─────────────
    scan_start = max(SWING_N, n - FVG_WINDOW - RETRACE_WIN - 5)

    for i in range(scan_start, n - 1):
        candle  = candles[i]
        session = get_session(candle['time'].hour)
        if not session:
            continue

        for lv in active_lvls:
            if lv.get('swept'):
                continue
            if lv['source'] in WEAK_SOURCES:
                continue

            lv_price = lv['price']
            side     = lv['side']

            if not is_sweep(candle, lv_price, side):
                continue

            lv['swept'] = True
            direction       = 'short' if side == 'BSL' else 'long'
            target_fvg_type = 'bullish' if direction == 'short' else 'bearish'

            # ── H1 Bias alignment check ────────────────────────────────────
            # Only skip if bias is confirmed opposite — neutral passes through
            if h1_bias == 'bullish' and direction == 'short':
                logger.info(
                    f"sweep_fvg [{pair}]: SHORT suppressed — H1 bias is bullish"
                )
                continue
            if h1_bias == 'bearish' and direction == 'long':
                logger.info(
                    f"sweep_fvg [{pair}]: LONG suppressed — H1 bias is bearish"
                )
                continue

            # Find FVG near the sweep
            sweep_fvg    = None
            search_start = max(SWING_N, i - FVG_WINDOW)
            for j in range(search_start, i + 2):
                for fvg in fvg_index.get(j, []):
                    if fvg['type'] == target_fvg_type:
                        if direction == 'short' and fvg['top'] <= candle['high']:
                            sweep_fvg = fvg; break
                        if direction == 'long'  and fvg['bottom'] >= candle['low']:
                            sweep_fvg = fvg; break
                if sweep_fvg:
                    break

            if not sweep_fvg:
                continue

            fvg_top    = sweep_fvg['top']
            fvg_bottom = sweep_fvg['bottom']
            fvg_mid    = round((fvg_top + fvg_bottom) / 2, 5)
            fvg_formed = sweep_fvg['formed_i']

            # Check if price has retraced into FVG between sweep and latest candle
            entry_triggered = False
            for k in range(i + 1, n):
                fvg_age = k - fvg_formed
                if fvg_age > FVG_MAX_AGE:
                    break

                fc = candles[k]
                if direction == 'short':
                    if fc['high'] >= fvg_bottom and fc['low'] <= fvg_top:
                        entry_triggered = True; break
                    if fc['low'] < fvg_bottom - 10 * get_pip(pair):
                        break
                else:
                    if fc['low'] <= fvg_top and fc['high'] >= fvg_bottom:
                        entry_triggered = True; break
                    if fc['high'] > fvg_top + 10 * get_pip(pair):
                        break

            if not entry_triggered:
                continue

            # SL beyond sweep wick
            sl_buf  = SL_BUF_PIPS * get_pip(pair)
            sl_val  = round(candle['high'] + sl_buf if direction == 'short'
                            else candle['low'] - sl_buf, 5)
            sl_dist = abs(fvg_mid - sl_val)
            if sl_dist < 2 * get_pip(pair):
                continue

            # ── TP: H4 pool first, fallback to opposing liquidity ──────────
            tp         = None
            tp_source  = None
            h4_tp      = find_h4_tp(direction, fvg_mid, sl_val, h4_pools)

            if h4_tp is not None:
                tp        = h4_tp
                tp_source = 'h4_pool'
                rr        = abs(tp - fvg_mid) / sl_dist
                logger.info(
                    f"sweep_fvg [{pair}]: H4 pool TP = {tp} | RR = 1:{rr:.1f}"
                )
            else:
                # No qualifying H4 pool — suppress the signal entirely
                logger.info(
                    f"sweep_fvg [{pair}]: Signal suppressed — no H4 pool with RR >= {MIN_RR_H4}"
                )
                continue

            rr = abs(tp - fvg_mid) / sl_dist

            # ── Persistent deduplication check ────────────────────────────
            dedup_key = f"{pair}_{direction}_{fvg_mid}_{sl_val}"
            if is_already_fired(dedup_key, fired):
                logger.info(f"sweep_fvg [{pair}]: Duplicate suppressed — {dedup_key}")
                continue
            mark_as_fired(dedup_key, fired)

            # ── Signal confirmed ───────────────────────────────────────────
            setup = {
                'pair':       pair,
                'direction':  direction,
                'entry':      fvg_mid,
                'sl':         sl_val,
                'tp':         tp,
                'rr':         round(rr, 2),
                'session':    current_session,
                'lv_source':  lv['source'],
                'h1_bias':    h1_bias,
                'tp_source':  tp_source,
                'sweep_time': str(candle['time']),
                'fired_at':   str(latest_time),
            }

            signal = format_signal(setup)
            signals_fired.append(signal)

            logger.info(
                f"sweep_fvg [{pair}]: Signal fired | {direction.upper()} "
                f"entry={fvg_mid} sl={sl_val} tp={tp} "
                f"rr=1:{rr:.1f} source={lv['source']} "
                f"h1={h1_bias} tp_src={tp_source}"
            )

            if send_signal_fn:
                try:
                    send_signal_fn(signal)
                except Exception as e:
                    logger.error(f"sweep_fvg [{pair}]: Failed to send signal: {e}")

    # Persist fired signals so next cron run doesn't re-fire same setup
    save_fired_signals(fired)

    return signals_fired


# ── STANDALONE TEST ────────────────────────────────────────────────────────
if __name__ == '__main__':
    import csv
    logging.basicConfig(level=logging.INFO)

    print("Loading candles for standalone test...")
    candles = []
    with open('../EURUSD15.csv', newline='') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            candles.append({
                'time':   datetime.strptime(row[0].strip(), '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc),
                'open':   float(row[1]),
                'high':   float(row[2]),
                'low':    float(row[3]),
                'close':  float(row[4]),
                'volume': int(row[5]),
            })

    # Test on last 500 candles (no H1/H4 in standalone — bias will be neutral)
    test_candles = candles[-500:]
    print(f"Scanning {len(test_candles)} candles...")
    results = scan(test_candles, pair='EURUSD')
    print(f"\nSignals found: {len(results)}")
    for r in results:
        print(f"  {r['direction'].upper():5} {r['pair']} | entry={r['entry']} sl={r['sl']} "
              f"tp={r['tp']} rr=1:{r['rr']} | {r['lv_source']} | {r['session']} "
              f"| H1={r['h1_bias']} | TP={r['tp_source']}")

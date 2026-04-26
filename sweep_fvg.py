"""
sweep_fvg.py — Sweep + FVG Fill Strategy Module for EDGE Signal Engine
=======================================================================
Detects liquidity sweeps followed by Fair Value Gap retracements on M15.
Runs as a standalone module called from signal_engine.py alongside the
existing Break & Retest engine. Completely isolated — shares no state
files with B&R.

Changelog v3 (post-backtest filter pass):
  - Equal H/L removed as sweep source (largest loss driver across all 5 pairs)
  - Valid sweep sources: PDH, AsianH, NewYorkH only
  - MAX_RR = 5.0 ceiling added alongside MIN_RR = 3.0 floor
  - Fallback TP removed entirely — no H1 level in 3–5R window = signal dropped
  - Forex session gate: London only (07–12 UTC)
  - XAUUSD session gate: Asian + London only (00–12 UTC), NY hard blocked
  - XAUUSD SL: ATR-derived buffer (ATR×0.25) and floor (ATR×0.50)
  - XAUUSD has dedicated config block; forex pairs share default config
  - signal_engine.py call signature unchanged — no changes needed there

Changelog v2:
  - Runs on ALL 5 strategy pairs (was EURUSD only)
  - TP targets sourced from H1 swing highs/lows (was M15 active_lvls)
  - MIN_RR raised to 1:3 (was 1.5)
  - pip_size is now pair-aware (supports JPY pairs and XAUUSD)
  - scan() signature: m15_candles, h1_candles, h4_candles, pair
  - All state files prefixed sweep_fvg_* — zero overlap with B&R state
"""

import json
import os
import logging
from datetime import datetime, timezone
from collections import defaultdict
import numpy as np

logger = logging.getLogger(__name__)

# ── PIP SIZE PER PAIR ──────────────────────────────────────────────────────
PIP_SIZE = {
    'EURUSD': 0.0001,
    'GBPUSD': 0.0001,
    'USDJPY': 0.01,
    'AUDJPY': 0.01,
    'XAUUSD': 0.10,
}
DEFAULT_PIP = 0.0001


def get_pip(pair: str) -> float:
    return PIP_SIZE.get(pair, DEFAULT_PIP)


# ── SHARED STRATEGY PARAMETERS ─────────────────────────────────────────────
SWING_N      = 3     # M15 swing confirmation candles each side
H1_SWING_N   = 2     # H1 swing confirmation candles each side
MIN_FVG_PIPS = 2     # minimum FVG gap size in pips
MIN_RR       = 3.0   # minimum RR — signals below this are dropped
MAX_RR       = 5.0   # maximum RR — signals beyond this are dropped (no fallback)
LOOKBACK     = 50    # M15 candles before external levels expire
FVG_WINDOW   = 10    # candles before sweep to search for matching FVG
RETRACE_WIN  = 30    # candles after sweep to wait for FVG retrace
FVG_MAX_AGE  = 10    # FVG expires after N candles with no retrace
H1_LOOKBACK  = 30    # H1 candles used for TP target pool

# ── VALID SWEEP SOURCES ────────────────────────────────────────────────────
# equal_hl removed — negative P&L across all 5 pairs in backtest.
# LondonH, LondonL, AsianL, PDL, NewYorkL removed — underperforming.
# Only BSL sources (highs) retained: PDH, AsianH, NewYorkH.
VALID_SWEEP_SOURCES = {'PDH', 'AsianH', 'NewYorkH'}

# ── SESSION DEFINITIONS (UTC) ──────────────────────────────────────────────
SESSIONS = {
    'Asian':   (0,  6),
    'London':  (7,  12),
    'NewYork': (13, 17),
}

# Allowed sessions per pair — confirmed from backtest session breakdown.
# Forex: London only. Gold: Asian + London. NY blocked for all pairs.
PAIR_SESSIONS = {
    'EURUSD': {'London'},
    'GBPUSD': {'London'},
    'USDJPY': {'London'},
    'AUDJPY': {'London'},
    'XAUUSD': {'Asian', 'London'},
}

# ── XAUUSD-SPECIFIC CONFIG ─────────────────────────────────────────────────
# Forex pairs use module-level defaults above.
# Gold uses ATR-derived SL sizing and its own session gate.
XAUUSD_CONFIG = {
    'sessions':       {'Asian', 'London'},  # NY hard blocked
    'atr_period':     14,                   # ATR lookback (M15 candles)
    'atr_buf_mult':   0.25,                 # SL buffer = ATR × this
    'atr_floor_mult': 0.50,                 # min sl_dist = ATR × this
    'risk_pct':       0.5,                  # matches B&R engine Gold risk
    'min_rr':         MIN_RR,               # inherits shared floor
    'max_rr':         MAX_RR,               # inherits shared ceiling
}

# ── STATE FILES — isolated from B&R engine ────────────────────────────────
STATE_FILE         = 'state/sweep_fvg_state.json'
FIRED_SIGNALS_FILE = 'state/sweep_fvg_fired.json'
SIGNAL_TTL_HOURS   = 4


# ── SESSION HELPERS ────────────────────────────────────────────────────────
def get_session(hour_utc: int) -> str | None:
    for name, (start, end) in SESSIONS.items():
        if start <= hour_utc < end:
            return name
    return None


def is_valid_session(pair: str, dt: datetime) -> bool:
    """Return True only if the current session is allowed for this pair."""
    session = get_session(dt.hour)
    if session is None:
        return False
    allowed = PAIR_SESSIONS.get(pair, {'London'})
    return session in allowed


# ── ATR CALCULATION (XAUUSD SL SIZING) ────────────────────────────────────
def calc_atr(candles: list[dict], period: int = 14) -> float:
    """
    Average True Range over the last `period` candles.
    Called with the most recent 20 M15 candles for XAUUSD SL sizing.
    """
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        high = candles[i]['high']
        low  = candles[i]['low']
        prev = candles[i-1]['close']
        trs.append(max(
            high - low,
            abs(high - prev),
            abs(low  - prev),
        ))
    return float(np.mean(trs[-period:]))


# ── M15 SWING DETECTION ────────────────────────────────────────────────────
def find_swing_highs(candles: list[dict], N: int = SWING_N) -> list[int]:
    highs = []
    for i in range(N, len(candles) - N):
        h = candles[i]['high']
        if all(h > candles[i-j]['high'] for j in range(1, N+1)) and \
           all(h > candles[i+j]['high'] for j in range(1, N+1)):
            highs.append(i)
    return highs


def find_swing_lows(candles: list[dict], N: int = SWING_N) -> list[int]:
    lows = []
    for i in range(N, len(candles) - N):
        l = candles[i]['low']
        if all(l < candles[i-j]['low'] for j in range(1, N+1)) and \
           all(l < candles[i+j]['low'] for j in range(1, N+1)):
            lows.append(i)
    return lows


# ── M15 EXTERNAL LEVELS (sweep targets) ───────────────────────────────────
def build_external_levels(candles: list[dict]) -> dict[int, list[dict]]:
    """
    Build Previous Day High and Previous Session High levels from M15 candles.
    Only sources in VALID_SWEEP_SOURCES are retained (PDH, AsianH, NewYorkH).
    PDL and all session lows are excluded.
    Returns dict: candle_index → list of levels active from that index.
    """
    levels_by_idx = defaultdict(list)

    by_date      = defaultdict(list)
    for idx, c in enumerate(candles):
        by_date[c['time'].date()].append((idx, c))
    sorted_dates = sorted(by_date.keys())

    # Previous Day High only (PDL excluded from VALID_SWEEP_SOURCES)
    for d_idx in range(len(sorted_dates) - 1):
        today_data  = by_date[sorted_dates[d_idx]]
        next_day    = sorted_dates[d_idx + 1]
        pdh         = max(c['high'] for _, c in today_data)
        activate_at = by_date[next_day][0][0]
        levels_by_idx[activate_at].append(
            {'price': round(pdh, 5), 'side': 'BSL', 'source': 'PDH', 'swept': False}
        )

    # Previous Session Highs only — filtered to VALID_SWEEP_SOURCES
    by_date_session = defaultdict(lambda: defaultdict(list))
    for idx, c in enumerate(candles):
        sess = get_session(c['time'].hour)
        if sess:
            by_date_session[c['time'].date()][sess].append((idx, c))

    session_order    = ['Asian', 'London', 'NewYork']
    all_session_keys = []
    for date in sorted_dates:
        for sess in session_order:
            if sess in by_date_session[date]:
                all_session_keys.append((date, sess))

    for s_idx in range(len(all_session_keys) - 1):
        date,      sess      = all_session_keys[s_idx]
        next_date, next_sess = all_session_keys[s_idx + 1]
        sess_data   = by_date_session[date][sess]
        sh          = max(c['high'] for _, c in sess_data)
        activate_at = by_date_session[next_date][next_sess][0][0]
        src         = f'{sess}H'
        if src in VALID_SWEEP_SOURCES:
            levels_by_idx[activate_at].append(
                {'price': round(sh, 5), 'side': 'BSL', 'source': src, 'swept': False}
            )

    return levels_by_idx


# ── H1 TARGET LEVELS (TP sourcing) ────────────────────────────────────────
def build_h1_target_levels(h1_candles: list[dict],
                            lookback: int = H1_LOOKBACK) -> list[dict]:
    """
    Extract H1 swing highs/lows as TP target pool.
    BSL = H1 swing high (target for longs going up).
    SSL = H1 swing low  (target for shorts going down).
    """
    if not h1_candles or len(h1_candles) < H1_SWING_N * 2 + 1:
        return []
    recent   = h1_candles[-lookback:]
    h1_highs = find_swing_highs(recent, N=H1_SWING_N)
    h1_lows  = find_swing_lows(recent,  N=H1_SWING_N)
    levels   = []
    for idx in h1_highs:
        levels.append({'price': round(recent[idx]['high'], 5),
                       'side': 'BSL', 'source': 'H1_swing_high', 'swept': False})
    for idx in h1_lows:
        levels.append({'price': round(recent[idx]['low'],  5),
                       'side': 'SSL', 'source': 'H1_swing_low',  'swept': False})
    return levels


# ── FVG DETECTION ──────────────────────────────────────────────────────────
def build_fvg_index(candles: list[dict],
                    pip: float,
                    min_gap_pips: int = MIN_FVG_PIPS) -> dict[int, list[dict]]:
    """
    Index FVGs by the candle index they form on (M15).
    Bullish FVG: c3.high < c1.low  — gap downward (entry zone for longs)
    Bearish FVG: c3.low  > c1.high — gap upward   (entry zone for shorts)
    """
    fvg_index = defaultdict(list)
    min_gap   = min_gap_pips * pip
    for i in range(1, len(candles) - 1):
        c1, c3 = candles[i-1], candles[i+1]
        if c3['high'] < c1['low']:
            gap = c1['low'] - c3['high']
            if gap >= min_gap:
                fvg_index[i+1].append({
                    'type': 'bullish', 'top': round(c1['low'], 5),
                    'bottom': round(c3['high'], 5), 'formed_i': i+1,
                })
        if c3['low'] > c1['high']:
            gap = c3['low'] - c1['high']
            if gap >= min_gap:
                fvg_index[i+1].append({
                    'type': 'bearish', 'top': round(c3['low'], 5),
                    'bottom': round(c1['high'], 5), 'formed_i': i+1,
                })
    return fvg_index


# ── SWEEP DETECTION ────────────────────────────────────────────────────────
def is_sweep(candle: dict, level_price: float, side: str) -> bool:
    """
    BSL sweep: wick pierces above level, candle closes back below.
    SSL sweep: wick pierces below level, candle closes back above.
    Note: only BSL sweeps fire post-filter (all active sources are highs),
    but SSL logic is retained for future extensibility.
    """
    if side == 'BSL':
        return candle['high'] > level_price and candle['close'] < level_price
    else:
        return candle['low'] < level_price and candle['close'] > level_price


# ── STATE MANAGEMENT ───────────────────────────────────────────────────────
def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {'pending_setups': [], 'active_levels': []}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


# ── PERSISTENT DEDUP ───────────────────────────────────────────────────────
def load_fired_signals() -> dict:
    if os.path.exists(FIRED_SIGNALS_FILE):
        try:
            with open(FIRED_SIGNALS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_fired_signals(fired: dict):
    os.makedirs(os.path.dirname(FIRED_SIGNALS_FILE), exist_ok=True)
    with open(FIRED_SIGNALS_FILE, 'w') as f:
        json.dump(fired, f, indent=2)


def is_already_fired(dedup_key: str, fired: dict) -> bool:
    if dedup_key not in fired:
        return False
    try:
        fired_at  = datetime.fromisoformat(fired[dedup_key])
        age_hours = (datetime.now(timezone.utc) - fired_at).total_seconds() / 3600
        return age_hours < SIGNAL_TTL_HOURS
    except Exception:
        return False


def mark_as_fired(dedup_key: str, fired: dict):
    now    = datetime.now(timezone.utc)
    pruned = {
        k: v for k, v in fired.items()
        if (now - datetime.fromisoformat(v)).total_seconds() / 3600 < SIGNAL_TTL_HOURS * 2
    }
    fired.clear()
    fired.update(pruned)
    fired[dedup_key] = now.isoformat()


# ── SIGNAL FORMATTER ───────────────────────────────────────────────────────
def format_signal(setup: dict) -> dict:
    direction  = setup['direction']
    pair       = setup['pair']
    entry      = setup['entry']
    sl         = setup['sl']
    tp         = setup['tp']
    rr         = setup['rr']
    session    = setup['session']
    lv_source  = setup['lv_source']
    tp_source  = setup['tp_source']
    sweep_time = setup['sweep_time']

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
        'tp_source':  tp_source,
        'sweep_time': sweep_time,
        'fired_at':   setup.get('fired_at', ''),
        'h1_bias':    setup.get('h1_bias', ''),
        'message': (
            f"{emoji} *{pair} — Sweep+FVG Signal*\n"
            f"Direction  : `{side}`\n"
            f"Entry      : `{entry}`\n"
            f"Stop Loss  : `{sl}`\n"
            f"Take Profit: `{tp}`\n"
            f"RR         : `1:{rr:.1f}`\n"
            f"Session    : `{session}`\n"
            f"Sweep src  : `{lv_source}`\n"
            f"TP src     : `{tp_source}`\n"
            f"Swept at   : `{sweep_time}`"
        ),
    }


# ── CORE SCAN FUNCTION ─────────────────────────────────────────────────────
def scan(m15_candles: list[dict],
         h1_candles:  list[dict] | None = None,
         h4_candles:  list[dict] | None = None,
         pair:        str = 'EURUSD') -> list[dict]:
    """
    Main entry point called each cron run by signal_engine.py.

    Args:
        m15_candles : M15 candle dicts — time(datetime), open, high, low, close, volume
        h1_candles  : H1 candle dicts from HTF cache (TP target levels)
        h4_candles  : H4 candle dicts — accepted, reserved for future bias filter
        pair        : Instrument string e.g. 'EURUSD', 'USDJPY', 'XAUUSD'

    Returns:
        List of signal dicts fired this run (may be empty).
    """
    pip     = get_pip(pair)
    is_gold = (pair == 'XAUUSD')
    cfg     = XAUUSD_CONFIG if is_gold else {}
    min_rr  = cfg.get('min_rr', MIN_RR)
    max_rr  = cfg.get('max_rr', MAX_RR)

    min_candles = SWING_N * 2 + FVG_WINDOW + RETRACE_WIN + 5
    if len(m15_candles) < min_candles:
        logger.warning(
            f"sweep_fvg [{pair}]: Only {len(m15_candles)} M15 candles "
            f"— need {min_candles}. Skipping."
        )
        return []

    signals_fired = []
    fired         = load_fired_signals()
    latest        = m15_candles[-1]
    latest_time   = latest['time']

    # ── Pair-specific session gate ────────────────────────────────────────
    if not is_valid_session(pair, latest_time):
        logger.info(
            f"sweep_fvg [{pair}]: Session blocked — "
            f"'{get_session(latest_time.hour) or 'off-hours'}' "
            f"not in allowed sessions for {pair}."
        )
        return []

    current_session = get_session(latest_time.hour)

    # ── XAUUSD: ATR-derived SL parameters ────────────────────────────────
    gold_sl_buf   = None
    gold_sl_floor = None
    if is_gold:
        recent_20   = m15_candles[-20:]
        current_atr = calc_atr(recent_20, period=cfg['atr_period'])
        if current_atr <= 0:
            logger.warning(f"sweep_fvg [XAUUSD]: ATR=0, skipping run.")
            return []
        gold_sl_buf   = current_atr * cfg['atr_buf_mult']
        gold_sl_floor = current_atr * cfg['atr_floor_mult']
        logger.info(
            f"sweep_fvg [XAUUSD]: ATR={current_atr:.3f} "
            f"| sl_buf={gold_sl_buf:.3f} | sl_floor={gold_sl_floor:.3f}"
        )

    # ── Build sweep target pool (external levels only, no equal_hl) ───────
    ext_levels = build_external_levels(m15_candles)
    fvg_index  = build_fvg_index(m15_candles, pip)

    n           = len(m15_candles)
    active_lvls = []
    for idx, lvs in ext_levels.items():
        if idx <= n - 1:
            for lv in lvs:
                if lv['source'] in VALID_SWEEP_SOURCES:
                    active_lvls.append({**lv, 'swept': False})

    # ── Build H1 TP target pool ───────────────────────────────────────────
    h1_levels = build_h1_target_levels(h1_candles) if h1_candles else []
    if not h1_levels:
        logger.info(
            f"sweep_fvg [{pair}]: No H1 levels — "
            f"all setups this run dropped (no fallback TP)."
        )
        # Don't return early — active_lvls sweep detection still runs,
        # but every setup will hit the tp=None guard below and be dropped.

    # ── Scan for setups ───────────────────────────────────────────────────
    scan_start = max(SWING_N, n - FVG_WINDOW - RETRACE_WIN - 5)

    for i in range(scan_start, n - 1):
        candle  = m15_candles[i]
        session = get_session(candle['time'].hour)
        if not session:
            continue

        for lv in active_lvls:
            if lv.get('swept'):
                continue

            lv_price = lv['price']
            side     = lv['side']

            if not is_sweep(candle, lv_price, side):
                continue

            lv['swept'] = True
            direction       = 'short' if side == 'BSL' else 'long'
            target_fvg_type = 'bullish' if direction == 'short' else 'bearish'

            # ── Find matching FVG near sweep ──────────────────────────────
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

            # ── Wait for retrace into FVG ─────────────────────────────────
            entry_triggered = False
            for k in range(i + 1, n):
                if k - fvg_formed > FVG_MAX_AGE:
                    break
                fc = m15_candles[k]
                if direction == 'short':
                    if fc['high'] >= fvg_bottom and fc['low'] <= fvg_top:
                        entry_triggered = True; break
                    if fc['low'] < fvg_bottom - 10 * pip:
                        break
                else:
                    if fc['low'] <= fvg_top and fc['high'] >= fvg_bottom:
                        entry_triggered = True; break
                    if fc['high'] > fvg_top + 10 * pip:
                        break

            if not entry_triggered:
                continue

            # ── SL calculation ────────────────────────────────────────────
            if is_gold:
                sl_val  = round(
                    candle['high'] + gold_sl_buf if direction == 'short'
                    else candle['low'] - gold_sl_buf,
                    5
                )
                sl_dist = abs(fvg_mid - sl_val)
                if sl_dist < gold_sl_floor:
                    logger.info(
                        f"sweep_fvg [XAUUSD]: Dropped — sl_dist {sl_dist:.3f} "
                        f"< ATR floor {gold_sl_floor:.3f}"
                    )
                    continue
            else:
                sl_buf  = 2 * pip
                sl_val  = round(
                    candle['high'] + sl_buf if direction == 'short'
                    else candle['low'] - sl_buf,
                    5
                )
                sl_dist = abs(fvg_mid - sl_val)
                if sl_dist < 2 * pip:
                    continue

            # ── TP from H1 levels — 3R–5R window only, no fallback ────────
            # Candidate levels must sit within [MIN_RR, MAX_RR] of sl_dist.
            # If no qualifying level exists the setup is dropped entirely.
            tp        = None
            tp_source = ''

            if direction == 'short':
                cands = [
                    lv2['price'] for lv2 in h1_levels
                    if lv2['side'] == 'SSL'
                    and lv2['price'] < fvg_mid
                    and min_rr <= abs(fvg_mid - lv2['price']) / sl_dist <= max_rr
                ]
                if cands:
                    tp        = round(max(cands), 5)   # nearest valid level
                    tp_source = 'H1_swing_low'
            else:
                cands = [
                    lv2['price'] for lv2 in h1_levels
                    if lv2['side'] == 'BSL'
                    and lv2['price'] > fvg_mid
                    and min_rr <= abs(lv2['price'] - fvg_mid) / sl_dist <= max_rr
                ]
                if cands:
                    tp        = round(min(cands), 5)   # nearest valid level
                    tp_source = 'H1_swing_high'

            if tp is None:
                logger.info(
                    f"sweep_fvg [{pair}]: Dropped — no H1 level in "
                    f"{min_rr}–{max_rr}R window "
                    f"| entry={fvg_mid} sl_dist={round(sl_dist, 5)}"
                )
                continue

            # ── RR confirmation ───────────────────────────────────────────
            rr = abs(tp - fvg_mid) / sl_dist
            if not (min_rr <= rr <= max_rr):
                continue

            # ── Persistent dedup ──────────────────────────────────────────
            dedup_key = f"{pair}_{direction}_{fvg_mid}_{sl_val}"
            if is_already_fired(dedup_key, fired):
                logger.info(f"sweep_fvg [{pair}]: Duplicate suppressed — {dedup_key}")
                continue
            mark_as_fired(dedup_key, fired)

            # ── Signal confirmed ──────────────────────────────────────────
            setup = {
                'pair':       pair,
                'direction':  direction,
                'entry':      fvg_mid,
                'sl':         sl_val,
                'tp':         tp,
                'rr':         round(rr, 2),
                'session':    current_session,
                'lv_source':  lv['source'],
                'tp_source':  tp_source,
                'sweep_time': str(candle['time']),
                'fired_at':   str(latest_time),
                'h1_bias':    '',   # reserved for future H4 bias filter
            }

            signal = format_signal(setup)
            signals_fired.append(signal)

            logger.info(
                f"sweep_fvg [{pair}]: ✅ Signal | {direction.upper()} "
                f"entry={fvg_mid} sl={sl_val} tp={tp} rr=1:{rr:.1f} "
                f"sweep={lv['source']} tp={tp_source} session={current_session}"
            )

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

    test_candles = candles[-500:]
    print(f"Scanning {len(test_candles)} candles...")
    results = scan(m15_candles=test_candles, pair='EURUSD')
    print(f"\nSignals found: {len(results)}")
    for r in results:
        print(
            f"  {r['direction'].upper():5} | entry={r['entry']} sl={r['sl']} "
            f"tp={r['tp']} rr=1:{r['rr']} | sweep={r['lv_source']} "
            f"tp_src={r['tp_source']} | {r['session']}"
        )

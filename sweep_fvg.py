"""
sweep_fvg.py — Sweep + FVG Fill Strategy Module for EDGE Signal Engine
=======================================================================
Detects liquidity sweeps followed by Fair Value Gap retracements on M15.
Runs as a standalone module called from signal_engine.py alongside the
existing Break & Retest engine. Completely isolated — shares no state
files with B&R.

Changes in this version:
  - Runs on ALL 5 strategy pairs (was EURUSD only)
  - TP targets sourced from H1 swing highs/lows (was M15 active_lvls)
  - MIN_RR raised to 1:3 (was 1.5)
  - pip_size is now pair-aware (supports JPY pairs and XAUUSD)
  - scan() signature updated: m15_candles, h1_candles, h4_candles, pair
  - H4 candles accepted but reserved for future bias filter (unused now)
  - All state files prefixed sweep_fvg_* — zero overlap with B&R state

Strategy spec (backtested 2022–2026, EURUSD M15):
  - Win rate      : 45.1%
  - Avg win       : 32.1 pips
  - Avg loss      : -9.7 pips
  - Total pips    : +3,428
  - Avg RR        : 4.17
  - Expectancy    : +9.14 pips/trade
  - Trades/month  : ~7.7
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


# ── STRATEGY PARAMETERS ────────────────────────────────────────────────────
SWING_N       = 3          # N candles each side to confirm a swing point (M15)
H1_SWING_N    = 2          # N candles each side for H1 swing detection
EQ_TOL_PIPS   = 5          # tolerance for equal highs/lows clustering (in M15 pips)
MIN_FVG_PIPS  = 2          # minimum FVG size in pips
SL_BUF_PIPS   = 2          # buffer beyond sweep wick for stop loss
MIN_RR        = 3.0        # minimum risk/reward — signals below this are dropped
LOOKBACK      = 50         # M15 candles before equal H/L levels expire
FVG_WINDOW    = 10         # candles before sweep to search for FVG
RETRACE_WIN   = 30         # candles after sweep to wait for retrace
FVG_MAX_AGE   = 10         # FVG expires if retrace doesn't happen within N candles
H1_LOOKBACK   = 30         # H1 candles to consider for TP target levels

# Excluded M15 liquidity sources for sweep detection (underperforming in backtest)
WEAK_SOURCES  = {'NewYorkL', 'PDL'}

# Active sessions (UTC hours) — sweep detection only fires inside these
SESSIONS = {
    'Asian':   (0,  6),
    'London':  (7,  12),
    'NewYork': (13, 17),
}

# ── STATE FILES — isolated from B&R engine ────────────────────────────────
# These files are NEVER read or written by signal_engine.py B&R logic.
STATE_FILE         = 'state/sweep_fvg_state.json'
FIRED_SIGNALS_FILE = 'state/sweep_fvg_fired.json'
SIGNAL_TTL_HOURS   = 4   # dedup window — prevents the same setup re-firing


# ── SESSION HELPERS ────────────────────────────────────────────────────────
def get_session(hour_utc: int) -> str | None:
    for name, (start, end) in SESSIONS.items():
        if start <= hour_utc < end:
            return name
    return None


def is_trading_session(dt: datetime) -> bool:
    return get_session(dt.hour) is not None


# ── M15 SWING DETECTION ────────────────────────────────────────────────────
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


# ── M15 EQUAL HIGHS / LOWS (sweep targets) ────────────────────────────────
def find_equal_levels(candles: list[dict],
                      swing_indices: list[int],
                      col: str,
                      side: str,
                      pip: float,
                      tol_pips: int = EQ_TOL_PIPS) -> list[dict]:
    """
    Cluster swing points within pip tolerance into equal high/low levels.
    Returns list of level dicts: price, confirmed_at, side, source.
    """
    tol    = tol_pips * pip
    levels = []
    used   = set()
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
                'price':        round(float(np.mean([p for _, p in group])), 5),
                'confirmed_at': max(i for i, _ in group),
                'side':         side,
                'source':       'equal_hl',
                'swept':        False,
            })
        used.add(a)

    return levels


# ── M15 PREVIOUS DAY / SESSION LEVELS (sweep targets) ─────────────────────
def build_external_levels(candles: list[dict]) -> dict[int, list[dict]]:
    """
    Build previous day H/L and previous session H/L levels from M15 candles.
    Used as sweep detection targets only — NOT for TP.
    Returns dict: candle_index → list of levels active from that index.
    """
    levels_by_idx = defaultdict(list)

    by_date = defaultdict(list)
    for idx, c in enumerate(candles):
        by_date[c['time'].date()].append((idx, c))
    sorted_dates = sorted(by_date.keys())

    # Previous Day High / Low
    for d_idx in range(len(sorted_dates) - 1):
        today_data  = by_date[sorted_dates[d_idx]]
        next_day    = sorted_dates[d_idx + 1]
        pdh = max(c['high'] for _, c in today_data)
        pdl = min(c['low']  for _, c in today_data)
        activate_at = by_date[next_day][0][0]
        levels_by_idx[activate_at].append(
            {'price': round(pdh, 5), 'side': 'BSL', 'source': 'PDH', 'swept': False})
        levels_by_idx[activate_at].append(
            {'price': round(pdl, 5), 'side': 'SSL', 'source': 'PDL', 'swept': False})

    # Previous Session High / Low
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
        sh = max(c['high'] for _, c in sess_data)
        sl = min(c['low']  for _, c in sess_data)
        activate_at = by_date_session[next_date][next_sess][0][0]
        levels_by_idx[activate_at].append(
            {'price': round(sh, 5), 'side': 'BSL', 'source': f'{sess}H', 'swept': False})
        levels_by_idx[activate_at].append(
            {'price': round(sl, 5), 'side': 'SSL', 'source': f'{sess}L', 'swept': False})

    return levels_by_idx


# ── H1 TARGET LEVELS (TP sourcing) ────────────────────────────────────────
def build_h1_target_levels(h1_candles: list[dict],
                            lookback: int = H1_LOOKBACK) -> list[dict]:
    """
    Extract H1 swing highs and lows as TP target levels.
    Uses the most recent `lookback` H1 candles only.
    Returns list of level dicts: { price, side, source, swept }
      - BSL = swing high on H1 (target for longs going up)
      - SSL = swing low on H1 (target for shorts going down)
    """
    if not h1_candles or len(h1_candles) < H1_SWING_N * 2 + 1:
        return []

    recent   = h1_candles[-lookback:]
    h1_highs = find_swing_highs(recent, N=H1_SWING_N)
    h1_lows  = find_swing_lows(recent,  N=H1_SWING_N)

    levels = []
    for idx in h1_highs:
        levels.append({
            'price':  round(recent[idx]['high'], 5),
            'side':   'BSL',
            'source': 'H1_swing_high',
            'swept':  False,
        })
    for idx in h1_lows:
        levels.append({
            'price':  round(recent[idx]['low'], 5),
            'side':   'SSL',
            'source': 'H1_swing_low',
            'swept':  False,
        })

    return levels


# ── FVG DETECTION ──────────────────────────────────────────────────────────
def build_fvg_index(candles: list[dict],
                    pip: float,
                    min_gap_pips: int = MIN_FVG_PIPS) -> dict[int, list[dict]]:
    """
    Index FVGs by the candle index they form on (M15).
    Bullish FVG: c3.high < c1.low  — price gap downward (entry for longs)
    Bearish FVG: c3.low  > c1.high — price gap upward   (entry for shorts)
    """
    fvg_index = defaultdict(list)
    min_gap   = min_gap_pips * pip

    for i in range(1, len(candles) - 1):
        c1, c3 = candles[i-1], candles[i+1]

        # Bullish FVG
        if c3['high'] < c1['low']:
            gap = c1['low'] - c3['high']
            if gap >= min_gap:
                fvg_index[i+1].append({
                    'type':     'bullish',
                    'top':      round(c1['low'],  5),
                    'bottom':   round(c3['high'], 5),
                    'formed_i': i + 1,
                })

        # Bearish FVG
        if c3['low'] > c1['high']:
            gap = c3['low'] - c1['high']
            if gap >= min_gap:
                fvg_index[i+1].append({
                    'type':     'bearish',
                    'top':      round(c3['low'],  5),
                    'bottom':   round(c1['high'], 5),
                    'formed_i': i + 1,
                })

    return fvg_index


# ── SWEEP DETECTION ────────────────────────────────────────────────────────
def is_sweep(candle: dict, level_price: float, side: str) -> bool:
    """
    BSL sweep: wick pierces above level, candle closes back below.
    SSL sweep: wick pierces below level, candle closes back above.
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
         h1_candles: list[dict] | None = None,
         h4_candles: list[dict] | None = None,
         pair: str = 'EURUSD') -> list[dict]:
    """
    Main entry point. Called each cron run by signal_engine.py.

    Args:
        m15_candles : M15 candle dicts — time(datetime), open, high, low, close, volume
        h1_candles  : H1 candle dicts from HTF cache (used for TP target levels)
        h4_candles  : H4 candle dicts — accepted, reserved for future bias filter
        pair        : Instrument string e.g. 'EURUSD', 'USDJPY', 'XAUUSD'

    Returns:
        List of signal dicts fired this run.
    """
    pip = get_pip(pair)

    min_candles = SWING_N * 2 + FVG_WINDOW + RETRACE_WIN + 5
    if len(m15_candles) < min_candles:
        logger.warning(f"sweep_fvg [{pair}]: Only {len(m15_candles)} M15 candles — need {min_candles}. Skipping.")
        return []

    signals_fired = []
    fired         = load_fired_signals()
    latest        = m15_candles[-1]
    latest_time   = latest['time']

    # Session gate — only scan during active trading hours
    if not is_trading_session(latest_time):
        logger.info(f"sweep_fvg [{pair}]: Off-hours ({latest_time.hour}:xx UTC), skipping.")
        return []

    current_session = get_session(latest_time.hour)

    # ── Build M15 sweep detection structures ──────────────────────────────
    swing_highs = find_swing_highs(m15_candles)
    swing_lows  = find_swing_lows(m15_candles)

    bsl_eq    = find_equal_levels(m15_candles, swing_highs, 'high', 'BSL', pip)
    ssl_eq    = find_equal_levels(m15_candles, swing_lows,  'low',  'SSL', pip)
    eq_levels = sorted(bsl_eq + ssl_eq, key=lambda x: x['confirmed_at'])

    ext_levels = build_external_levels(m15_candles)
    fvg_index  = build_fvg_index(m15_candles, pip)

    # ── Build M15 active sweep-target pool ────────────────────────────────
    n           = len(m15_candles)
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

    # ── Build H1 TP target pool ───────────────────────────────────────────
    # H1 swing highs → BSL targets (for long TPs)
    # H1 swing lows  → SSL targets (for short TPs)
    h1_levels = build_h1_target_levels(h1_candles) if h1_candles else []
    if not h1_levels:
        logger.info(f"sweep_fvg [{pair}]: No H1 levels available — TP will use fallback (2x SL distance).")

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
            if lv['source'] in WEAK_SOURCES:
                continue

            lv_price = lv['price']
            side     = lv['side']

            if not is_sweep(candle, lv_price, side):
                continue

            lv['swept'] = True
            direction       = 'short' if side == 'BSL' else 'long'
            target_fvg_type = 'bullish' if direction == 'short' else 'bearish'

            # ── Find FVG near the sweep candle ────────────────────────────
            sweep_fvg    = None
            search_start = max(SWING_N, i - FVG_WINDOW)
            for j in range(search_start, i + 2):
                for fvg in fvg_index.get(j, []):
                    if fvg['type'] == target_fvg_type:
                        if direction == 'short' and fvg['top'] <= candle['high']:
                            sweep_fvg = fvg
                            break
                        if direction == 'long' and fvg['bottom'] >= candle['low']:
                            sweep_fvg = fvg
                            break
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
                fvg_age = k - fvg_formed
                if fvg_age > FVG_MAX_AGE:
                    break
                fc = m15_candles[k]
                if direction == 'short':
                    if fc['high'] >= fvg_bottom and fc['low'] <= fvg_top:
                        entry_triggered = True
                        break
                    if fc['low'] < fvg_bottom - 10 * pip:
                        break
                else:
                    if fc['low'] <= fvg_top and fc['high'] >= fvg_bottom:
                        entry_triggered = True
                        break
                    if fc['high'] > fvg_top + 10 * pip:
                        break

            if not entry_triggered:
                continue

            # ── SL beyond sweep wick + buffer ─────────────────────────────
            sl_buf  = SL_BUF_PIPS * pip
            sl_val  = round(
                candle['high'] + sl_buf if direction == 'short'
                else candle['low'] - sl_buf,
                5
            )
            sl_dist = abs(fvg_mid - sl_val)
            if sl_dist < 2 * pip:
                continue

            # ── TP from H1 swing levels ───────────────────────────────────
            # Use the nearest unswept H1 level on the opposing side.
            # Falls back to 3x SL distance if no H1 level is available
            # (ensures MIN_RR is always technically achievable via fallback).
            tp_source = ''
            if direction == 'short':
                cands = [lv2['price'] for lv2 in h1_levels
                         if lv2['side'] == 'SSL'
                         and lv2['price'] < fvg_mid]
                if cands:
                    tp        = round(max(cands), 5)
                    tp_source = 'H1_swing_low'
                else:
                    tp        = round(fvg_mid - sl_dist * MIN_RR, 5)
                    tp_source = 'fallback_3R'
            else:
                cands = [lv2['price'] for lv2 in h1_levels
                         if lv2['side'] == 'BSL'
                         and lv2['price'] > fvg_mid]
                if cands:
                    tp        = round(min(cands), 5)
                    tp_source = 'H1_swing_high'
                else:
                    tp        = round(fvg_mid + sl_dist * MIN_RR, 5)
                    tp_source = 'fallback_3R'

            # ── RR gate — hard floor at MIN_RR (1:3) ─────────────────────
            rr = abs(tp - fvg_mid) / sl_dist
            if rr < MIN_RR:
                logger.info(
                    f"sweep_fvg [{pair}]: Setup dropped — RR {rr:.2f} < {MIN_RR} "
                    f"| entry={fvg_mid} sl={sl_val} tp={tp}"
                )
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
                'h1_bias':    '',   # placeholder for future H4 bias filter
            }

            signal = format_signal(setup)
            signals_fired.append(signal)

            logger.info(
                f"sweep_fvg [{pair}]: Signal fired | {direction.upper()} "
                f"entry={fvg_mid} sl={sl_val} tp={tp} "
                f"rr=1:{rr:.1f} sweep_src={lv['source']} tp_src={tp_source}"
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
            f"tp={r['tp_source']} | {r['session']}"
        )

"""
sweep_fvg.py — Sweep + FVG Fill Strategy Module for EDGE Signal Engine
=======================================================================
Detects liquidity sweeps followed by Fair Value Gap retracements on EURUSD M15.
Runs as a standalone module called from main.py / run.py alongside the existing
Break & Retest engine.

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

# ── STRATEGY PARAMETERS ────────────────────────────────────────────────────
PIP           = 0.0001
SWING_N       = 3          # N candles each side to confirm a swing point
EQ_TOL_PIPS   = 5          # tolerance for equal highs/lows clustering
MIN_FVG_PIPS  = 2          # minimum FVG size in pips
SL_BUF_PIPS   = 2          # buffer beyond sweep wick for stop loss
MIN_RR        = 1.5        # minimum risk/reward ratio to fire signal
LOOKBACK      = 50         # candles before equal H/L levels expire
FVG_WINDOW    = 10         # candles before sweep to search for FVG
RETRACE_WIN   = 30         # candles after sweep to wait for retrace
FVG_MAX_AGE   = 10         # FVG expires if retrace doesn't happen within N candles

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


# ── EQUAL HIGHS / LOWS ─────────────────────────────────────────────────────
def find_equal_levels(candles: list[dict],
                      swing_indices: list[int],
                      col: str,
                      side: str,
                      tol_pips: int = EQ_TOL_PIPS) -> list[dict]:
    """
    Cluster swing points within tolerance into equal high/low levels.
    Returns list of level dicts with price, confirmed_at index, side, source.
    """
    tol = tol_pips * PIP
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
                    min_gap_pips: int = MIN_FVG_PIPS) -> dict[int, list[dict]]:
    """
    Build index of FVGs by the candle index they form on.
    Bullish FVG: gap DOWN — c3.high < c1.low (fill = price moves UP)
    Bearish FVG: gap UP   — c3.low > c1.high (fill = price moves DOWN)
    """
    fvg_index = defaultdict(list)
    min_gap   = min_gap_pips * PIP

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
    entry      = setup['entry']
    sl         = setup['sl']
    tp         = setup['tp']
    rr         = setup['rr']
    session    = setup['session']
    lv_source  = setup['lv_source']
    sweep_time = setup['sweep_time']

    emoji = '🔻' if direction == 'short' else '🔺'
    side  = 'SELL' if direction == 'short' else 'BUY'

    return {
        'strategy':   'Sweep+FVG',
        'pair':       'EURUSD',
        'direction':  direction,
        'side':       side,
        'entry':      entry,
        'sl':         sl,
        'tp':         tp,
        'rr':         round(rr, 2),
        'session':    session,
        'lv_source':  lv_source,
        'sweep_time': sweep_time,
        'fired_at':   setup.get('fired_at', ''),
        'message': (
            f"{emoji} *EURUSD — Sweep+FVG Signal*\n"
            f"Direction : `{side}`\n"
            f"Entry     : `{entry}`\n"
            f"Stop Loss : `{sl}`\n"
            f"Take Profit: `{tp}`\n"
            f"RR        : `1:{rr:.1f}`\n"
            f"Session   : `{session}`\n"
            f"Source    : `{lv_source}`\n"
            f"Swept at  : `{sweep_time}`"
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
    fired[dedup_key] = datetime.now(timezone.utc).isoformat()
    # Prune old entries beyond TTL to keep file small
    now = datetime.now(timezone.utc)
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
def scan(candles: list[dict], send_signal_fn=None) -> list[dict]:
    """
    Main entry point. Called each cron run with the latest M15 candles.

    Args:
        candles        : List of candle dicts with keys:
                         time (datetime), open, high, low, close, volume
        send_signal_fn : Optional callable(signal_dict) to fire Telegram/Sheets

    Returns:
        List of signal dicts fired this run.
    """
    if len(candles) < SWING_N * 2 + FVG_WINDOW + RETRACE_WIN + 5:
        logger.warning("sweep_fvg: Not enough candles to scan.")
        return []

    signals_fired  = []
    fired          = load_fired_signals()   # persistent dedup across runs
    latest         = candles[-1]
    latest_time    = latest['time']

    # Session check
    if not is_trading_session(latest_time):
        logger.info(f"sweep_fvg: Off-hours ({latest_time.hour}:xx UTC), skipping.")
        return []

    current_session = get_session(latest_time.hour)

    # ── Build detection structures ─────────────────────────────────────────
    swing_highs = find_swing_highs(candles)
    swing_lows  = find_swing_lows(candles)

    bsl_eq = find_equal_levels(candles, swing_highs, 'high', 'BSL')
    ssl_eq = find_equal_levels(candles, swing_lows,  'low',  'SSL')
    eq_levels = sorted(bsl_eq + ssl_eq, key=lambda x: x['confirmed_at'])

    ext_levels = build_external_levels(candles)
    fvg_index  = build_fvg_index(candles)

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
    # We scan a window ending at the latest candle so we catch:
    # 1) Sweeps that happened recently and haven't retraced yet
    # 2) Retraces happening right now on this candle
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
                    if fc['low'] < fvg_bottom - 10 * PIP:
                        break
                else:
                    if fc['low'] <= fvg_top and fc['high'] >= fvg_bottom:
                        entry_triggered = True; break
                    if fc['high'] > fvg_top + 10 * PIP:
                        break

            if not entry_triggered:
                continue

            # SL beyond sweep wick
            sl_buf  = SL_BUF_PIPS * PIP
            sl_val  = round(candle['high'] + sl_buf if direction == 'short'
                            else candle['low'] - sl_buf, 5)
            sl_dist = abs(fvg_mid - sl_val)
            if sl_dist < 2 * PIP:
                continue

            # TP at nearest opposing unswept liquidity
            if direction == 'short':
                cands = [lv2['price'] for lv2 in active_lvls
                         if lv2['side'] == 'SSL'
                         and lv2['price'] < fvg_mid
                         and not lv2.get('swept')]
                tp = round(max(cands), 5) if cands else round(fvg_mid - sl_dist * 2, 5)
            else:
                cands = [lv2['price'] for lv2 in active_lvls
                         if lv2['side'] == 'BSL'
                         and lv2['price'] > fvg_mid
                         and not lv2.get('swept')]
                tp = round(min(cands), 5) if cands else round(fvg_mid + sl_dist * 2, 5)

            rr = abs(tp - fvg_mid) / sl_dist
            if rr < MIN_RR:
                continue

            # ── Persistent deduplication check ────────────────────────────
            dedup_key = f"{direction}_{fvg_mid}_{sl_val}"
            if is_already_fired(dedup_key, fired):
                logger.info(f"sweep_fvg: Duplicate suppressed — {dedup_key}")
                continue
            mark_as_fired(dedup_key, fired)

            # ── Signal confirmed ───────────────────────────────────────────
            setup = {
                'direction':  direction,
                'entry':      fvg_mid,
                'sl':         sl_val,
                'tp':         tp,
                'rr':         round(rr, 2),
                'session':    current_session,
                'lv_source':  lv['source'],
                'sweep_time': str(candle['time']),
                'fired_at':   str(latest_time),
            }

            signal = format_signal(setup)
            signals_fired.append(signal)

            logger.info(
                f"sweep_fvg: Signal fired | {direction.upper()} "
                f"entry={fvg_mid} sl={sl_val} tp={tp} "
                f"rr=1:{rr:.1f} source={lv['source']}"
            )

            if send_signal_fn:
                try:
                    send_signal_fn(signal)
                except Exception as e:
                    logger.error(f"sweep_fvg: Failed to send signal: {e}")

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

    # Test on last 500 candles
    test_candles = candles[-500:]
    print(f"Scanning {len(test_candles)} candles...")
    results = scan(test_candles)
    print(f"\nSignals found: {len(results)}")
    for r in results:
        print(f"  {r['direction'].upper():5} | entry={r['entry']} sl={r['sl']} "
              f"tp={r['tp']} rr=1:{r['rr']} | {r['lv_source']} | {r['session']}")

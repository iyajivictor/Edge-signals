"""
sweep_fvg.py — H4 Zone Sweep + M15 FVG Strategy Module
========================================================
v7 — Two-stage signal architecture

Signal lifecycle:
  Stage 1 — FVG FORMATION (scan() fires here):
    · H4 sweep detected + M15 FVG confirmed in aftermath
    · Telegram alert 1: "Set pending order at X"
    · Sheet row written: status = PENDING_ENTRY
    · Pending state saved to sweep_fvg_pending.json

  Stage 2 — ENTRY TRIGGERED (check_pending_entries() detects retrace):
    · Price touches FVG zone on subsequent M15 scan
    · sweep_fvg_live.json entry written → Replit begins monitoring
    · Telegram alert 2: "Entry triggered — trade ACTIVE"
    · Sheet status: PENDING_ENTRY → ACTIVE, entry_time filled

  Stage 3 — TP1 HIT (Replit real-time):
    · Telegram alert 3: "TP1 hit — 50% closed, move SL to BE"
    · Sheet: tp1_outcome = WIN, tp1_close_time filled

  Stage 4 — FINAL OUTCOME (Replit real-time):
    · FULL_WIN / TP1_BE / LOSS
    · Telegram alert 4: final result + pnl_r
    · Sheet: tp2_outcome + pnl_r filled

FVG direction (corrected ICT):
  Bearish FVG ↓: c3['high'] < c1['low']
    top=c1['low'], bottom=c3['high'], SL=c1['high']+buf
  Bullish FVG ↑: c3['low'] > c1['high']
    top=c3['low'], bottom=c1['high'], SL=c1['low']-buf

Sheet columns (SweepFVG tab, 18 cols):
  1 fired_at   5 sl    9  rr_tp2    13 sweep_time   17 tp2_outcome
  2 pair        6 tp1   10 sl_pips   14 status        18 pnl_r
  3 direction   7 tp2   11 zone_src  15 entry_time
  4 entry       8 rr_tp1 12 session  16 tp1_outcome

Pairs: EURUSD GBPUSD USDJPY AUDJPY XAUUSD
       CADJPY USDCAD EURJPY GBPJPY GBPAUD
"""

import json
import os
import logging
from datetime import datetime, timezone
from collections import defaultdict
import numpy as np

logger = logging.getLogger(__name__)

# ── PIP / DP ────────────────────────────────────────────────────────────────
PIP_SIZE = {
    'EURUSD': 0.0001, 'GBPUSD': 0.0001, 'USDCAD': 0.0001, 'GBPAUD': 0.0001,
    'USDJPY': 0.01,   'AUDJPY': 0.01,   'CADJPY': 0.01,
    'EURJPY': 0.01,   'GBPJPY': 0.01,   'XAUUSD': 0.10,
}
DP_SIZE = {
    'EURUSD': 5, 'GBPUSD': 5, 'USDCAD': 5, 'GBPAUD': 5,
    'USDJPY': 3, 'AUDJPY': 3, 'CADJPY': 3,
    'EURJPY': 3, 'GBPJPY': 3, 'XAUUSD': 2,
}
def get_pip(pair): return PIP_SIZE.get(pair, 0.0001)
def get_dp(pair):  return DP_SIZE.get(pair, 5)

# ── PARAMS ───────────────────────────────────────────────────────────────────
ATR_PERIOD      = 14
EQ_ATR_PCT      = 0.20
EQ_MIN_CANDLES  = 2
ZONE_LOOKBACK   = 60
MIN_FVG_PIPS    = 1.5
FVG_M15_WINDOW  = 20
TP1_RR          = 1.5
MIN_RR          = 2.0
MAX_RR          = 6.0
H4_SWING_N      = 3
H4_TP_LOOKBACK  = 40
SIGNAL_TTL_HOURS  = 4    # signal expires after 4h — prevents stale overnight delivery
MAX_SWEEP_AGE_M15 = 6    # drop signal if M15 sweep candle is older than 6 bars (~90 mins)

XAUUSD_CONFIG = {'atr_period': 14, 'atr_buf_mult': 0.25, 'atr_floor_mult': 0.50}

SESSIONS = {'Asian': (0, 6), 'London': (7, 12), 'NewYork': (13, 17)}

PAIR_SESSIONS = {p: {'Asian', 'London'} for p in PIP_SIZE}

# State files
FIRED_FILE   = 'state/sweep_fvg_fired.json'
LIVE_FILE    = 'state/sweep_fvg_live.json'
PENDING_FILE = 'state/sweep_fvg_pending.json'


# ── HELPERS ───────────────────────────────────────────────────────────────────
def get_session(h):
    for name, (s, e) in SESSIONS.items():
        if s <= h < e: return name
    return None

def is_valid_session(pair, dt):
    sess = get_session(dt.hour)
    return sess in PAIR_SESSIONS.get(pair, {'London'}) if sess else False

def calc_atr(candles, period=ATR_PERIOD):
    if len(candles) < 2: return 0.0
    trs = [max(candles[i]['high'] - candles[i]['low'],
               abs(candles[i]['high'] - candles[i-1]['close']),
               abs(candles[i]['low']  - candles[i-1]['close']))
           for i in range(1, len(candles))]
    return float(np.mean(trs[-period:]))

def _load_json(path):
    if os.path.exists(path):
        try:
            with open(path) as f: return json.load(f)
        except Exception: return {}
    return {}

def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f: json.dump(data, f, indent=2)

load_fired   = lambda: _load_json(FIRED_FILE)
save_fired   = lambda d: _save_json(FIRED_FILE, d)
load_live    = lambda: _load_json(LIVE_FILE)
save_live    = lambda d: _save_json(LIVE_FILE, d)
load_pending = lambda: _load_json(PENDING_FILE)
save_pending = lambda d: _save_json(PENDING_FILE, d)

def is_fired(key, fired):
    if key not in fired: return False
    try:
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(fired[key])).total_seconds() / 3600
        return age < SIGNAL_TTL_HOURS
    except Exception: return False

def mark_fired(key, fired):
    now = datetime.now(timezone.utc)
    fired.update({k: v for k, v in fired.items()
                  if (now - datetime.fromisoformat(v)).total_seconds() / 3600
                  < SIGNAL_TTL_HOURS * 2})
    fired[key] = now.isoformat()


# ── H4 ZONE DETECTION ────────────────────────────────────────────────────────
def find_eq_zones(h4, up_to_i, side='high'):
    atr = calc_atr(h4[max(0, up_to_i - ATR_PERIOD):up_to_i])
    if atr <= 0: return []
    tol    = atr * EQ_ATR_PCT
    window = h4[max(0, up_to_i - ZONE_LOOKBACK):up_to_i]
    vals   = [(i, c['high'] if side == 'high' else c['low'])
              for i, c in enumerate(window)]
    zones  = []; used = set()
    for i, (ci, vi) in enumerate(vals):
        if ci in used: continue
        cluster = [ci]; cv = [vi]
        for j, (cj, vj) in enumerate(vals):
            if j == i or cj in used: continue
            if abs(vj - vi) <= tol: cluster.append(cj); cv.append(vj)
        if len(cluster) >= EQ_MIN_CANDLES:
            for c in cluster: used.add(c)
            zones.append({'top': round(max(cv), 5), 'bottom': round(min(cv), 5),
                          'touches': len(cluster),
                          'side': 'BSL' if side == 'high' else 'SSL'})
    return zones

def is_bsl_sweep(c, z): return c['high'] > z['top']    and c['close'] < z['top']
def is_ssl_sweep(c, z): return c['low']  < z['bottom'] and c['close'] > z['bottom']


# ── M15 FVG INDEX ─────────────────────────────────────────────────────────────
def build_fvg_index(candles, pip):
    idx = defaultdict(list); min_gap = MIN_FVG_PIPS * pip
    for i in range(1, len(candles) - 1):
        c1, c3 = candles[i-1], candles[i+1]
        if c3['high'] < c1['low'] and (c1['low'] - c3['high']) >= min_gap:
            idx[i+1].append({'type': 'bearish', 'top': round(c1['low'], 5),
                'bottom': round(c3['high'], 5), 'sl_anchor': round(c1['high'], 5),
                'formed_i': i+1})
        if c3['low'] > c1['high'] and (c3['low'] - c1['high']) >= min_gap:
            idx[i+1].append({'type': 'bullish', 'top': round(c3['low'], 5),
                'bottom': round(c1['high'], 5), 'sl_anchor': round(c1['low'], 5),
                'formed_i': i+1})
    return idx


# ── H4 TP2 SOURCING ───────────────────────────────────────────────────────────
def get_h4_tp2(h4, up_to_i, entry, sl_dist, direction):
    if sl_dist <= 0: return None, None
    start  = max(H4_SWING_N, up_to_i - H4_TP_LOOKBACK)
    window = h4[start:up_to_i]; N = H4_SWING_N
    atr    = calc_atr(h4[max(0, up_to_i - ATR_PERIOD):up_to_i])
    tol    = atr * EQ_ATR_PCT if atr > 0 else 1e-9
    levels = []
    if direction == 'short':
        for i in range(N, len(window) - N):
            l = window[i]['low']
            if all(l < window[i-j]['low'] for j in range(1, N+1)) and \
               all(l < window[i+j]['low'] for j in range(1, N+1)): levels.append(round(l, 5))
        bkts = defaultdict(list)
        for c in window: bkts[round(c['low'] / tol) * tol].append(c['low'])
        for g in bkts.values():
            if len(g) >= EQ_MIN_CANDLES: levels.append(round(min(g), 5))
        cands = [(lv, (entry - lv) / sl_dist) for lv in set(levels)
                 if lv < entry and MIN_RR <= (entry - lv) / sl_dist <= MAX_RR]
        if not cands: return None, None
        cands.sort(key=lambda x: x[0], reverse=True)
    else:
        for i in range(N, len(window) - N):
            h = window[i]['high']
            if all(h > window[i-j]['high'] for j in range(1, N+1)) and \
               all(h > window[i+j]['high'] for j in range(1, N+1)): levels.append(round(h, 5))
        bkts = defaultdict(list)
        for c in window: bkts[round(c['high'] / tol) * tol].append(c['high'])
        for g in bkts.values():
            if len(g) >= EQ_MIN_CANDLES: levels.append(round(max(g), 5))
        cands = [(lv, (lv - entry) / sl_dist) for lv in set(levels)
                 if lv > entry and MIN_RR <= (lv - entry) / sl_dist <= MAX_RR]
        if not cands: return None, None
        cands.sort(key=lambda x: x[0])
    return round(cands[0][0], 5), round(cands[0][1], 2)


# ── TELEGRAM MESSAGE FORMATTERS ───────────────────────────────────────────────
def format_pending_message(sig):
    pair  = sig['pair']; dp = get_dp(pair)
    side  = 'SELL' if sig['direction'] == 'short' else 'BUY'
    emoji = '🔻' if side == 'SELL' else '🔺'
    return (
        f"{emoji} *{pair} — H4 Zone Sweep*\n\n"
        f"_FVG confirmed. Set pending order._\n\n"
        f"Direction : `{side}`\n"
        f"Entry     : `{sig['entry']:.{dp}f}`  ← pending order\n"
        f"Stop Loss : `{sig['sl']:.{dp}f}`\n"
        f"TP1 (50%) : `{sig['tp1']:.{dp}f}`  RR `1:{sig['rr_tp1']:.1f}`\n"
        f"TP2 (50%) : `{sig['tp2']:.{dp}f}`  RR `1:{sig['rr_tp2']:.1f}`\n\n"
        f"Session   : `{sig['session']}`\n"
        f"Zone      : `{sig['zone_src']}`\n"
        f"Swept at  : `{sig['sweep_time']}`\n\n"
        f"_⏳ Waiting for retrace into FVG..._"
    )

def format_entry_message(sig):
    pair  = sig['pair']; dp = get_dp(pair)
    side  = 'SELL' if sig['direction'] == 'short' else 'BUY'
    return (
        f"⚡ *{pair} — Entry Triggered!*\n\n"
        f"Direction : `{side}`\n"
        f"Entry     : `{sig['entry']:.{dp}f}` ✅\n"
        f"Stop Loss : `{sig['sl']:.{dp}f}`\n"
        f"TP1 (50%) : `{sig['tp1']:.{dp}f}`\n"
        f"TP2 (50%) : `{sig['tp2']:.{dp}f}`\n\n"
        f"_🟢 Trade ACTIVE. Monitoring started._"
    )

def format_tp1_alert(sig):
    pair = sig['pair']; dp = get_dp(pair)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'
    return (
        f"🎯 *{pair} — TP1 Hit!*\n\n"
        f"Direction : `{side}`\n"
        f"TP1       : `{sig['tp1']:.{dp}f}` ✅  50% closed\n"
        f"SL → BE   : `{sig['entry']:.{dp}f}`  ← move stop now\n"
        f"TP2 active: `{sig['tp2']:.{dp}f}`  RR `1:{sig['rr_tp2']:.1f}`\n\n"
        f"_Remainder running to TP2._"
    )


# ── STAGE 1: scan() ────────────────────────────────────────────────────────────
def scan(m15_candles, h1_candles=None, h4_candles=None, pair='EURUSD'):
    """
    Called every 15min by signal_engine.py.
    Detects H4 sweep + M15 FVG formation.
    Fires Stage 1 alert at FVG confirmation (not retrace).
    Returns list of stage-1 signals for signal_engine to send.
    """
    pip     = get_pip(pair)
    is_gold = (pair == 'XAUUSD')

    if len(m15_candles) < FVG_M15_WINDOW + 5:
        logger.warning(f"[{pair}] Insufficient M15 candles"); return []
    if not h4_candles or len(h4_candles) < ZONE_LOOKBACK + H4_SWING_N * 2 + 5:
        logger.warning(f"[{pair}] Insufficient H4 candles"); return []

    latest_time = m15_candles[-1]['time']

    gold_sl_buf = gold_sl_floor = None
    if is_gold:
        cfg = XAUUSD_CONFIG
        atr = calc_atr(m15_candles[-20:], period=cfg['atr_period'])
        if atr <= 0: logger.warning("[XAUUSD] ATR=0"); return []
        gold_sl_buf   = atr * cfg['atr_buf_mult']
        gold_sl_floor = atr * cfg['atr_floor_mult']

    fired   = load_fired()
    pending = load_pending()
    signals = []

    fvg_index = build_fvg_index(m15_candles, pip)
    n_m15     = len(m15_candles)
    n_h4      = len(h4_candles)
    m15_ts    = [c['time'] for c in m15_candles]

    def first_m15_at(ts):
        lo, hi = 0, n_m15
        while lo < hi:
            mid = (lo + hi) // 2
            if m15_ts[mid] < ts: lo = mid + 1
            else: hi = mid
        return lo if lo < n_m15 else None

    for h4_i in range(max(ZONE_LOOKBACK + 1, n_h4 - 3), n_h4):
        h4c     = h4_candles[h4_i]
        h4_time = h4c['time']
        ms_start = first_m15_at(h4_time)
        if ms_start is None: continue

        setups = (
            [(z, 'short', 'bearish') for z in find_eq_zones(h4_candles, h4_i, 'high')] +
            [(z, 'long',  'bullish') for z in find_eq_zones(h4_candles, h4_i, 'low')]
        )
        for zone, direction, fvg_type in setups:
            if direction == 'short' and not is_bsl_sweep(h4c, zone): continue
            if direction == 'long'  and not is_ssl_sweep(h4c, zone): continue

            # Zone dedup — one signal per zone per calendar day
            # Daily lock prevents same zone firing on consecutive H4 boundaries
            zone_key  = f"LVL_{pair}_{zone['side']}_{zone['top']:.5f}_{h4_time.strftime('%Y%m%d')}"
            if is_fired(zone_key, fired): continue

            # Find exact M15 candle where sweep occurred
            # More precise than H4 candle open — sweep is a wick event
            # that happens within minutes, not hours
            sweep_m15_time = None
            for m15_i in range(ms_start, min(n_m15, ms_start + 16)):
                mc = m15_candles[m15_i]
                if direction == 'short' and mc['high'] > zone['top']:
                    sweep_m15_time = mc['time']; break
                if direction == 'long'  and mc['low']  < zone['bottom']:
                    sweep_m15_time = mc['time']; break

            # Fall back to H4 open if M15 sweep candle not found
            sweep_m15_time = sweep_m15_time or h4_time

            # Staleness check using M15 sweep time — much tighter than H4
            # MAX_SWEEP_AGE_M15 candles × 15 mins = maximum age in minutes
            sweep_age_min = (latest_time - sweep_m15_time).total_seconds() / 60
            if sweep_age_min > MAX_SWEEP_AGE_M15 * 15:
                logger.info(f"[{pair}] Stale sweep ({sweep_age_min:.0f}m old, "
                            f"limit {MAX_SWEEP_AGE_M15 * 15}m) — skipping")
                continue

            # Find FVG in M15 aftermath
            sweep_fvg = None
            for j in range(ms_start, min(n_m15, ms_start + FVG_M15_WINDOW)):
                for fvg in fvg_index.get(j, []):
                    if fvg['type'] == fvg_type: sweep_fvg = fvg; break
                if sweep_fvg: break
            if not sweep_fvg: continue

            fvg_top    = sweep_fvg['top']
            fvg_bottom = sweep_fvg['bottom']
            fvg_mid    = round((fvg_top + fvg_bottom) / 2, 5)
            sl_anchor  = sweep_fvg['sl_anchor']

            if is_gold:
                sl_val  = round(sl_anchor + gold_sl_buf if direction == 'short'
                                else sl_anchor - gold_sl_buf, 5)
                sl_dist = abs(fvg_mid - sl_val)
                if sl_dist < gold_sl_floor: continue
            else:
                buf     = 2 * pip
                sl_val  = round(sl_anchor + buf if direction == 'short'
                                else sl_anchor - buf, 5)
                sl_dist = abs(fvg_mid - sl_val)
                if sl_dist < 2 * pip: continue

            tp1       = round(fvg_mid - TP1_RR * sl_dist if direction == 'short'
                              else fvg_mid + TP1_RR * sl_dist, 5)
            tp2, rr2  = get_h4_tp2(h4_candles, h4_i, fvg_mid, sl_dist, direction)
            if tp2 is None: continue

            setup_key = f"{pair}_{direction}_{fvg_mid:.5f}_{h4_time.strftime('%Y%m%d%H')}"
            if is_fired(setup_key, fired): continue
            mark_fired(zone_key, fired); mark_fired(setup_key, fired)

            # Capture exact M15 candle when FVG was confirmed
            fvg_formed_at = m15_candles[sweep_fvg['formed_i']]['time'].strftime('%Y-%m-%d %H:%M UTC') \
                            if sweep_fvg.get('formed_i') and sweep_fvg['formed_i'] < n_m15 \
                            else now_str

            now_str      = latest_time.strftime('%Y-%m-%d %H:%M UTC')
            sweep_ts_str = sweep_m15_time.strftime('%Y-%m-%d %H:%M UTC')
            session      = get_session(latest_time.hour) or 'London'
            pending_key  = setup_key

            sig = {
                'pair':          pair,
                'direction':     direction,
                'entry':         fvg_mid,
                'sl':            sl_val,
                'tp1':           tp1,
                'tp2':           tp2,
                'rr_tp1':        round(TP1_RR, 2),
                'rr_tp2':        rr2,
                'sl_pips':       round(sl_dist / pip, 1),
                'session':       session,
                'zone_src':      f"{zone['side']}_{zone['touches']}touch",
                'fired_at':      now_str,
                'sweep_time':    sweep_ts_str,
                'fvg_formed_at': fvg_formed_at,
                'pending_key':   pending_key,
                'fvg_top':       fvg_top,
                'fvg_bottom':    fvg_bottom,
            }

            # Save to pending state (stage 1 → 2 bridge)
            pending[pending_key] = {
                **sig,
                'status':     'PENDING_ENTRY',
                'created_at': now_str,
            }

            sig['message'] = format_pending_message(sig)
            signals.append(sig)
            logger.info(
                f"[{pair}] ✅ Stage 1 | {direction.upper()} entry={fvg_mid} "
                f"sl={sl_val} tp1={tp1} tp2={tp2} rr=1:{rr2}"
            )

    save_fired(fired)
    save_pending(pending)
    return signals


# ── STAGE 2: check_pending_entries() ─────────────────────────────────────────
def check_pending_entries(m15_candles, pair):
    """
    Called every 15min after scan().
    Monitors PENDING_ENTRY signals for this pair.
    When price retraces into FVG → writes to live state, returns entry signals.
    """
    pending   = load_pending()
    live      = load_live()
    pip       = get_pip(pair)
    triggered = []

    pair_pending = {k: v for k, v in pending.items()
                    if v.get('pair') == pair and v.get('status') == 'PENDING_ENTRY'}
    if not pair_pending: return []

    latest = m15_candles[-1]

    for pk, sig in pair_pending.items():
        fvg_top    = sig['fvg_top']
        fvg_bottom = sig['fvg_bottom']
        direction  = sig['direction']

        # Same-run guard — skip signals created in this run
        # Prevents Stage 1 + Stage 2 firing simultaneously when price
        # is already inside the FVG at the moment of detection
        try:
            created = datetime.fromisoformat(
                sig.get('created_at', '').replace(' UTC', '+00:00'))
            age_min = (datetime.now(timezone.utc) - created).total_seconds() / 60
            if age_min < 1:
                logger.info(f"[{pair}] Skipping same-run pending entry — created {age_min:.1f}m ago")
                continue
        except Exception:
            pass

        # Staleness check — drop if created more than SIGNAL_TTL_HOURS ago
        try:
            created = datetime.fromisoformat(
                sig.get('created_at', '').replace(' UTC', '+00:00'))
            age_h = (datetime.now(timezone.utc) - created).total_seconds() / 3600
            if age_h > SIGNAL_TTL_HOURS:
                pending[pk]['status'] = 'INVALIDATED'
                logger.info(f"[{pair}] Pending expired ({age_h:.1f}h old) — dropping")
                continue
        except Exception:
            pass

        # Retrace check
        if direction == 'short':
            triggered_entry = latest['high'] >= fvg_bottom and latest['low'] <= fvg_top
            invalidated     = latest['low'] < fvg_bottom - 10 * pip
        else:
            triggered_entry = latest['low'] <= fvg_top and latest['high'] >= fvg_bottom
            invalidated     = latest['high'] > fvg_top + 10 * pip

        if invalidated:
            pending[pk]['status'] = 'INVALIDATED'
            logger.info(f"[{pair}] Pending invalidated — price broke through FVG")
            continue

        if not triggered_entry:
            continue

        # Entry confirmed
        entry_time_str          = latest['time'].strftime('%Y-%m-%d %H:%M UTC')
        pending[pk]['status']   = 'ACTIVE'
        pending[pk]['entry_time'] = entry_time_str

        # Write to live state — Replit monitoring begins
        live[pk] = {
            'pair':       sig['pair'],
            'direction':  sig['direction'],
            'entry':      sig['entry'],
            'sl':         sig['sl'],
            'tp1':        sig['tp1'],
            'tp2':        sig['tp2'],
            'rr_tp1':     sig['rr_tp1'],
            'rr_tp2':     sig['rr_tp2'],
            'sl_pips':    sig['sl_pips'],
            'fired_at':   sig['fired_at'],
            'entry_time': entry_time_str,
            'tp1_hit':    False,
            'status':     'ACTIVE',
        }

        sig['entry_message'] = format_entry_message(sig)
        sig['entry_time']    = entry_time_str
        triggered.append(sig)
        logger.info(
            f"[{pair}] ⚡ Stage 2 — Entry triggered | "
            f"{direction.upper()} @ {sig['entry']} | {entry_time_str}"
        )

    save_pending(pending)
    save_live(live)
    return triggered


# ── PENDING CLEANUP ────────────────────────────────────────────────────────────
def cleanup_expired_pending():
    """Remove expired or resolved entries from pending state."""
    pending = load_pending()
    now     = datetime.now(timezone.utc)
    drop    = []
    for pk, sig in pending.items():
        if sig.get('status') in ('ACTIVE', 'INVALIDATED'):
            drop.append(pk)
            continue
        try:
            created = datetime.fromisoformat(
                sig.get('created_at', now.isoformat()).replace(' UTC', '+00:00')
            )
            if (now - created).total_seconds() / 3600 > SIGNAL_TTL_HOURS:
                drop.append(pk)
        except Exception:
            drop.append(pk)
    for pk in drop: pending.pop(pk, None)
    if drop: logger.info(f"sweep_fvg: Cleaned {len(drop)} pending entries")
    save_pending(pending)

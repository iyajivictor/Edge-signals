"""
EDGE Sweep+FVG Strategy Module — v2
=====================================
Detects H4 zone sweeps on M15 timeframe and confirms
Fair Value Gap (FVG) formation for entry.

Pipeline:
  ZoneManager    → 3-layer H4 zone validation
  SweepDetector  → M15 sweep detection + session gate
  FVGScanner     → FVG detection + composite zone selection
  RiskManager    → SL + TP computation
  DedupManager   → 3-key deduplication
  SignalBuilder  → final signal dict + Telegram message
  scan()         → main entry point (called by signal_engine.py)

Strategy Logic:
  1. H4 EQ zones identified (BSL/SSL) — 120 candle lookback
     Layer 2: mitigation filter (no close through zone)
     Layer 3: recency proximity (approached within 20 H4 bars)

  2. M15 price closes inside zone → N=14 counter starts
     Zone must hold (no close beyond boundary) during N
     Sweep candle within N=14:
       - Wick pierces zone >= 1× ATR14
       - Close back inside zone
       - Close in correct half of candle range

  3. FVG search — N=5 M15 candles forward from sweep
     Include C-1 (sweep can be the displacement)
     Every consecutive triplet checked
     Multiple FVGs → composite zone → premium/discount selection

  4. SL = sweep_open ± (0.10 × ATR14)
     Cap: if SL crosses sweep high/low
          → SL = sweep high/low ± (0.10 × pip)

  5. TP = nearest opposing H4 liquidity cluster near edge
     DUAL TP if RR_to_equilibrium >= 2.5
     SINGLE TP if RR_to_cluster >= 2.0
     DISCARD if RR_to_cluster < 2.0

Session gates (WAT = UTC+1):
  Group 1 (EURUSD,GBPUSD,USDCAD,GBPAUD,XAUUSD): 06:00-20:00 UTC
  Group 2 (USDJPY,AUDJPY,CADJPY,EURJPY,GBPJPY):  06:00-15:00 UTC

Changes vs v1:
  · Full rework — all detection logic replaced
  · M15-based sweep detection (was H4 candle close)
  · N=14 zone monitoring window
  · ATR-based SL (was C1 high/low)
  · Opposing cluster TP (was fixed RR multiples)
  · Adaptive DUAL/SINGLE TP system
  · 3-layer zone validation
  · check_pending_entries() removed — Replit handles Phase 0
  · Stage 2 fully removed from this module

Changes vs v2 (this file — v3):
  · Pre-fire price validation gate added in scan() before SignalBuilder:
      1. Direction/price-side check — for shorts, current_price must be
         ABOVE entry (FVG gap still open and reachable). For longs,
         current_price must be BELOW entry. Prevents unactionable signals
         where the FVG was consumed before the alert fired due to the
         15-min scheduler lag.
      2. Zone integrity check — BSL zone must not have closed above zone
         top, SSL zone must not have closed below zone bottom at the
         moment the signal fires.
      Both checks use m15_candles[-1]['close'] (most recent bar).
  · DedupManager zone lock now expires after 24 hours (ZONE_TTL_MINS=1440)
      Previously zone keys were permanent — _prune() skipped ZONE_ keys
      entirely, meaning a zone that fired once could never fire again even
      after a fresh sweep days later. This was the primary cause of signal
      frequency collapse. Zone TTL now consistent with sweep/signal TTL
      pattern. is_zone_fired() updated to use _is_expired() check.
  · Telegram message updated to note price validation
"""

import os
import json
import logging
import numpy as np
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
PENDING_FILE     = 'state/sweep_fvg_pending.json'
FIRED_FILE       = 'state/sweep_fvg_fired.json'
LIVE_FILE        = 'state/sweep_fvg_live.json'
PENDING_TTL_MINS = 60

PIP_SIZE = {
    'EURUSD': 0.0001, 'GBPUSD': 0.0001,
    'USDCAD': 0.0001, 'GBPAUD': 0.0001,
    'USDJPY': 0.01,   'AUDJPY': 0.01,
    'CADJPY': 0.01,   'EURJPY': 0.01,
    'GBPJPY': 0.01,   'XAUUSD': 0.10,
}

DP_SIZE = {
    'EURUSD': 5, 'GBPUSD': 5,
    'USDCAD': 5, 'GBPAUD': 5,
    'USDJPY': 3, 'AUDJPY': 3,
    'CADJPY': 3, 'EURJPY': 3,
    'GBPJPY': 3, 'XAUUSD': 2,
}


# ── STATE HELPERS ─────────────────────────────────────────────────────────────
def _load_json(path: str) -> dict:
    """Load JSON state file. Returns empty dict if missing or corrupt."""
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_json(path: str, data: dict):
    """Persist dict to JSON state file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


load_pending = lambda: _load_json(PENDING_FILE)
save_pending = lambda d: _save_json(PENDING_FILE, d)
load_live    = lambda: _load_json(LIVE_FILE)
save_live    = lambda d: _save_json(LIVE_FILE, d)


# ── ZONE MANAGER ──────────────────────────────────────────────────────────────
class ZoneManager:
    """
    Manages the three-layer H4 zone validation pipeline:
      Layer 1 — Lookback    : 120 H4 candles
      Layer 2 — Mitigation  : structure-based freshness
      Layer 3 — Recency     : proximity anchor (1× ATR14)

    Instantiated once per scan() call with fresh H4 data.
    All three layers run sequentially — a zone must pass
    all three to be returned as valid.
    """

    LOOKBACK        = 120   # H4 candles
    RECENCY_WINDOW  = 20    # H4 candles for proximity check
    EQ_MIN_CANDLES  = 2     # minimum touches to form zone
    EQ_ATR_PCT      = 0.20  # clustering tolerance as % of ATR

    def __init__(self, h4_candles: list, pair: str):
        self.pair    = pair
        self.candles = h4_candles[-self.LOOKBACK:]
        self.n       = len(self.candles)
        self.atr     = self._calc_atr()
        self._zones  = None

    def _calc_atr(self, period: int = 14) -> float:
        if len(self.candles) < 2:
            return 0.0
        trs = [
            max(
                self.candles[i]['high'] - self.candles[i]['low'],
                abs(self.candles[i]['high'] - self.candles[i-1]['close']),
                abs(self.candles[i]['low']  - self.candles[i-1]['close'])
            )
            for i in range(1, len(self.candles))
        ]
        return float(np.mean(trs[-period:]))

    def _find_eq_zones(self, side: str) -> list:
        """
        Cluster H4 highs (BSL) or lows (SSL) into EQ zones.
        side: 'high' → BSL | 'low' → SSL
        """
        if self.atr <= 0:
            return []

        tol   = self.atr * self.EQ_ATR_PCT
        vals  = [
            (i, c['high'] if side == 'high' else c['low'])
            for i, c in enumerate(self.candles)
        ]
        zones = []
        used  = set()

        for i, (ci, vi) in enumerate(vals):
            if ci in used:
                continue
            cluster = [ci]
            cv      = [vi]
            for j, (cj, vj) in enumerate(vals):
                if j == i or cj in used:
                    continue
                if abs(vj - vi) <= tol:
                    cluster.append(cj)
                    cv.append(vj)
            if len(cluster) >= self.EQ_MIN_CANDLES:
                for c in cluster:
                    used.add(c)
                zones.append({
                    'side'        : 'BSL' if side == 'high' else 'SSL',
                    'top'         : round(max(cv), 5),
                    'bottom'      : round(min(cv), 5),
                    'touches'     : len(cluster),
                    'formed_i'    : max(cluster),  # most recent touch — mitigation ref
                    'oldest_i'    : min(cluster),  # first touch — zone age ref
                    'mitigated'   : False,
                    'in_proximity': False,
                })

        return zones

    def _apply_mitigation(self, zones: list) -> list:
        """
        Remove zones where price closed through boundary
        AFTER the zone was formed.
        BSL dead: subsequent H4 close > zone_top
        SSL dead: subsequent H4 close < zone_bottom
        """
        live = []
        for zone in zones:
            formed_i   = zone['formed_i']
            subsequent = self.candles[formed_i + 1:]
            mitigated  = False

            for c in subsequent:
                if zone['side'] == 'BSL' and c['close'] > zone['top']:
                    mitigated = True
                    break
                if zone['side'] == 'SSL' and c['close'] < zone['bottom']:
                    mitigated = True
                    break

            if not mitigated:
                live.append(zone)

        return live

    def _apply_recency_proximity(self, zones: list) -> list:
        """
        Keep only zones approached within RECENCY_WINDOW H4 candles
        without closing through.
        Proximity threshold: 1× ATR14 from zone boundary.
        BSL: recent high >= zone_top - ATR AND close < zone_top
        SSL: recent low  <= zone_bottom + ATR AND close > zone_bottom
        """
        recent  = self.candles[-self.RECENCY_WINDOW:]
        in_play = []

        for zone in zones:
            approached = False
            for c in recent:
                if zone['side'] == 'BSL':
                    if (c['high'] >= zone['top'] - self.atr and
                            c['close'] < zone['top']):
                        approached = True
                        break
                else:
                    if (c['low'] <= zone['bottom'] + self.atr and
                            c['close'] > zone['bottom']):
                        approached = True
                        break

            if approached:
                zone['in_proximity'] = True
                in_play.append(zone)

        return in_play

    def get_valid_zones(self) -> dict:
        """
        Run all three layers sequentially.
        Returns {'bsl': [...], 'ssl': [...]}.
        Lazy-loaded and cached.
        """
        if self._zones is not None:
            return self._zones

        raw_bsl   = self._find_eq_zones('high')
        raw_ssl   = self._find_eq_zones('low')
        live_bsl  = self._apply_mitigation(raw_bsl)
        live_ssl  = self._apply_mitigation(raw_ssl)
        valid_bsl = self._apply_recency_proximity(live_bsl)
        valid_ssl = self._apply_recency_proximity(live_ssl)

        self._zones = {'bsl': valid_bsl, 'ssl': valid_ssl}
        return self._zones

    def summary(self) -> str:
        z = self.get_valid_zones()
        return (
            f"[ZoneManager:{self.pair}] "
            f"H4={self.n} ATR={self.atr:.5f} "
            f"BSL={len(z['bsl'])} SSL={len(z['ssl'])} valid zones"
        )


# ── SWEEP DETECTOR ────────────────────────────────────────────────────────────
class SweepDetector:
    """
    Detects institutional sweep events on M15 candles
    against valid H4 zones from ZoneManager.

    For each valid zone:
      1. M15 close inside zone → start N=14 counter
         (no session gate — engine runs 24/7)
      2. Monitor zone integrity within N
      3. Detect valid sweep candle within N:
           - Wick pierces zone >= 1× ATR14
           - Close back inside zone
           - Close in correct half of candle range
      4. Return sweep event dict for FVGScanner

    Session logged to analytics only — no gating.
    """

    N_COUNTER      = 14
    MIN_WICK_ATR   = 1.0
    RECENT_CANDLES = 200   # ~50 hours of M15 — prevents ancient zone entries

    def __init__(self, pair: str, zones: dict,
                 m15_candles: list, atr_m15: float):
        self.pair    = pair
        self.zones   = zones
        self.candles = m15_candles
        self.atr     = atr_m15
        self.n       = len(m15_candles)
        self._sweeps = None

    @staticmethod
    def calc_atr_m15(candles: list, period: int = 14) -> float:
        if len(candles) < 2:
            return 0.0
        trs = [
            max(
                candles[i]['high'] - candles[i]['low'],
                abs(candles[i]['high'] - candles[i-1]['close']),
                abs(candles[i]['low']  - candles[i-1]['close'])
            )
            for i in range(1, len(candles))
        ]
        return float(np.mean(trs[-period:]))

    def _find_zone_entry(self, zone: dict, start_i: int) -> int | None:
        """
        Find first M15 candle that CLOSES inside zone.
        No session gate — runs 24/7.
        """
        for i in range(start_i, self.n):
            c     = self.candles[i]
            close = c['close']
            if zone['bottom'] <= close <= zone['top']:
                return i
        return None

    def _is_valid_sweep(self, c: dict, zone: dict) -> bool:
        """
        Validate M15 candle as genuine institutional sweep.
        All four conditions must be satisfied:
          1. Wick pierces zone boundary
          2. Close back inside zone
          3. Wick beyond zone >= 1× ATR14
          4. Close in correct half of candle range
             (bearish: bottom half | bullish: top half)
        """
        candle_range = c['high'] - c['low']
        if candle_range <= 0:
            return False

        close_position = (c['close'] - c['low']) / candle_range

        if zone['side'] == 'BSL':
            wick_beyond  = c['high'] - zone['top']
            wick_pierces = c['high'] > zone['top']
            close_inside = c['close'] < zone['top']
            wick_valid   = wick_beyond >= self.MIN_WICK_ATR * self.atr
            body_valid   = close_position < 0.5
        else:
            wick_beyond  = zone['bottom'] - c['low']
            wick_pierces = c['low'] < zone['bottom']
            close_inside = c['close'] > zone['bottom']
            wick_valid   = wick_beyond >= self.MIN_WICK_ATR * self.atr
            body_valid   = close_position > 0.5

        return all([wick_pierces, close_inside, wick_valid, body_valid])

    def _find_sweep(self, zone: dict, entry_i: int) -> tuple:
        """
        Search N=14 candles from zone entry for a valid sweep.
        Checks zone integrity on every bar.

        Returns tuple (reason, payload):
          ('found',     sweep_dict) — valid sweep — zone consumed
          ('breakout',  i)          — close through boundary — zone dead
          ('exhausted', window_end) — N=14 ran out, price still in zone
                                      setup dropped but zone alive
          ('exited',    i)          — price closed cleanly outside zone
                                      within N=14, no break, no sweep
                                      zone still valid
          ('end',       None)       — hit end of candle array
        """
        window_end = min(entry_i + self.N_COUNTER, self.n)

        for i in range(entry_i, window_end):
            c = self.candles[i]

            # ── Breakout check — close through zone boundary ──────────────
            if zone['side'] == 'BSL' and c['close'] > zone['top']:
                logger.info(
                    f"[{self.pair}] BSL {zone['top']:.5f} "
                    f"BREAKOUT bar {i} ({c['time']}) — zone dead"
                )
                return ('breakout', i)

            if zone['side'] == 'SSL' and c['close'] < zone['bottom']:
                logger.info(
                    f"[{self.pair}] SSL {zone['bottom']:.5f} "
                    f"BREAKOUT bar {i} ({c['time']}) — zone dead"
                )
                return ('breakout', i)

            # ── Clean exit — close moved back outside zone, no break ──────
            # BSL zone: price below zone bottom (exited downward, no breakout)
            # SSL zone: price above zone top (exited upward, no breakout)
            if zone['side'] == 'BSL' and c['close'] < zone['bottom']:
                logger.info(
                    f"[{self.pair}] BSL clean exit bar {i} ({c['time']}) "
                    f"— zone valid, awaiting re-entry"
                )
                return ('exited', i)

            if zone['side'] == 'SSL' and c['close'] > zone['top']:
                logger.info(
                    f"[{self.pair}] SSL clean exit bar {i} ({c['time']}) "
                    f"— zone valid, awaiting re-entry"
                )
                return ('exited', i)

            # ── Sweep validation ──────────────────────────────────────────
            if self._is_valid_sweep(c, zone):
                n_val = i - entry_i + 1
                logger.info(
                    f"[{self.pair}] ✅ Sweep | "
                    f"{zone['side']} @ {c['time']} "
                    f"H={c['high']} L={c['low']} C={c['close']} "
                    f"N={n_val}"
                )
                return ('found', {
                    'zone'           : zone,
                    'direction'      : 'short' if zone['side'] == 'BSL' else 'long',
                    'sweep_candle'   : c,
                    'sweep_i'        : i,
                    'sweep_time'     : c['time'],
                    'entry_i'        : entry_i,
                    'atr'            : self.atr,
                    'n_counter_value': n_val,
                })

        # N=14 exhausted — price stayed in zone all 14 bars
        # Setup is dead for this entry attempt
        # Zone is still alive unless boundary gets broken later
        if window_end >= self.n:
            return ('end', None)

        logger.info(
            f"[{self.pair}] N=14 exhausted — "
            f"{zone['side']} {zone['top']:.5f} | "
            f"setup dropped, zone alive — monitoring"
        )
        return ('exhausted', window_end)

    def _monitor_post_exhaustion(self, zone: dict, from_i: int) -> tuple:
        """
        Called after N=14 exhaustion (price stayed in zone all 14 bars).
        Scans forward bar by bar indefinitely until one of:

          ('breakout', i) — close through zone boundary — zone dead
          ('exited',   i) — close outside zone cleanly — zone valid,
                            re-entry will restart the counter
          ('end',      None) — hit end of candle array

        This monitoring has no time limit — the zone remains in this
        watching state for as long as the candle history allows.
        """
        for i in range(from_i, self.n):
            c = self.candles[i]

            # Breakout — zone dies
            if zone['side'] == 'BSL' and c['close'] > zone['top']:
                logger.info(
                    f"[{self.pair}] BSL {zone['top']:.5f} "
                    f"post-exhaustion BREAKOUT bar {i} — zone dead"
                )
                return ('breakout', i)

            if zone['side'] == 'SSL' and c['close'] < zone['bottom']:
                logger.info(
                    f"[{self.pair}] SSL {zone['bottom']:.5f} "
                    f"post-exhaustion BREAKOUT bar {i} — zone dead"
                )
                return ('breakout', i)

            # Clean exit — zone valid, re-entry can restart counter
            if zone['side'] == 'BSL' and c['close'] < zone['bottom']:
                logger.info(
                    f"[{self.pair}] BSL clean exit post-exhaustion "
                    f"bar {i} — zone valid, re-entry search starts"
                )
                return ('exited', i)

            if zone['side'] == 'SSL' and c['close'] > zone['top']:
                logger.info(
                    f"[{self.pair}] SSL clean exit post-exhaustion "
                    f"bar {i} — zone valid, re-entry search starts"
                )
                return ('exited', i)

            # Price still inside zone — keep watching

        return ('end', None)

    def detect(self) -> list:
        """
        Run sweep detection across all valid zones.

        Zone entry search covers full M15 history (bar 0 onwards) so
        older zones that price re-enters today are not missed.

        The SWEEP CANDLE must be recent (within RECENT_CANDLES = 200
        bars, ~50 hours). N=14 windows that end before recent_start
        are skipped — the FVG entry level would be stale.

        Per zone, the state machine is:

          entry found
              ↓
          _find_sweep (N=14 window)
              ├─ found     → signal fires, zone DEAD — stop
              ├─ breakout  → zone DEAD — stop
              ├─ exited    → price left cleanly within N=14
              │              zone valid, start_i = exit bar
              │              loop back → find next re-entry
              ├─ exhausted → setup dropped, price still in zone
              │              _monitor_post_exhaustion (no time limit)
              │                  ├─ breakout → zone DEAD — stop
              │                  ├─ exited   → zone valid
              │                  │             start_i = exit bar
              │                  │             loop back → re-entry
              │                  └─ end      → stop
              └─ end       → stop
        """
        if self._sweeps is not None:
            return self._sweeps

        sweeps       = []
        recent_start = max(0, self.n - self.RECENT_CANDLES)

        for side in ('bsl', 'ssl'):
            for zone in self.zones[side]:

                start_i      = 0
                zone_sweeps  = []
                attempts     = 0
                MAX_ATTEMPTS = 50

                while start_i < self.n and attempts < MAX_ATTEMPTS:
                    attempts += 1

                    # ── Find next zone entry ──────────────────────────────
                    entry_i = self._find_zone_entry(zone, start_i)
                    if entry_i is None:
                        break  # no more entries in candle history

                    # ── Skip stale N=14 windows ───────────────────────────
                    window_end = min(entry_i + self.N_COUNTER, self.n)
                    if window_end <= recent_start:
                        start_i = window_end
                        continue

                    # ── Run N=14 sweep search ─────────────────────────────
                    reason, payload = self._find_sweep(zone, entry_i)

                    if reason == 'found':
                        # Zone consumed — signal fires, stop
                        zone_sweeps.append(payload)
                        break

                    elif reason == 'breakout':
                        # Zone dead — stop
                        break

                    elif reason == 'exited':
                        # Clean exit within N=14 — zone valid
                        # Wait for re-entry (start_i = exit bar)
                        start_i = payload
                        logger.info(
                            f"[{self.pair}] {zone['side']} "
                            f"clean exit within N=14 — re-entry search "
                            f"from bar {start_i}"
                        )

                    elif reason == 'exhausted':
                        # Setup dropped — monitor zone until
                        # breakout or clean exit
                        mon_reason, mon_i = self._monitor_post_exhaustion(
                            zone, payload  # payload = window_end
                        )
                        if mon_reason == 'breakout':
                            break  # zone dead
                        elif mon_reason == 'exited':
                            # Zone valid — re-entry can restart counter
                            start_i = mon_i
                            logger.info(
                                f"[{self.pair}] {zone['side']} "
                                f"exited post-exhaustion bar {mon_i} "
                                f"— re-entry search"
                            )
                        else:  # 'end'
                            break

                    else:  # 'end'
                        break

                if zone_sweeps:
                    # Only one sweep per zone (we break on found)
                    sweeps.append(zone_sweeps[0])
                    logger.info(
                        f"[{self.pair}] {zone['side']} — "
                        f"sweep @ {zone_sweeps[0]['sweep_time']}"
                    )

        self._sweeps = sweeps
        return sweeps

    def summary(self) -> str:
        s = self.detect()
        return (
            f"[SweepDetector:{self.pair}] "
            f"M15={self.n} ATR={self.atr:.5f} "
            f"Sweeps={len(s)}"
        )


# ── FVG SCANNER ───────────────────────────────────────────────────────────────
class FVGScanner:
    """
    Scans M15 candles within N=5 window after a sweep event
    for valid Fair Value Gaps (FVGs) and confirms MSS.

    Handles three formation scenarios:
      Case 1 — Sweep IS the displacement (C-1, SWEEP, C+1)
      Case 2 — Displacement immediately after sweep
      Case 3 — Delayed displacement within N=5 window

    All cases handled by scanning every consecutive triplet
    in the window — no special casing needed.

    MSS Filter:
      Swing high/low found in N=12 candles before zone entry
      Must be broken (close beyond) within N=5 FVG window
      No MSS → setup discarded

    Multiple FVGs → composite zone → premium/discount selection
    Single FVG   → used directly
    No FVG       → setup discarded
    """

    FVG_WINDOW    = 5     # M15 candles forward from sweep
    FVG_ATR_MULT  = 0.25  # minimum FVG size as % of ATR14
    MSS_LOOKBACK  = 12    # M15 candles before zone entry to find swing
    MSS_SWING_LB  = 2     # candles each side to confirm swing pivot

    def __init__(self, sweep: dict, m15_candles: list, pair: str):
        self.sweep     = sweep
        self.candles   = m15_candles
        self.pair      = pair
        self.direction = sweep['direction']
        self.sweep_i   = sweep['sweep_i']
        self.entry_i   = sweep['entry_i']   # zone entry candle index
        self.atr       = sweep['atr']
        self.min_gap   = self.FVG_ATR_MULT * self.atr
        self._result   = None

    # ── FVG Detection ─────────────────────────────────────────
    def _is_valid_fvg(self, c1: dict, c3: dict) -> dict | None:
        """
        Check if c1 and c3 form a valid FVG gap.
        Color of c2 (middle candle) is irrelevant.
        Bearish FVG: c1['low'] - c3['high'] >= min_gap
        Bullish FVG: c3['low'] - c1['high'] >= min_gap
        """
        if self.direction == 'short':
            gap = c1['low'] - c3['high']
            if gap >= self.min_gap:
                return {
                    'top'       : round(c1['low'],  5),
                    'bottom'    : round(c3['high'], 5),
                    'sl_anchor' : c1['high'],
                    'gap'       : gap,
                    'formed_at' : c3['time'],
                }
        else:
            gap = c3['low'] - c1['high']
            if gap >= self.min_gap:
                return {
                    'top'       : round(c3['low'],  5),
                    'bottom'    : round(c1['high'], 5),
                    'sl_anchor' : c1['low'],
                    'gap'       : gap,
                    'formed_at' : c3['time'],
                }
        return None

    def _scan_window(self) -> list:
        """
        Scan every consecutive triplet in window:
        [C-1, C0(sweep), C1, C2, C3, C4, C5]
        Include C-1 to catch Case 1 (sweep IS displacement).
        """
        start  = max(0, self.sweep_i - 1)
        end    = min(len(self.candles), self.sweep_i + self.FVG_WINDOW + 1)
        window = self.candles[start:end]
        fvgs   = []

        for i in range(len(window) - 2):
            c1  = window[i]
            c3  = window[i + 2]
            fvg = self._is_valid_fvg(c1, c3)
            if fvg:
                fvgs.append(fvg)
                logger.info(
                    f"[{self.pair}] FVG | "
                    f"{self.direction.upper()} "
                    f"top={fvg['top']} bottom={fvg['bottom']} "
                    f"gap={fvg['gap']:.5f}"
                )

        return fvgs

    # ── MSS Detection ─────────────────────────────────────────
    def _find_mss_swing(self) -> tuple | None:
        """
        Find most recent swing high (bearish) or swing low (bullish)
        within N=12 candles BEFORE the zone entry candle.

        Uses lb=2: candle must be highest/lowest of 2 neighbours
        on each side to qualify as a swing pivot.

        Anchor: self.entry_i (first M15 close inside zone)
        Search window: [entry_i - MSS_LOOKBACK, entry_i)

        Returns (swing_level: float, swing_i: int) or None.
        """
        lb       = self.MSS_SWING_LB
        start    = max(0, self.entry_i - self.MSS_LOOKBACK)
        end      = self.entry_i
        segment  = self.candles[start:end]
        n        = len(segment)

        if n < (lb * 2 + 1):
            return None

        # Search from most recent backwards — want the LAST swing
        if self.direction == 'short':
            # Bearish setup — find most recent swing HIGH
            for i in range(n - lb - 1, lb - 1, -1):
                h = segment[i]['high']
                is_swing = all(
                    segment[i-j]['high'] < h for j in range(1, lb+1)
                ) and all(
                    segment[i+j]['high'] < h for j in range(1, lb+1)
                )
                if is_swing:
                    swing_level = h
                    swing_i     = start + i
                    logger.info(
                        f"[{self.pair}] MSS swing HIGH "
                        f"@ {swing_level:.5f} (i={swing_i})"
                    )
                    return swing_level, swing_i
        else:
            # Bullish setup — find most recent swing LOW
            for i in range(n - lb - 1, lb - 1, -1):
                l = segment[i]['low']
                is_swing = all(
                    segment[i-j]['low'] > l for j in range(1, lb+1)
                ) and all(
                    segment[i+j]['low'] > l for j in range(1, lb+1)
                )
                if is_swing:
                    swing_level = l
                    swing_i     = start + i
                    logger.info(
                        f"[{self.pair}] MSS swing LOW "
                        f"@ {swing_level:.5f} (i={swing_i})"
                    )
                    return swing_level, swing_i

        logger.info(
            f"[{self.pair}] No MSS swing found in N=12 — discarding"
        )
        return None

    def _confirm_mss(self, swing_level: float) -> dict | None:
        """
        Within N=5 FVG window after sweep, check for a candle
        that CLOSES beyond the swing level.

        BEARISH: close below swing HIGH → MSS confirmed
        BULLISH: close above swing LOW  → MSS confirmed

        Returns the confirming candle dict or None.
        """
        start = self.sweep_i
        end   = min(len(self.candles), self.sweep_i + self.FVG_WINDOW + 1)

        for c in self.candles[start:end]:
            if self.direction == 'short':
                if c['close'] < swing_level:
                    logger.info(
                        f"[{self.pair}] MSS confirmed ✅ | "
                        f"close={c['close']:.5f} < "
                        f"swing_high={swing_level:.5f} "
                        f"@ {c['time']}"
                    )
                    return c
            else:
                if c['close'] > swing_level:
                    logger.info(
                        f"[{self.pair}] MSS confirmed ✅ | "
                        f"close={c['close']:.5f} > "
                        f"swing_low={swing_level:.5f} "
                        f"@ {c['time']}"
                    )
                    return c

        logger.info(
            f"[{self.pair}] MSS not confirmed in N=5 window — discarding"
        )
        return None

    # ── Composite Zone ────────────────────────────────────────
    def _build_composite(self, fvgs: list) -> dict:
        """Merge all FVGs into composite zone using outer boundaries."""
        outer_top    = max(f['top']    for f in fvgs)
        outer_bottom = min(f['bottom'] for f in fvgs)
        equilibrium  = (outer_top + outer_bottom) / 2
        return {
            'outer_top'   : outer_top,
            'outer_bottom': outer_bottom,
            'equilibrium' : equilibrium,
        }

    def _select_optimal_fvg(self, fvgs: list,
                             composite: dict) -> tuple:
        """
        Select optimal FVG based on midpoint position
        relative to composite zone equilibrium.

        BEARISH — want FVG in PREMIUM (above equilibrium):
          Both in premium  → farthest from equilibrium
          One each side    → pick premium one
          Both in discount → closest to equilibrium ⚠ suboptimal

        BULLISH — want FVG in DISCOUNT (below equilibrium):
          Both in discount → farthest from equilibrium
          One each side    → pick discount one
          Both in premium  → closest to equilibrium ⚠ suboptimal

        Returns (selected_fvg, quality)
        """
        eq = composite['equilibrium']

        def mid(fvg):
            return (fvg['top'] + fvg['bottom']) / 2

        if self.direction == 'short':
            premium  = [f for f in fvgs if mid(f) > eq]
            discount = [f for f in fvgs if mid(f) <= eq]
            if premium:
                selected = max(premium, key=lambda f: mid(f) - eq)
                quality  = 'optimal'
            else:
                selected = min(discount, key=lambda f: eq - mid(f))
                quality  = 'suboptimal'
        else:
            discount = [f for f in fvgs if mid(f) < eq]
            premium  = [f for f in fvgs if mid(f) >= eq]
            if discount:
                selected = max(discount, key=lambda f: eq - mid(f))
                quality  = 'optimal'
            else:
                selected = min(premium, key=lambda f: mid(f) - eq)
                quality  = 'suboptimal'

        logger.info(
            f"[{self.pair}] FVG selected | "
            f"quality={quality} mid={mid(selected):.5f} eq={eq:.5f}"
        )
        return selected, quality

    # ── Main Scan ─────────────────────────────────────────────
    def scan(self) -> dict | None:
        """
        Run full FVG + MSS scan for this sweep event.
        Pipeline:
          1. Find FVGs in N=5 window
          2. Find MSS swing level in N=12 pre-zone window
          3. Confirm MSS close in N=5 window
          4. Select optimal FVG
          5. Return result dict

        Returns result dict or None if any step fails.
        Lazy-loaded and cached.
        """
        if self._result is not None:
            return self._result

        # Step 1 — FVG scan
        fvgs = self._scan_window()
        if not fvgs:
            logger.info(
                f"[{self.pair}] No FVG in N=5 window "
                f"after sweep @ {self.sweep['sweep_time']} — discarding"
            )
            self._result = None
            return None

        # Step 2 — Find MSS swing
        mss_swing = self._find_mss_swing()
        if mss_swing is None:
            self._result = None
            return None

        swing_level, swing_i = mss_swing

        # Step 3 — Confirm MSS close
        mss_candle = self._confirm_mss(swing_level)
        if mss_candle is None:
            self._result = None
            return None

        # Step 4 — FVG selection
        if len(fvgs) == 1:
            fvg       = fvgs[0]
            entry     = round((fvg['top'] + fvg['bottom']) / 2, 5)
            quality   = 'standard'
            composite = None
        else:
            composite     = self._build_composite(fvgs)
            fvg, quality  = self._select_optimal_fvg(fvgs, composite)
            entry         = round((fvg['top'] + fvg['bottom']) / 2, 5)

        # Step 5 — Sweep analytics
        sc           = self.sweep['sweep_candle']
        candle_range = sc['high'] - sc['low']
        body         = abs(sc['close'] - sc['open'])

        if self.direction == 'short':
            wick_beyond = sc['high'] - self.sweep['zone']['top']
        else:
            wick_beyond = self.sweep['zone']['bottom'] - sc['low']

        sweep_body_pct  = round(body / candle_range, 3) \
                          if candle_range > 0 else 0
        sweep_wick_ratio= round(wick_beyond / candle_range, 3) \
                          if candle_range > 0 else 0
        fvg_size_atr    = round(fvg['gap'] / self.atr, 3) \
                          if self.atr > 0 else 0
        sweep_to_fvg_bars = self.candles.index(
            next((c for c in self.candles
                  if c['time'] == fvg['formed_at']), sc),
        ) - self.sweep_i if fvg['formed_at'] != sc['time'] else 0

        self._result = {
            'fvg'              : fvg,
            'composite'        : composite,
            'quality'          : quality,
            'fvg_count'        : len(fvgs),
            'fvg_top'          : fvg['top'],
            'fvg_bottom'       : fvg['bottom'],
            'entry'            : entry,
            'sl_anchor'        : fvg['sl_anchor'],
            'formed_at'        : fvg['formed_at'],
            # MSS data
            'mss_level'        : swing_level,
            'mss_candle_time'  : mss_candle['time'],
            # Sweep analytics
            'sweep_body_pct'   : sweep_body_pct,
            'sweep_wick_ratio' : sweep_wick_ratio,
            'fvg_size_atr_mult': fvg_size_atr,
            'sweep_to_fvg_bars': sweep_to_fvg_bars,
        }

        return self._result

    def summary(self) -> str:
        r = self.scan()
        if not r:
            return f"[FVGScanner:{self.pair}] No FVG/MSS found"
        return (
            f"[FVGScanner:{self.pair}] "
            f"FVGs={r['fvg_count']} quality={r['quality']} "
            f"entry={r['entry']:.5f} "
            f"mss={r['mss_level']:.5f} "
            f"top={r['fvg_top']:.5f} bottom={r['fvg_bottom']:.5f}"
        )


# ── RISK MANAGER ──────────────────────────────────────────────────────────────
class RiskManager:
    """
    Computes SL and TP levels for a validated sweep+FVG setup.

    SL:
      Base : sweep_open ± (0.10 × ATR14)
      Cap  : if SL crosses sweep high/low
             → SL = sweep high/low ± (0.10 × pip)

    TP:
      Target: nearest opposing H4 liquidity cluster
        Bearish → nearest SSL zone_top below entry
        Bullish → nearest BSL zone_bottom above entry

      Macro range  = entry to cluster near edge
      Equilibrium  = 50% of macro range

      DUAL TP   : RR_to_equilibrium >= 2.5
                  TP1 = equilibrium, TP2 = cluster near edge
      SINGLE TP : RR_to_equilibrium < 2.5, RR_to_cluster >= 2.0
                  TP1 = cluster near edge
      DISCARD   : RR_to_cluster < 2.0
    """

    SL_ATR_MULT     = 0.10
    SL_CAP_FRAC     = 0.10
    MIN_RR_TP1_DUAL = 2.5
    MIN_RR_SINGLE   = 2.0

    def __init__(self, sweep: dict, fvg_result: dict,
                 h4_zones: dict, pair: str):
        self.sweep     = sweep
        self.fvg       = fvg_result
        self.h4_zones  = h4_zones
        self.pair      = pair
        self.direction = sweep['direction']
        self.atr       = sweep['atr']
        self.pip       = PIP_SIZE.get(pair, 0.0001)
        self.dp        = DP_SIZE.get(pair, 5)
        self.entry     = fvg_result['entry']
        self._result   = None

    def _compute_sl(self) -> float | None:
        sc           = self.sweep['sweep_candle']
        sweep_open   = sc['open']
        sweep_high   = sc['high']
        sweep_low    = sc['low']
        buffer       = self.SL_ATR_MULT * self.atr
        cap_buffer   = self.SL_CAP_FRAC * self.pip

        if self.direction == 'short':
            proposed = sweep_open + buffer
            sl       = proposed if proposed < sweep_high \
                       else sweep_high + cap_buffer
        else:
            proposed = sweep_open - buffer
            sl       = proposed if proposed > sweep_low \
                       else sweep_low - cap_buffer

        return round(sl, self.dp)

    def _find_opposing_cluster(self) -> dict | None:
        if self.direction == 'short':
            candidates = [
                z for z in self.h4_zones['ssl']
                if z['top'] < self.entry
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda z: z['top'])
        else:
            candidates = [
                z for z in self.h4_zones['bsl']
                if z['bottom'] > self.entry
            ]
            if not candidates:
                return None
            return min(candidates, key=lambda z: z['bottom'])

    def _compute_tp(self, sl: float) -> dict | None:
        cluster = self._find_opposing_cluster()
        if cluster is None:
            logger.info(f"[{self.pair}] No opposing cluster — discarding")
            return None

        cluster_edge = (
            cluster['top'] if self.direction == 'short'
            else cluster['bottom']
        )

        macro_range = abs(self.entry - cluster_edge)
        equilibrium = (
            self.entry - macro_range * 0.50
            if self.direction == 'short'
            else self.entry + macro_range * 0.50
        )

        sl_dist = abs(self.entry - sl)
        if sl_dist <= 0:
            return None

        rr_to_eq      = round(macro_range * 0.50 / sl_dist, 2)
        rr_to_cluster = round(macro_range / sl_dist, 2)

        if rr_to_eq >= self.MIN_RR_TP1_DUAL:
            logger.info(
                f"[{self.pair}] DUAL TP | "
                f"TP1={equilibrium:.{self.dp}f} RR={rr_to_eq} "
                f"TP2={cluster_edge:.{self.dp}f} RR={rr_to_cluster}"
            )
            return {
                'tp1'    : round(equilibrium,  self.dp),
                'tp2'    : round(cluster_edge, self.dp),
                'rr_tp1' : rr_to_eq,
                'rr_tp2' : rr_to_cluster,
                'tp_mode': 'DUAL',
                'cluster': cluster,
            }
        elif rr_to_cluster >= self.MIN_RR_SINGLE:
            logger.info(
                f"[{self.pair}] SINGLE TP | "
                f"TP1={cluster_edge:.{self.dp}f} RR={rr_to_cluster}"
            )
            return {
                'tp1'    : round(cluster_edge, self.dp),
                'tp2'    : None,
                'rr_tp1' : rr_to_cluster,
                'rr_tp2' : None,
                'tp_mode': 'SINGLE',
                'cluster': cluster,
            }
        else:
            logger.info(
                f"[{self.pair}] RR insufficient "
                f"eq={rr_to_eq} cluster={rr_to_cluster} — discarding"
            )
            return None

    def compute(self) -> dict | None:
        """
        Run full SL + TP computation.
        Returns risk dict or None if setup fails validation.
        Lazy-loaded and cached.
        """
        if self._result is not None:
            return self._result

        sl = self._compute_sl()
        if sl is None:
            return None

        tp = self._compute_tp(sl)
        if tp is None:
            return None

        sl_dist = abs(self.entry - sl)

        self._result = {
            'entry'                  : self.entry,
            'sl'                     : sl,
            'tp1'                    : tp['tp1'],
            'tp2'                    : tp['tp2'],
            'rr_tp1'                 : tp['rr_tp1'],
            'rr_tp2'                 : tp['rr_tp2'],
            'sl_pips'                : round(sl_dist / self.pip, 1),
            'tp_mode'                : tp['tp_mode'],
            'cluster'                : tp['cluster'],
            'distance_to_cluster_pips': round(
                abs(self.entry -
                    (tp['cluster']['top']
                     if self.direction == 'short'
                     else tp['cluster']['bottom'])
                ) / self.pip, 1
            ),
        }

        return self._result

    def summary(self) -> str:
        r = self.compute()
        if not r:
            return f"[RiskManager:{self.pair}] Setup discarded"
        tp2_str = f"{r['tp2']:.{self.dp}f}" if r['tp2'] else 'N/A'
        return (
            f"[RiskManager:{self.pair}] "
            f"entry={r['entry']:.{self.dp}f} "
            f"sl={r['sl']:.{self.dp}f} "
            f"tp1={r['tp1']:.{self.dp}f} "
            f"tp2={tp2_str} "
            f"sl_pips={r['sl_pips']} mode={r['tp_mode']}"
        )


# ── DEDUP MANAGER ─────────────────────────────────────────────────────────────
class DedupManager:
    """
    Three-key deduplication system.

    Key 1 — ZONE LOCK   : structure-based, until zone mitigated
    Key 2 — SWEEP LOCK  : TTL 75 minutes
    Key 3 — SIGNAL LOCK : TTL 60 minutes

    State persisted to state/sweep_fvg_fired.json.
    """

    ZONE_TTL_MINS   = 1440  # 24 hours — zone re-tradeable after this
    SWEEP_TTL_MINS  = 75
    SIGNAL_TTL_MINS = 60

    def __init__(self, pair: str):
        self.pair   = pair
        self.dp     = DP_SIZE.get(pair, 5)
        self._fired = self._load()

    def _load(self) -> dict:
        if os.path.exists(FIRED_FILE):
            try:
                with open(FIRED_FILE) as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save(self):
        """Prune expired entries and persist to JSON."""
        os.makedirs(os.path.dirname(FIRED_FILE), exist_ok=True)
        self._prune()
        with open(FIRED_FILE, 'w') as f:
            json.dump(self._fired, f, indent=2)

    def _zone_key(self, zone: dict) -> str:
        return f"ZONE_{self.pair}_{zone['side']}_{zone['top']:.5f}"

    def _sweep_key(self, sweep_candle: dict) -> str:
        ts = sweep_candle['time'].strftime('%Y%m%d%H%M')
        return f"SWEEP_{self.pair}_{ts}"

    def _signal_key(self, entry: float, direction: str) -> str:
        return f"SIG_{self.pair}_{direction}_{entry:.{self.dp}f}"

    def _is_expired(self, key: str, ttl_mins: float) -> bool:
        if key not in self._fired:
            return True
        try:
            fired_at = datetime.fromisoformat(
                self._fired[key]['fired_at']
            )
            age_mins = (
                datetime.now(timezone.utc) - fired_at
            ).total_seconds() / 60
            return age_mins > ttl_mins
        except Exception:
            return True

    def _prune(self):
        """Remove expired entries for all key types including zones."""
        now  = datetime.now(timezone.utc)
        drop = []
        for key, meta in self._fired.items():
            try:
                fired_at = datetime.fromisoformat(meta['fired_at'])
                age_mins = (now - fired_at).total_seconds() / 60
                if key.startswith('ZONE_'):
                    ttl = self.ZONE_TTL_MINS
                elif key.startswith('SWEEP_'):
                    ttl = self.SWEEP_TTL_MINS
                else:
                    ttl = self.SIGNAL_TTL_MINS
                if age_mins > ttl:
                    drop.append(key)
            except Exception:
                drop.append(key)
        for key in drop:
            self._fired.pop(key, None)

    def is_zone_fired(self, zone: dict) -> bool:
        return not self._is_expired(
            self._zone_key(zone), self.ZONE_TTL_MINS
        )

    def lock_zone(self, zone: dict):
        key = self._zone_key(zone)
        self._fired[key] = {
            'fired_at': datetime.now(timezone.utc).isoformat(),
            'type': 'zone', 'pair': self.pair,
            'side': zone['side'], 'top': zone['top'],
        }
        logger.info(f"[DEDUP] Zone locked: {key}")

    def release_zone(self, zone: dict):
        key = self._zone_key(zone)
        self._fired.pop(key, None)
        logger.info(f"[DEDUP] Zone released: {key}")

    def is_sweep_fired(self, sweep_candle: dict) -> bool:
        return not self._is_expired(
            self._sweep_key(sweep_candle), self.SWEEP_TTL_MINS
        )

    def lock_sweep(self, sweep_candle: dict):
        key = self._sweep_key(sweep_candle)
        self._fired[key] = {
            'fired_at': datetime.now(timezone.utc).isoformat(),
            'type': 'sweep', 'pair': self.pair,
            'time': sweep_candle['time'].isoformat(),
        }
        logger.info(f"[DEDUP] Sweep locked: {key}")

    def is_signal_fired(self, entry: float, direction: str) -> bool:
        return not self._is_expired(
            self._signal_key(entry, direction), self.SIGNAL_TTL_MINS
        )

    def lock_signal(self, entry: float, direction: str):
        key = self._signal_key(entry, direction)
        self._fired[key] = {
            'fired_at' : datetime.now(timezone.utc).isoformat(),
            'type'     : 'signal',
            'pair'     : self.pair,
            'direction': direction,
            'entry'    : entry,
        }
        logger.info(f"[DEDUP] Signal locked: {key}")

    def is_duplicate(self, zone: dict, sweep_candle: dict,
                     entry: float, direction: str) -> bool:
        """Master check — returns True if ANY key already fired."""
        if self.is_zone_fired(zone):
            logger.info(f"[DEDUP] Zone fired: {self._zone_key(zone)}")
            return True
        if self.is_sweep_fired(sweep_candle):
            logger.info(f"[DEDUP] Sweep fired: {self._sweep_key(sweep_candle)}")
            return True
        if self.is_signal_fired(entry, direction):
            logger.info(f"[DEDUP] Signal fired: {self._signal_key(entry, direction)}")
            return True
        return False

    def lock_all(self, zone: dict, sweep_candle: dict,
                 entry: float, direction: str):
        """Lock all three keys after signal confirmed."""
        self.lock_zone(zone)
        self.lock_sweep(sweep_candle)
        self.lock_signal(entry, direction)
        logger.info(
            f"[DEDUP] All locked — "
            f"{self.pair} {direction} {entry:.{self.dp}f}"
        )

    def summary(self) -> str:
        zones   = sum(1 for k in self._fired if k.startswith('ZONE_'))
        sweeps  = sum(1 for k in self._fired if k.startswith('SWEEP_'))
        signals = sum(1 for k in self._fired if k.startswith('SIG_'))
        return (
            f"[DedupManager:{self.pair}] "
            f"zones={zones} sweeps={sweeps} signals={signals}"
        )


# ── SIGNAL BUILDER ────────────────────────────────────────────────────────────
class SignalBuilder:
    """
    Assembles the final signal dict and Telegram Stage 1 message
    from validated sweep + FVG + risk components.

    Output goes to:
      → Telegram (Stage 1 alert)
      → Google Sheet row (signal_engine writes)
      → sweep_fvg_pending.json (local state)
    """

    DIRECTION_EMOJI = {'short': '🔻', 'long': '🔺'}
    SESSION_EMOJI   = {
        'Asian'  : '🌏',
        'London' : '🇬🇧',
        'NewYork': '🗽',
        'Other'  : '🌐',
    }
    TP_MODE_EMOJI = {'DUAL': '🎯🎯', 'SINGLE': '🎯'}

    def __init__(self, sweep: dict, fvg_result: dict,
                 risk: dict, pair: str, htf_trend: str = ''):
        self.sweep     = sweep
        self.fvg       = fvg_result
        self.risk      = risk
        self.pair      = pair
        self.direction = sweep['direction']
        self.dp        = DP_SIZE.get(pair, 5)
        self.htf_trend = htf_trend
        self._signal   = None

    def _get_session(self, dt: datetime) -> str:
        h = dt.hour
        if 0  <= h < 6:  return 'Asian'
        if 6  <= h < 12: return 'London'
        if 12 <= h < 17: return 'NewYork'
        return 'Other'

    def _build_pending_key(self) -> str:
        ts = self.sweep['sweep_time'].strftime('%Y%m%d%H%M')
        return (
            f"{self.pair}_{self.direction}_"
            f"{self.risk['entry']:.{self.dp}f}_{ts}"
        )

    def _build_message(self, sig: dict) -> str:
        dp         = self.dp
        side       = 'SELL' if sig['direction'] == 'short' else 'BUY'
        emoji      = self.DIRECTION_EMOJI[sig['direction']]
        sess_emoji = self.SESSION_EMOJI.get(sig['session'], '🌐')
        tp_emoji   = self.TP_MODE_EMOJI[sig['tp_mode']]
        quality    = sig['quality'].upper()
        trend_icon = (
            '↗️' if sig.get('htf_trend') == 'bullish'
            else '↘️' if sig.get('htf_trend') == 'bearish'
            else '➡️'
        )

        if sig['tp_mode'] == 'DUAL':
            tp_lines = (
                f"TP1 (50%) : `{sig['tp1']:.{dp}f}`  "
                f"RR `1:{sig['rr_tp1']}`\n"
                f"TP2 (50%) : `{sig['tp2']:.{dp}f}`  "
                f"RR `1:{sig['rr_tp2']}`"
            )
        else:
            tp_lines = (
                f"TP1       : `{sig['tp1']:.{dp}f}`  "
                f"RR `1:{sig['rr_tp1']}`"
            )

        return (
            f"{emoji} *{sig['pair']} — H4 Zone Sweep*\n\n"
            f"_FVG confirmed. MSS validated. Price validated. Set pending order._\n\n"
            f"Direction : `{side}`\n"
            f"Entry     : `{sig['entry']:.{dp}f}`  ← pending order\n"
            f"Stop Loss : `{sig['sl']:.{dp}f}`\n"
            f"{tp_lines}\n\n"
            f"Zone      : `{sig['zone_src']}`\n"
            f"Quality   : `{quality}`\n"
            f"HTF Trend : {trend_icon} `{sig.get('htf_trend','').capitalize()}`\n"
            f"Swept at  : `{sig['sweep_time']}`\n"
            f"Session   : {sess_emoji} `{sig['session']}`\n\n"
            f"⏳ _Waiting for retrace into FVG..._\n"
            f"_{tp_emoji} EDGE Signal Engine — Sweep+FVG_"
        )

    def build(self) -> dict:
        """
        Assemble complete signal dict.
        Lazy-loaded and cached.
        """
        if self._signal is not None:
            return self._signal

        sc          = self.sweep['sweep_candle']
        zone        = self.sweep['zone']
        now_utc     = datetime.now(timezone.utc)
        fired_at    = now_utc.strftime('%Y-%m-%d %H:%M UTC')
        created_at  = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
        sweep_time  = self.sweep['sweep_time'].strftime('%Y-%m-%d %H:%M UTC')
        fvg_formed  = self.fvg['formed_at'].strftime('%Y-%m-%d %H:%M UTC')
        session     = self._get_session(self.sweep['sweep_time'])
        pending_key = self._build_pending_key()

        # Zone age — bars from oldest touch to now
        zone_age_h4_bars = (
            self.sweep.get('h4_current_index', 0) -
            zone.get('oldest_i', zone.get('formed_i', 0))
        )

        mss_candle_time = self.fvg.get('mss_candle_time')
        mss_time_str    = (
            mss_candle_time.strftime('%Y-%m-%d %H:%M UTC')
            if mss_candle_time else ''
        )

        sig = {
            # Identity
            'pair'                   : self.pair,
            'direction'              : self.direction,
            'pending_key'            : pending_key,
            'fired_at'               : fired_at,
            'created_at'             : created_at,
            # Levels
            'entry'                  : self.risk['entry'],
            'sl'                     : self.risk['sl'],
            'tp1'                    : self.risk['tp1'],
            'tp2'                    : self.risk['tp2'],
            'rr_tp1'                 : self.risk['rr_tp1'],
            'rr_tp2'                 : self.risk['rr_tp2'],
            'sl_pips'                : self.risk['sl_pips'],
            'tp_mode'                : self.risk['tp_mode'],
            # FVG context (Replit Phase 0)
            'fvg_top'                : self.fvg['fvg_top'],
            'fvg_bottom'             : self.fvg['fvg_bottom'],
            'fvg_formed_at'          : fvg_formed,
            'fvg_count'              : self.fvg['fvg_count'],
            # Sweep context
            'sweep_time'             : sweep_time,
            'sweep_high'             : sc['high'],
            'sweep_low'              : sc['low'],
            'sweep_open'             : sc['open'],
            # Zone context
            'zone_src'               : f"{zone['side']}_{zone['touches']}touch",
            'zone_top'               : zone['top'],
            'zone_bottom'            : zone['bottom'],
            # Analytics
            'quality'                : self.fvg['quality'],
            'session'                : session,
            'htf_trend'              : self.htf_trend,
            # New analytics fields
            'sweep_body_pct'         : self.fvg.get('sweep_body_pct', ''),
            'sweep_wick_ratio'       : self.fvg.get('sweep_wick_ratio', ''),
            'n_candles_in_zone'      : self.sweep.get('n_counter_value', ''),
            'zone_age_h4_bars'       : zone_age_h4_bars,
            'distance_to_cluster_pips': self.risk.get('distance_to_cluster_pips', ''),
            'fvg_size_atr_mult'      : self.fvg.get('fvg_size_atr_mult', ''),
            'sweep_to_fvg_bars'      : self.fvg.get('sweep_to_fvg_bars', ''),
            'mss_level'              : self.fvg.get('mss_level', ''),
            'mss_candle_time'        : mss_time_str,
            # Lifecycle
            'status'                 : 'PENDING_ENTRY',
            'tp1_outcome'            : '',
            'tp2_outcome'            : '',
            'pnl_r'                  : '',
            'entry_time'             : '',
        }

        sig['message'] = self._build_message(sig)
        self._signal   = sig
        return sig

    def save_pending(self):
        """Write signal to sweep_fvg_pending.json."""
        sig     = self.build()
        pending = load_pending()
        pending[sig['pending_key']] = sig
        save_pending(pending)
        logger.info(f"[{self.pair}] Pending saved → {sig['pending_key']}")

    def _load_pending(self) -> dict:
        return load_pending()

    def summary(self) -> str:
        s = self.build()
        return (
            f"[SignalBuilder:{self.pair}] "
            f"{s['direction'].upper()} "
            f"entry={s['entry']:.{self.dp}f} "
            f"sl={s['sl']:.{self.dp}f} "
            f"tp1={s['tp1']:.{self.dp}f} "
            f"mode={s['tp_mode']} quality={s['quality']} "
            f"htf={s['htf_trend']} mss={s['mss_level']:.5f}"
        )


# ── MAIN SCAN ─────────────────────────────────────────────────────────────────
def scan(m15_candles: list,
         h4_candles:  list,
         pair:        str,
         htf_trend:   str = '') -> list:
    """
    Main entry point — called by signal_engine.py every 15 minutes.

    Orchestrates full detection pipeline:
      ZoneManager → SweepDetector → FVGScanner
      → RiskManager → DedupManager → SignalBuilder

    Parameters
    ----------
    m15_candles : M15 OHLC dicts, oldest → newest
    h4_candles  : H4 OHLC dicts, oldest → newest
    pair        : e.g. 'EURUSD'
    htf_trend   : 'bullish' / 'bearish' / 'neutral'
                  computed from H4 EMA20 in signal_engine

    Returns
    -------
    list of signal dicts (one per valid setup found this run)
    """
    logger.info(f"\n{'─'*50}")
    logger.info(f"[SCAN] {pair} | M15={len(m15_candles)} H4={len(h4_candles)}")

    if len(m15_candles) < 30:
        logger.warning(f"[{pair}] Insufficient M15 — skipping")
        return []

    if len(h4_candles) < 40:
        logger.warning(f"[{pair}] Insufficient H4 — skipping")
        return []

    signals = []

    # Step 1 — Zone validation
    try:
        zm    = ZoneManager(h4_candles, pair)
        zones = zm.get_valid_zones()
        logger.info(zm.summary())
    except Exception as e:
        logger.error(f"[{pair}] ZoneManager failed: {e}")
        return []

    if not zones['bsl'] and not zones['ssl']:
        logger.info(f"[{pair}] No valid zones — skipping")
        return []

    # Step 2 — M15 ATR
    try:
        atr_m15 = SweepDetector.calc_atr_m15(m15_candles)
        if atr_m15 <= 0:
            logger.warning(f"[{pair}] ATR=0 — skipping")
            return []
    except Exception as e:
        logger.error(f"[{pair}] ATR failed: {e}")
        return []

    # Step 3 — Sweep detection
    try:
        detector = SweepDetector(pair, zones, m15_candles, atr_m15)
        sweeps   = detector.detect()
        logger.info(detector.summary())
    except Exception as e:
        logger.error(f"[{pair}] SweepDetector failed: {e}")
        return []

    if not sweeps:
        logger.info(f"[{pair}] No sweeps detected")
        return []

    # Step 4 — DedupManager
    try:
        dedup = DedupManager(pair)
        logger.info(dedup.summary())
    except Exception as e:
        logger.error(f"[{pair}] DedupManager failed: {e}")
        return []

    # Step 5 — FVG + Risk + Signal per sweep
    for sweep in sweeps:
        tag = f"[{pair}:{sweep['direction'].upper()}]"

        try:
            scanner = FVGScanner(sweep, m15_candles, pair)
            result  = scanner.scan()
            if result is None:
                logger.info(f"{tag} No FVG/MSS — discarding")
                continue
            logger.info(scanner.summary())
        except Exception as e:
            logger.error(f"{tag} FVGScanner: {e}")
            continue

        try:
            rm   = RiskManager(sweep, result, zones, pair)
            risk = rm.compute()
            if risk is None:
                logger.info(f"{tag} Risk invalid — discarding")
                continue
            logger.info(rm.summary())
        except Exception as e:
            logger.error(f"{tag} RiskManager: {e}")
            continue

        try:
            if dedup.is_duplicate(
                sweep['zone'],
                sweep['sweep_candle'],
                risk['entry'],
                sweep['direction']
            ):
                logger.info(f"{tag} Duplicate — skipping")
                continue
        except Exception as e:
            logger.error(f"{tag} Dedup check: {e}")
            continue

        # ── Pre-fire validation ───────────────────────────────────────────
        # Guard against stale signals where price has already moved through
        # the FVG entry by the time the scan runs.
        #
        # Bug 1: price on wrong side of entry
        #   BSL sweep → short → entry must be ABOVE current price
        #   SSL sweep → long  → entry must be BELOW current price
        #
        # Bug 2: zone already broken at current price
        #   BSL broken if current price closed above zone top
        #   SSL broken if current price closed below zone bottom
        #
        # Both checks use the close of the most recent M15 candle.
        try:
            current_price = m15_candles[-1]['close']
            zone          = sweep['zone']

            if sweep['direction'] == 'short' and current_price <= risk['entry']:
                logger.info(
                    f"{tag} Price {current_price:.{DP_SIZE.get(pair,5)}f} "
                    f"already at or below entry {risk['entry']:.{DP_SIZE.get(pair,5)}f} "
                    f"— FVG consumed, signal unactionable — skipping"
                )
                continue

            if sweep['direction'] == 'long' and current_price >= risk['entry']:
                logger.info(
                    f"{tag} Price {current_price:.{DP_SIZE.get(pair,5)}f} "
                    f"already at or above entry {risk['entry']:.{DP_SIZE.get(pair,5)}f} "
                    f"— FVG consumed, signal unactionable — skipping"
                )
                continue

            if zone['side'] == 'BSL' and current_price > zone['top']:
                logger.info(
                    f"{tag} BSL zone broken — current price "
                    f"{current_price:.{DP_SIZE.get(pair,5)}f} > zone top "
                    f"{zone['top']:.5f} — skipping"
                )
                continue

            if zone['side'] == 'SSL' and current_price < zone['bottom']:
                logger.info(
                    f"{tag} SSL zone broken — current price "
                    f"{current_price:.{DP_SIZE.get(pair,5)}f} < zone bottom "
                    f"{zone['bottom']:.5f} — skipping"
                )
                continue

        except Exception as e:
            logger.error(f"{tag} Pre-fire validation: {e}")
            continue

        try:
            # Inject H4 current index for zone age calculation
            sweep['h4_current_index'] = len(h4_candles) - 1

            sb     = SignalBuilder(sweep, result, risk, pair, htf_trend)
            signal = sb.build()
            logger.info(sb.summary())

            dedup.lock_all(
                sweep['zone'],
                sweep['sweep_candle'],
                risk['entry'],
                sweep['direction']
            )
            sb.save_pending()
            signals.append(signal)

            logger.info(
                f"{tag} ✅ Signal ready | "
                f"entry={risk['entry']:.{DP_SIZE.get(pair,5)}f} "
                f"mode={risk['tp_mode']} quality={result['quality']}"
            )
        except Exception as e:
            logger.error(f"{tag} SignalBuilder: {e}")
            continue

    # Step 6 — Persist dedup state
    try:
        dedup.save()
    except Exception as e:
        logger.error(f"[{pair}] Dedup save: {e}")

    logger.info(
        f"[SCAN] {pair} done | "
        f"sweeps={len(sweeps)} signals={len(signals)}"
    )
    logger.info(f"{'─'*50}\n")

    return signals


# ── CLEANUP ───────────────────────────────────────────────────────────────────
def cleanup_expired_pending():
    """
    Called once per signal_engine run before scanning.
    Removes expired or resolved entries from pending.json.
    """
    pending  = load_pending()
    now      = datetime.now(timezone.utc)
    drop     = []
    retained = 0

    for pk, sig in pending.items():
        status = sig.get('status', '')

        if status in ('ACTIVE', 'INVALIDATED', 'CLOSED'):
            drop.append(pk)
            continue

        try:
            created_str = sig.get('created_at', '')
            created     = datetime.fromisoformat(
                created_str.replace(' UTC', '+00:00')
            )
            age_mins = (now - created).total_seconds() / 60

            if age_mins > PENDING_TTL_MINS:
                drop.append(pk)
                logger.info(
                    f"[CLEANUP] Expired: {pk} ({age_mins:.0f}m)"
                )
            else:
                retained += 1
        except Exception:
            drop.append(pk)

    for pk in drop:
        pending.pop(pk, None)

    if drop:
        save_pending(pending)
        logger.info(
            f"[CLEANUP] Removed {len(drop)} entries. "
            f"Retained {retained}."
        )
    else:
        logger.info(
            f"[CLEANUP] Nothing to clean. {retained} retained."
        )


# ── TELEGRAM FORMATTERS (used by replit_tracker) ──────────────────────────────
def format_entry_message(sig: dict) -> str:
    """Stage 2 — Entry triggered. Called by Replit."""
    dp   = DP_SIZE.get(sig['pair'], 5)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'

    if sig.get('tp_mode') == 'DUAL':
        tp_lines = (
            f"TP1 (50%) : `{sig['tp1']:.{dp}f}`\n"
            f"TP2 (50%) : `{sig['tp2']:.{dp}f}`"
        )
    else:
        tp_lines = f"TP1       : `{sig['tp1']:.{dp}f}`"

    return (
        f"⚡ *{sig['pair']} — Entry Triggered!*\n\n"
        f"Direction : `{side}`\n"
        f"Entry     : `{sig['entry']:.{dp}f}` ✅\n"
        f"Stop Loss : `{sig['sl']:.{dp}f}`\n"
        f"{tp_lines}\n\n"
        f"_🟢 Trade ACTIVE. Replit monitoring started._\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )

def format_tp1_alert(sig: dict) -> str:
    """Stage 3 — TP1 hit. Called by Replit."""
    dp   = DP_SIZE.get(sig['pair'], 5)
    side = 'SELL' if sig['direction'] == 'short' else 'BUY'
    return (
        f"🎯 *{sig['pair']} — TP1 Hit!*\n\n"
        f"Direction : `{side}`\n"
        f"TP1       : `{sig['tp1']:.{dp}f}` ✅  50% closed\n"
        f"SL → BE   : `{sig['entry']:.{dp}f}` ← move stop now\n"
        f"TP2 active: `{sig['tp2']:.{dp}f}`  "
        f"RR `1:{sig.get('rr_tp2','?')}`\n\n"
        f"_Remainder running to TP2._\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )

def format_final_alert(sig: dict,
                        outcome: str,
                        price: float) -> str:
    """Stage 4 — Final outcome. Called by Replit."""
    dp      = DP_SIZE.get(sig['pair'], 5)
    side    = 'SELL' if sig['direction'] == 'short' else 'BUY'
    r1      = float(sig.get('rr_tp1', 2.0))
    r2      = float(sig.get('rr_tp2') or 0)
    tp_mode = sig.get('tp_mode', 'DUAL')

    if outcome == 'FULL_WIN':
        emoji = '✅✅'
        label = 'FULL WIN'
        pnl   = f"+{r1*0.5 + r2*0.5:.2f}R" \
                if tp_mode == 'DUAL' else f"+{r1:.2f}R"
    elif outcome == 'TP1_BE':
        emoji = '✅'
        label = 'TP1 + Breakeven'
        pnl   = f"+{r1*0.5:.2f}R"
    else:
        emoji = '❌'
        label = 'LOSS'
        pnl   = '-1.00R'

    return (
        f"{emoji} *SWEEP RESULT — {side} {sig['pair']}*\n\n"
        f"Outcome : *{label}*\n"
        f"Entry   : `{sig['entry']:.{dp}f}`\n"
        f"Close   : `{price:.{dp}f}`\n"
        f"P&L     : `{pnl}`\n\n"
        f"_Logged → EDGE Journal SweepFVG tab_\n"
        f"_⚡ EDGE Replit Tracker — Sweep+FVG_"
    )

"""
prop_engine.py — London Open Liquidity Grab (LOLG) Strategy
=============================================================
Dedicated prop firm challenge signal module for the EDGE Signal Engine.
Completely isolated from B&R and Sweep+FVG engines — separate state files,
separate Sheets tab, separate Telegram label.

Strategy: London Open Liquidity Grab + PDH/PDL Rejection (secondary)
─────────────────────────────────────────────────────────────────────
Primary setup (LOLG):
  1. Asian range (00:00–06:59 UTC) identified on H1 candles
  2. At London open (07:00–09:00 UTC), M15 candle sweeps Asian high or low
     — wick pierces the level, candle closes BACK inside the range
  3. Entry on next M15 candle open after confirmed sweep close
  4. SL: 3 pips beyond the sweep wick
  5. TP: opposing Asian range boundary
  6. RR must fall within 1.5–3.0

Secondary setup (PDH/PDL Rejection):
  Fires only when no LOLG setup has fired during the London window.
  Price taps Previous Day High/Low during London session, shows a
  rejection candle (close away from the level), entry on confirmation.
  Same SL/TP structure as LOLG.

Pairs: EURUSD, GBPUSD only
  London open produces cleanest sweeps on dollar pairs.
  JPY pairs and Gold excluded — different open dynamics.

Prop firm risk parameters (designed to survive tightest rules):
  Target firms: FundedNext, FundingPips, Maven, NairaTrader, FTMO
  Binding constraints: 3% daily DD (FundingPips 1-Step), 8% max DD (Maven)
  PROP_RISK_PER_TRADE : 0.5% — survives 6 consecutive losses within 3% daily
  PROP_MAX_DAILY_LOSS : 1.5% — hard cut after 3 losses in a day
  PROP_MAX_TRADES_DAY : 2    — London open produces max 2 clean setups/day

Telegram label: 🏆 PROP — distinguishes from B&R and Sweep+FVG signals.

State files (isolated):
  state/prop_state.json   — daily trade count, daily loss tracker
  state/prop_fired.json   — dedup log (4-hour TTL)
"""

import json
import os
import logging
from datetime import datetime, timezone, date
from collections import defaultdict

logger = logging.getLogger(__name__)

# ── PAIRS ──────────────────────────────────────────────────────────────────
PROP_PAIRS = {'EURUSD', 'GBPUSD'}

PIP_SIZE = {
    'EURUSD': 0.0001,
    'GBPUSD': 0.0001,
}

# ── SESSION WINDOWS (UTC hours, inclusive start, exclusive end) ────────────
ASIAN_START   = 0     # 00:00 UTC
ASIAN_END     = 7     # 07:00 UTC  (end of Asian session)
LONDON_START  = 7     # 07:00 UTC
LONDON_END    = 9     # 09:00 UTC  (prop window — tighter than full London)

# ── STRATEGY PARAMETERS ────────────────────────────────────────────────────
SL_BUF_PIPS   = 3     # pips beyond sweep wick for SL
MIN_RR        = 1.5   # minimum RR — below this, skip (range too tight)
MAX_RR        = 3.0   # maximum RR — above this, skip (range too wide)
MIN_RANGE_PIPS = 10   # minimum Asian range size — below this, range is noise
MAX_RANGE_PIPS = 80   # maximum Asian range size — above this, too wide for clean TP

# ── PROP RISK PARAMETERS ───────────────────────────────────────────────────
PROP_RISK_PCT       = 0.5    # % of account risked per trade
PROP_MAX_DAILY_LOSS = 1.5    # % — hard stop trading for the day after this
PROP_MAX_TRADES_DAY = 2      # hard cap — no more than 2 signals per pair per day

# ── STATE FILES ────────────────────────────────────────────────────────────
STATE_FILE  = 'state/prop_state.json'
FIRED_FILE  = 'state/prop_fired.json'
TTL_HOURS   = 4


# ══════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


def load_fired() -> dict:
    if os.path.exists(FIRED_FILE):
        try:
            with open(FIRED_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_fired(fired: dict):
    os.makedirs(os.path.dirname(FIRED_FILE), exist_ok=True)
    with open(FIRED_FILE, 'w') as f:
        json.dump(fired, f, indent=2)


def is_fired(key: str, fired: dict) -> bool:
    if key not in fired:
        return False
    try:
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(fired[key])).total_seconds() / 3600
        return age < TTL_HOURS
    except Exception:
        return False


def mark_fired(key: str, fired: dict):
    now    = datetime.now(timezone.utc)
    pruned = {k: v for k, v in fired.items()
              if (now - datetime.fromisoformat(v)).total_seconds() / 3600 < TTL_HOURS * 2}
    fired.clear()
    fired.update(pruned)
    fired[key] = now.isoformat()


# ══════════════════════════════════════════════════════════════════════════
# DAILY RISK TRACKER
# ══════════════════════════════════════════════════════════════════════════

def get_daily_state(state: dict, pair: str, today: str) -> dict:
    """Return today's trading state for a pair, reset if new day."""
    key = f"{pair}_{today}"
    if key not in state or state[key].get('date') != today:
        state[key] = {
            'date':        today,
            'trades':      0,
            'daily_loss':  0.0,   # cumulative % loss today
            'lolg_fired':  False, # primary setup fired today
        }
    return state[key]


def can_trade(daily: dict) -> tuple[bool, str]:
    """Check if trading is still allowed today for this pair."""
    if daily['trades'] >= PROP_MAX_TRADES_DAY:
        return False, f"daily trade cap reached ({PROP_MAX_TRADES_DAY})"
    if daily['daily_loss'] >= PROP_MAX_DAILY_LOSS:
        return False, f"daily loss limit reached ({PROP_MAX_DAILY_LOSS}%)"
    return True, ""


# ══════════════════════════════════════════════════════════════════════════
# ASIAN RANGE BUILDER (H1 candles)
# ══════════════════════════════════════════════════════════════════════════

def build_asian_range(h1_candles: list[dict],
                      trade_date: date) -> dict | None:
    """
    Build the Asian session range for trade_date from H1 candles.
    Looks for H1 candles whose time falls in 00:00–06:59 UTC on trade_date.
    Returns { high, low, date } or None if insufficient data.
    """
    session_candles = [
        c for c in h1_candles
        if c['time'].date() == trade_date
        and ASIAN_START <= c['time'].hour < ASIAN_END
    ]

    if not session_candles:
        logger.warning(f"prop_engine: No H1 candles for Asian session on {trade_date}")
        return None

    asian_high = max(c['high'] for c in session_candles)
    asian_low  = min(c['low']  for c in session_candles)

    return {
        'high': round(asian_high, 5),
        'low':  round(asian_low,  5),
        'date': str(trade_date),
    }


# ══════════════════════════════════════════════════════════════════════════
# PREVIOUS DAY HIGH / LOW (secondary setup)
# ══════════════════════════════════════════════════════════════════════════

def build_pdhl(h1_candles: list[dict],
               trade_date: date) -> dict | None:
    """
    Build Previous Day High/Low from H1 candles for the day before trade_date.
    Used for the PDH/PDL rejection secondary setup.
    """
    from datetime import timedelta
    prev_date = trade_date - timedelta(days=1)
    # Walk back to find the most recent trading day (skip weekends)
    for _ in range(5):
        prev_candles = [
            c for c in h1_candles
            if c['time'].date() == prev_date
        ]
        if prev_candles:
            return {
                'high': round(max(c['high'] for c in prev_candles), 5),
                'low':  round(min(c['low']  for c in prev_candles), 5),
                'date': str(prev_date),
            }
        prev_date -= timedelta(days=1)
    return None


# ══════════════════════════════════════════════════════════════════════════
# SWEEP DETECTION (M15)
# ══════════════════════════════════════════════════════════════════════════

def detect_lolg_sweep(candle: dict,
                      asian_high: float,
                      asian_low:  float,
                      pip:        float) -> dict | None:
    """
    Check if this M15 candle is a valid London Open Liquidity Grab sweep.

    BSL sweep (bearish setup):
      - Wick pierces ABOVE Asian high
      - Candle CLOSES back below Asian high (back inside range)

    SSL sweep (bullish setup):
      - Wick pierces BELOW Asian low
      - Candle CLOSES back above Asian low (back inside range)

    Returns setup dict or None.
    """
    high  = candle['high']
    low   = candle['low']
    close = candle['close']

    # BSL sweep → short trade (price swept highs, reverses down)
    if high > asian_high and close < asian_high:
        return {
            'direction':  'short',
            'side':       'SELL',
            'sweep_level': asian_high,
            'sweep_type':  'BSL',
            'sl_raw':      round(high + SL_BUF_PIPS * pip, 5),
            'tp_raw':      round(asian_low, 5),
            'sweep_src':   'Asian_High',
        }

    # SSL sweep → long trade (price swept lows, reverses up)
    if low < asian_low and close > asian_low:
        return {
            'direction':  'long',
            'side':       'BUY',
            'sweep_level': asian_low,
            'sweep_type':  'SSL',
            'sl_raw':      round(low - SL_BUF_PIPS * pip, 5),
            'tp_raw':      round(asian_high, 5),
            'sweep_src':   'Asian_Low',
        }

    return None


def detect_pdhl_rejection(candle:   dict,
                           pdh:      float,
                           pdl:      float,
                           pip:      float) -> dict | None:
    """
    Secondary setup: PDH/PDL rejection during London session.
    Candle must tap the level and close away from it — no full close through.

    PDH rejection → short (wick above PDH, close below PDH)
    PDL rejection → long  (wick below PDL, close above PDL)
    """
    high  = candle['high']
    low   = candle['low']
    close = candle['close']
    open_ = candle['open']

    # PDH rejection → short
    if high >= pdh and close < pdh and close < open_:
        return {
            'direction':  'short',
            'side':       'SELL',
            'sweep_level': pdh,
            'sweep_type':  'PDH_Rejection',
            'sl_raw':      round(high + SL_BUF_PIPS * pip, 5),
            'tp_raw':      round(pdl, 5),
            'sweep_src':   'PDH',
        }

    # PDL rejection → long
    if low <= pdl and close > pdl and close > open_:
        return {
            'direction':  'long',
            'side':       'BUY',
            'sweep_level': pdl,
            'sweep_type':  'PDL_Rejection',
            'sl_raw':      round(low - SL_BUF_PIPS * pip, 5),
            'tp_raw':      round(pdh, 5),
            'sweep_src':   'PDL',
        }

    return None


# ══════════════════════════════════════════════════════════════════════════
# SIGNAL FORMATTER
# ══════════════════════════════════════════════════════════════════════════

def format_signal(setup:       dict,
                  pair:        str,
                  entry:       float,
                  asian_range: dict,
                  fired_at:    str) -> dict:
    """Format confirmed prop setup into EDGE signal dict."""
    direction = setup['direction']
    side      = setup['side']
    sl        = setup['sl_raw']
    tp        = setup['tp_raw']
    pip       = PIP_SIZE[pair]
    sl_dist   = abs(entry - sl)
    tp_dist   = abs(entry - tp)
    rr        = round(tp_dist / sl_dist, 2) if sl_dist > 0 else 0

    emoji = '🔻' if direction == 'short' else '🔺'

    return {
        'strategy':    'PropFirm_LOLG',
        'pair':        pair,
        'direction':   direction,
        'side':        side,
        'entry':       round(entry, 5),
        'sl':          round(sl, 5),
        'tp':          round(tp, 5),
        'sl_pips':     round(sl_dist / pip, 1),
        'rr':          rr,
        'asian_high':  asian_range['high'],
        'asian_low':   asian_range['low'],
        'sweep_src':   setup['sweep_src'],
        'sweep_type':  setup['sweep_type'],
        'risk_pct':    PROP_RISK_PCT,
        'session':     'London',
        'fired_at':    fired_at,
        'message': (
            f"🏆 *PROP | {pair} — London Open Liquidity Grab*\n"
            f"Direction   : `{side}`\n"
            f"Entry       : `{round(entry, 5)}`\n"
            f"Stop Loss   : `{round(sl, 5)}` ({round(sl_dist/pip, 1)} pips)\n"
            f"Take Profit : `{round(tp, 5)}`\n"
            f"RR          : `1:{rr}`\n"
            f"Risk        : `{PROP_RISK_PCT}% per trade`\n"
            f"Asian Range : `{asian_range['low']} – {asian_range['high']}`\n"
            f"Sweep       : `{setup['sweep_src']}`\n"
            f"⚠️ Max 2 trades/day | Stop after 1.5% daily loss"
        ),
    }


# ══════════════════════════════════════════════════════════════════════════
# CORE SCAN FUNCTION
# ══════════════════════════════════════════════════════════════════════════

def scan(m15_candles: list[dict],
         h1_candles:  list[dict] | None = None,
         pair:        str = 'EURUSD') -> list[dict]:
    """
    Main entry point called each cron run by signal_engine.py.

    Args:
        m15_candles : M15 candle dicts — time(datetime), open, high, low, close
        h1_candles  : H1 candle dicts from HTF cache — used for Asian range
        pair        : Must be 'EURUSD' or 'GBPUSD'

    Returns:
        List of prop signal dicts fired this run (usually 0 or 1).
    """
    if pair not in PROP_PAIRS:
        logger.debug(f"prop_engine [{pair}]: Not a prop pair, skipping.")
        return []

    if not h1_candles:
        logger.warning(f"prop_engine [{pair}]: No H1 candles — cannot build Asian range.")
        return []

    if not m15_candles:
        return []

    pip         = PIP_SIZE[pair]
    latest      = m15_candles[-1]
    latest_time = latest['time']
    today       = latest_time.date()
    today_str   = str(today)
    now_hour    = latest_time.hour

    # ── Gate: only run during London prop window ──────────────────────────
    if not (LONDON_START <= now_hour < LONDON_END):
        logger.debug(
            f"prop_engine [{pair}]: Outside London prop window "
            f"({now_hour}:xx UTC). Skipping."
        )
        return []

    # ── Load state and daily tracker ─────────────────────────────────────
    state = load_state()
    fired = load_fired()
    daily = get_daily_state(state, pair, today_str)

    ok, reason = can_trade(daily)
    if not ok:
        logger.info(f"prop_engine [{pair}]: Trading paused today — {reason}")
        save_state(state)
        return []

    # ── Build Asian range from H1 ─────────────────────────────────────────
    asian = build_asian_range(h1_candles, today)
    if not asian:
        logger.warning(f"prop_engine [{pair}]: Asian range unavailable for {today}")
        save_state(state)
        return []

    asian_high = asian['high']
    asian_low  = asian['low']
    range_pips = round((asian_high - asian_low) / pip, 1)

    # ── Range size gate ───────────────────────────────────────────────────
    if range_pips < MIN_RANGE_PIPS:
        logger.info(
            f"prop_engine [{pair}]: Asian range too tight "
            f"({range_pips} pips < {MIN_RANGE_PIPS}). Skipping."
        )
        save_state(state)
        return []

    if range_pips > MAX_RANGE_PIPS:
        logger.info(
            f"prop_engine [{pair}]: Asian range too wide "
            f"({range_pips} pips > {MAX_RANGE_PIPS}). Skipping."
        )
        save_state(state)
        return []

    # ── Build PDH/PDL for secondary setup ────────────────────────────────
    pdhl = build_pdhl(h1_candles, today)

    signals_fired = []

    # ── Scan M15 candles in London prop window ────────────────────────────
    london_candles = [
        c for c in m15_candles
        if c['time'].date() == today
        and LONDON_START <= c['time'].hour < LONDON_END
    ]

    for candle in london_candles:
        ok, reason = can_trade(daily)
        if not ok:
            break

        # ── Primary: LOLG sweep ───────────────────────────────────────────
        # Only one LOLG per pair per day — first clean sweep wins
        if not daily['lolg_fired']:
            setup = detect_lolg_sweep(candle, asian_high, asian_low, pip)
            if setup:
                entry    = candle['close']   # entry at close of sweep candle
                sl       = setup['sl_raw']
                tp       = setup['tp_raw']
                sl_dist  = abs(entry - sl)
                tp_dist  = abs(entry - tp)

                if sl_dist < 2 * pip:
                    logger.info(f"prop_engine [{pair}]: LOLG SL too tight, skipping.")
                    continue

                rr = tp_dist / sl_dist if sl_dist > 0 else 0

                # RR gate
                if not (MIN_RR <= rr <= MAX_RR):
                    logger.info(
                        f"prop_engine [{pair}]: LOLG RR {rr:.2f} outside "
                        f"{MIN_RR}–{MAX_RR} window. Skipping."
                    )
                    continue

                dedup_key = f"{pair}_LOLG_{setup['sweep_type']}_{today_str}"
                if is_fired(dedup_key, fired):
                    logger.info(f"prop_engine [{pair}]: LOLG duplicate suppressed.")
                    continue

                mark_fired(dedup_key, fired)
                daily['lolg_fired'] = True
                daily['trades']    += 1
                fired_at            = str(latest_time)

                signal = format_signal(setup, pair, entry, asian, fired_at)
                signals_fired.append(signal)

                logger.info(
                    f"prop_engine [{pair}]: 🏆 LOLG signal | "
                    f"{setup['side']} entry={round(entry,5)} "
                    f"sl={sl} tp={tp} rr=1:{rr:.2f} "
                    f"src={setup['sweep_src']} range={range_pips}pips"
                )
                continue

        # ── Secondary: PDH/PDL rejection (only if no LOLG today) ─────────
        if not daily['lolg_fired'] and pdhl:
            setup = detect_pdhl_rejection(
                candle, pdhl['high'], pdhl['low'], pip
            )
            if setup:
                entry   = candle['close']
                sl      = setup['sl_raw']
                tp      = setup['tp_raw']
                sl_dist = abs(entry - sl)
                tp_dist = abs(entry - tp)

                if sl_dist < 2 * pip:
                    continue

                rr = tp_dist / sl_dist if sl_dist > 0 else 0

                if not (MIN_RR <= rr <= MAX_RR):
                    logger.info(
                        f"prop_engine [{pair}]: PDH/L RR {rr:.2f} outside window."
                    )
                    continue

                dedup_key = f"{pair}_PDHL_{setup['sweep_type']}_{today_str}"
                if is_fired(dedup_key, fired):
                    continue

                mark_fired(dedup_key, fired)
                daily['trades'] += 1
                fired_at         = str(latest_time)

                # Use PDH/PDL as the "range" for formatting
                pdhl_range = {
                    'high': pdhl['high'],
                    'low':  pdhl['low'],
                }
                signal = format_signal(setup, pair, entry, pdhl_range, fired_at)
                # Override strategy label for secondary
                signal['strategy'] = 'PropFirm_PDHL'
                signal['message']  = signal['message'].replace(
                    'London Open Liquidity Grab', 'PDH/PDL Rejection'
                )
                signals_fired.append(signal)

                logger.info(
                    f"prop_engine [{pair}]: 🏆 PDH/L signal | "
                    f"{setup['side']} entry={round(entry,5)} "
                    f"sl={sl} tp={tp} rr=1:{rr:.2f} src={setup['sweep_src']}"
                )

    save_state(state)
    save_fired(fired)
    return signals_fired


# ══════════════════════════════════════════════════════════════════════════
# DAILY LOSS UPDATER
# Called by signal_engine.py when a prop trade outcome is known
# ══════════════════════════════════════════════════════════════════════════

def record_loss(pair: str, loss_pct: float):
    """
    Record a realised loss for today's prop trading session.
    signal_engine.py calls this when a prop trade hits SL.
    Prevents further trading if daily loss limit reached.

    Args:
        pair     : 'EURUSD' or 'GBPUSD'
        loss_pct : positive float — percentage of account lost (e.g. 0.5)
    """
    state     = load_state()
    today_str = str(date.today())
    daily     = get_daily_state(state, pair, today_str)
    daily['daily_loss'] = round(daily['daily_loss'] + loss_pct, 4)
    logger.info(
        f"prop_engine [{pair}]: Loss recorded {loss_pct}% | "
        f"Daily total: {daily['daily_loss']}% / {PROP_MAX_DAILY_LOSS}%"
    )
    save_state(state)


# ══════════════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import csv
    logging.basicConfig(level=logging.INFO)

    def load_csv(path, tf):
        candles = []
        with open(path, newline='') as f:
            for row in csv.reader(f, delimiter='\t'):
                candles.append({
                    'time':   datetime.strptime(
                        row[0].strip(), '%Y-%m-%d %H:%M'
                    ).replace(tzinfo=timezone.utc),
                    'open':   float(row[1]),
                    'high':   float(row[2]),
                    'low':    float(row[3]),
                    'close':  float(row[4]),
                    'volume': int(row[5]),
                })
        print(f"  {tf}: {len(candles)} candles")
        return candles

    print("\nLoading EURUSD data...")
    m15 = load_csv('../EURUSD15.csv',    'M15')
    h1  = load_csv('../EURUSD60__1_.csv','H1')

    # Test on last 500 M15 bars
    test_m15 = m15[-500:]
    print(f"\nScanning {len(test_m15)} M15 bars for prop signals...")
    results = scan(m15_candles=test_m15, h1_candles=h1, pair='EURUSD')

    print(f"\nProp signals found: {len(results)}")
    for r in results:
        print(
            f"  {r['side']:4} | entry={r['entry']} sl={r['sl']} "
            f"tp={r['tp']} rr=1:{r['rr']} | "
            f"src={r['sweep_src']} range={r['asian_high']}–{r['asian_low']}"
        )

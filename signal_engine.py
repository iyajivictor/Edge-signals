"""
EDGE Signal Engine
==================
Break & Retest signal detector for:
  EURUSD, GBPUSD, USDJPY, AUDJPY
  M15 timeframe | 1:2 RR | Trend-filtered

Runs every 15 minutes via GitHub Actions.
Sends Telegram alerts when a signal fires.
Logs all signals to signals_log.csv
"""

import os
import json
import requests
import time
from datetime import datetime, timezone
from pathlib import Path

# ══════════════════════════════════════════════
#  CONFIG  — set these as GitHub Secrets
# ══════════════════════════════════════════════
FCS_API_KEY  = os.environ.get("FCS_API_KEY",  "fbNM6BharH9sgbq0HO6EgEl0h")
TG_TOKEN     = os.environ.get("TG_TOKEN",     "")
TG_CHAT_ID   = os.environ.get("TG_CHAT_ID",   "")

# ── Trading pairs (the 4 that passed real backtest) ──
PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "AUDJPY"]

# ── FCS API symbol map ──
FCS_SYMBOLS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "AUDJPY": "AUD/JPY",
}

# ── Pip config ──
PIP_SIZE = {
    "EURUSD": 0.0001,
    "GBPUSD": 0.0001,
    "USDJPY": 0.01,
    "AUDJPY": 0.01,
}
DP = {
    "EURUSD": 5,
    "GBPUSD": 5,
    "USDJPY": 3,
    "AUDJPY": 3,
}

# ── Strategy params ──
RR             = 2.0
SWING_LB       = 5
RETEST_TOL     = 0.0008   # fraction of price
RETEST_WINDOW  = 30
TREND_N        = 3
BREAK_WINDOW   = 20
CANDLES_NEEDED = 200      # minimum candles to run strategy

# ── State file (persists between runs via GitHub Actions cache) ──
STATE_FILE   = Path("state/price_history.json")
SIGNALS_LOG  = Path("state/signals_log.csv")

# ══════════════════════════════════════════════
#  1. FETCH LIVE PRICES
# ══════════════════════════════════════════════
def safe_float(val):
    """Convert FCS API values to float — handles '+0.16%', '-0.5%', etc."""
    try:
        if val is None: return 0.0
        return float(str(val).replace('%','').replace('+','').strip())
    except:
        return 0.0

def fetch_prices():
    """Fetch current bid/ask/high/low for all pairs from FCS API."""
    symbols = ",".join(FCS_SYMBOLS.values())
    url     = f"https://fcsapi.com/api-v3/forex/latest?symbol={requests.utils.quote(symbols)}&access_key={FCS_API_KEY}"
    try:
        res  = requests.get(url, timeout=15)
        data = res.json()
        prices = {}
        if data and data.get("response"):
            for item in data["response"]:
                pair = next((k for k, v in FCS_SYMBOLS.items()
                             if v.replace("/","") == item.get("s","") or v == item.get("s","")), None)
                if pair:
                    prices[pair] = {
                        "price":  safe_float(item.get("c", 0)),
                        "high":   safe_float(item.get("h", 0)),
                        "low":    safe_float(item.get("l", 0)),
                        "change": safe_float(item.get("cp", 0)),
                        "time":   datetime.now(timezone.utc).isoformat(),
                    }
        return prices
    except Exception as e:
        print(f"[ERROR] Price fetch failed: {e}")
        return {}

# ══════════════════════════════════════════════
#  2. STATE MANAGEMENT
#     GitHub Actions is stateless — each run
#     starts fresh. We persist price history
#     and active setups in a JSON file that
#     gets cached between runs.
# ══════════════════════════════════════════════
def load_state():
    STATE_FILE.parent.mkdir(exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except:
            pass
    return {
        "price_history": {p: [] for p in PAIRS},
        "active_setups": {},    # { pair: { dir, level, sl, breakout_idx, created } }
        "fired_signals": [],    # recent signal IDs to prevent duplicates
    }

def save_state(state):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ══════════════════════════════════════════════
#  3. STRATEGY LOGIC
# ══════════════════════════════════════════════
def find_swings(prices, lb=SWING_LB):
    sh, sl = [], []
    for i in range(lb, len(prices) - lb):
        win = prices[i-lb:i+lb+1]
        if prices[i] == max(win): sh.append(i)
        if prices[i] == min(win): sl.append(i)
    return sh, sl

def detect_trend(prices, sh, sl, n=TREND_N):
    psh = sh[-n:]
    psl = sl[-n:]
    if len(psh) < 2 or len(psl) < 2:
        return "neutral"
    shv = [prices[i] for i in psh]
    slv = [prices[i] for i in psl]
    bull = all(shv[i] < shv[i+1] for i in range(len(shv)-1)) and \
           all(slv[i] < slv[i+1] for i in range(len(slv)-1))
    bear = all(shv[i] > shv[i+1] for i in range(len(shv)-1)) and \
           all(slv[i] > slv[i+1] for i in range(len(slv)-1))
    if bull: return "bullish"
    if bear: return "bearish"
    return "neutral"

def run_signal_check(pair, prices_list, active_setups, current_price):
    """
    Run B&R check on price history.
    Returns signal dict if entry triggered, None otherwise.
    Updates active_setups in place.
    """
    if len(prices_list) < CANDLES_NEEDED:
        print(f"  [{pair}] Only {len(prices_list)} price ticks — need {CANDLES_NEEDED} minimum")
        return None

    pip = PIP_SIZE[pair]
    sh, sl = find_swings(prices_list, SWING_LB)
    n   = len(prices_list)
    cur = current_price
    tol = cur * RETEST_TOL

    setup = active_setups.get(pair)

    # ── Expire stale setup ──
    if setup and n - setup.get("breakout_idx", 0) > RETEST_WINDOW:
        print(f"  [{pair}] Setup expired — resetting")
        active_setups.pop(pair, None)
        setup = None

    # ── Check if retest entry triggered ──
    if setup:
        lv = setup["level"]
        if setup["dir"] == "long":
            touched = cur <= lv + tol
            held    = cur > lv - tol
            if touched and held:
                sl_p  = setup["sl"]
                risk  = cur - sl_p
                if risk > pip * 3:
                    tp = cur + risk * RR
                    active_setups.pop(pair, None)
                    return {
                        "pair":   pair,
                        "side":   "BUY",
                        "entry":  cur,
                        "sl":     sl_p,
                        "tp":     tp,
                        "level":  lv,
                        "sl_pips": round(risk / pip, 1),
                        "rr":     RR,
                        "time":   datetime.now(timezone.utc).isoformat(),
                    }
        else:  # short
            touched = cur >= lv - tol
            held    = cur < lv + tol
            if touched and held:
                sl_p  = setup["sl"]
                risk  = sl_p - cur
                if risk > pip * 3:
                    tp = cur - risk * RR
                    active_setups.pop(pair, None)
                    return {
                        "pair":   pair,
                        "side":   "SELL",
                        "entry":  cur,
                        "sl":     sl_p,
                        "tp":     tp,
                        "level":  lv,
                        "sl_pips": round(risk / pip, 1),
                        "rr":     RR,
                        "time":   datetime.now(timezone.utc).isoformat(),
                    }

    # ── Look for new breakout ──
    if not setup:
        trend = detect_trend(prices_list, sh, sl)
        if trend == "bullish" and sh:
            valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
            if valid_sh:
                last_sh = valid_sh[-1]
                if (n - 1) - (last_sh + SWING_LB) <= BREAK_WINDOW and cur > prices_list[last_sh]:
                    valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
                    if valid_sl:
                        print(f"  [{pair}] 🔍 Bullish breakout above {prices_list[last_sh]:.{DP[pair]}f} — watching for retest")
                        active_setups[pair] = {
                            "dir":          "long",
                            "level":        prices_list[last_sh],
                            "sl":           prices_list[valid_sl[-1]],
                            "breakout_idx": n - 1,
                            "created":      datetime.now(timezone.utc).isoformat(),
                        }

        elif trend == "bearish" and sl:
            valid_sl = [s for s in sl if s + SWING_LB <= n - 1]
            if valid_sl:
                last_sl = valid_sl[-1]
                if (n - 1) - (last_sl + SWING_LB) <= BREAK_WINDOW and cur < prices_list[last_sl]:
                    valid_sh = [s for s in sh if s + SWING_LB <= n - 1]
                    if valid_sh:
                        print(f"  [{pair}] 🔍 Bearish breakout below {prices_list[last_sl]:.{DP[pair]}f} — watching for retest")
                        active_setups[pair] = {
                            "dir":          "short",
                            "level":        prices_list[last_sl],
                            "sl":           prices_list[valid_sh[-1]],
                            "breakout_idx": n - 1,
                            "created":      datetime.now(timezone.utc).isoformat(),
                        }

    return None

# ══════════════════════════════════════════════
#  4. TELEGRAM ALERT
# ══════════════════════════════════════════════
def send_telegram(signal):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("  [ALERT] Telegram not configured — skipping")
        return

    pair  = signal["pair"]
    side  = signal["side"]
    dp    = DP[pair]
    arrow = "🟢" if side == "BUY" else "🔴"
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    msg = (
        f"{arrow} *EDGE SIGNAL — {side} {pair}*\n\n"
        f"📍 Entry:       `{signal['entry']:.{dp}f}`\n"
        f"🛑 Stop Loss:   `{signal['sl']:.{dp}f}`\n"
        f"🎯 Take Profit: `{signal['tp']:.{dp}f}`\n\n"
        f"📊 RR: 1:{signal['rr']}  |  Risk: {signal['sl_pips']:.1f} pips\n"
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
#  5. SIGNAL LOG
# ══════════════════════════════════════════════
def log_signal(signal):
    SIGNALS_LOG.parent.mkdir(exist_ok=True)
    header = not SIGNALS_LOG.exists()
    with open(SIGNALS_LOG, "a") as f:
        if header:
            f.write("time,pair,side,entry,sl,tp,sl_pips,rr,level\n")
        f.write(
            f"{signal['time']},{signal['pair']},{signal['side']},"
            f"{signal['entry']},{signal['sl']},{signal['tp']},"
            f"{signal['sl_pips']},{signal['rr']},{signal['level']}\n"
        )

# ══════════════════════════════════════════════
#  6. DUPLICATE GUARD
#     Prevent firing the same signal twice
#     within a 4-hour window
# ══════════════════════════════════════════════
def is_duplicate(signal, fired_signals):
    sig_id = f"{signal['pair']}_{signal['side']}_{round(signal['level'], 4)}"
    now    = datetime.now(timezone.utc)
    # Clean old signals (older than 4 hours)
    recent = []
    for s in fired_signals:
        try:
            t = datetime.fromisoformat(s["time"])
            if (now - t).total_seconds() < 14400:  # 4 hours
                recent.append(s)
        except:
            pass
    fired_signals.clear()
    fired_signals.extend(recent)
    # Check if this signal already fired
    for s in recent:
        if s.get("id") == sig_id:
            return True
    fired_signals.append({"id": sig_id, "time": now.isoformat()})
    return False

# ══════════════════════════════════════════════
#  7. MAIN
# ══════════════════════════════════════════════
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*55}")
    print(f"  EDGE Signal Engine — {now}")
    print(f"{'='*55}")

    # Load persisted state
    state          = load_state()
    price_history  = state["price_history"]
    active_setups  = state["active_setups"]
    fired_signals  = state["fired_signals"]

    # Fetch live prices
    print("\n[1/3] Fetching live prices...")
    prices = fetch_prices()

    if not prices:
        print("  No prices returned — check FCS API key")
        return
      send_telegram({"pair":"TEST","side":"BUY","entry":1.1000,"sl":1.0900,"tp":1.1200,"level":1.0950,"sl_pips":100,"rr":2.0,"time":"test"})

    for pair, data in prices.items():
        print(f"  {pair}: {data['price']:.{DP[pair]}f}  ({data['change']:+.2f}%)")

    # Append prices to history & run signal check
    print("\n[2/3] Running signal scan...")
    signals_fired = []

    for pair in PAIRS:
        if pair not in prices:
            print(f"  [{pair}] No price data — skipping")
            continue

        cur = prices[pair]["price"]

        # Append to rolling history (keep last 500 ticks)
        if pair not in price_history:
            price_history[pair] = []
        price_history[pair].append(cur)
        if len(price_history[pair]) > 500:
            price_history[pair] = price_history[pair][-500:]

        hist_len = len(price_history[pair])
        setup    = active_setups.get(pair)
        setup_str= f"({setup['dir']} setup active)" if setup else "(no setup)"
        print(f"  [{pair}] {hist_len} ticks  {setup_str}")

        signal = run_signal_check(pair, price_history[pair], active_setups, cur)

        if signal:
            if not is_duplicate(signal, fired_signals):
                signals_fired.append(signal)
                print(f"  [{pair}] 🔔 SIGNAL: {signal['side']}  Entry: {signal['entry']:.{DP[pair]}f}  SL: {signal['sl']:.{DP[pair]}f}  TP: {signal['tp']:.{DP[pair]}f}")
            else:
                print(f"  [{pair}] Signal already fired recently — skipping duplicate")

    # Send alerts & log
    print(f"\n[3/3] Sending alerts...")
    if signals_fired:
        for sig in signals_fired:
            send_telegram(sig)
            log_signal(sig)
            print(f"  ✓ {sig['side']} {sig['pair']} logged")
    else:
        print("  No new signals this run")

    # Save updated state
    state["price_history"] = price_history
    state["active_setups"] = active_setups
    state["fired_signals"] = fired_signals
    save_state(state)

    # Summary
    print(f"\n{'='*55}")
    print(f"  Pairs monitored:   {len(PAIRS)}")
    print(f"  Active setups:     {len(active_setups)}")
    print(f"  Signals this run:  {len(signals_fired)}")
    total_ticks = sum(len(v) for v in price_history.values())
    print(f"  Total price ticks: {total_ticks}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()

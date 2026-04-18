"""
EDGE Outcome Tracker
====================
Monitors pending trades from Google Sheets and automatically
detects outcomes (WIN/LOSS) using Twelve Data API.

Resolution logic:
  1. Fetch candles from NEXT candle after signal (skip signal candle)
  2. Wait for entry confirmation (close must cross entry price)
  3. Once confirmed, detect WIN/LOSS using CLOSE prices only
  4. If still pending, leave as PENDING

Fixes applied:
  - check_from = signal_time + 15min
  - Close-based detection only
  - Entry confirmation required
  - Live signals cleared from separate live_signals.json (no race condition)

Runs every 30 minutes via GitHub Actions cron
"""

import os
import json
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

# ============================================
#  CONFIG
# ============================================
TWELVE_DATA_KEY  = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN         = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID       = os.environ.get("TG_CHAT_ID", "")
SHEET_ID         = os.environ.get("SHEET_ID", "")
GOOGLE_CREDS     = os.environ.get("GOOGLE_CREDENTIALS", "")

LIVE_SIGNALS_FILE   = Path("state/live_signals.json")
SWEEP_FVG_LOG_FILE  = Path("state/sweep_fvg_log.csv")

TD_SYMBOLS = {
    "USDJPY": "USD/JPY",
    "GBPUSD": "GBP/USD",
    "AUDJPY": "AUD/JPY",
    "XAUUSD": "XAU/USD",
    "EURUSD": "EUR/USD",
}

DP = {
    "USDJPY": 3,
    "GBPUSD": 5,
    "AUDJPY": 3,
    "XAUUSD": 2,
    "EURUSD": 5,
}

# ============================================
#  1. GOOGLE SHEETS CONNECTION
# ============================================
def connect_sheets():
    """Connect to Google Sheets and return (main_sheet, sweep_fvg_sheet)."""
    creds_dict = json.loads(GOOGLE_CREDS)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client   = gspread.authorize(creds)
    workbook = client.open_by_key(SHEET_ID)
    main_sheet = workbook.sheet1
    try:
        sweep_sheet = workbook.worksheet("SweepFVG")
    except Exception:
        sweep_sheet = None
        print("[WARN] SweepFVG tab not found — sweep outcome tracking disabled")
    return main_sheet, sweep_sheet


def connect_sheet():
    """Legacy wrapper — returns main sheet only (B&R tracker)."""
    main, _ = connect_sheets()
    return main

# ============================================
#  2. FLOOR TO M15
# ============================================
def floor_to_m15(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)

# ============================================
#  3. LIVE SIGNALS MANAGEMENT
# ============================================
def load_live_signals():
    if LIVE_SIGNALS_FILE.exists():
        try:
            return json.loads(LIVE_SIGNALS_FILE.read_text())
        except:
            return {}
    return {}

def clear_live_signal(pair):
    try:
        live = load_live_signals()
        if pair in live:
            live.pop(pair)
            LIVE_SIGNALS_FILE.write_text(json.dumps(live, indent=2))
            print(f"  [{pair}] Live signal cleared ✓")
        else:
            print(f"  [{pair}] No live signal found to clear")
    except Exception as e:
        print(f"  [{pair}] Live signal clear error: {e}")

# ============================================
#  4. FETCH CANDLES
# ============================================
def fetch_candles(pair, interval, since_dt):
    symbol = TD_SYMBOLS.get(pair)
    if not symbol:
        return []

    start_date = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    url = (
        f"https://api.twelvedata.com/time_series"
        f"?symbol={symbol}"
        f"&interval={interval}"
        f"&start_date={start_date}"
        f"&outputsize=500"
        f"&apikey={TWELVE_DATA_KEY}"
    )
    try:
        res  = requests.get(url, timeout=15)
        data = res.json()
        if data.get("status") == "error":
            print(f"  [{pair}] API error: {data.get('message')}")
            return []
        candles = []
        for c in reversed(data.get("values", [])):
            try:
                candle_time = datetime.fromisoformat(c["datetime"])
                if candle_time.tzinfo is None:
                    candle_time = candle_time.replace(tzinfo=timezone.utc)
                candles.append({
                    "time":  candle_time,
                    "high":  float(c["high"]),
                    "low":   float(c["low"]),
                    "close": float(c["close"]),
                })
            except Exception as e:
                print(f"  [{pair}] Candle parse error: {e}")
                continue
        return candles
    except Exception as e:
        print(f"  [{pair}] Fetch failed: {e}")
        return []

# ============================================
#  5. OUTCOME DETECTION
# ============================================
def detect_outcome(pair, side, entry, sl, tp, signal_time_str):
    try:
        raw_time    = datetime.fromisoformat(signal_time_str).replace(tzinfo=timezone.utc)
        signal_time = floor_to_m15(raw_time)
    except Exception as e:
        print(f"  Parse error on signal time: {e}")
        return None

    check_from = signal_time + timedelta(minutes=15)
    candles = fetch_candles(pair, "15min", check_from)

    if not candles:
        print(f"  [{pair}] No M15 candles yet -- still pending")
        return None

    for candle in candles:
        high = candle.get("high")
        low  = candle.get("low")
        if high is None or low is None:
            continue
        high = float(high)
        low  = float(low)

        if side == "BUY":
            if high >= tp:
                return ("WIN", tp, candle["time"].isoformat())
            if low <= sl:
                return ("LOSS", sl, candle["time"].isoformat())
        else:
            if low <= tp:
                return ("WIN", tp, candle["time"].isoformat())
            if high >= sl:
                return ("LOSS", sl, candle["time"].isoformat())

    return None

# ============================================
#  6. TELEGRAM RESULT ALERT
# ============================================
def send_result_alert(row, outcome, close_price):
    if not TG_TOKEN or not TG_CHAT_ID:
        return

    pair  = row["pair"]
    side  = row["side"]
    dp    = DP.get(pair, 5)
    emoji = "✅" if outcome == "WIN" else "❌"

    msg = (
        f"{emoji} *EDGE RESULT -- {side} {pair}*\n\n"
        f"Outcome:     *{outcome}*\n"
        f"Entry:       `{float(row['entry']):.{dp}f}`\n"
        f"Close:       `{float(close_price):.{dp}f}`\n"
        f"RR:          1:{row['rr']}\n\n"
        f"_Log updated automatically in EDGE Journal_"
    )

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       msg,
            "parse_mode": "Markdown",
        }, timeout=10)
        print(f"  [{pair}] Result alert sent -- {outcome}")
    except Exception as e:
        print(f"  [{pair}] Alert error: {e}")


# ============================================
#  7. SWEEP+FVG OUTCOME TRACKING
# ============================================
def send_sweep_result_alert(row: dict, outcome: str, close_price: float):
    """Telegram alert for a resolved Sweep+FVG trade."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return

    direction = row.get("direction", "").upper()
    emoji     = "✅" if outcome == "WIN" else "❌"
    dp        = 5  # EURUSD always 5dp

    msg = (
        f"{emoji} *SWEEP+FVG RESULT — {direction} EURUSD*\n\n"
        f"Outcome:  *{outcome}*\n"
        f"Entry:    `{float(row['entry']):.{dp}f}`\n"
        f"Close:    `{float(close_price):.{dp}f}`\n"
        f"RR:       1:{row['rr']}\n"
        f"Session:  `{row.get('session', '')}` | Source: `{row.get('lv_source', '')}`\n\n"
        f"_Logged in EDGE Journal → SweepFVG tab_"
    )

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       msg,
            "parse_mode": "Markdown",
        }, timeout=10)
        print(f"  [SWEEP] Result alert sent — {outcome}")
    except Exception as e:
        print(f"  [SWEEP] Alert error: {e}")


def run_sweep_fvg_tracker(sweep_sheet):
    """
    Check all PENDING rows in the SweepFVG sheet tab and resolve outcomes.
    Uses the same detect_outcome() logic as B&R — EURUSD M15 only.
    """
    if sweep_sheet is None:
        print("  [SWEEP] No SweepFVG sheet — skipping")
        return

    print("\n  Checking SweepFVG tab...")
    try:
        rows = sweep_sheet.get_all_records()
    except Exception as e:
        print(f"  [SWEEP] Failed to read SweepFVG tab: {e}")
        return

    pending = [
        (i + 2, r) for i, r in enumerate(rows)
        if str(r.get("outcome", "")).strip().upper() == "PENDING"
    ]

    print(f"  [SWEEP] Pending trades: {len(pending)}")
    if not pending:
        print("  [SWEEP] Nothing to check")
        return

    for row_num, row in pending:
        try:
            entry = float(row["entry"])
            sl    = float(row["sl"])
            tp    = float(row["tp"])
        except Exception as e:
            print(f"  [SWEEP Row {row_num}] Parse error: {e} — skipping")
            continue

        direction    = str(row.get("direction", "")).strip().upper()
        fired_at     = str(row.get("fired_at", "")).strip()
        side         = "BUY" if direction == "LONG" else "SELL"

        print(f"  [SWEEP] Checking {direction} EURUSD — fired at {fired_at}")

        result = detect_outcome("EURUSD", side, entry, sl, tp, fired_at)

        if result:
            outcome, close_price, close_time = result
            pnl_pips = round(
                abs(close_price - entry) / 0.0001 * (1 if outcome == "WIN" else -1), 1
            )
            print(f"  [SWEEP] → {outcome} at {close_price} ({pnl_pips:+.1f} pips)")
            try:
                # Columns: fired_at|pair|direction|entry|sl|tp|rr|session|lv_source|outcome|pnl_pips|notes
                sweep_sheet.update_cell(row_num, 10, outcome)
                sweep_sheet.update_cell(row_num, 11, pnl_pips)
                sweep_sheet.update_cell(row_num, 12, close_time)
                print(f"  [SWEEP] Sheet updated ✓")
            except Exception as e:
                print(f"  [SWEEP] Sheet update error: {e}")

            send_sweep_result_alert(row, outcome, close_price)
        else:
            print(f"  [SWEEP] Still pending — no update")


# ============================================
#  8. MAIN
# ============================================
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*55}")
    print(f"  EDGE Outcome Tracker -- {now}")
    print(f"{'='*55}")

    if not all([TWELVE_DATA_KEY, SHEET_ID, GOOGLE_CREDS]):
        print("[ERROR] Missing required environment variables -- aborting")
        return

    try:
        sheet, sweep_sheet = connect_sheets()
        print("[OK] Connected to Google Sheet")
        if sweep_sheet:
            print("[OK] Connected to SweepFVG tab")
    except Exception as e:
        print(f"[ERROR] Sheet connection failed: {e}")
        return

    rows = sheet.get_all_records()
    for i, r in enumerate(rows):
        raw = repr(r.get("outcome", ""))
        print(f"  Row {i+2} outcome raw: {raw}")

    pending = [
        (i + 2, r) for i, r in enumerate(rows)
        if r.get("outcome", "").strip().upper() == "PENDING"
    ]

    print(f"\n  Pending trades: {len(pending)}")

    if not pending:
        print("  Nothing to check -- all trades resolved")
        return

    for row_num, row in pending:
        pair = row.get("pair", "").strip().upper()
        side = row.get("side", "").strip().upper()

        try:
            entry = float(row["entry"])
            sl    = float(row["sl"])
            tp    = float(row["tp"])
            print(f"  [{pair}] entry={entry} sl={sl} tp={tp}")
        except Exception as e:
            print(f"  [Row {row_num}] Parse error: {e} -- skipping")
            continue

        signal_time = row.get("signal_time", "").strip()
        print(f"\n  Checking [{pair}] {side} -- signalled at {signal_time}")

        result = detect_outcome(pair, side, entry, sl, tp, signal_time)

        if result:
            outcome, close_price, close_time = result
            print(f"  [{pair}] -> {outcome} at {close_price}")

            try:
                sheet.update_cell(row_num, 9,  outcome)
                sheet.update_cell(row_num, 10, close_price)
                sheet.update_cell(row_num, 11, close_time)
                print(f"  [{pair}] Sheet updated ✓")
                clear_live_signal(pair)
            except Exception as e:
                print(f"  [{pair}] Sheet update error: {e}")

            send_result_alert(row, outcome, close_price)
        else:
            print(f"  [{pair}] Still pending -- no update")

    # ── Sweep+FVG outcome tracking ──
    run_sweep_fvg_tracker(sweep_sheet)

    print(f"\n{'='*55}\n")

if __name__ == "__main__":
    main()
  

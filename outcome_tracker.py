"""
EDGE Outcome Tracker
====================
Monitors pending trades from Google Sheets and automatically
detects outcomes (WIN/LOSS) using Twelve Data API.

Resolution logic:
  1. Fetch candles from signal time onwards (API-level filtering)
  2. If ambiguous (single candle hits both), drop to M1
  3. If still ambiguous, leave as PENDING

Runs every 30 minutes via GitHub Actions cron
"""

import os
import json
import requests
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
import gspread

# ============================================
#  CONFIG
# ============================================
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN        = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID      = os.environ.get("TG_CHAT_ID", "")
SHEET_ID        = os.environ.get("SHEET_ID", "")
GOOGLE_CREDS    = os.environ.get("GOOGLE_CREDENTIALS", "")

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
def connect_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

# ============================================
#  2. FLOOR TO M15
# ============================================
def floor_to_m15(dt):
    """Round datetime down to nearest 15-minute boundary."""
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)

# ============================================
#  3. FETCH CANDLES
# ============================================
def fetch_candles(pair, interval, since_dt):
    """Fetch candles from Twelve Data starting from since_dt."""
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
                })
            except Exception as e:
                print(f"  [{pair}] Candle parse error: {e}")
                continue
        return candles

    except Exception as e:
        print(f"  [{pair}] Fetch failed: {e}")
        return []

# ============================================
#  4. OUTCOME DETECTION
# ============================================
def detect_outcome(pair, side, entry, sl, tp, signal_time_str):
    """
    Scan candles since signal time to detect WIN or LOSS.
    Returns (outcome, close_price, close_time) or None if still pending.
    """
    try:
        raw_time    = datetime.fromisoformat(signal_time_str).replace(tzinfo=timezone.utc)
        signal_time = floor_to_m15(raw_time)
    except Exception as e:
        print(f"  Parse error on signal time: {e}")
        return None

    # Step 1: Scan M15 candles from signal time
    candles = fetch_candles(pair, "15min", signal_time)
    if not candles:
        print(f"  [{pair}] No M15 candles yet -- still pending")
        return None

    for candle in candles:
        tp_hit = candle["high"] >= tp if side == "BUY" else candle["low"] <= tp
        sl_hit = candle["low"] <= sl if side == "BUY" else candle["high"] >= sl

        if tp_hit and not sl_hit:
            return ("WIN", tp, candle["time"].isoformat())

        if sl_hit and not tp_hit:
            return ("LOSS", sl, candle["time"].isoformat())

        if tp_hit and sl_hit:
            # Step 2: Ambiguous -- drop to M1
            print(f"  [{pair}] Ambiguous M15 candle -- checking M1...")
            m1_candles = fetch_candles(pair, "1min", signal_time)
            m1_window = [
                c for c in m1_candles
                if c["time"] <= candle["time"]
            ]
            for m1 in m1_window:
                m1_tp_hit = m1["high"] >= tp if side == "BUY" else m1["low"] <= tp
                m1_sl_hit = m1["low"] <= sl if side == "BUY" else m1["high"] >= sl

                if m1_tp_hit and not m1_sl_hit:
                    return ("WIN", tp, m1["time"].isoformat())
                if m1_sl_hit and not m1_tp_hit:
                    return ("LOSS", sl, m1["time"].isoformat())

            # Step 3: M1 still ambiguous -- leave as PENDING
            print(f"  [{pair}] M1 ambiguous -- leaving as PENDING")
            return None

    return None  # Still pending -- neither level hit yet

# ============================================
#  5. TELEGRAM RESULT ALERT
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
#  6. MAIN
# ============================================
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*55}")
    print(f"  EDGE Outcome Tracker -- {now}")
    print(f"{'='*55}")

    if not all([TWELVE_DATA_KEY, SHEET_ID, GOOGLE_CREDS]):
        print("[ERROR] Missing required environment variables -- aborting")
        return

    # Connect to sheet
    try:
        sheet = connect_sheet()
        print("[OK] Connected to Google Sheet")
    except Exception as e:
        print(f"[ERROR] Sheet connection failed: {e}")
        return

    # Read all rows
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
        except Exception as e:
            print(f"  [Row {row_num}] Parse error: {e} -- skipping")
            continue

        signal_time = row.get("signal_time", "").strip()
        print(f"\n  Checking [{pair}] {side} -- signalled at {signal_time}")

        result = detect_outcome(pair, side, entry, sl, tp, signal_time)

        if result:
            outcome, close_price, close_time = result
            print(f"  [{pair}] -> {outcome} at {close_price}")

            # Update sheet row
            try:
                sheet.update_cell(row_num, 9,  outcome)
                sheet.update_cell(row_num, 10, close_price)
                sheet.update_cell(row_num, 11, close_time)
                print(f"  [{pair}] Sheet updated ✓")
            except Exception as e:
                print(f"  [{pair}] Sheet update error: {e}")

            send_result_alert(row, outcome, close_price)
        else:
            print(f"  [{pair}] Still pending -- no update")

    print(f"\n{'='*55}\n")

if __name__ == "__main__":
    main()
  

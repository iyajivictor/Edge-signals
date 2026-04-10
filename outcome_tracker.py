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

LIVE_SIGNALS_FILE = Path("state/live_signals.json")

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

    print(f"  [{pair}] First candle: {candles[0]['time']} close={candles[0].get('close', 'N/A')}")
    print(f"  [{pair}] check_from was: {check_from}")

    entry_confirmed = False

    for candle in candles:
        close = candle.get("close")
        if close is None:
            continue
        close = float(close)

        if not entry_confirmed:
            if side == "BUY" and close >= entry:
                entry_confirmed = True
            elif side == "SELL" and close <= entry:
                entry_confirmed = True
            else:
                continue

        if side == "BUY":
            if close >= tp:
                return ("WIN", tp, candle["time"].isoformat())
            if close <= sl:
                return ("LOSS", sl, candle["time"].isoformat())
        else:
            if close <= tp:
                return ("WIN", tp, candle["time"].isoformat())
            if close >= sl:
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
#  7. MAIN
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
        sheet = connect_sheet()
        print("[OK] Connected to Google Sheet")
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

    print(f"\n{'='*55}\n")

if __name__ == "__main__":
    main()
  

"""
EDGE Outcome Tracker
====================
B&R resolution: unchanged — M15 close-based, single TP.
Sweep+FVG resolution: fallback reconciler (primary = Replit tracker).
  · Reads SweepFVG state DIRECTLY from sheet — no stale JSON dependency
  · Processes PENDING_ENTRY: monitors for FVG retrace trigger, updates ACTIVE
  · Processes ACTIVE (Phase 1 fallback): checks TP1 / SL
  · Processes ACTIVE tp1_hit (Phase 2 fallback): checks TP2 / BE

Changes vs previous version:
  · run_sweep_tracker() now reads sheet rows directly instead of
    sweep_fvg_live.json (which Render never writes — was always empty)
  · main() uses get_all_values() for Sheet1 B&R to avoid
    get_all_records() duplicate-header crash
  · Safe float conversion helper added
"""

import os, json, requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
TG_TOKEN        = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID      = os.environ.get("TG_CHAT_ID", "")
SHEET_ID        = os.environ.get("SHEET_ID", "")
GOOGLE_CREDS    = os.environ.get("GOOGLE_CREDENTIALS", "")

LIVE_SIGNALS_FILE = Path("state/live_signals.json")

TD_SYMBOLS = {
    "EURUSD":"EUR/USD","GBPUSD":"GBP/USD","USDJPY":"USD/JPY",
    "AUDJPY":"AUD/JPY","XAUUSD":"XAU/USD","CADJPY":"CAD/JPY",
    "USDCAD":"USD/CAD","EURJPY":"EUR/JPY","GBPJPY":"GBP/JPY","GBPAUD":"GBP/AUD",
}
DP = {
    "EURUSD":5,"GBPUSD":5,"USDCAD":5,"GBPAUD":5,
    "USDJPY":3,"AUDJPY":3,"CADJPY":3,"EURJPY":3,"GBPJPY":3,"XAUUSD":2,
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def now_str():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def _f(val, default=0.0):
    """Safe float conversion."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default

def _tg(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10)
    except Exception as e:
        print(f"  [TG] {e}")

def load_json(path):
    if Path(path).exists():
        try:
            return json.loads(Path(path).read_text())
        except:
            return {}
    return {}

def save_json(path, d):
    Path(path).parent.mkdir(exist_ok=True)
    Path(path).write_text(json.dumps(d, indent=2))

load_live_br = lambda: load_json(LIVE_SIGNALS_FILE)

def clear_live_br(pair):
    live = load_live_br()
    if pair in live:
        live.pop(pair)
        save_json(LIVE_SIGNALS_FILE, live)

# ── SHEETS ────────────────────────────────────────────────────────────────────
def connect_sheets():
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    wb = gspread.authorize(creds).open_by_key(SHEET_ID)
    try:
        sweep = wb.worksheet("SweepFVG")
    except:
        sweep = None
        print("[WARN] SweepFVG tab not found")
    return wb.sheet1, sweep

# ── FETCH ─────────────────────────────────────────────────────────────────────
def fetch_candles_since(pair, since_dt):
    sym = TD_SYMBOLS.get(pair)
    if not sym:
        return []
    url = (f"https://api.twelvedata.com/time_series?symbol={sym}"
           f"&interval=15min&start_date={since_dt.strftime('%Y-%m-%d %H:%M:%S')}"
           f"&outputsize=500&apikey={TWELVE_DATA_KEY}")
    try:
        data = requests.get(url, timeout=15).json()
        if data.get("status") == "error":
            return []
        out = []
        for c in reversed(data.get("values", [])):
            try:
                t = datetime.fromisoformat(c["datetime"])
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                out.append({"time": t, "high": float(c["high"]),
                            "low": float(c["low"]), "close": float(c["close"])})
            except:
                continue
        return out
    except Exception as e:
        print(f"  [{pair}] fetch error: {e}")
        return []

def fetch_price(pair):
    sym = TD_SYMBOLS.get(pair)
    if not sym:
        return None
    try:
        p = requests.get(
            f"https://api.twelvedata.com/price?symbol={sym}&apikey={TWELVE_DATA_KEY}",
            timeout=10).json().get("price")
        return float(p) if p else None
    except:
        return None

# ── B&R DETECTION ─────────────────────────────────────────────────────────────
def detect_br_outcome(pair, side, entry, sl, tp, sig_time_str):
    try:
        raw   = datetime.fromisoformat(sig_time_str).replace(tzinfo=timezone.utc)
        since = raw.replace(minute=(raw.minute // 15) * 15, second=0,
                            microsecond=0) + timedelta(minutes=15)
    except Exception as e:
        print(f"  Parse: {e}")
        return None

    candles = fetch_candles_since(pair, since)
    if not candles:
        return None
    confirmed = False
    for c in candles:
        cl = c["close"]
        if not confirmed:
            if side == "BUY"  and cl >= entry: confirmed = True
            elif side == "SELL" and cl <= entry: confirmed = True
            else: continue
        if side == "BUY":
            if cl >= tp: return ("WIN",  cl, c["time"].isoformat())
            if cl <= sl: return ("LOSS", cl, c["time"].isoformat())
        else:
            if cl <= tp: return ("WIN",  cl, c["time"].isoformat())
            if cl >= sl: return ("LOSS", cl, c["time"].isoformat())
    return None

def send_br_result(row_dict, outcome, close_price):
    pair = row_dict.get("pair", "")
    dp   = DP.get(pair, 5)
    emoji = "✅" if outcome == "WIN" else "❌"
    _tg(
        f"{emoji} *EDGE RESULT — {row_dict.get('side','')} {pair}*\n\n"
        f"Outcome : *{outcome}*\n"
        f"Entry   : `{_f(row_dict.get('entry')):.{dp}f}`\n"
        f"Close   : `{float(close_price):.{dp}f}`\n"
        f"RR      : 1:{row_dict.get('rr','')}\n\n"
        f"_Log updated in EDGE Journal_"
    )

# ── SWEEP ALERTS ──────────────────────────────────────────────────────────────
def send_tp1_alert(sig):
    pair = sig['pair']
    dp   = DP.get(pair, 5)
    side = 'SELL' if sig.get('direction') == 'short' else 'BUY'
    tp2  = sig.get('tp2')
    tp2_line = (f"TP2 active : `{_f(tp2):.{dp}f}`  RR `1:{sig.get('rr_tp2','?')}`"
                if tp2 else "TP2 : N/A (SINGLE mode)")
    _tg(
        f"🎯 *{pair} — TP1 Hit!*\n\n"
        f"Direction : `{side}`\n"
        f"TP1       : `{_f(sig['tp1']):.{dp}f}` ✅\n"
        f"SL → BE   : `{_f(sig['entry']):.{dp}f}` ← move stop now\n"
        f"{tp2_line}\n\n"
        f"_50% closed. Remainder running to TP2._\n"
        f"_⚡ EDGE Outcome Tracker — Sweep+FVG fallback_"
    )
    print(f"  [{pair}] TP1 alert sent")

def send_outcome_alert(sig, outcome, price):
    pair = sig['pair']
    dp   = DP.get(pair, 5)
    side = 'SELL' if sig.get('direction') == 'short' else 'BUY'
    r1   = _f(sig.get('rr_tp1'), 1.5)
    r2   = _f(sig.get('rr_tp2'), 0.0)
    if outcome == 'FULL_WIN':
        emoji = '✅✅'; label = 'FULL WIN'
        pnl = f"+{r1 * 0.5 + r2 * 0.5:.2f}R"
    elif outcome == 'TP1_BE':
        emoji = '✅'; label = 'TP1 + Breakeven'
        pnl = f"+{r1 * 0.5:.2f}R"
    else:
        emoji = '❌'; label = 'LOSS'; pnl = "-1.00R"
    _tg(
        f"{emoji} *SWEEP RESULT — {side} {pair}*\n\n"
        f"Outcome : *{label}*\n"
        f"Entry   : `{_f(sig['entry']):.{dp}f}`\n"
        f"Close   : `{price:.{dp}f}`\n"
        f"P&L     : `{pnl}`\n\n"
        f"_Logged → SweepFVG tab_\n"
        f"_⚡ EDGE Outcome Tracker — Sweep+FVG fallback_"
    )
    print(f"  [{pair}] Outcome alert — {outcome}")

# ── SWEEP SHEET UPDATERS ──────────────────────────────────────────────────────
def _find_sweep_row(sheet, sig):
    rows = sheet.get_all_values()
    for i, row in enumerate(rows[1:], start=2):
        if (len(row) >= 3 and
                row[0] == sig.get('fired_at', '') and
                row[1] == sig.get('pair', '') and
                row[2].upper() == sig.get('direction', '').upper()):
            return i
    return None

def mark_tp1_in_sheet(sheet, sig):
    try:
        row = _find_sweep_row(sheet, sig)
        if row:
            sheet.update_cell(row, 16, 'WIN')
            print(f"  TP1 marked row {row}")
        else:
            print(f"  TP1 row not found — {sig.get('pair')}")
    except Exception as e:
        print(f"  TP1 sheet err: {e}")

def mark_active_in_sheet(sheet, sig):
    try:
        row = _find_sweep_row(sheet, sig)
        if row:
            sheet.update_cell(row, 14, 'ACTIVE')
            sheet.update_cell(row, 15, now_str())
            print(f"  [{sig.get('pair')}] Sheet updated ACTIVE row {row}")
        else:
            print(f"  [{sig.get('pair')}] ACTIVE row not found")
    except Exception as e:
        print(f"  [{sig.get('pair')}] ACTIVE sheet err: {e}")

def mark_final_in_sheet(sheet, sig, outcome):
    try:
        row = _find_sweep_row(sheet, sig)
        if not row:
            print(f"  Final row not found — {sig.get('pair')}")
            return
        r1 = _f(sig.get('rr_tp1'), 1.5)
        r2 = _f(sig.get('rr_tp2'), 0.0)
        if outcome == 'LOSS':
            pnl = -1.0
            sheet.update_cell(row, 16, 'LOSS')
            sheet.update_cell(row, 17, 'LOSS')
        elif outcome == 'TP1_BE':
            pnl = round(r1 * 0.5, 2)
            sheet.update_cell(row, 17, 'TP1_BE')
        else:
            pnl = round(r1 * 0.5 + r2 * 0.5, 2)
            sheet.update_cell(row, 17, 'FULL_WIN')
        sheet.update_cell(row, 14, 'CLOSED')
        sheet.update_cell(row, 18, pnl)
        print(f"  Final row {row} — {outcome} {pnl:+.2f}R ✓")
    except Exception as e:
        print(f"  Final sheet err: {e}")

# ── ORPHAN CLEANUP ────────────────────────────────────────────────────────────
def cleanup_orphans(pending_pairs):
    live = load_live_br()
    for pair in list(live.keys()):
        if pair not in pending_pairs:
            print(f"  [{pair}] Orphaned B&R — clearing")
            clear_live_br(pair)

# ── SWEEP FALLBACK TRACKER ────────────────────────────────────────────────────
def run_sweep_tracker(sweep_sheet):
    """
    Sweep+FVG outcome resolution.
    Reads state DIRECTLY from the SweepFVG sheet — no JSON file dependency.

    PENDING_ENTRY → outcome tracker PRIMARY:
        Checks current price for FVG retrace. On trigger, marks ACTIVE in
        sheet so Render picks it up on next sync cycle.

    ACTIVE tp1_hit=False → Replit PRIMARY, outcome tracker FALLBACK:
        Checks TP1 and SL. On hit, sends alert and updates sheet.

    ACTIVE tp1_hit=True  → Replit PRIMARY, outcome tracker FALLBACK:
        Checks TP2 and BE. On hit, sends alert and updates sheet.
    """
    if not sweep_sheet:
        print("  [SWEEP] No sheet")
        return

    # ── Read all rows directly from sheet ────────────────────────────────
    try:
        all_rows = sweep_sheet.get_all_values()[1:]  # skip header
    except Exception as e:
        print(f"  [SWEEP] Sheet read error: {e}")
        return

    pending_entry = {}
    phase1        = {}
    phase2        = {}

    for row in all_rows:
        if len(row) < 14:
            continue
        status = row[13].strip()
        if status not in ('PENDING_ENTRY', 'ACTIVE'):
            continue

        pair      = row[1].strip().upper()
        direction = row[2].strip().lower()
        fired_at  = row[0].strip()

        if pair not in TD_SYMBOLS:
            continue

        key      = f"{pair}_{direction}_{fired_at}"
        tp1_hit  = (row[15].strip() == 'WIN') if len(row) > 15 else False

        sig = {
            'fired_at'  : fired_at,
            'pair'      : pair,
            'direction' : direction,
            'entry'     : row[3],
            'sl'        : row[4],
            'tp1'       : row[5],
            'tp2'       : row[6] if len(row) > 6 and row[6] else None,
            'rr_tp1'    : row[7] if len(row) > 7 and row[7] else '1.5',
            'rr_tp2'    : row[8] if len(row) > 8 and row[8] else None,
            'status'    : status,
            'tp1_hit'   : tp1_hit,
        }

        if status == 'PENDING_ENTRY':
            # Only need FVG bounds for entry trigger check
            fvg_top    = row[18] if len(row) > 18 and row[18] else None
            fvg_bottom = row[19] if len(row) > 19 and row[19] else None
            if not fvg_top or not fvg_bottom:
                continue
            sig['fvg_top']    = fvg_top
            sig['fvg_bottom'] = fvg_bottom
            pending_entry[key] = sig
        elif not tp1_hit:
            phase1[key] = sig
        else:
            phase2[key] = sig

    if not pending_entry and not phase1 and not phase2:
        print("  [SWEEP] No active signals in sheet")
        return

    print(f"  [SWEEP] Pending: {len(pending_entry)} | "
          f"Phase1 fallback: {len(phase1)} | Phase2 fallback: {len(phase2)}")

    # ── PENDING ENTRY — outcome tracker is primary ────────────────────────
    for key, sig in pending_entry.items():
        pair       = sig['pair']
        direction  = sig['direction']
        fvg_top    = _f(sig['fvg_top'])
        fvg_bottom = _f(sig['fvg_bottom'])

        price = fetch_price(pair)
        if price is None:
            continue

        dp = DP.get(pair, 5)
        # Retrace logic: same as Render's Phase 0
        if direction == 'short':
            triggered = price >= fvg_bottom
        else:
            triggered = price <= fvg_top

        print(f"  [{pair}] Pending price={price:.{dp}f} "
              f"fvg=[{fvg_bottom:.{dp}f}–{fvg_top:.{dp}f}] "
              f"triggered={triggered}")

        if triggered:
            print(f"  [{pair}] Entry triggered — marking ACTIVE, Replit takes over")
            mark_active_in_sheet(sweep_sheet, sig)

    # ── PHASE 1 FALLBACK — Replit is primary ─────────────────────────────
    for key, sig in phase1.items():
        pair      = sig['pair']
        direction = sig['direction']
        sl        = _f(sig['sl'])
        tp1       = _f(sig['tp1'])

        price = fetch_price(pair)
        if price is None:
            continue

        dp = DP.get(pair, 5)
        print(f"  [{pair}] Phase1 fallback price={price:.{dp}f}")

        tp1_hit = price <= tp1 if direction == 'short' else price >= tp1
        sl_hit  = price >= sl  if direction == 'short' else price <= sl

        if tp1_hit:
            print(f"  [{pair}] TP1 hit (fallback)")
            send_tp1_alert(sig)
            mark_tp1_in_sheet(sweep_sheet, sig)
        elif sl_hit:
            print(f"  [{pair}] LOSS (fallback)")
            send_outcome_alert(sig, 'LOSS', price)
            mark_final_in_sheet(sweep_sheet, sig, 'LOSS')

    # ── PHASE 2 FALLBACK — Replit is primary ─────────────────────────────
    for key, sig in phase2.items():
        pair      = sig['pair']
        direction = sig['direction']
        entry     = _f(sig['entry'])
        tp2_raw   = sig.get('tp2')

        if not tp2_raw:
            continue  # SINGLE mode — no TP2 to monitor

        tp2 = _f(tp2_raw)
        be  = entry

        price = fetch_price(pair)
        if price is None:
            continue

        dp = DP.get(pair, 5)
        print(f"  [{pair}] Phase2 fallback price={price:.{dp}f}")

        tp2_reached = price <= tp2 if direction == 'short' else price >= tp2
        be_reached  = price >= be  if direction == 'short' else price <= be

        if tp2_reached:
            print(f"  [{pair}] FULL WIN (fallback)")
            send_outcome_alert(sig, 'FULL_WIN', price)
            mark_final_in_sheet(sweep_sheet, sig, 'FULL_WIN')
        elif be_reached:
            print(f"  [{pair}] TP1_BE (fallback)")
            send_outcome_alert(sig, 'TP1_BE', price)
            mark_final_in_sheet(sweep_sheet, sig, 'TP1_BE')

    print(f"  [SWEEP] Cycle complete")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  EDGE Outcome Tracker — {now_str()}")
    print(f"{'='*55}")

    if not all([TWELVE_DATA_KEY, SHEET_ID, GOOGLE_CREDS]):
        print("[ERROR] Missing env vars")
        return

    try:
        sheet, sweep_sheet = connect_sheets()
        print("[OK] Sheets connected")
    except Exception as e:
        print(f"[ERROR] Sheet connection: {e}")
        return

    # ── B&R section — use get_all_values() to avoid header duplicate crash ──
    try:
        all_rows = sheet.get_all_values()
    except Exception as e:
        print(f"[ERROR] Sheet1 read: {e}")
        return

    if len(all_rows) < 2:
        print("  B&R: no data rows")
    else:
        header = all_rows[0]
        # Map column names to indices safely
        col = {h.strip(): i for i, h in enumerate(header)}

        pending = []
        for i, row in enumerate(all_rows[1:], start=2):
            def get(name, default=''):
                idx = col.get(name)
                return row[idx].strip() if idx is not None and idx < len(row) else default

            outcome_val = get('outcome', '')
            if outcome_val.upper() == 'PENDING':
                pending.append((i, {
                    'pair'       : get('pair'),
                    'side'       : get('side'),
                    'entry'      : get('entry'),
                    'sl'         : get('sl'),
                    'tp'         : get('tp'),
                    'rr'         : get('rr'),
                    'signal_time': get('signal_time'),
                }))

        print(f"\n  B&R pending: {len(pending)}")
        cleanup_orphans({r['pair'].upper() for _, r in pending})

        for row_num, row in pending:
            pair = row.get('pair', '').upper()
            side = row.get('side', '').upper()
            try:
                entry = float(row['entry'])
                sl    = float(row['sl'])
                tp    = float(row['tp'])
            except Exception as e:
                print(f"  [Row {row_num}] Parse error: {e}")
                continue

            print(f"\n  [{pair}] {side} checking...")
            result = detect_br_outcome(
                pair, side, entry, sl, tp,
                row.get('signal_time', '').strip()
            )
            if result:
                outcome, close_price, close_time = result
                print(f"  [{pair}] {outcome} @ {close_price}")
                try:
                    sheet.update_cell(row_num, 9,  outcome)
                    sheet.update_cell(row_num, 10, close_price)
                    sheet.update_cell(row_num, 11, close_time)
                    clear_live_br(pair)
                except Exception as e:
                    print(f"  [{pair}] Sheet err: {e}")
                send_br_result(row, outcome, close_price)
            else:
                print(f"  [{pair}] Still pending")

    # ── Sweep+FVG section ────────────────────────────────────────────────────
    run_sweep_tracker(sweep_sheet)

    print(f"\n{'='*55}\n")

if __name__ == "__main__":
    main()

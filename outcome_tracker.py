"""
EDGE Outcome Tracker
====================
B&R resolution: unchanged — M15 close-based, single TP.
Sweep+FVG resolution: fallback reconciler (primary = Replit tracker).
  · Only processes signals with status=ACTIVE or tp1_hit=True
  · Ignores PENDING_ENTRY — that's pre-entry, no trade open yet
  · Checks current price via /price endpoint each run
  · Sends TP1 alert + updates sheet on phase-1 hit
  · Sends final outcome + updates sheet on phase-2 resolution

Sheet layout SweepFVG tab (18 cols):
  1 fired_at  5 sl    9 rr_tp2   13 sweep_time  17 tp2_outcome
  2 pair       6 tp1  10 sl_pips  14 status       18 pnl_r
  3 direction  7 tp2  11 zone_src 15 entry_time
  4 entry      8 rr_tp1 12 session 16 tp1_outcome

Pairs: all 10
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
SWEEP_LIVE_FILE   = Path("state/sweep_fvg_live.json")

TD_SYMBOLS = {
    "EURUSD":"EUR/USD","GBPUSD":"GBP/USD","USDJPY":"USD/JPY",
    "AUDJPY":"AUD/JPY","XAUUSD":"XAU/USD","CADJPY":"CAD/JPY",
    "USDCAD":"USD/CAD","EURJPY":"EUR/JPY","GBPJPY":"GBP/JPY","GBPAUD":"GBP/AUD",
}
DP = {
    "EURUSD":5,"GBPUSD":5,"USDCAD":5,"GBPAUD":5,
    "USDJPY":3,"AUDJPY":3,"CADJPY":3,"EURJPY":3,"GBPJPY":3,"XAUUSD":2,
}

# ── SHEETS ────────────────────────────────────────────────────────────────────
def connect_sheets():
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    wb = gspread.authorize(creds).open_by_key(SHEET_ID)
    try:    sweep = wb.worksheet("SweepFVG")
    except: sweep = None; print("[WARN] SweepFVG tab not found")
    return wb.sheet1, sweep

# ── HELPERS ───────────────────────────────────────────────────────────────────
def now_str(): return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def _tg(msg):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id":TG_CHAT_ID,"text":msg,"parse_mode":"Markdown"},
                      timeout=10)
    except Exception as e: print(f"  [TG] {e}")

def load_json(path):
    if Path(path).exists():
        try: return json.loads(Path(path).read_text())
        except: return {}
    return {}

def save_json(path, d):
    Path(path).parent.mkdir(exist_ok=True)
    Path(path).write_text(json.dumps(d, indent=2))

load_live_br     = lambda: load_json(LIVE_SIGNALS_FILE)
load_sweep_live  = lambda: load_json(SWEEP_LIVE_FILE)
save_sweep_live  = lambda d: save_json(SWEEP_LIVE_FILE, d)

def clear_live_br(pair):
    live = load_live_br()
    if pair in live:
        live.pop(pair)
        save_json(LIVE_SIGNALS_FILE, live)

# ── FETCH ─────────────────────────────────────────────────────────────────────
def fetch_candles_since(pair, since_dt):
    sym = TD_SYMBOLS.get(pair)
    if not sym: return []
    url = (f"https://api.twelvedata.com/time_series?symbol={sym}"
           f"&interval=15min&start_date={since_dt.strftime('%Y-%m-%d %H:%M:%S')}"
           f"&outputsize=500&apikey={TWELVE_DATA_KEY}")
    try:
        data = requests.get(url, timeout=15).json()
        if data.get("status") == "error": return []
        out = []
        for c in reversed(data.get("values", [])):
            try:
                t = datetime.fromisoformat(c["datetime"])
                if t.tzinfo is None: t = t.replace(tzinfo=timezone.utc)
                out.append({"time":t,"high":float(c["high"]),
                            "low":float(c["low"]),"close":float(c["close"])})
            except: continue
        return out
    except Exception as e: print(f"  [{pair}] fetch error: {e}"); return []

def fetch_price(pair):
    sym = TD_SYMBOLS.get(pair)
    if not sym: return None
    try:
        p = requests.get(
            f"https://api.twelvedata.com/price?symbol={sym}&apikey={TWELVE_DATA_KEY}",
            timeout=10).json().get("price")
        return float(p) if p else None
    except: return None

# ── B&R DETECTION ─────────────────────────────────────────────────────────────
def detect_br_outcome(pair, side, entry, sl, tp, sig_time_str):
    try:
        raw   = datetime.fromisoformat(sig_time_str).replace(tzinfo=timezone.utc)
        since = raw.replace(minute=(raw.minute//15)*15, second=0,
                             microsecond=0) + timedelta(minutes=15)
    except Exception as e: print(f"  Parse: {e}"); return None

    candles = fetch_candles_since(pair, since)
    if not candles: return None
    confirmed = False
    for c in candles:
        cl = c["close"]
        if not confirmed:
            if side=="BUY"  and cl>=entry: confirmed=True
            elif side=="SELL" and cl<=entry: confirmed=True
            else: continue
        if side=="BUY":
            if cl>=tp: return ("WIN", cl, c["time"].isoformat())
            if cl<=sl: return ("LOSS",cl, c["time"].isoformat())
        else:
            if cl<=tp: return ("WIN", cl, c["time"].isoformat())
            if cl>=sl: return ("LOSS",cl, c["time"].isoformat())
    return None

def send_br_result(row, outcome, close_price):
    pair=row["pair"]; dp=DP.get(pair,5); emoji="✅" if outcome=="WIN" else "❌"
    _tg(f"{emoji} *EDGE RESULT — {row['side']} {pair}*\n\n"
        f"Outcome : *{outcome}*\nEntry : `{float(row['entry']):.{dp}f}`\n"
        f"Close : `{float(close_price):.{dp}f}`\nRR : 1:{row['rr']}\n\n"
        f"_Log updated in EDGE Journal_")

# ── SWEEP ALERTS ──────────────────────────────────────────────────────────────
def send_tp1_alert(sig):
    pair=sig['pair']; dp=DP.get(pair,5)
    side='SELL' if sig.get('direction')=='short' else 'BUY'
    _tg(f"🎯 *{pair} — TP1 Hit!*\n\n"
        f"Direction : `{side}`\n"
        f"TP1 : `{float(sig['tp1']):.{dp}f}` ✅\n"
        f"SL → BE : `{float(sig['entry']):.{dp}f}` ← move stop now\n"
        f"TP2 active : `{float(sig['tp2']):.{dp}f}`  RR `1:{sig.get('rr_tp2','?')}`\n\n"
        f"_50% closed. Remainder running to TP2._")
    print(f"  [{pair}] TP1 alert sent")

def send_outcome_alert(sig, outcome, price):
    pair=sig['pair']; dp=DP.get(pair,5)
    side='SELL' if sig.get('direction')=='short' else 'BUY'
    r1=float(sig.get('rr_tp1',1.5)); r2=float(sig.get('rr_tp2',3.0))
    if outcome=='FULL_WIN': emoji='✅✅'; label='FULL WIN'; pnl=f"+{r1*.5+r2*.5:.2f}R"
    elif outcome=='TP1_BE': emoji='✅';  label='TP1 + Breakeven'; pnl=f"+{r1*.5:.2f}R"
    else:                   emoji='❌';  label='LOSS'; pnl="-1.00R"
    _tg(f"{emoji} *SWEEP RESULT — {side} {pair}*\n\n"
        f"Outcome : *{label}*\nEntry : `{float(sig['entry']):.{dp}f}`\n"
        f"Close : `{price:.{dp}f}`\nP&L : `{pnl}`\n\n"
        f"_Logged → SweepFVG tab_")
    print(f"  [{pair}] Outcome alert — {outcome}")

# ── SWEEP SHEET UPDATERS ──────────────────────────────────────────────────────
def _find_row(sheet, sig):
    rows = sheet.get_all_values()
    for i, row in enumerate(rows[1:], start=2):
        if (len(row)>=3 and row[0]==sig.get('fired_at','') and
                row[1]==sig.get('pair','') and
                row[2].upper()==sig.get('direction','').upper()):
            return i
    return None

def mark_tp1_in_sheet(sheet, sig):
    try:
        row = _find_row(sheet, sig)
        if row: sheet.update_cell(row, 16, 'WIN'); print(f"  TP1 marked row {row}")
        else:   print(f"  TP1 row not found — {sig.get('pair')}")
    except Exception as e: print(f"  TP1 sheet err: {e}")

def mark_final_in_sheet(sheet, sig, outcome):
    try:
        row = _find_row(sheet, sig)
        if not row: print(f"  Final row not found — {sig.get('pair')}"); return
        r1=float(sig.get('rr_tp1',1.5)); r2=float(sig.get('rr_tp2',3.0))
        if outcome=='LOSS':
            pnl=-1.0; sheet.update_cell(row,16,'LOSS'); sheet.update_cell(row,17,'LOSS')
        elif outcome=='TP1_BE':
            pnl=round(r1*.5,2); sheet.update_cell(row,17,'TP1_BE')
        else:
            pnl=round(r1*.5+r2*.5,2); sheet.update_cell(row,17,'FULL_WIN')
        sheet.update_cell(row,14,'CLOSED')
        sheet.update_cell(row,18,pnl)
        print(f"  Final row {row} — {outcome} {pnl:+.2f}R ✓")
    except Exception as e: print(f"  Final sheet err: {e}")

# ── ORPHAN CLEANUP ────────────────────────────────────────────────────────────
def cleanup_orphans(pending_pairs):
    live = load_live_br()
    for pair in list(live.keys()):
        if pair not in pending_pairs:
            print(f"  [{pair}] Orphaned B&R — clearing"); clear_live_br(pair)

# ── SWEEP FALLBACK TRACKER ────────────────────────────────────────────────────
def run_sweep_tracker(sweep_sheet):
    """
    Fallback reconciler — catches anything Replit missed.
    Only monitors ACTIVE signals (tp1_hit=False or True).
    Ignores PENDING_ENTRY — no trade open yet.
    """
    if not sweep_sheet: print("  [SWEEP] No sheet"); return

    live   = load_sweep_live()
    active = {k: v for k, v in live.items()
              if v.get('status') == 'ACTIVE' or v.get('tp1_hit', False)}

    if not active: print("  [SWEEP] No active signals"); return
    print(f"  [SWEEP] Checking {len(active)} signal(s)...")

    resolved = []
    for key, sig in active.items():
        pair    = sig.get('pair'); direction = sig.get('direction')
        entry   = float(sig.get('entry',0)); sl = float(sig.get('sl',0))
        tp1     = float(sig.get('tp1',0));   tp2 = float(sig.get('tp2',0))
        tp1_hit = sig.get('tp1_hit', False)
        if pair not in TD_SYMBOLS: continue

        price = fetch_price(pair)
        if price is None: continue

        dp = DP.get(pair, 5)
        print(f"  [{pair}] price={price:.{dp}f} tp1_hit={tp1_hit}")

        if not tp1_hit:
            if (price<=tp1 if direction=='short' else price>=tp1):
                print(f"  [{pair}] TP1 hit"); sig['tp1_hit']=True
                send_tp1_alert(sig); mark_tp1_in_sheet(sweep_sheet, sig)
            elif (price>=sl if direction=='short' else price<=sl):
                print(f"  [{pair}] LOSS")
                send_outcome_alert(sig,'LOSS',price)
                mark_final_in_sheet(sweep_sheet,sig,'LOSS')
                resolved.append(key)
        else:
            be = entry
            if (price<=tp2 if direction=='short' else price>=tp2):
                print(f"  [{pair}] FULL WIN")
                send_outcome_alert(sig,'FULL_WIN',price)
                mark_final_in_sheet(sweep_sheet,sig,'FULL_WIN')
                resolved.append(key)
            elif (price>=be if direction=='short' else price<=be):
                print(f"  [{pair}] TP1_BE")
                send_outcome_alert(sig,'TP1_BE',price)
                mark_final_in_sheet(sweep_sheet,sig,'TP1_BE')
                resolved.append(key)

    for key in resolved: live.pop(key, None)
    save_sweep_live(live)
    print(f"  [SWEEP] {len(resolved)} resolved, {len(live)} remaining")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  EDGE Outcome Tracker — {now_str()}")
    print(f"{'='*55}")
    if not all([TWELVE_DATA_KEY, SHEET_ID, GOOGLE_CREDS]):
        print("[ERROR] Missing env vars"); return
    try:
        sheet, sweep_sheet = connect_sheets()
        print("[OK] Sheets connected")
    except Exception as e:
        print(f"[ERROR] {e}"); return

    rows    = sheet.get_all_records()
    pending = [(i+2,r) for i,r in enumerate(rows)
               if str(r.get("outcome","")).strip().upper()=="PENDING"]
    print(f"\n  B&R pending: {len(pending)}")
    cleanup_orphans({r.get("pair","").strip().upper() for _,r in pending})

    for row_num, row in pending:
        pair=row.get("pair","").strip().upper()
        side=row.get("side","").strip().upper()
        try: entry=float(row["entry"]); sl=float(row["sl"]); tp=float(row["tp"])
        except Exception as e: print(f"  [Row {row_num}] {e}"); continue
        print(f"\n  [{pair}] {side} checking...")
        result = detect_br_outcome(pair,side,entry,sl,tp,row.get("signal_time","").strip())
        if result:
            outcome,close_price,close_time = result
            print(f"  [{pair}] {outcome} @ {close_price}")
            try:
                sheet.update_cell(row_num,9,outcome)
                sheet.update_cell(row_num,10,close_price)
                sheet.update_cell(row_num,11,close_time)
                clear_live_br(pair)
            except Exception as e: print(f"  [{pair}] Sheet err: {e}")
            send_br_result(row,outcome,close_price)
        else:
            print(f"  [{pair}] Still pending")

    run_sweep_tracker(sweep_sheet)
    print(f"\n{'='*55}\n")

if __name__ == "__main__":
    main()

# EDGE Signal Engine 🔔

Automated Break & Retest signal detector running 24/7 on GitHub Actions.
Sends Telegram alerts every time a valid setup fires on your trading pairs.

## Pairs Monitored
- EURUSD
- GBPUSD
- USDJPY
- AUDJPY

## Strategy
- **Timeframe:** M15 (price ticks every 15 min)
- **Setup:** Break & Retest of swing highs/lows
- **Trend filter:** HH/HL (bullish) or LH/LL (bearish)
- **Risk:Reward:** 1:2
- **Entry:** Close above/below broken level on retest

---

## Setup Guide (5 minutes)

### Step 1 — Fork or create this repo on GitHub
1. Go to [github.com](https://github.com) and sign up (free)
2. Click **New Repository** → name it `edge-signals`
3. Upload all these files to the repo

### Step 2 — Add your secrets
GitHub Secrets keep your API keys safe — they're never visible in the code.

1. In your repo go to **Settings → Secrets and variables → Actions**
2. Click **New repository secret** and add these three:

| Secret Name | Value |
|------------|-------|
| `FCS_API_KEY` | `fbNM6BharH9sgbq0HO6EgEl0h` |
| `TG_TOKEN` | Your Telegram bot token from @BotFather |
| `TG_CHAT_ID` | Your Telegram chat ID |

### Step 3 — Enable GitHub Actions
1. Go to the **Actions** tab in your repo
2. Click **Enable Actions** if prompted
3. The workflow will start automatically on its schedule

### Step 4 — Verify it's working
1. Go to **Actions** tab
2. You should see runs every 15 minutes
3. Click any run to see the logs
4. You can also trigger it manually by clicking **Run workflow**

---

## How It Works

```
Every 15 minutes:
  GitHub servers wake up
      ↓
  Fetch live prices (FCS API)
      ↓
  Append to price history (cached between runs)
      ↓
  Run Break & Retest detection
      ↓
  Signal found? → Send Telegram alert
      ↓
  Save state for next run
```

The price history cache grows with each run. After ~3 hours (12 runs)
there's enough history for reliable swing detection. After 24 hours
it's running at full strength.

---

## Telegram Alert Format

```
🟢 EDGE SIGNAL — BUY GBPUSD

📍 Entry:       1.29845
🛑 Stop Loss:   1.29620
🎯 Take Profit: 1.30295

📊 RR: 1:2  |  Risk: 22.5 pips
🕐 2024-03-19 14:30 UTC

Break & Retest setup — M15 | Log it in EDGE Journal
```

---

## Signal Log
Every signal is saved to `state/signals_log.csv` and committed to the repo.
You can download this anytime for your trading journal or investor report.

---

## Limitations
- GitHub Actions free tier allows 2,000 minutes/month
- Running every 15 min = 96 runs/day × ~1 min each = ~96 min/day = ~2,880 min/month
- **This slightly exceeds the free tier limit (~2,000 min/month)**
- **Fix:** Change the cron to run every 20 minutes instead: `0,20,40 * * * *`
  That gives ~72 runs/day × ~1 min = ~2,160 min/month — just within limits
  Or run only during your trading hours (9AM–11PM WAT = 8AM–10PM UTC):
  `0,20,40 8-22 * * 1-5` — weekdays only during your session

---

## Files
```
edge-signals/
├── .github/
│   └── workflows/
│       └── signal_engine.yml    ← GitHub Actions schedule
├── src/
│   └── signal_engine.py         ← Main signal detection logic
├── state/
│   └── signals_log.csv          ← Auto-created, logged to repo
├── requirements.txt
├── .gitignore
└── README.md
```

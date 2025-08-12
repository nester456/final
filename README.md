# WA → TG Forwarder (13 groups)

This package forwards filtered WhatsApp group messages to mapped Telegram channels, sends the WA QR to a report channel, and posts a daily report at 08:00 Europe/Kyiv.

## Files
- `index.js` — main app with dedup (24h), history guard, queue with 429 handling, daily report.
- `mapping.json` — your 13 WA group → TG bot+channel mappings.
- `package.json` — Node >= 20 required.
- `.env.example` — environment variables template.

## 0) Security note
Your mapping contains live Telegram bot tokens. Keep this repository **private**. If any token was exposed earlier, regenerate via BotFather and update `mapping.json`.

## 1) Local quick run
```bash
# Node 20+
node -v

# install
npm i

# copy .env.example -> .env and edit if needed
cp .env.example .env

# run
node index.js
```
On first run, a QR will be sent as a **photo** to `REPORT_CHAT_ID`. Scan it with the WhatsApp account that has access to all 13 groups.

## 2) Railway deployment
1. Create a new Railway service from your GitHub repo (private).
2. Add a **Persistent Volume** and mount it at `/data`.
3. Set **Variables** (ENV):
   - `REPORT_BOT_TOKEN` = your report bot token (pre-filled in `.env.example`).
   - `REPORT_CHAT_ID`   = `-1002882177145`.
   - `STORAGE_DIR`      = `/data`.
   - (optional) `TEST_TG_ON_START=true` to send "Bot online" in all 13 TG channels at deploy.
   - **Do not** set `ALLOW_HISTORY` for production (prevents spam from old history).
4. Ensure Node 20+ build (Nixpacks respects `engines` in `package.json`).
5. Deploy. In **Logs** you should see `📂 STORAGE_DIR = /data`. On first run, check your report channel for the QR.

## 3) Anti-spam / single-forward guarantees
- We **ignore history** unless `ALLOW_HISTORY=1` (default off).
- We **deduplicate** via `seen.json` with 24h TTL (stored on the persistent volume).
- We **drop messages older than process start** (`START_TS`) to avoid loops on restarts.
- Telegram sending is **queued per channel** and **retries 429** with `retry_after` delays.

## 4) Mapping updates
Edit `mapping.json` and redeploy. Structure:
```json
{
  "1203...@g.us": {
    "telegramBotToken": "BOT_TOKEN",
    "telegramChannelId": "-1001234567890"
  }
}
```

## 5) Custom filter
Function `isAllowed(text)` uses `alertRegexes`. Send your filter sample and update this function accordingly.

## 6) Troubleshooting
- No QR in report channel → ensure `REPORT_BOT_TOKEN`/`REPORT_CHAT_ID` and that the bot was added to the channel with permission to post.
- App re-sends old messages → confirm `ALLOW_HISTORY` is **not** set and the volume is mounted; check logs for `history set` lines.
- TG 429 errors → queue will auto-retry; consider lowering traffic or merging channels.

Good luck!

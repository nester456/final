// === WA -> TG forwarder with robust decryption handling, 10m history window,
//     file logs, strong anti-dup, and daily report (08:00 Europe/Kyiv) ===

const {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
  makeCacheableSignalKeyStore,
} = require('@whiskeysockets/baileys')
const pino = require('pino')
const { Boom } = require('@hapi/boom')
const QRCode = require('qrcode')
const TelegramBot = require('node-telegram-bot-api')
const cron = require('node-cron')
const fs = require('fs')
const path = require('path')
const NodeCache = require('node-cache')

// ---------- STORAGE ----------
const STORAGE_DIR = process.env.STORAGE_DIR || __dirname
fs.mkdirSync(STORAGE_DIR, { recursive: true })
console.log('📂 STORAGE_DIR =', STORAGE_DIR)

const AUTH_DIR = path.join(STORAGE_DIR, 'auth')
fs.mkdirSync(AUTH_DIR, { recursive: true })

// ---------- ENV ----------
const REPORT_BOT_TOKEN = process.env.REPORT_BOT_TOKEN || ''
const REPORT_CHAT_ID  = process.env.REPORT_CHAT_ID  || ''
const HISTORY_BACK_MIN = Number(process.env.HISTORY_BACK_MIN || 10) // history window on reconnect (min)
const SKIP_DUP_MIN = Number(process.env.SKIP_DUPLICATES_MINUTES || 10) // text dedup per JID (min)
const START_TS = Math.floor(Date.now() / 1000) - 30 // skip anything older than process start (w/ 30s slack)

const ERRORS_FILE  = path.join(STORAGE_DIR, process.env.ERRORS_FILE  || 'errors.log')
const SKIPPED_FILE = path.join(STORAGE_DIR, process.env.SKIPPED_FILE || 'skipped.log')

// NEW: опційний форс-запуск звіту на старті
const FORCE_REPORT_NOW = String(process.env.FORCE_REPORT_NOW || '').toLowerCase() === 'true'

// optional: force-forward some JIDs (bypass filters/dedup) for diagnostics
const FORCE_FORWARD_JIDS = (process.env.FORCE_FORWARD_JIDS || '').split(',').map(s => s.trim()).filter(Boolean)
const FORCE_SET = new Set(FORCE_FORWARD_JIDS)
const shouldForceForward = (jid) => FORCE_SET.has(jid)

// ---------- Human-friendly group names ----------
const NAMES_MAP = {
  "120363044356063512@g.us": "DRC Chernihiv Team",
  "120363023446341119@g.us": "DRC Dnipro Team",
  "120363029286365519@g.us": "DRC Kharkiv Team",
  "120363279744372436@g.us": "DRC Kherson",
  "120363230226839729@g.us": "Kryvyi Rih Alerts",
  "120363022703522334@g.us": "Kyiv Country Office",
  "120363415406262452@g.us": "Lviv Alerts",
  "120363062976584533@g.us": "DRC Mykolaiv",
  "120363284910289399@g.us": "Alerts in Odesa",
  "120363280813470075@g.us": "Shostka Alerts",
  "120363221232729996@g.us": "Slovyansk Alerts",
  "120363121851681827@g.us": "DRC Sumy Area Office",
  "120363166224916518@g.us": "Alerts in Zaporizka",
}
const nameOf = (jid) => NAMES_MAP[jid] || jid

// ---------- global traps ----------
process.on('uncaughtException', (err) => { console.error('❌ uncaughtException:', err) })
process.on('unhandledRejection', (err) => { console.error('❌ unhandledRejection:', err) })
process.on('SIGTERM', () => { try { saveMsgStore(); saveSeen(true) } catch {} process.exit(0) })
process.on('SIGINT',  () => { try { saveMsgStore(); saveSeen(true) } catch {} process.exit(0) })
setInterval(() => console.log('💓 alive', new Date().toISOString()), 5 * 60 * 1000)

// ---------- mapping ----------
const mappingPath = path.join(__dirname, 'mapping.json')
const groupMapping = JSON.parse(fs.readFileSync(mappingPath, 'utf-8'))
function validateMapping(mapping) {
  const errors = []
  for (const [jid, cfg] of Object.entries(mapping)) {
    if (!/@g\.us$/.test(jid)) errors.push(`❌ JID "${jid}" не схожий на WA-групу (*@g.us)`)
    if (!cfg?.telegramBotToken) errors.push(`❌ Немає telegramBotToken для ${jid}`)
    if (!cfg?.telegramChannelId || !/^-\d+$/.test(cfg.telegramChannelId)) errors.push(`❌ Невалідний telegramChannelId для ${jid}`)
  }
  return errors
}
{
  const errs = validateMapping(groupMapping)
  if (errs.length) { console.error('⚠️ mapping.json errors:\n' + errs.join('\n')); process.exit(1) }
  console.log(`✅ Завантажено ${Object.keys(groupMapping).length} мапінг(ів) груп`)
}

// ---------- file utils ----------
function appendJsonLine(file, obj) {
  try { fs.appendFileSync(file, JSON.stringify({ ts: new Date().toISOString(), ...obj }) + '\n') }
  catch (e) { console.warn('⚠️ write fail', file, e?.message || e) }
}
function readJsonLinesSince(file, fromTsMs) {
  try {
    const txt = fs.readFileSync(file, 'utf8')
    const lines = txt.split('\n').filter(Boolean)
    const out = []
    for (const line of lines) {
      try {
        const row = JSON.parse(line)
        const ts = Date.parse(row.ts)
        if (isFinite(ts) && ts >= fromTsMs) out.push(row)
      } catch {}
    }
    return out
  } catch { return [] }
}

// ---------- dedup by WA id (persist 24h) ----------
const SEEN_PATH = path.join(STORAGE_DIR, 'seen.json')
const SEEN_TTL_MS = 24 * 60 * 60 * 1000
const SEEN_MAX = 10000
let seenMap = new Map()
function loadSeen() {
  try {
    if (!fs.existsSync(SEEN_PATH)) return
    const arr = JSON.parse(fs.readFileSync(SEEN_PATH, 'utf8'))
    const now = Date.now()
    seenMap = new Map(arr.filter(([_, ts]) => now - ts < SEEN_TTL_MS))
    console.log(`🧠 seen: завантажено ${seenMap.size} id`)
  } catch (e) { console.warn('⚠️ read seen.json:', e.message) }
}
function saveSeen(force = false) {
  try {
    const now = Date.now()
    const entries = [...seenMap.entries()].filter(([_, ts]) => now - ts < SEEN_TTL_MS)
    if (entries.length > SEEN_MAX) entries.splice(0, entries.length - SEEN_MAX)
    fs.writeFileSync(SEEN_PATH, JSON.stringify(entries))
    if (force) console.log(`💾 seen: збережено ${entries.length} id`)
  } catch (e) { console.warn('⚠️ write seen.json:', e.message) }
}
function wasSeen(id) { const ts = seenMap.get(id); return ts && (Date.now() - ts < SEEN_TTL_MS) }
function markSeen(id) { seenMap.set(id, Date.now()); if (seenMap.size % 200 === 0) saveSeen(false) }
loadSeen()
setInterval(() => saveSeen(false), 60 * 1000)

// ---------- short dedup by TEXT per JID ----------
const SHORT_DEDUP_TTL = SKIP_DUP_MIN * 60 * 1000
const recentTextByJid = new Map() // jid -> [{hash, ts}]
function hashText(s) { let h = 0; for (let i=0;i<s.length;i++) { h = (h*31 + s.charCodeAt(i)) | 0 } return h>>>0 }
function shortDupSeen(jid, text) {
  const now = Date.now()
  const arr = recentTextByJid.get(jid) || []
  const h = hashText(text)
  const alive = arr.filter(e => now - e.ts < SHORT_DEDUP_TTL)
  const exists = alive.some(e => e.hash === h)
  if (!exists) alive.push({ hash: h, ts: now })
  recentTextByJid.set(jid, alive)
  return exists
}

// ---------- TG bots + queue ----------
const tgBots = new Map()
function getTgBot(token) {
  if (!tgBots.has(token)) tgBots.set(token, new TelegramBot(token, { polling: false }))
  return tgBots.get(token)
}
const channelQueues = new Map()
async function enqueueSend(bot, channelId, text, meta) {
  if (!channelQueues.has(channelId)) channelQueues.set(channelId, { q: [], busy: false })
  const q = channelQueues.get(channelId)
  q.q.push({ bot, text, meta })
  if (!q.busy) processQueue(channelId)
}
async function processQueue(channelId) {
  const q = channelQueues.get(channelId)
  if (!q || q.busy) return
  q.busy = true
  while (q.q.length) {
    const { bot, text, meta } = q.q.shift()
    let ok = false, errMsg = ''
    for (let attempt = 1; attempt <= 5; attempt++) {
      try { await bot.sendMessage(channelId, text); ok = true; break }
      catch (err) {
        const retryAfter = err?.response?.body?.parameters?.retry_after
        const wait = retryAfter ? retryAfter * 1000 : Math.min(2000 * attempt, 8000)
        errMsg = err?.message || String(err)
        await new Promise(r => setTimeout(r, wait))
      }
    }
    if (meta) {
      if (ok) logEvent({ type: 'sent_ok', ...meta })
      else { logEvent({ type: 'tg_fail', ...meta, error: errMsg }); appendJsonLine(ERRORS_FILE, { ...meta, error: errMsg }) }
    }
    await new Promise(r => setTimeout(r, 60))
  }
  q.busy = false
}

// ---------- WA text helpers ----------
function unwrap(msg) {
  let m = msg?.message || {}
  while (m?.ephemeralMessage?.message) m = m.ephemeralMessage.message
  while (m?.viewOnceMessageV2?.message) m = m.viewOnceMessageV2.message
  while (m?.viewOnceMessageV2Extension?.message) m = m.viewOnceMessageV2Extension.message
  return m || {}
}
function extractText(msg) {
  const m = unwrap(msg)
  const base =
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption ||
    m.videoMessage?.caption ||
    m.documentMessage?.caption ||
    m.templateMessage?.hydratedTemplate?.hydratedContentText ||
    m.buttonsMessage?.contentText
  if (base) return base
  const irm = m.interactiveResponseMessage
  if (irm) {
    if (irm?.body?.text) return irm.body.text
    const n1 = irm?.nativeFlowResponseMessage?.paramsJson
    if (n1) { try { const p = JSON.parse(n1); return p?.display_text || p?.id || JSON.stringify(p) } catch {} }
    const n2 = irm?.nativeFlowResponseMessage?.messageParamsJson
    if (n2) { try { const p = JSON.parse(n2); return p?.title || p?.subtitle || p?.description || JSON.stringify(p) } catch {} }
  }
  const listResp = m.listResponseMessage
  if (listResp) return listResp?.title || listResp?.singleSelectReply?.selectedRowId || null
  const btnResp = m.buttonsResponseMessage
  if (btnResp) return btnResp?.selectedDisplayText || btnResp?.selectedButtonId || null
  const tplBtn = m.templateButtonReplyMessage
  if (tplBtn) return tplBtn?.selectedDisplayText || tplBtn?.selectedId || null
  const q = m?.extendedTextMessage?.contextInfo?.quotedMessage
  if (q) { const qt = extractText({ message: q }); if (qt) return qt }
  return null
}
function normalizeForMatch(s) {
  if (!s) return ''
  return s.normalize('NFC')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/\u00A0/g, ' ')
    .replace(/[^\S\r\n]+/g, ' ')
    .trim()
    .toLowerCase()
}

// ---- FILTER ----
const alertRegexes = [
  /\b(alert:)?\s*level\s*blue\b|\bтривога:\s*р[іi]вень\s*син(ий|iй)\b/i,
  /\b(alert:)?\s*level\s*yellow\b|\bтривога:\s*р[іi]вень\s*жовт(ий|iй)\b/i,
  /\b(alert:)?\s*level\s*red\b|\bтривога:\s*р[іi]вень\s*червон(ий|iй)\b/i,
  /\b(alert:)?\s*level\s*green\b|\bвідбій:\s*р[іi]вень\s*зелен(ий|iй)\b/i,
  /\bповітряна\s+тривога\b/i,
  /\bвідбій\s+тривоги\b/i,
  /alert:\s*level\s*blue/i,
  /🔷\s*alert:\s*level\s*blue/i,
  /🔷\s*тривога:\s*р[іi]вень\s*син(ий|iй)/i,
]
function isAllowed(text) {
  const raw = text || ''
  const norm = normalizeForMatch(raw)
  return alertRegexes.some((r) => r.test(raw) || r.test(norm))
}

// ---------- local msgstore ----------
const STORE_PATH = path.join(STORAGE_DIR, 'msgstore.json')
const MAX_STORE = 3000
let recentMessages = new Map()
let recentOrder = []
function loadMsgStore() {
  try {
    if (!fs.existsSync(STORE_PATH)) return
    const arr = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'))
    recentMessages.clear(); recentOrder = []
    for (const msg of arr) {
      const id = msg?.key?.id; if (!id) continue
      recentMessages.set(id, msg); recentOrder.push(id)
    }
    console.log(`🗂 Завантажено з кеша: ${recentMessages.size} повідомлень`)
  } catch (e) { console.warn('⚠️ read msgstore:', e.message) }
}
function saveMsgStore() {
  try {
    const arr = recentOrder.map(id => recentMessages.get(id)).filter(Boolean)
    fs.writeFileSync(STORE_PATH, JSON.stringify(arr.slice(-MAX_STORE)))
  } catch (e) { console.warn('⚠️ write msgstore:', e.message) }
}
function storeMsg(msg) {
  const id = msg?.key?.id; if (!id) return
  if (!recentMessages.has(id)) {
    recentOrder.push(id)
    if (recentOrder.length > MAX_STORE) {
      const old = recentOrder.shift(); recentMessages.delete(old)
    }
  }
  recentMessages.set(id, msg)
}
loadMsgStore()
setInterval(() => { try { saveMsgStore() } catch {} }, 30 * 1000)

// ---------- metrics (jsonl) ----------
const METRICS_PATH = path.join(STORAGE_DIR, 'forward_metrics.jsonl')
function logEvent(ev) {
  const row = { ts: new Date().toISOString(), ...ev }
  try { fs.appendFileSync(METRICS_PATH, JSON.stringify(row) + '\n') }
  catch (e) { console.warn('⚠️ metrics write fail:', e.message) }
}

// ---------- reporter ----------
const reporterBot = (REPORT_BOT_TOKEN && REPORT_CHAT_ID)
  ? new TelegramBot(REPORT_BOT_TOKEN, { polling: false })
  : null

const LAST_REPORT_PATH = path.join(STORAGE_DIR, 'last_report.json')
function getLastReportDateStr() { try { return JSON.parse(fs.readFileSync(LAST_REPORT_PATH,'utf8')).date || null } catch { return null } }
function setLastReportToday() {
  try {
    const today = new Date().toLocaleDateString('uk-UA', { timeZone: 'Europe/Kyiv' })
    fs.writeFileSync(LAST_REPORT_PATH, JSON.stringify({ date: today }))
  } catch {}
}

function formatLineSuccess(jid, s) {
  const g = groupMapping[jid]; const chan = g ? g.telegramChannelId : '?'
  const name = nameOf(jid)
  const detected = s.detected || 0
  const sent = s.sent_ok || 0
  const rate = detected ? Math.round((sent / detected) * 100) : 0
  return `• ${name} → ${chan}
  ├ detected: ${detected}
  └ sent_ok: ${sent} (${rate}%)`
}
function formatLineSkipped(jid, s) {
  const name = nameOf(jid)
  return `• ${name}
  ├ no_text: ${s.skip_no_text || 0}
  ├ not_allowed: ${s.skip_not_allowed || 0}
  ├ no_mapping: ${s.skip_no_mapping || 0}
  ├ old_ts: ${s.skip_old_ts || 0}
  └ dedup: ${s.dedup_skip || 0}`
}
function formatLineErrors(jid, s) {
  const name = nameOf(jid)
  return `• ${name} — tg_fail: ${s.tg_fail || 0}`
}

async function sendDailyReport() {
  if (!reporterBot) { console.log('ℹ️ Reporter disabled'); return }

  // aggregate from metrics.jsonl
  let lines; try { lines = fs.readFileSync(METRICS_PATH,'utf8').trim().split('\n') } catch { lines = [] }
  const now = Date.now(), fromTs = now - 24*60*60*1000

  const perJid = new Map()
  const totals = {
    detected: 0, sent_ok: 0, tg_fail: 0, dedup_skip: 0,
    skip_no_text: 0, skip_not_allowed: 0, skip_no_mapping: 0, skip_old_ts: 0
  }
  for (const line of lines) {
    if (!line) continue
    let row; try { row = JSON.parse(line) } catch { continue }
    const ts = Date.parse(row.ts); if (!isFinite(ts) || ts < fromTs) continue
    const jid = row.jid || 'unknown'
    if (!perJid.has(jid)) perJid.set(jid, {})
    const b = perJid.get(jid)

    if (row.type === 'sent_ok') { b.sent_ok=(b.sent_ok||0)+1; totals.sent_ok++; b.detected=(b.detected||0)+1; totals.detected++ }
    else if (row.type === 'tg_fail') { b.tg_fail=(b.tg_fail||0)+1; totals.tg_fail++; b.detected=(b.detected||0)+1; totals.detected++ }
    else if (row.type === 'dedup_skip') { b.dedup_skip=(b.dedup_skip||0)+1; totals.dedup_skip++ }
    else if (row.type === 'skip_no_text') { b.skip_no_text=(b.skip_no_text||0)+1; totals.skip_no_text++ }
    else if (row.type === 'skip_not_allowed') { b.skip_not_allowed=(b.skip_not_allowed||0)+1; totals.skip_not_allowed++ }
    else if (row.type === 'skip_no_mapping') { b.skip_no_mapping=(b.skip_no_mapping||0)+1; totals.skip_no_mapping++ }
    else if (row.type === 'skip_old_ts' || row.type === 'skip_history_window') { b.skip_old_ts=(b.skip_old_ts||0)+1; totals.skip_old_ts++ }
  }

  // tail from file logs (last 24h)
  const recentErrors = readJsonLinesSince(ERRORS_FILE, fromTs).slice(-10)
  const recentSkips  = readJsonLinesSince(SKIPPED_FILE, fromTs).slice(-10)

  let txt = `📊 *Forwarder — добовий звіт*\nЗа останні 24 години (до ${new Date().toLocaleString('uk-UA',{timeZone:'Europe/Kyiv'})}):\n`

  // Success section
  txt += `\n*Успішні повідомлення:*\n`
  for (const [jid, s] of perJid.entries()) txt += formatLineSuccess(jid, s) + '\n'
  txt += `\n*Разом:* detected: ${totals.detected} • sent_ok: ${totals.sent_ok}\n`

  // Skipped section
  txt += `\n*Пропущені:*\n`
  for (const [jid, s] of perJid.entries()) txt += formatLineSkipped(jid, s) + '\n'
  txt += `\nΣ: no_text ${totals.skip_no_text} • not_allowed ${totals.skip_not_allowed} • no_mapping ${totals.skip_no_mapping} • old_ts ${totals.skip_old_ts} • dedup ${totals.dedup_skip}\n`

  // Errors section
  txt += `\n*Помилки:*\n`
  for (const [jid, s] of perJid.entries()) txt += formatLineErrors(jid, s) + '\n'
  if (recentErrors.length) {
    txt += `\n_Останні помилки (до 10):_\n` + recentErrors.map(e => `• ${e.ts} ${nameOf(e.jid||'?')} wa:${e.wa_id||'?'} — ${String(e.error||'').slice(0,120)}`).join('\n')
  }
  if (recentSkips.length) {
    txt += `\n_Останні пропуски (до 10):_\n` + recentSkips.map(s => `• ${s.ts} ${nameOf(s.jid||'?')} — ${s.reason}${s.snippet?` "${String(s.snippet).slice(0,80)}"`:''}`).join('\n')
  }

  // NEW: Markdown → fallback без форматування
  try {
    await reporterBot.sendMessage(REPORT_CHAT_ID, txt, { parse_mode: 'Markdown' })
    setLastReportToday()
    console.log('✅ Daily report sent (Markdown)')
  } catch (e1) {
    appendJsonLine(ERRORS_FILE, { where:'daily_report_md_fail', error: e1?.message || String(e1) })
    console.warn('⚠️ Markdown failed, retrying without parse_mode...')
    try {
      await reporterBot.sendMessage(REPORT_CHAT_ID, txt)
      setLastReportToday()
      console.log('✅ Daily report sent (plain)')
    } catch (e2) {
      console.error('❌ Report send error (plain):', e2?.message || e2)
      appendJsonLine(ERRORS_FILE, { where:'daily_report_plain_fail', error: e2?.message || String(e2) })
    }
  }
}

// NEW: опційний миттєвий запуск звіту на старті (через env)
if (FORCE_REPORT_NOW) {
  console.log('ℹ️ FORCE_REPORT_NOW is true — sending report immediately…')
  sendDailyReport().catch(e =>
    appendJsonLine(ERRORS_FILE, { where:'force_report_now', error: e?.message || String(e) })
  )
}

// Schedule 08:00 Europe/Kyiv
cron.schedule('0 8 * * *', sendDailyReport, { timezone: 'Europe/Kyiv' })
console.log('🕗 Daily report scheduled at 08:00 Europe/Kyiv')

// catch-up daily report if missed
async function reportCatchUpIfMissing() {
  if (!reporterBot) return
  try {
    const today = new Date().toLocaleDateString('uk-UA', { timeZone: 'Europe/Kyiv' })
    const last = getLastReportDateStr()
    if (last !== today) {
      console.log('ℹ️ No report today yet — sending catch-up now...')
      await sendDailyReport()
    }
  } catch (e) { appendJsonLine(ERRORS_FILE, { where:'report_catchup', error: e?.message || String(e) }) }
}

// ---------- core ----------
const logger = pino({ level: process.env.BAILEYS_LOG_LEVEL || 'info' })
let starting = false

async function startBot() {
  if (starting) return; starting = true
  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)
    const { version } = await fetchLatestBaileysVersion()
    const msgRetryCounterCache = new NodeCache() // <— важливо для ретраїв дешифрування

    const sock = makeWASocket({
      version,
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      keepAliveIntervalMs: 20_000,
      markOnlineOnConnect: false,
      syncFullHistory: true,        // ми відрізаємо історію своїм вікном часу
      msgRetryCounterCache,         // <— ключ до стабільного дешифрування
      getMessage: async (key) => {
        const id = key?.id
        return (id && recentMessages.get(id)) || undefined
      }
    })

    sock.ev.on('creds.update', async () => { try { await saveCreds() } catch {} })

    sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        try {
          const buf = await QRCode.toBuffer(qr, { width: 400 })
          if (REPORT_BOT_TOKEN && REPORT_CHAT_ID) {
            const bot = reporterBot
            await bot.sendPhoto(REPORT_CHAT_ID, { source: buf, filename: 'qr.png' }, { caption: '🔐 QR для підключення WhatsApp' })
            console.log('✅ QR надіслано у звітний канал')
          } else {
            await fs.promises.writeFile(path.join(STORAGE_DIR, 'qr.png'), buf)
            console.log('✅ QR-код збережено у qr.png — відкрий і скануй 📱')
          }
        } catch (err) { appendJsonLine(ERRORS_FILE, { where:'send_qr', error: err?.message || String(err) }) }
      }

      if (connection === 'close') {
        logEvent({ type: 'wa_down' })
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut
        starting = false
        console.log('⚠️ З’єднання закрито. statusCode:', statusCode, 'reconnect:', shouldReconnect)
        if (shouldReconnect) startBot()
        else process.exit(1)
      }
      if (connection === 'open') {
        logEvent({ type: 'wa_up' })
        console.log('✅ WhatsApp підключено')
      }
    })

    // keep-alive
    setInterval(async () => { try { await sock.sendPresenceUpdate('available') } catch {} }, 5 * 1000 * 60)

    // main handler (history window + decrypt waits + anti-dup)
    async function handleOneMessage(msg, sourceTag = '') {
      if (msg?.message) storeMsg(msg)

      const msgId = msg.key?.id
      const jid = msg.key?.remoteJid
      const ts = Number(msg.messageTimestamp) || 0
      if (!msgId || !jid || !ts) return

      const nowSec = Math.floor(Date.now() / 1000)
      const isHistory = sourceTag.startsWith('history') || sourceTag.includes('append')
      if (isHistory) {
        const backSec = HISTORY_BACK_MIN * 60
        if (ts < (nowSec - backSec)) {
          logEvent({ type: 'skip_history_window', jid, wa_id: msgId, source: sourceTag })
          appendJsonLine(SKIPPED_FILE, { reason: 'history_window', jid, wa_id: msgId, source: sourceTag })
          return
        }
      }

      if (ts < START_TS) {
        logEvent({ type: 'skip_old_ts', jid, wa_id: msgId, source: sourceTag })
        appendJsonLine(SKIPPED_FILE, { reason: 'old_ts', jid, wa_id: msgId, source: sourceTag })
        return
      }

      const mapping = groupMapping[jid]
      if (!mapping) {
        logEvent({ type: 'skip_no_mapping', jid, wa_id: msgId, source: sourceTag })
        appendJsonLine(SKIPPED_FILE, { reason: 'no_mapping', jid, wa_id: msgId, source: sourceTag })
        return
      }

      const mUnwrapped = unwrap(msg)
      const typeKeys = Object.keys(mUnwrapped || {})
      let textRaw = extractText(msg)

      // wait for late decryption (3 quick retries)
      if (!textRaw) {
        for (const delay of [400, 800, 1200]) {
          await new Promise(r => setTimeout(r, delay))
          const cachedAgain = recentMessages.get(msgId) || msg
          textRaw = extractText(cachedAgain)
          if (textRaw) break
        }
      }

      if (!textRaw) {
        console.warn(`⏭️ SKIP: no-text ${nameOf(jid)} types=${JSON.stringify(typeKeys)} source=${sourceTag}`)
        logEvent({ type: 'skip_no_text', jid, wa_id: msgId, source: sourceTag })
        appendJsonLine(SKIPPED_FILE, { reason: 'no_text', jid, wa_id: msgId, source: sourceTag, types: typeKeys })
        if (shouldForceForward(jid)) {
          const bot = getTgBot(mapping.telegramBotToken)
          const placeholder = `[no-text message from ${nameOf(jid)}]`
          enqueueSend(bot, mapping.telegramChannelId, placeholder, { jid, wa_id: msgId, source: sourceTag, snippet: placeholder })
        }
        return
      }

      // filter
      if (!shouldForceForward(jid) && !isAllowed(textRaw)) {
        logEvent({ type: 'skip_not_allowed', jid, wa_id: msgId, source: sourceTag, snippet: (textRaw||'').slice(0,140) })
        appendJsonLine(SKIPPED_FILE, { reason: 'not_allowed', jid, wa_id: msgId, source: sourceTag, snippet: (textRaw||'').slice(0,140) })
        return
      }

      const normalizedText = textRaw.normalize('NFC')

      // short dedup by text per JID
      if (!shouldForceForward(jid) && shortDupSeen(jid, normalizedText)) {
        logEvent({ type: 'dedup_skip', jid, wa_id: msgId, source: sourceTag })
        return
      }

      // NOW do dedup by WA message id (only when we actually have text & ready to send)
      if (!shouldForceForward(jid)) {
        if (wasSeen(msgId)) {
          logEvent({ type: 'dedup_skip', jid, wa_id: msgId, source: sourceTag })
          return
        }
        markSeen(msgId)
      }

      const bot = getTgBot(mapping.telegramBotToken)
      enqueueSend(bot, mapping.telegramChannelId, normalizedText, {
        jid, wa_id: msgId, source: sourceTag, snippet: normalizedText.slice(0, 140)
      })
      console.log(`📤 queued (${sourceTag}) ${nameOf(jid)}:`, normalizedText.slice(0, 120))
    }

    // live / append
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify' && type !== 'append') return
      for (const msg of messages) {
        try { await handleOneMessage(msg, `upsert:${type}`) }
        catch (e) { appendJsonLine(ERRORS_FILE, { where:'handle_upsert', error: e?.message || String(e) }) }
      }
    })

    // late decrypt via messages.update
    sock.ev.on('messages.update', (updates) => {
      for (const u of updates) {
        const id = u?.key?.id; if (!id) continue
        if (u.update?.message) {
          const prev = recentMessages.get(id) || { key: u.key, messageTimestamp: u.messageTimestamp }
          const merged = { ...prev, ...u, message: u.update.message }
          storeMsg(merged)
          // do NOT check wasSeen() here — let handleOneMessage decide after it has text
          handleOneMessage(merged, 'update:decrypted').catch(e =>
            appendJsonLine(ERRORS_FILE, { where:'handle_update', error: e?.message || String(e) })
          )
        }
      }
    })

    // history.set with our time window
    sock.ev.on('messaging-history.set', async ({ messages }) => {
      const list = Array.isArray(messages) ? messages.slice() : []
      list.sort((a,b)=>Number(a?.messageTimestamp||0)-Number(b?.messageTimestamp||0))
      for (const msg of list) {
        try { await handleOneMessage(msg, 'history') }
        catch (e) { appendJsonLine(ERRORS_FILE, { where:'handle_history', error: e?.message || String(e) }) }
      }
    })

    // run catch-up report
    reportCatchUpIfMissing().catch(()=>{})

  } catch (e) {
    starting = false
    appendJsonLine(ERRORS_FILE, { where:'startBot', error: e?.message || String(e) })
    setTimeout(() => startBot(), 3000)
  }
}

startBot()

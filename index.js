// === WA -> TG forwarder (multi-groups) with history window, late-decrypt handling,
//     daily report (with group names), catch-up, and strong anti-duplication ===

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

// ---------- CONFIG: де зберігати персистентні файли ----------
const STORAGE_DIR = process.env.STORAGE_DIR || __dirname
fs.mkdirSync(STORAGE_DIR, { recursive: true })
console.log('📂 STORAGE_DIR =', STORAGE_DIR)

const AUTH_DIR = path.join(STORAGE_DIR, 'auth')
fs.mkdirSync(AUTH_DIR, { recursive: true })

// ---------- ENV / дефолти ----------
const REPORT_BOT_TOKEN = process.env.REPORT_BOT_TOKEN || ''
const REPORT_CHAT_ID  = process.env.REPORT_CHAT_ID  || ''
const HISTORY_BACK_MIN = Number(process.env.HISTORY_BACK_MIN || 10) // <— 10 хв за замовчуванням
const START_TS = Math.floor(Date.now() / 1000) - 30 // не беремо, що старше старту (30с запас)

// --- для діагностики: форс-проксі для конкретних груп (обхід фільтра/дедуп)
const FORCE_FORWARD_JIDS = (process.env.FORCE_FORWARD_JIDS || '')
  .split(',').map(s => s.trim()).filter(Boolean)
const FORCE_SET = new Set(FORCE_FORWARD_JIDS)
const shouldForceForward = (jid) => FORCE_SET.has(jid)

// ---------- читабельні назви груп для звітів ----------
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

// ---------- глобальні перехоплення ----------
process.on('uncaughtException', (err) => { console.error('❌ uncaughtException:', err) })
process.on('unhandledRejection', (err) => { console.error('❌ unhandledRejection:', err) })
process.on('SIGTERM', () => { console.log('🛑 SIGTERM'); try { saveMsgStore(); saveSeen(true); saveRetryMap(msgRetryCounterMap) } catch {} process.exit(0) })
process.on('SIGINT',  () => { console.log('🛑 SIGINT');  try { saveMsgStore(); saveSeen(true); saveRetryMap(msgRetryCounterMap) } catch {} process.exit(0) })
setInterval(() => console.log('💓 alive', new Date().toISOString()), 5 * 60 * 1000)

// ---------- mapping.json ----------
const mappingPath = path.join(__dirname, 'mapping.json')
const groupMapping = JSON.parse(fs.readFileSync(mappingPath, 'utf-8'))

function validateMapping(mapping) {
  const errors = []
  for (const [jid, cfg] of Object.entries(mapping)) {
    if (!/@g\.us$/.test(jid)) errors.push(`❌ JID "${jid}" не схожий на WA-групу (*@g.us)`)
    if (!cfg?.telegramBotToken) errors.push(`❌ Немає telegramBotToken для ${jid}`)
    if (!cfg?.telegramChannelId || !/^-\d+$/.test(cfg.telegramChannelId)) {
      errors.push(`❌ Невалідний telegramChannelId для ${jid} (має бути -100...)`)
    }
  }
  return errors
}
{
  const errs = validateMapping(groupMapping)
  if (errs.length) { console.error('⚠️ Помилки в mapping.json:\n' + errs.join('\n')); process.exit(1) }
  console.log(`✅ Завантажено ${Object.keys(groupMapping).length} мапінг(ів) груп`)
}

// ---------- уникнення дублювання (RAM + диск, TTL 24h) ----------
const SEEN_PATH = path.join(STORAGE_DIR, 'seen.json')
const SEEN_TTL_MS = 24 * 60 * 60 * 1000
const SEEN_MAX = 10000
let seenMap = new Map()

function loadSeen() {
  try {
    if (!fs.existsSync(SEEN_PATH)) return
    const arr = JSON.parse(fs.readFileSync(SEEN_PATH, 'utf8')) // [[id, ts], ...]
    const now = Date.now()
    seenMap = new Map(arr.filter(([_, ts]) => now - ts < SEEN_TTL_MS))
    console.log(`🧠 seen: завантажено ${seenMap.size} id`)
  } catch (e) { console.warn('⚠️ Неможливо прочитати seen.json:', e.message) }
}
function saveSeen(force = false) {
  try {
    const now = Date.now()
    const entries = [...seenMap.entries()].filter(([_, ts]) => now - ts < SEEN_TTL_MS)
    if (entries.length > SEEN_MAX) entries.splice(0, entries.length - SEEN_MAX)
    fs.writeFileSync(SEEN_PATH, JSON.stringify(entries))
    if (force) console.log(`💾 seen: збережено ${entries.length} id`)
  } catch (e) { console.warn('⚠️ Неможливо зберегти seen.json:', e.message) }
}
function wasSeen(id) { const ts = seenMap.get(id); return ts && (Date.now() - ts < SEEN_TTL_MS) }
function markSeen(id) {
  seenMap.set(id, Date.now())
  if (seenMap.size % 200 === 0) saveSeen(false)
}
loadSeen()
setInterval(() => saveSeen(false), 60 * 1000)

// ---------- Telegram боти (reuse) + черга відправок по каналах ----------
const tgBots = new Map()
function getTgBot(token) {
  if (!tgBots.has(token)) tgBots.set(token, new TelegramBot(token, { polling: false }))
  return tgBots.get(token)
}

const channelQueues = new Map() // channelId -> {q:[], busy:boolean}
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
      try {
        await bot.sendMessage(channelId, text)
        ok = true
        break
      } catch (err) {
        const retryAfter = err?.response?.body?.parameters?.retry_after
        const wait = retryAfter ? (retryAfter * 1000) : Math.min(2000 * attempt, 8000)
        errMsg = err?.message || String(err)
        console.warn(`⚠️ TG send retry ${attempt}/5 (${wait}ms):`, errMsg)
        await new Promise(r => setTimeout(r, wait))
      }
    }
    if (meta) {
      if (ok) logEvent({ type: 'sent_ok', jid: meta.jid, wa_id: meta.wa_id, tg_channel: channelId, source: meta.source, snippet: meta.snippet })
      else logEvent({ type: 'tg_fail', jid: meta.jid, wa_id: meta.wa_id, tg_channel: channelId, source: meta.source, error: errMsg, snippet: meta.snippet })
    }
    await new Promise(r => setTimeout(r, 60))
  }
  q.busy = false
}

// ---------- утиліти тексту з WA ----------
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
  if (q) {
    const qt = extractText({ message: q })
    if (qt) return qt
  }
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

// ---- ФІЛЬТР (можна розширити під свій шаблон) ----
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

// ---------- локальний стор повідомлень для getMessage ----------
const STORE_PATH = path.join(STORAGE_DIR, 'msgstore.json')
const MAX_STORE = 3000
let recentMessages = new Map() // id -> full msg
let recentOrder = []           // LRU

function loadMsgStore() {
  try {
    if (!fs.existsSync(STORE_PATH)) return
    const arr = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'))
    recentMessages.clear()
    recentOrder = []
    for (const msg of arr) {
      const id = msg?.key?.id
      if (!id) continue
      recentMessages.set(id, msg)
      recentOrder.push(id)
    }
    console.log(`🗂 Завантажено з кеша: ${recentMessages.size} повідомлень`)
  } catch (e) {
    console.warn('⚠️ Неможливо прочитати msgstore:', e.message)
  }
}
function saveMsgStore() {
  try {
    const arr = recentOrder.map(id => recentMessages.get(id)).filter(Boolean)
    fs.writeFileSync(STORE_PATH, JSON.stringify(arr.slice(-MAX_STORE)))
  } catch (e) {
    console.warn('⚠️ Неможливо зберегти msgstore:', e.message)
  }
}
function storeMsg(msg) {
  const id = msg?.key?.id
  if (!id) return
  if (!recentMessages.has(id)) {
    recentOrder.push(id)
    if (recentOrder.length > MAX_STORE) {
      const old = recentOrder.shift()
      recentMessages.delete(old)
    }
  }
  recentMessages.set(id, msg)
}
loadMsgStore()
setInterval(() => { try { saveMsgStore() } catch {} }, 30 * 1000)

// ---------- персистентний лічильник ретраїв ----------
const RETRY_PATH = path.join(AUTH_DIR, 'retry.json')
function loadRetryMap() {
  try { return new Map(JSON.parse(fs.readFileSync(RETRY_PATH, 'utf8'))) } catch { return new Map() }
}
function saveRetryMap(map) {
  try {
    fs.mkdirSync(AUTH_DIR, { recursive: true })
    fs.writeFileSync(RETRY_PATH, JSON.stringify([...map]))
  } catch {}
}
const msgRetryCounterMap = loadRetryMap()

// ---------- подієвий лог для звітів ----------
const METRICS_PATH = path.join(STORAGE_DIR, 'forward_metrics.jsonl')
function logEvent(ev) {
  const row = { ts: new Date().toISOString(), ...ev }
  try { fs.appendFileSync(METRICS_PATH, JSON.stringify(row) + '\n') }
  catch (e) { console.warn('⚠️ metrics write fail:', e.message) }
}

// ---------- reporter (щоденний звіт 08:00 Europe/Kyiv) ----------
const reporterBot = (REPORT_BOT_TOKEN && REPORT_CHAT_ID)
  ? new TelegramBot(REPORT_BOT_TOKEN, { polling: false })
  : null

const LAST_REPORT_PATH = path.join(STORAGE_DIR, 'last_report.json')
function getLastReportDateStr() {
  try { return JSON.parse(fs.readFileSync(LAST_REPORT_PATH,'utf8')).date || null } catch { return null }
}
function setLastReportToday() {
  try {
    const today = new Date().toLocaleDateString('uk-UA', { timeZone: 'Europe/Kyiv' })
    fs.writeFileSync(LAST_REPORT_PATH, JSON.stringify({ date: today }))
  } catch {}
}

function formatReportLine(jid, s) {
  const g = groupMapping[jid]
  const chan = g ? g.telegramChannelId : '?'
  const name = nameOf(jid)
  const detected = s.detected || 0
  const sent = s.sent_ok || 0
  const rate = detected ? Math.round((sent / detected) * 100) : 0
  return `• ${name} → ${chan}
    ├─ detected: ${detected} (sent_ok: ${sent}, ${rate}%)
    ├─ tg_fail:  ${s.tg_fail || 0}
    ├─ dedup:    ${s.dedup_skip || 0}
    ├─ skip_no_text:     ${s.skip_no_text || 0}
    ├─ skip_not_allowed: ${s.skip_not_allowed || 0}
    ├─ skip_no_mapping:  ${s.skip_no_mapping || 0}
    └─ skip_old_ts:      ${s.skip_old_ts || 0}`
}

async function sendDailyReport() {
  if (!reporterBot) { console.log('ℹ️ Reporter disabled'); return }
  let lines
  try { lines = fs.readFileSync(METRICS_PATH, 'utf8').trim().split('\n') } catch { lines = [] }

  const now = Date.now()
  const fromTs = now - 24 * 60 * 60 * 1000

  const perJid = new Map()
  let total = {
    detected: 0, sent_ok: 0, tg_fail: 0, dedup_skip: 0,
    skip_no_text: 0, skip_not_allowed: 0, skip_no_mapping: 0, skip_old_ts: 0
  }
  const recentFails = []
  const recentSkips = []
  const downtimeWindows = []
  let lastDown = null

  for (const line of lines) {
    if (!line) continue
    let row
    try { row = JSON.parse(line) } catch { continue }
    const ts = Date.parse(row.ts)
    if (!isFinite(ts) || ts < fromTs) continue

    const jid = row.jid || 'unknown'
    if (!perJid.has(jid)) perJid.set(jid, {})
    const bucket = perJid.get(jid)

    if (row.type === 'sent_ok') {
      bucket.sent_ok = (bucket.sent_ok || 0) + 1
      total.sent_ok++
      bucket.detected = (bucket.detected || 0) + 1
      total.detected++
    } else if (row.type === 'tg_fail') {
      bucket.tg_fail = (bucket.tg_fail || 0) + 1
      total.tg_fail++
      bucket.detected = (bucket.detected || 0) + 1
      total.detected++
      if (recentFails.length < 10) recentFails.push(`• ${row.ts} ${nameOf(jid)} wa:${row.wa_id} — ${String(row.error || '').slice(0,120)}`)
    } else if (row.type === 'dedup_skip') {
      bucket.dedup_skip = (bucket.dedup_skip || 0) + 1
      total.dedup_skip++
    } else if (row.type === 'skip_no_text') {
      bucket.skip_no_text = (bucket.skip_no_text || 0) + 1
      total.skip_no_text++
      if (recentSkips.length < 10) recentSkips.push(`• ${row.ts} ${nameOf(jid)} wa:${row.wa_id} — no-text`)
    } else if (row.type === 'skip_not_allowed') {
      bucket.skip_not_allowed = (bucket.skip_not_allowed || 0) + 1
      total.skip_not_allowed++
      if (recentSkips.length < 10) recentSkips.push(`• ${row.ts} ${nameOf(jid)} — filtered: "${String(row.snippet||'').slice(0,80)}"`)
    } else if (row.type === 'skip_no_mapping') {
      bucket.skip_no_mapping = (bucket.skip_no_mapping || 0) + 1
      total.skip_no_mapping++
      if (recentSkips.length < 10) recentSkips.push(`• ${row.ts} ${nameOf(jid)} wa:${row.wa_id} — no mapping`)
    } else if (row.type === 'skip_old_ts') {
      bucket.skip_old_ts = (bucket.skip_old_ts || 0) + 1
      total.skip_old_ts++
      if (recentSkips.length < 10) recentSkips.push(`• ${row.ts} ${nameOf(jid)} wa:${row.wa_id} — old ts`)
    }

    if (row.type === 'wa_down') lastDown = ts
    else if (row.type === 'wa_up' && lastDown) { downtimeWindows.push({ start: lastDown, end: ts }); lastDown = null }
  }
  if (lastDown) downtimeWindows.push({ start: lastDown, end: now })

  let text = `📊 *Forwarder — добовий звіт*\nЗа останні 24 години (до ${new Date().toLocaleString('uk-UA', { timeZone: 'Europe/Kyiv' })}):\n\n`
  for (const [jid, stats] of perJid.entries()) text += formatReportLine(jid, stats) + '\n'
  text += `\n*Разом:*\n• detected: ${total.detected}\n• sent_ok: ${total.sent_ok}\n• tg_fail: ${total.tg_fail}\n• dedup: ${total.dedup_skip}\n`
  text += `• skip_no_text: ${total.skip_no_text}\n• skip_not_allowed: ${total.skip_not_allowed}\n• skip_no_mapping: ${total.skip_no_mapping}\n• skip_old_ts: ${total.skip_old_ts}\n`

  if (downtimeWindows.length) {
    text += `\n🕒 *Вікна простою (ост. 24г):*\n`
    for (const win of downtimeWindows) {
      const startStr = new Date(win.start).toLocaleTimeString('uk-UA', { timeZone: 'Europe/Kyiv' })
      const endStr   = new Date(win.end).toLocaleTimeString('uk-UA', { timeZone: 'Europe/Kyiv' })
      const mins = Math.round((win.end - win.start) / 60000)
      text += `• ${startStr} — ${endStr} (${mins} хв)\n`
    }
  }
  if (recentFails.length) text += `\n*Останні помилки TG (до 10):*\n` + recentFails.join('\n')
  if (recentSkips.length) text += `\n*Останні пропуски (до 10):*\n` + recentSkips.join('\n')

  try {
    await reporterBot.sendMessage(REPORT_CHAT_ID, text, { parse_mode: 'Markdown' })
    setLastReportToday()
    console.log('✅ Daily report sent')
  } catch (e) {
    console.error('❌ Report send error:', e?.message || e)
  }
}

// крон на 08:00 Europe/Kyiv
cron.schedule('0 8 * * *', sendDailyReport, { timezone: 'Europe/Kyiv' })

// catch-up звіту при старті (якщо процес пропустив 08:00)
async function reportCatchUpIfMissing() {
  if (!reporterBot) return
  try {
    const today = new Date().toLocaleDateString('uk-UA', { timeZone: 'Europe/Kyiv' })
    const last = getLastReportDateStr()
    if (last !== today) {
      console.log('ℹ️ No report today yet — sending catch-up now...')
      await sendDailyReport()
    }
  } catch (e) { console.warn('⚠️ Catch-up report failed:', e?.message || e) }
}

// ---------- старт WA сокета ----------
let starting = false
async function startBot() {
  if (starting) return
  starting = true
  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)
    const { version } = await fetchLatestBaileysVersion()
    const logger = pino({ level: process.env.BAILEYS_LOG_LEVEL || 'info' })

    const sock = makeWASocket({
      version,
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      keepAliveIntervalMs: 20_000,
      markOnlineOnConnect: false,
      // Дозволяємо історичні нотифікації, але ріжемо їх нашим time-window на 10 хв
      syncFullHistory: true,
      msgRetryCounterMap,
      getMessage: async (key) => {
        const id = key?.id
        const cached = id ? recentMessages.get(id) : undefined
        return cached || undefined
      }
    })

    sock.ev.on('creds.update', async () => {
      try { await saveCreds() } catch {}
      try { saveRetryMap(msgRetryCounterMap) } catch {}
    })

    sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        try {
          const buf = await QRCode.toBuffer(qr, { width: 400 })
          if (REPORT_CHAT_ID && REPORT_BOT_TOKEN) {
            const bot = reporterBot
            await bot.sendPhoto(REPORT_CHAT_ID, { source: buf, filename: 'qr.png' }, { caption: '🔐 QR для підключення WhatsApp' })
            console.log('✅ QR надіслано у звітний канал')
          } else {
            const qrImagePath = path.join(STORAGE_DIR, 'qr.png')
            await fs.promises.writeFile(qrImagePath, buf)
            console.log('✅ QR-код збережено у qr.png — відкрий і скануй 📱')
          }
        } catch (err) { console.error('❌ Помилка генерації/відправки QR:', err?.message || err) }
      }

      if (connection === 'close') {
        logEvent({ type: 'wa_down' })
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut
        console.log('⚠️ З’єднання закрито. statusCode:', statusCode, 'reconnect:', shouldReconnect)
        starting = false
        if (shouldReconnect) startBot()
        else process.exit(1)
      }

      if (connection === 'open') {
        logEvent({ type: 'wa_up' })
        console.log('✅ WhatsApp підключено')
      }
    })

    // keep-alive
    setInterval(async () => { try { await sock.sendPresenceUpdate('available') } catch {} }, 5 * 60 * 1000)

    // 1) live/append повідомлення (із history window)
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
          logEvent({ type: 'skip_history_window', wa_id: msgId, jid, source: sourceTag })
          return
        }
      }

      // — відсікаємо все, що до старту процесу
      if (ts < START_TS) {
        logEvent({ type: 'skip_old_ts', wa_id: msgId, jid, source: sourceTag })
        return
      }

      const mapping = groupMapping[jid]
      if (!mapping) {
        logEvent({ type: 'skip_no_mapping', wa_id: msgId, jid, source: sourceTag })
        return
      }

      const force = shouldForceForward(jid)
      if (!force) {
        if (wasSeen(msgId)) { logEvent({ type: 'dedup_skip', wa_id: msgId, jid, source: sourceTag }); return }
        markSeen(msgId)
      }

      const mUnwrapped = unwrap(msg)
      const typeKeys = Object.keys(mUnwrapped || {})
      let textRaw = extractText(msg)

      // якщо нема тексту — коротко чекаємо, можливо, ще дешифрується
      if (!textRaw) {
        await new Promise(r => setTimeout(r, 600))
        const cachedAgain = recentMessages.get(msgId) || msg
        textRaw = extractText(cachedAgain)
      }

      if (!textRaw) {
        console.warn(`⏭️ SKIP: no-text ${nameOf(jid)} types=${JSON.stringify(typeKeys)} source=${sourceTag}`)
        logEvent({ type: 'skip_no_text', wa_id: msgId, jid, source: sourceTag })
        if (force) {
          const bot = getTgBot(mapping.telegramBotToken)
          const placeholder = `[no-text message from ${nameOf(jid)}]`
          enqueueSend(bot, mapping.telegramChannelId, placeholder, {
            jid, wa_id: msgId, source: sourceTag, snippet: placeholder
          })
          console.log(`📤 [FORCE no-text] (${sourceTag}) ${nameOf(jid)}`)
        }
        return
      }

      if (!force && !isAllowed(textRaw)) {
        logEvent({ type: 'skip_not_allowed', wa_id: msgId, jid, source: sourceTag, snippet: (textRaw||'').slice(0,140) })
        return
      }

      const normalizedText = textRaw.normalize('NFC')
      const bot = getTgBot(mapping.telegramBotToken)
      enqueueSend(bot, mapping.telegramChannelId, normalizedText, {
        jid, wa_id: msgId, source: sourceTag, snippet: normalizedText.slice(0, 140)
      })
      console.log(`${force ? '📤 [FORCE queued]' : '📤 queued'} (${sourceTag}) ${nameOf(jid)}:`, normalizedText.slice(0, 120))
    }

    // головний live/upsert
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify' && type !== 'append') return
      for (const msg of messages) {
        try { await handleOneMessage(msg, `upsert:${type}`) }
        catch (e) { console.error('❌ handleOneMessage error', e?.stack || e, 'key:', msg?.key) }
      }
    })

    // 2) пізнє дешифрування: добираємо текст із messages.update
    sock.ev.on('messages.update', (updates) => {
      for (const u of updates) {
        const id = u?.key?.id
        if (!id) continue
        if (u.update?.message) {
          const prev = recentMessages.get(id) || { key: u.key, messageTimestamp: u.messageTimestamp }
          const merged = { ...prev, ...u, message: u.update.message }
          storeMsg(merged)
          if (!wasSeen(id)) {
            // не позначаємо тут seen — хай handleOneMessage зробить це централізовано
            handleOneMessage(merged, 'update:decrypted').catch(e =>
              console.error('❌ handleOneMessage (update) error', e?.stack || e)
            )
          }
        }
      }
    })

    // 3) історичні "set" від WA — обробляємо через наше time-window
    sock.ev.on('messaging-history.set', async ({ messages }) => {
      const list = Array.isArray(messages) ? messages.slice() : []
      list.sort((a, b) => Number(a?.messageTimestamp || 0) - Number(b?.messageTimestamp || 0))
      console.log(`🕘 history set: ${list.length} msgs`)
      for (const msg of list) {
        try { await handleOneMessage(msg, 'history') }
        catch (e) { console.error('❌ handleOneMessage error (history)', e?.stack || e, 'key:', msg?.key) }
      }
    })

    // запустимо catch-up звіту
    reportCatchUpIfMissing().catch(()=>{})

  } catch (e) {
    starting = false
    console.error('❌ startBot error:', e)
    setTimeout(() => startBot(), 3000)
  }
}

startBot()

// (Опціонально) точковий ресет сесії проблемного учасника
async function resetSenderSession(jid) {
  try {
    const { state } = await useMultiFileAuthState(AUTH_DIR)
    await state.keys.set({ 'session': { [jid]: null } })
    console.log('Session for', jid, 'cleared.')
  } catch (e) { console.error('Failed to clear session for', jid, e?.message || e) }
}

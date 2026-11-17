require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 4000);
const JWT_SECRET = process.env.JWT_SECRET || 'grambook_dev_secret_change_me';
const JWT_REFRESH_SECRET = process.env.JWT_REFRESH_SECRET || 'grambook_refresh_secret_change_me';
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || '';
const NTFY_BASE_URL = process.env.NTFY_BASE_URL || '';
const DB_FILE = path.resolve(__dirname, 'grambook.db');
const UPLOAD_DIR = path.resolve(__dirname, 'uploads');

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ---------------- SQLite डेटाबेस (Termux-सुरक्षित: node:sqlite या sql.js) ----------------
let dbDriver = null;

async function initDatabase() {
  try {
    const { DatabaseSync } = require('node:sqlite');
    const nativeDb = new DatabaseSync(DB_FILE);
    nativeDb.exec('PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;');
    dbDriver = {
      exec: (sql) => nativeDb.exec(sql),
      run: (sql, params = []) => nativeDb.prepare(sql).run(...params),
      get: (sql, params = []) => nativeDb.prepare(sql).get(...params),
      all: (sql, params = []) => nativeDb.prepare(sql).all(...params),
    };
    console.log('[DB] बिल्ट-इन node:sqlite चालू हो गया।');
  } catch {
    const initSqlJs = require('sql.js');
    const SQL = await initSqlJs();
    const fileBuf = fs.existsSync(DB_FILE) ? fs.readFileSync(DB_FILE) : null;
    const sqlDb = fileBuf ? new SQL.Database(fileBuf) : new SQL.Database();
    sqlDb.exec('PRAGMA foreign_keys = ON;');
    let flushTimer = null;
    const flushDisk = () => {
      clearTimeout(flushTimer);
      flushTimer = setTimeout(() => {
        try {
          fs.writeFileSync(DB_FILE, Buffer.from(sqlDb.export()));
        } catch (e) {
          console.error('[DB Save Error]', e.message);
        }
      }, 150);
    };
    dbDriver = {
      exec: (sql) => { sqlDb.exec(sql); flushDisk(); },
      run: (sql, params = []) => { sqlDb.run(sql, params); flushDisk(); },
      get: (sql, params = []) => {
        const stmt = sqlDb.prepare(sql);
        stmt.bind(params);
        const row = stmt.step() ? stmt.getAsObject() : undefined;
        stmt.free();
        return row;
      },
      all: (sql, params = []) => {
        const stmt = sqlDb.prepare(sql);
        stmt.bind(params);
        const rows = [];
        while (stmt.step()) rows.push(stmt.getAsObject());
        stmt.free();
        return rows;
      },
    };
    console.log('[DB] शुद्ध JS sql.js SQLite चालू हो गया।');
  }

  dbDriver.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      public_uid TEXT UNIQUE NOT NULL,
      phone TEXT UNIQUE NOT NULL,
      username TEXT UNIQUE NOT NULL,
      full_name TEXT NOT NULL,
      email TEXT DEFAULT '',
      password_hash TEXT NOT NULL,
      dob TEXT DEFAULT '',
      bio TEXT DEFAULT '',
      avatar_url TEXT DEFAULT '',
      cover_url TEXT DEFAULT '',
      is_private INTEGER DEFAULT 0,
      is_verified INTEGER DEFAULT 0,
      is_creator INTEGER DEFAULT 0,
      role TEXT DEFAULT 'user',
      status TEXT DEFAULT 'active',
      who_can_comment TEXT DEFAULT 'everyone',
      who_can_message TEXT DEFAULT 'everyone',
      who_can_tag TEXT DEFAULT 'everyone',
      default_post_visibility TEXT DEFAULT 'public',
      hide_profile_visits INTEGER DEFAULT 0,
      theme_mode TEXT DEFAULT 'light',
      language TEXT DEFAULT 'hi',
      data_saver INTEGER DEFAULT 0,
      autoplay_videos INTEGER DEFAULT 1,
      notify_likes INTEGER DEFAULT 1,
      notify_comments INTEGER DEFAULT 1,
      notify_messages INTEGER DEFAULT 1,
      is_online INTEGER DEFAULT 0,
      last_seen INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS user_sessions (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      refresh_token_hash TEXT NOT NULL,
      device_name TEXT DEFAULT 'Android',
      ip_address TEXT DEFAULT '',
      expires_at INTEGER NOT NULL,
      revoked INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS password_resets (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      code_hash TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      used INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS follows (
      follower_id TEXT NOT NULL,
      following_id TEXT NOT NULL,
      status TEXT DEFAULT 'accepted',
      created_at INTEGER NOT NULL,
      PRIMARY KEY (follower_id, following_id)
    );

    CREATE TABLE IF NOT EXISTS friendships (
      requester_id TEXT NOT NULL,
      addressee_id TEXT NOT NULL,
      status TEXT DEFAULT 'pending',
      created_at INTEGER NOT NULL,
      PRIMARY KEY (requester_id, addressee_id)
    );

    CREATE TABLE IF NOT EXISTS close_friends (
      user_id TEXT NOT NULL,
      friend_id TEXT NOT NULL,
      PRIMARY KEY (user_id, friend_id)
    );

    CREATE TABLE IF NOT EXISTS blocks (
      blocker_id TEXT NOT NULL,
      blocked_id TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (blocker_id, blocked_id)
    );

    CREATE TABLE IF NOT EXISTS music_tracks (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      artist TEXT NOT NULL,
      audio_url TEXT NOT NULL,
      duration_sec INTEGER DEFAULT 30,
      license_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS posts (
      id TEXT PRIMARY KEY,
      author_id TEXT NOT NULL,
      idempotency_key TEXT UNIQUE,
      post_kind TEXT DEFAULT 'standard',
      caption TEXT DEFAULT '',
      location TEXT DEFAULT '',
      hashtags_json TEXT DEFAULT '[]',
      tagged_ids_json TEXT DEFAULT '[]',
      allow_download INTEGER DEFAULT 1,
      visibility TEXT DEFAULT 'public',
      reposted_from_id TEXT DEFAULT '',
      music_track_id TEXT DEFAULT '',
      local_audio_title TEXT DEFAULT '',
      music_start_sec INTEGER DEFAULT 0,
      music_volume INTEGER DEFAULT 70,
      original_volume INTEGER DEFAULT 80,
      youtube_video_id TEXT DEFAULT '',
      youtube_title TEXT DEFAULT '',
      youtube_thumbnail TEXT DEFAULT '',
      is_draft INTEGER DEFAULT 0,
      publish_at INTEGER NOT NULL,
      likes_count INTEGER DEFAULT 0,
      comments_count INTEGER DEFAULT 0,
      views_count INTEGER DEFAULT 0,
      shares_count INTEGER DEFAULT 0,
      is_deleted INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS post_media (
      id TEXT PRIMARY KEY,
      post_id TEXT NOT NULL,
      media_url TEXT NOT NULL,
      media_type TEXT NOT NULL,
      filter_name TEXT DEFAULT 'normal',
      sort_order INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS post_likes (
      post_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      PRIMARY KEY (post_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS post_comments (
      id TEXT PRIMARY KEY,
      post_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      parent_comment_id TEXT DEFAULT '',
      content TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS post_saves (
      post_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      PRIMARY KEY (post_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS post_polls (
      id TEXT PRIMARY KEY,
      post_id TEXT NOT NULL,
      question TEXT NOT NULL,
      options_json TEXT NOT NULL,
      correct_index INTEGER DEFAULT -1
    );

    CREATE TABLE IF NOT EXISTS poll_votes (
      poll_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      option_index INTEGER NOT NULL,
      PRIMARY KEY (poll_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS reels (
      id TEXT PRIMARY KEY,
      author_id TEXT NOT NULL,
      idempotency_key TEXT UNIQUE,
      video_url TEXT NOT NULL,
      thumbnail_url TEXT DEFAULT '',
      caption TEXT DEFAULT '',
      music_track_id TEXT DEFAULT '',
      music_start_sec INTEGER DEFAULT 0,
      music_volume INTEGER DEFAULT 70,
      original_volume INTEGER DEFAULT 80,
      reposted_from_id TEXT DEFAULT '',
      likes_count INTEGER DEFAULT 0,
      comments_count INTEGER DEFAULT 0,
      views_count INTEGER DEFAULT 0,
      is_deleted INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reel_likes (
      reel_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      PRIMARY KEY (reel_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS reel_comments (
      id TEXT PRIMARY KEY,
      reel_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      content TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reel_saves (
      reel_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      PRIMARY KEY (reel_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS stories (
      id TEXT PRIMARY KEY,
      author_id TEXT NOT NULL,
      media_url TEXT DEFAULT '',
      media_type TEXT NOT NULL,
      text_overlay TEXT DEFAULT '',
      bg_color TEXT DEFAULT '#FF5A36',
      stickers_json TEXT DEFAULT '[]',
      music_track_id TEXT DEFAULT '',
      music_start_sec INTEGER DEFAULT 0,
      youtube_video_id TEXT DEFAULT '',
      youtube_title TEXT DEFAULT '',
      privacy TEXT DEFAULT 'followers',
      expires_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS story_views (
      story_id TEXT NOT NULL,
      viewer_id TEXT NOT NULL,
      liked INTEGER DEFAULT 0,
      viewed_at INTEGER NOT NULL,
      PRIMARY KEY (story_id, viewer_id)
    );

    CREATE TABLE IF NOT EXISTS story_replies (
      id TEXT PRIMARY KEY,
      story_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS story_highlights (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      title TEXT NOT NULL,
      cover_url TEXT DEFAULT '',
      story_ids_json TEXT DEFAULT '[]',
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS communities (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      photo_url TEXT DEFAULT '',
      privacy TEXT DEFAULT 'public',
      is_broadcast INTEGER DEFAULT 0,
      creator_id TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS community_members (
      community_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      role TEXT DEFAULT 'member',
      joined_at INTEGER NOT NULL,
      PRIMARY KEY (community_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS community_posts (
      id TEXT PRIMARY KEY,
      community_id TEXT NOT NULL,
      author_id TEXT NOT NULL,
      content TEXT NOT NULL,
      media_url TEXT DEFAULT '',
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS chats (
      id TEXT PRIMARY KEY,
      is_group INTEGER DEFAULT 0,
      group_name TEXT DEFAULT '',
      group_photo TEXT DEFAULT '',
      created_by TEXT DEFAULT '',
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS chat_members (
      chat_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      role TEXT DEFAULT 'member',
      is_pinned INTEGER DEFAULT 0,
      is_muted INTEGER DEFAULT 0,
      is_archived INTEGER DEFAULT 0,
      PRIMARY KEY (chat_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      chat_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      msg_type TEXT DEFAULT 'text',
      content TEXT DEFAULT '',
      media_url TEXT DEFAULT '',
      file_name TEXT DEFAULT '',
      reply_to_id TEXT DEFAULT '',
      forwarded_from_id TEXT DEFAULT '',
      reactions_json TEXT DEFAULT '{}',
      seen_by_json TEXT DEFAULT '[]',
      is_edited INTEGER DEFAULT 0,
      is_deleted INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS calls (
      id TEXT PRIMARY KEY,
      caller_id TEXT NOT NULL,
      callee_ids_json TEXT NOT NULL,
      chat_id TEXT DEFAULT '',
      call_type TEXT NOT NULL,
      is_group INTEGER DEFAULT 0,
      status TEXT DEFAULT 'ringing',
      started_at INTEGER NOT NULL,
      ended_at INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS live_streams (
      id TEXT PRIMARY KEY,
      host_id TEXT NOT NULL,
      title TEXT NOT NULL,
      status TEXT DEFAULT 'live',
      guest_ids_json TEXT DEFAULT '[]',
      viewers_count INTEGER DEFAULT 0,
      started_at INTEGER NOT NULL,
      ended_at INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS live_comments (
      id TEXT PRIMARY KEY,
      live_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      content TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      actor_id TEXT DEFAULT '',
      type TEXT NOT NULL,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      target_id TEXT DEFAULT '',
      is_read INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reports (
      id TEXT PRIMARY KEY,
      reporter_id TEXT NOT NULL,
      target_type TEXT NOT NULL,
      target_id TEXT NOT NULL,
      reason TEXT NOT NULL,
      status TEXT DEFAULT 'pending',
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS admin_audit_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      admin_id TEXT NOT NULL,
      action TEXT NOT NULL,
      target_id TEXT DEFAULT '',
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS server_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      level TEXT NOT NULL,
      route TEXT DEFAULT '',
      message TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
  `);

  // प्रारंभिक म्यूज़िक ट्रैक व डिफ़ॉल्ट एडमिन अकाउंट (यदि पहले से न हो)
  const trackCount = dbDriver.get('SELECT COUNT(*) AS c FROM music_tracks').c;
  if (trackCount === 0) {
    dbDriver.run('INSERT INTO music_tracks VALUES (?,?,?,?,?,?)', ['trk_1', 'सुबह की धुन', 'ग्रामबुक स्टूडियो', '/uploads/music/morning.mp3', 45, 'Royalty Free CC-BY']);
    dbDriver.run('INSERT INTO music_tracks VALUES (?,?,?,?,?,?)', ['trk_2', 'लोफ़ी नाइट्स', 'देसी बीट्स', '/uploads/music/lofi.mp3', 60, 'Royalty Free CC-BY']);
    dbDriver.run('INSERT INTO music_tracks VALUES (?,?,?,?,?,?)', ['trk_3', 'उत्सव ढोल', 'लोक संगीत', '/uploads/music/dhol.mp3', 35, 'Royalty Free CC-BY']);
  }

  const adminExists = dbDriver.get("SELECT id FROM users WHERE role = 'admin' LIMIT 1");
  if (!adminExists) {
    const hash = bcrypt.hashSync('admin123', 10);
    dbDriver.run(
      `INSERT INTO users (id, public_uid, phone, username, full_name, email, password_hash, bio, is_verified, role, created_at)
       VALUES (?,?,?,?,?,?,?,?,1,'admin',?)`,
      ['u_admin', 'GBADMIN1', '9000000000', 'admin', 'GRAMBOOK एडमिन', 'admin@grambook.local', hash, 'सिस्टम व्यवस्थापक', Date.now()]
    );
  }
}

// ---------------- हेल्पर्स, अपलोड, WebSocket और पुश ----------------
const uid = (prefix = '') => prefix + crypto.randomUUID();
const safeJson = (raw, fallback) => { try { return JSON.parse(raw); } catch { return fallback; } };
const socketsByUser = new Map();

function emitToUser(userId, event, payload) {
  const set = socketsByUser.get(userId);
  if (!set) return;
  const msg = JSON.stringify({ event, payload });
  for (const ws of set) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function sendNotification({ userId, actorId = '', type, title, body, targetId = '' }) {
  if (!userId || userId === actorId) return;
  const id = uid('notif_');
  const now = Date.now();
  dbDriver.run(
    'INSERT INTO notifications (id, user_id, actor_id, type, title, body, target_id, created_at) VALUES (?,?,?,?,?,?,?,?)',
    [id, userId, actorId, type, title, body, targetId, now]
  );
  const row = dbDriver.get('SELECT * FROM notifications WHERE id = ?', [id]);
  emitToUser(userId, 'notification', row);

  if (NTFY_BASE_URL) {
    fetch(`${NTFY_BASE_URL.replace(/\/$/, '')}/grambook_${userId.replace(/[^a-zA-Z0-9]/g, '')}`, {
      method: 'POST',
      headers: { Title: title },
      body,
    }).catch(() => {});
  }
}

function issueTokens(userId, role) {
  const accessToken = jwt.sign({ userId, role }, JWT_SECRET, { expiresIn: '7d' });
  const refreshToken = jwt.sign({ userId, role }, JWT_REFRESH_SECRET, { expiresIn: '60d' });
  return { accessToken, refreshToken };
}

function sanitizeUser(u) {
  if (!u) return null;
  const { password_hash, ...safe } = u;
  return safe;
}

// स्पैम रेट लिमिटर और Auth मिडलवेयर
const rateMap = new Map();
function rateLimit(maxPerMin) {
  return (req, res, next) => {
    const key = `${req.ip}:${req.path}`;
    const now = Date.now();
    const b = rateMap.get(key);
    if (!b || b.resetAt < now) {
      rateMap.set(key, { count: 1, resetAt: now + 60000 });
      return next();
    }
    b.count += 1;
    if (b.count > maxPerMin) return res.status(429).json({ error: 'बहुत अधिक प्रयास! कृपया 1 मिनट बाद पुनः प्रयास करें।' });
    next();
  };
}

function requireAuth(req, res, next) {
  const h = req.headers.authorization;
  if (!h || !h.startsWith('Bearer ')) return res.status(401).json({ error: 'कृपया पहले लॉगिन करें।' });
  try {
    const dec = jwt.verify(h.slice(7), JWT_SECRET);
    const u = dbDriver.get('SELECT status, role FROM users WHERE id = ?', [dec.userId]);
    if (!u || u.status !== 'active') return res.status(403).json({ error: 'यह अकाउंट सक्रिय नहीं है या सस्पेंड है।' });
    req.authUser = { userId: dec.userId, role: u.role };
    next();
  } catch {
    return res.status(401).json({ error: 'सेशन समाप्त हो गया है। पुनः लॉगिन करें।' });
  }
}

function requireAdmin(req, res, next) {
  requireAuth(req, res, () => {
    if (req.authUser.role !== 'admin') return res.status(403).json({ error: 'यह अधिकार केवल एडमिन के पास है।' });
    next();
  });
}

// ---------------- Express ऐप और फ़ाइल अपलोड ----------------
const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use('/uploads', express.static(UPLOAD_DIR));

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const folder = String(req.query.folder || 'general').replace(/[^a-zA-Z0-9_-]/g, '');
    const dir = path.join(UPLOAD_DIR, folder);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}_${file.originalname.replace(/[^a-zA-Z0-9._-]/g, '')}`);
  },
});
const upload = multer({ storage, limits: { fileSize: 60 * 1024 * 1024 } });

app.get('/health', (req, res) => res.json({ ok: true, service: 'GRAMBOOK SQLite API', time: Date.now() }));

app.post('/media/upload', requireAuth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'कोई फ़ाइल प्राप्त नहीं हुई।' });
  const folder = String(req.query.folder || 'general').replace(/[^a-zA-Z0-9_-]/g, '');
  const host = `${req.protocol}://${req.get('host')}`;
  res.json({
    ok: true,
    url: `${host}/uploads/${folder}/${req.file.filename}`,
    mimeType: req.file.mimetype,
    fileName: req.file.originalname,
  });
});

// ---------------- 1. रजिस्ट्रेशन, लॉगिन, पासवर्ड रिकवरी ----------------
app.post('/auth/register', rateLimit(20), (req, res) => {
  const { fullName, username, phone, password, dob = '', email = '', bio = '', avatarUrl = '' } = req.body || {};
  if (!fullName?.trim() || !username?.trim() || !phone?.trim() || !password) {
    return res.status(400).json({ error: 'पूरा नाम, यूज़रनेम, मोबाइल नंबर और पासवर्ड अनिवार्य हैं।' });
  }
  const cleanUser = username.trim().toLowerCase().replace(/[^a-z0-9._]/g, '');
  if (cleanUser.length < 3) return res.status(400).json({ error: 'यूज़रनेम कम से कम 3 अक्षरों का रखें।' });
  if (password.length < 6) return res.status(400).json({ error: 'पासवर्ड कम से कम 6 अक्षरों का रखें।' });
  const cleanPhone = phone.replace(/\D/g, '');
  if (cleanPhone.length < 10) return res.status(400).json({ error: 'सही 10 अंकों का मोबाइल नंबर लिखें।' });

  const dup = dbDriver.get('SELECT id FROM users WHERE LOWER(username) = ? OR phone = ?', [cleanUser, cleanPhone]);
  if (dup) return res.status(409).json({ error: 'यह यूज़रनेम या मोबाइल नंबर पहले से उपयोग में है।' });

  const id = uid('u_');
  const publicUid = 'GB' + crypto.randomBytes(4).toString('hex').toUpperCase();
  const hash = bcrypt.hashSync(password, 10);
  const now = Date.now();

  dbDriver.run(
    `INSERT INTO users (id, public_uid, phone, username, full_name, email, password_hash, dob, bio, avatar_url, created_at)
     VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
    [id, publicUid, cleanPhone, cleanUser, fullName.trim(), email.trim(), hash, dob, bio.trim(), avatarUrl, now]
  );

  const user = sanitizeUser(dbDriver.get('SELECT * FROM users WHERE id = ?', [id]));
  const tokens = issueTokens(id, user.role);
  const rHash = crypto.createHash('sha256').update(tokens.refreshToken).digest('hex');
  dbDriver.run(
    'INSERT INTO user_sessions (id, user_id, refresh_token_hash, ip_address, expires_at, created_at) VALUES (?,?,?,?,?,?)',
    [uid('ses_'), id, rHash, req.ip || '', now + 60 * 86400000, now]
  );
  res.json({ ok: true, user, ...tokens });
});

app.post('/auth/login', rateLimit(30), (req, res) => {
  const { identifier, password, deviceName = 'Android' } = req.body || {};
  if (!identifier?.trim() || !password) return res.status(400).json({ error: 'यूज़रनेम/मोबाइल और पासवर्ड भरें।' });

  const idClean = identifier.trim().toLowerCase();
  const phoneClean = identifier.replace(/\D/g, '') || '___';
  const user = dbDriver.get('SELECT * FROM users WHERE LOWER(username) = ? OR phone = ? LIMIT 1', [idClean, phoneClean]);
  if (!user || !bcrypt.compareSync(password, user.password_hash)) {
    return res.status(401).json({ error: 'यूज़रनेम या पासवर्ड गलत है।' });
  }
  if (user.status === 'suspended' || user.status === 'deleted') {
    return res.status(403).json({ error: 'यह अकाउंट सस्पेंड या हटा दिया गया है।' });
  }
  if (user.status === 'deactivated') {
    dbDriver.run("UPDATE users SET status = 'active' WHERE id = ?", [user.id]);
    user.status = 'active';
  }

  const now = Date.now();
  const tokens = issueTokens(user.id, user.role);
  const rHash = crypto.createHash('sha256').update(tokens.refreshToken).digest('hex');
  dbDriver.run(
    'INSERT INTO user_sessions (id, user_id, refresh_token_hash, device_name, ip_address, expires_at, created_at) VALUES (?,?,?,?,?,?,?)',
    [uid('ses_'), user.id, rHash, deviceName, req.ip || '', now + 60 * 86400000, now]
  );
  res.json({ ok: true, user: sanitizeUser(user), ...tokens });
});

app.post('/auth/change-password', requireAuth, (req, res) => {
  const { oldPassword, newPassword } = req.body || {};
  if (!oldPassword || !newPassword || newPassword.length < 6) {
    return res.status(400).json({ error: 'कम से कम 6 अक्षरों का नया पासवर्ड लिखें।' });
  }
  const u = dbDriver.get('SELECT password_hash FROM users WHERE id = ?', [req.authUser.userId]);
  if (!bcrypt.compareSync(oldPassword, u.password_hash)) return res.status(400).json({ error: 'वर्तमान पासवर्ड गलत है।' });
  dbDriver.run('UPDATE users SET password_hash = ? WHERE id = ?', [bcrypt.hashSync(newPassword, 10), req.authUser.userId]);
  res.json({ ok: true, message: 'पासवर्ड बदल दिया गया।' });
});

app.post('/auth/forgot-password', rateLimit(5), (req, res) => {
  const { identifier } = req.body || {};
  const u = dbDriver.get('SELECT id, email FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?) LIMIT 1', [identifier || '', identifier || '']);
  if (!u) return res.status(404).json({ error: 'इस नाम या ईमेल से कोई अकाउंट नहीं मिला।' });
  const code = String(crypto.randomInt(100000, 999999));
  dbDriver.run(
    'INSERT INTO password_resets (id, user_id, code_hash, expires_at, created_at) VALUES (?,?,?,?,?)',
    [uid('pr_'), u.id, bcrypt.hashSync(code, 8), Date.now() + 15 * 60000, Date.now()]
  );
  console.log(`\n[GRAMBOOK RECOVERY CODE] यूज़र ID: ${u.id} | कोड: ${code}\n`);
  res.json({ ok: true, message: 'रिकवरी कोड जारी किया गया है (Termux कंसोल देखें)।' });
});

app.post('/auth/reset-password', (req, res) => {
  const { identifier, code, newPassword } = req.body || {};
  if (!identifier || !code || !newPassword || newPassword.length < 6) return res.status(400).json({ error: 'सभी जानकारी सही से भरें।' });
  const u = dbDriver.get('SELECT id FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?) LIMIT 1', [identifier, identifier]);
  if (!u) return res.status(404).json({ error: 'यूज़र नहीं मिला।' });
  const pr = dbDriver.get('SELECT * FROM password_resets WHERE user_id = ? AND used = 0 AND expires_at > ? ORDER BY created_at DESC LIMIT 1', [u.id, Date.now()]);
  if (!pr || !bcrypt.compareSync(String(code), pr.code_hash)) return res.status(400).json({ error: 'रिकवरी कोड गलत या समाप्त हो गया है।' });
  dbDriver.run('UPDATE users SET password_hash = ? WHERE id = ?', [bcrypt.hashSync(newPassword, 10), u.id]);
  dbDriver.run('UPDATE password_resets SET used = 1 WHERE id = ?', [pr.id]);
  res.json({ ok: true, message: 'पासवर्ड सफलतापूर्वक बदल दिया गया।' });
});

app.post('/auth/logout', requireAuth, (req, res) => {
  dbDriver.run('UPDATE users SET is_online = 0, last_seen = ? WHERE id = ?', [Date.now(), req.authUser.userId]);
  res.json({ ok: true });
});

app.get('/auth/download-data', requireAuth, (req, res) => {
  const id = req.authUser.userId;
  const profile = sanitizeUser(dbDriver.get('SELECT * FROM users WHERE id = ?', [id]));
  const posts = dbDriver.all('SELECT * FROM posts WHERE author_id = ? AND is_deleted = 0', [id]);
  const reels = dbDriver.all('SELECT * FROM reels WHERE author_id = ? AND is_deleted = 0', [id]);
  res.json({ ok: true, profile, posts, reels });
});

app.post('/auth/deactivate', requireAuth, (req, res) => {
  dbDriver.run("UPDATE users SET status = 'deactivated', is_online = 0 WHERE id = ?", [req.authUser.userId]);
  res.json({ ok: true });
});

app.delete('/auth/account', requireAuth, (req, res) => {
  dbDriver.run('DELETE FROM users WHERE id = ?', [req.authUser.userId]);
  res.json({ ok: true });
});

// ---------------- 2. प्रोफाइल, दोस्त, फॉलो, ब्लॉक व सेटिंग्स ----------------
app.get('/users/me/suggestions', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const suggested = dbDriver.all(
    `SELECT id, public_uid, username, full_name, avatar_url, is_verified FROM users
     WHERE id != ? AND status = 'active'
       AND id NOT IN (SELECT following_id FROM follows WHERE follower_id = ?)
       AND id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id = ?)
     ORDER BY created_at DESC LIMIT 15`,
    [meId, meId, meId]
  );
  const friendRequests = dbDriver.all(
    `SELECT u.id, u.username, u.full_name, u.avatar_url FROM friendships f JOIN users u ON u.id = f.requester_id
     WHERE f.addressee_id = ? AND f.status = 'pending'`,
    [meId]
  );
  const followRequests = dbDriver.all(
    `SELECT u.id, u.username, u.full_name, u.avatar_url FROM follows f JOIN users u ON u.id = f.follower_id
     WHERE f.following_id = ? AND f.status = 'pending'`,
    [meId]
  );
  res.json({ ok: true, suggested, friendRequests, followRequests });
});

app.get('/users/:id', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const targetId = req.params.id;
  const u = dbDriver.get(
    `SELECT id, public_uid, username, full_name, bio, avatar_url, cover_url, dob, is_private, is_verified, is_creator, is_online, last_seen,
      (SELECT COUNT(*) FROM follows WHERE following_id = users.id AND status = 'accepted') AS followers_count,
      (SELECT COUNT(*) FROM follows WHERE follower_id = users.id AND status = 'accepted') AS following_count,
      (SELECT COUNT(*) FROM friendships WHERE (requester_id = users.id OR addressee_id = users.id) AND status = 'accepted') AS friends_count,
      (SELECT COUNT(*) FROM posts WHERE author_id = users.id AND is_deleted = 0 AND is_draft = 0) AS posts_count
     FROM users WHERE id = ? AND status = 'active'`,
    [targetId]
  );
  if (!u) return res.status(404).json({ error: 'यूज़र नहीं मिला।' });

  const isFollowing = Boolean(dbDriver.get("SELECT 1 FROM follows WHERE follower_id = ? AND following_id = ? AND status = 'accepted'", [meId, targetId]));
  const followRequested = Boolean(dbDriver.get("SELECT 1 FROM follows WHERE follower_id = ? AND following_id = ? AND status = 'pending'", [meId, targetId]));
  const isFriend = Boolean(dbDriver.get("SELECT 1 FROM friendships WHERE ((requester_id = ? AND addressee_id = ?) OR (requester_id = ? AND addressee_id = ?)) AND status = 'accepted'", [meId, targetId, targetId, meId]));
  const isBlocked = Boolean(dbDriver.get('SELECT 1 FROM blocks WHERE blocker_id = ? AND blocked_id = ?', [meId, targetId]));

  const canView = meId === targetId || !u.is_private || isFollowing;
  let posts = [];
  let reels = [];
  if (canView && !isBlocked) {
    posts = dbDriver.all('SELECT * FROM posts WHERE author_id = ? AND is_deleted = 0 AND is_draft = 0 ORDER BY created_at DESC LIMIT 30', [targetId])
      .map(p => ({ ...p, media: dbDriver.all('SELECT * FROM post_media WHERE post_id = ? ORDER BY sort_order', [p.id]) }));
    reels = dbDriver.all('SELECT * FROM reels WHERE author_id = ? AND is_deleted = 0 ORDER BY created_at DESC LIMIT 20', [targetId]);
  }
  res.json({ ok: true, profile: { ...u, isFollowing, followRequested, isFriend, isBlocked }, canView, posts, reels });
});

app.patch('/users/me', requireAuth, (req, res) => {
  const allowed = ['full_name', 'bio', 'avatar_url', 'cover_url', 'is_private', 'is_creator', 'who_can_comment', 'who_can_message', 'who_can_tag', 'default_post_visibility', 'hide_profile_visits', 'theme_mode', 'language', 'data_saver', 'autoplay_videos', 'notify_likes', 'notify_comments', 'notify_messages'];
  const sets = [];
  const vals = [];
  for (const k of allowed) {
    if (req.body[k] !== undefined) {
      sets.push(`${k} = ?`);
      vals.push(typeof req.body[k] === 'boolean' ? (req.body[k] ? 1 : 0) : req.body[k]);
    }
  }
  if (sets.length) {
    vals.push(req.authUser.userId);
    dbDriver.run(`UPDATE users SET ${sets.join(', ')} WHERE id = ?`, vals);
  }
  const user = sanitizeUser(dbDriver.get('SELECT * FROM users WHERE id = ?', [req.authUser.userId]));
  res.json({ ok: true, user });
});

app.post('/users/:id/follow', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const tid = req.params.id;
  if (meId === tid) return res.json({ ok: false });
  const ex = dbDriver.get('SELECT status FROM follows WHERE follower_id = ? AND following_id = ?', [meId, tid]);
  if (ex) {
    dbDriver.run('DELETE FROM follows WHERE follower_id = ? AND following_id = ?', [meId, tid]);
    return res.json({ ok: true, status: 'none' });
  }
  const tu = dbDriver.get('SELECT is_private FROM users WHERE id = ?', [tid]);
  const st = tu?.is_private ? 'pending' : 'accepted';
  dbDriver.run('INSERT INTO follows VALUES (?,?,?,?)', [meId, tid, st, Date.now()]);
  sendNotification({ userId: tid, actorId: meId, type: 'follow', title: 'नया फॉलोअर', body: st === 'pending' ? 'ने फॉलो अनुरोध भेजा है।' : 'ने आपको फॉलो किया।', targetId: meId });
  res.json({ ok: true, status: st });
});

app.post('/users/:id/follow-accept', requireAuth, (req, res) => {
  dbDriver.run("UPDATE follows SET status = 'accepted' WHERE follower_id = ? AND following_id = ?", [req.params.id, req.authUser.userId]);
  res.json({ ok: true });
});

app.post('/users/:id/friend-request', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const tid = req.params.id;
  dbDriver.run("INSERT OR IGNORE INTO friendships VALUES (?,?,'pending',?)", [meId, tid, Date.now()]);
  sendNotification({ userId: tid, actorId: meId, type: 'friend_request', title: 'फ्रेंड रिक्वेस्ट', body: 'ने आपको फ्रेंड रिक्वेस्ट भेजा है।', targetId: meId });
  res.json({ ok: true });
});

app.post('/users/:id/friend-accept', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const tid = req.params.id;
  dbDriver.run("UPDATE friendships SET status = 'accepted' WHERE requester_id = ? AND addressee_id = ?", [tid, meId]);
  sendNotification({ userId: tid, actorId: meId, type: 'friend_accept', title: 'रिक्वेस्ट स्वीकार', body: 'ने आपका फ्रेंड रिक्वेस्ट स्वीकार कर लिया।', targetId: meId });
  res.json({ ok: true });
});

app.delete('/users/:id/friend', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const tid = req.params.id;
  dbDriver.run('DELETE FROM friendships WHERE (requester_id = ? AND addressee_id = ?) OR (requester_id = ? AND addressee_id = ?)', [meId, tid, tid, meId]);
  res.json({ ok: true });
});

app.post('/users/:id/block', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const tid = req.params.id;
  const ex = dbDriver.get('SELECT 1 FROM blocks WHERE blocker_id = ? AND blocked_id = ?', [meId, tid]);
  if (ex) {
    dbDriver.run('DELETE FROM blocks WHERE blocker_id = ? AND blocked_id = ?', [meId, tid]);
    return res.json({ ok: true, blocked: false });
  }
  dbDriver.run('INSERT INTO blocks VALUES (?,?,?)', [meId, tid, Date.now()]);
  dbDriver.run('DELETE FROM follows WHERE (follower_id = ? AND following_id = ?) OR (follower_id = ? AND following_id = ?)', [meId, tid, tid, meId]);
  res.json({ ok: true, blocked: true });
});

app.post('/users/:id/close-friend', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const tid = req.params.id;
  const ex = dbDriver.get('SELECT 1 FROM close_friends WHERE user_id = ? AND friend_id = ?', [meId, tid]);
  if (ex) {
    dbDriver.run('DELETE FROM close_friends WHERE user_id = ? AND friend_id = ?', [meId, tid]);
    return res.json({ ok: true, closeFriend: false });
  }
  dbDriver.run('INSERT INTO close_friends VALUES (?,?)', [meId, tid]);
  res.json({ ok: true, closeFriend: true });
});

// ---------------- 3. होम फीड, पोस्ट, पोल, क्विज़, लाइक व कमेंट ----------------
app.get('/posts/feed', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const now = Date.now();
  const rows = dbDriver.all(
    `SELECT p.*, u.username AS author_username, u.full_name AS author_name, u.avatar_url AS author_avatar, u.is_verified AS author_verified
     FROM posts p JOIN users u ON u.id = p.author_id
     WHERE p.is_deleted = 0 AND p.is_draft = 0 AND p.publish_at <= ? AND u.status = 'active'
       AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.blocker_id = ? AND b.blocked_id = p.author_id) OR (b.blocker_id = p.author_id AND b.blocked_id = ?))
       AND (
         p.author_id = ?
         OR (p.visibility = 'public' AND u.is_private = 0)
         OR (p.visibility IN ('public','followers') AND EXISTS (SELECT 1 FROM follows f WHERE f.follower_id = ? AND f.following_id = p.author_id AND f.status = 'accepted'))
         OR (p.visibility = 'close_friends' AND EXISTS (SELECT 1 FROM close_friends cf WHERE cf.user_id = p.author_id AND cf.friend_id = ?))
       )
     ORDER BY p.created_at DESC LIMIT 40`,
    [now, meId, meId, meId, meId, meId]
  );

  const posts = rows.map(p => ({
    ...p,
    hashtags: safeJson(p.hashtags_json, []),
    media: dbDriver.all('SELECT * FROM post_media WHERE post_id = ? ORDER BY sort_order', [p.id]),
    poll: dbDriver.get('SELECT * FROM post_polls WHERE post_id = ?', [p.id]) || null,
    is_liked: Boolean(dbDriver.get('SELECT 1 FROM post_likes WHERE post_id = ? AND user_id = ?', [p.id, meId])),
    is_saved: Boolean(dbDriver.get('SELECT 1 FROM post_saves WHERE post_id = ? AND user_id = ?', [p.id, meId])),
  }));
  res.json({ ok: true, posts });
});

app.post('/posts', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const b = req.body || {};
  if (b.idempotencyKey) {
    const ex = dbDriver.get('SELECT * FROM posts WHERE idempotency_key = ?', [b.idempotencyKey]);
    if (ex) return res.json({ ok: true, post: ex });
  }

  const postId = uid('p_');
  const now = Date.now();
  const hashtags = (b.caption || '').match(/#[\p{L}\p{N}_]+/gu) || [];

  dbDriver.run(
    `INSERT INTO posts (id, author_id, idempotency_key, post_kind, caption, location, hashtags_json, tagged_ids_json, allow_download, visibility, reposted_from_id, music_track_id, local_audio_title, music_start_sec, music_volume, original_volume, youtube_video_id, youtube_title, youtube_thumbnail, is_draft, publish_at, created_at)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
      postId, meId, b.idempotencyKey || null, b.postKind || 'standard', b.caption || '', b.location || '',
      JSON.stringify(hashtags), JSON.stringify(b.taggedUserIds || []), b.allowDownload === false ? 0 : 1,
      b.visibility || 'public', b.repostedFromId || '', b.musicTrackId || '', b.localAudioTitle || '',
      Number(b.musicStartSec || 0), Number(b.musicVolume ?? 70), Number(b.originalVolume ?? 80),
      b.youtubeVideoId || '', b.youtubeTitle || '', b.youtubeThumbnail || '', b.isDraft ? 1 : 0,
      Number(b.publishAt || now), now,
    ]
  );

  (b.media || []).forEach((m, i) => {
    dbDriver.run('INSERT INTO post_media VALUES (?,?,?,?,?,?)', [uid('pm_'), postId, m.url, m.type || 'image', m.filter || 'normal', i]);
  });

  if (b.poll && b.poll.question && Array.isArray(b.poll.options)) {
    dbDriver.run('INSERT INTO post_polls VALUES (?,?,?,?,?)', [
      uid('poll_'), postId, b.poll.question, JSON.stringify(b.poll.options),
      b.poll.correctIndex !== undefined ? Number(b.poll.correctIndex) : -1,
    ]);
  }

  for (const tid of (b.taggedUserIds || [])) {
    sendNotification({ userId: tid, actorId: meId, type: 'tag', title: 'पोस्ट में टैग', body: 'ने आपको एक पोस्ट में टैग किया है।', targetId: postId });
  }

  res.json({ ok: true, post: dbDriver.get('SELECT * FROM posts WHERE id = ?', [postId]) });
});

app.post('/posts/:id/like', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const pid = req.params.id;
  const ex = dbDriver.get('SELECT 1 FROM post_likes WHERE post_id = ? AND user_id = ?', [pid, meId]);
  if (ex) {
    dbDriver.run('DELETE FROM post_likes WHERE post_id = ? AND user_id = ?', [pid, meId]);
    dbDriver.run('UPDATE posts SET likes_count = MAX(0, likes_count - 1) WHERE id = ?', [pid]);
    return res.json({ ok: true, liked: false });
  }
  dbDriver.run('INSERT INTO post_likes VALUES (?,?)', [pid, meId]);
  dbDriver.run('UPDATE posts SET likes_count = likes_count + 1 WHERE id = ?', [pid]);
  const p = dbDriver.get('SELECT author_id FROM posts WHERE id = ?', [pid]);
  if (p) sendNotification({ userId: p.author_id, actorId: meId, type: 'like', title: 'नया लाइक', body: 'ने आपकी पोस्ट को लाइक किया।', targetId: pid });
  res.json({ ok: true, liked: true });
});

app.post('/posts/:id/save', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const pid = req.params.id;
  const ex = dbDriver.get('SELECT 1 FROM post_saves WHERE post_id = ? AND user_id = ?', [pid, meId]);
  if (ex) { dbDriver.run('DELETE FROM post_saves WHERE post_id = ? AND user_id = ?', [pid, meId]); return res.json({ ok: true, saved: false }); }
  dbDriver.run('INSERT INTO post_saves VALUES (?,?)', [pid, meId]);
  res.json({ ok: true, saved: true });
});

app.post('/posts/:id/vote', requireAuth, (req, res) => {
  const poll = dbDriver.get('SELECT id FROM post_polls WHERE post_id = ?', [req.params.id]);
  if (!poll) return res.status(404).json({ error: 'पोल नहीं मिला।' });
  dbDriver.run('INSERT OR REPLACE INTO poll_votes VALUES (?,?,?)', [poll.id, req.authUser.userId, Number(req.body.optionIndex)]);
  const counts = dbDriver.all('SELECT option_index, COUNT(*) AS votes FROM poll_votes WHERE poll_id = ? GROUP BY option_index', [poll.id]);
  res.json({ ok: true, counts });
});

app.get('/posts/:id/comments', requireAuth, (req, res) => {
  const comments = dbDriver.all(
    `SELECT c.*, u.username, u.full_name, u.avatar_url FROM post_comments c JOIN users u ON u.id = c.user_id
     WHERE c.post_id = ? ORDER BY c.created_at ASC`,
    [req.params.id]
  );
  res.json({ ok: true, comments });
});

app.post('/posts/:id/comments', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const pid = req.params.id;
  const { content, parentCommentId = '' } = req.body || {};
  if (!content?.trim()) return res.status(400).json({ error: 'कमेंट खाली नहीं हो सकता।' });
  const cid = uid('cmt_');
  dbDriver.run('INSERT INTO post_comments VALUES (?,?,?,?,?,?)', [cid, pid, meId, parentCommentId, content.trim(), Date.now()]);
  dbDriver.run('UPDATE posts SET comments_count = comments_count + 1 WHERE id = ?', [pid]);
  const p = dbDriver.get('SELECT author_id FROM posts WHERE id = ?', [pid]);
  if (p) sendNotification({ userId: p.author_id, actorId: meId, type: 'comment', title: 'नया कमेंट', body: `ने कमेंट किया: "${content.trim().slice(0, 40)}"`, targetId: pid });
  res.json({ ok: true, comment: dbDriver.get('SELECT * FROM post_comments WHERE id = ?', [cid]) });
});

app.patch('/posts/:id', requireAuth, (req, res) => {
  const p = dbDriver.get('SELECT author_id FROM posts WHERE id = ?', [req.params.id]);
  if (!p || p.author_id !== req.authUser.userId) return res.status(403).json({ error: 'अनुमति नहीं है।' });
  dbDriver.run('UPDATE posts SET caption = COALESCE(?, caption), location = COALESCE(?, location) WHERE id = ?', [req.body.caption, req.body.location, req.params.id]);
  res.json({ ok: true });
});

app.delete('/posts/:id', requireAuth, (req, res) => {
  const p = dbDriver.get('SELECT author_id FROM posts WHERE id = ?', [req.params.id]);
  if (!p || (p.author_id !== req.authUser.userId && req.authUser.role !== 'admin')) return res.status(403).json({ error: 'अनुमति नहीं है।' });
  dbDriver.run('UPDATE posts SET is_deleted = 1 WHERE id = ?', [req.params.id]);
  res.json({ ok: true });
});

// ---------------- 4. रील्स, स्टोरी, हाइलाइट्स, म्यूज़िक व आधिकारिक YouTube ----------------
app.get('/reels', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const reels = dbDriver.all(
    `SELECT r.*, u.username AS author_username, u.full_name AS author_name, u.avatar_url AS author_avatar
     FROM reels r JOIN users u ON u.id = r.author_id
     WHERE r.is_deleted = 0 AND u.status = 'active' ORDER BY r.created_at DESC LIMIT 30`
  ).map(r => ({
    ...r,
    is_liked: Boolean(dbDriver.get('SELECT 1 FROM reel_likes WHERE reel_id = ? AND user_id = ?', [r.id, meId])),
    is_saved: Boolean(dbDriver.get('SELECT 1 FROM reel_saves WHERE reel_id = ? AND user_id = ?', [r.id, meId])),
  }));
  res.json({ ok: true, reels });
});

app.post('/reels', requireAuth, (req, res) => {
  const b = req.body || {};
  if (!b.videoUrl) return res.status(400).json({ error: 'रील वीडियो अनिवार्य है।' });
  if (b.idempotencyKey) {
    const ex = dbDriver.get('SELECT * FROM reels WHERE idempotency_key = ?', [b.idempotencyKey]);
    if (ex) return res.json({ ok: true, reel: ex });
  }
  const id = uid('reel_');
  dbDriver.run(
    'INSERT INTO reels (id, author_id, idempotency_key, video_url, thumbnail_url, caption, music_track_id, music_start_sec, music_volume, original_volume, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
    [id, req.authUser.userId, b.idempotencyKey || null, b.videoUrl, b.thumbnailUrl || '', b.caption || '', b.musicTrackId || '', Number(b.musicStartSec || 0), Number(b.musicVolume ?? 70), Number(b.originalVolume ?? 80), Date.now()]
  );
  res.json({ ok: true, reel: dbDriver.get('SELECT * FROM reels WHERE id = ?', [id]) });
});

app.post('/reels/:id/like', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const rid = req.params.id;
  const ex = dbDriver.get('SELECT 1 FROM reel_likes WHERE reel_id = ? AND user_id = ?', [rid, meId]);
  if (ex) {
    dbDriver.run('DELETE FROM reel_likes WHERE reel_id = ? AND user_id = ?', [rid, meId]);
    dbDriver.run('UPDATE reels SET likes_count = MAX(0, likes_count - 1) WHERE id = ?', [rid]);
    return res.json({ ok: true, liked: false });
  }
  dbDriver.run('INSERT INTO reel_likes VALUES (?,?)', [rid, meId]);
  dbDriver.run('UPDATE reels SET likes_count = likes_count + 1 WHERE id = ?', [rid]);
  res.json({ ok: true, liked: true });
});

app.post('/reels/:id/view', requireAuth, (req, res) => {
  dbDriver.run('UPDATE reels SET views_count = views_count + 1 WHERE id = ?', [req.params.id]);
  res.json({ ok: true });
});

app.get('/reels/:id/comments', requireAuth, (req, res) => {
  const comments = dbDriver.all('SELECT c.*, u.username, u.full_name, u.avatar_url FROM reel_comments c JOIN users u ON u.id = c.user_id WHERE c.reel_id = ? ORDER BY c.created_at ASC', [req.params.id]);
  res.json({ ok: true, comments });
});

app.post('/reels/:id/comments', requireAuth, (req, res) => {
  if (!req.body.content?.trim()) return res.status(400).json({ error: 'कमेंट लिखें।' });
  const cid = uid('rc_');
  dbDriver.run('INSERT INTO reel_comments VALUES (?,?,?,?,?)', [cid, req.params.id, req.authUser.userId, req.body.content.trim(), Date.now()]);
  dbDriver.run('UPDATE reels SET comments_count = comments_count + 1 WHERE id = ?', [req.params.id]);
  res.json({ ok: true });
});

// स्टोरी (24 घंटे बाद स्वतः हटना) और हाइलाइट्स
app.get('/stories', requireAuth, (req, res) => {
  const now = Date.now();
  dbDriver.run('DELETE FROM stories WHERE expires_at <= ?', [now]);
  const stories = dbDriver.all(
    `SELECT s.*, u.username AS author_username, u.full_name AS author_name, u.avatar_url AS author_avatar
     FROM stories s JOIN users u ON u.id = s.author_id
     WHERE s.expires_at > ? ORDER BY s.created_at ASC`,
    [now]
  );
  res.json({ ok: true, stories });
});

app.post('/stories', requireAuth, (req, res) => {
  const b = req.body || {};
  const id = uid('st_');
  const now = Date.now();
  dbDriver.run(
    'INSERT INTO stories VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    [id, req.authUser.userId, b.mediaUrl || '', b.mediaType || 'image', b.textOverlay || '', b.bgColor || '#FF5A36', JSON.stringify(b.stickers || []), b.musicTrackId || '', Number(b.musicStartSec || 0), b.youtubeVideoId || '', b.youtubeTitle || '', b.privacy || 'followers', now + 24 * 3600000, now]
  );
  res.json({ ok: true, story: dbDriver.get('SELECT * FROM stories WHERE id = ?', [id]) });
});

app.post('/stories/:id/view', requireAuth, (req, res) => {
  dbDriver.run('INSERT OR REPLACE INTO story_views VALUES (?,?,?,?)', [req.params.id, req.authUser.userId, req.body.liked ? 1 : 0, Date.now()]);
  res.json({ ok: true });
});

app.get('/stories/:id/views', requireAuth, (req, res) => {
  const views = dbDriver.all('SELECT v.*, u.username, u.full_name, u.avatar_url FROM story_views v JOIN users u ON u.id = v.viewer_id WHERE v.story_id = ?', [req.params.id]);
  const replies = dbDriver.all('SELECT r.*, u.username, u.full_name FROM story_replies r JOIN users u ON u.id = r.sender_id WHERE r.story_id = ?', [req.params.id]);
  res.json({ ok: true, views, replies });
});

app.post('/stories/:id/reply', requireAuth, (req, res) => {
  if (!req.body.message?.trim()) return res.status(400).json({ error: 'प्रतिक्रिया लिखें।' });
  dbDriver.run('INSERT INTO story_replies VALUES (?,?,?,?)', [uid('sr_'), req.params.id, req.authUser.userId, req.body.message.trim(), Date.now()]);
  res.json({ ok: true });
});

app.post('/highlights', requireAuth, (req, res) => {
  const id = uid('hl_');
  dbDriver.run('INSERT INTO story_highlights VALUES (?,?,?,?,?,?)', [id, req.authUser.userId, req.body.title || 'मेरी यादें', req.body.coverUrl || '', JSON.stringify(req.body.storyIds || []), Date.now()]);
  res.json({ ok: true });
});

app.get('/highlights/:userId', requireAuth, (req, res) => {
  res.json({ ok: true, highlights: dbDriver.all('SELECT * FROM story_highlights WHERE user_id = ?', [req.params.userId]) });
});

// लाइसेंस्ड म्यूज़िक और आधिकारिक YouTube Data API सर्च
app.get('/music/tracks', requireAuth, (req, res) => {
  const q = `%${(req.query.q || '').trim().toLowerCase()}%`;
  const tracks = dbDriver.all('SELECT * FROM music_tracks WHERE LOWER(title) LIKE ? OR LOWER(artist) LIKE ?', [q, q]);
  res.json({ ok: true, tracks });
});

app.get('/youtube/search', requireAuth, async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (!q) return res.json({ ok: true, items: [] });
  if (!YOUTUBE_API_KEY) {
    return res.status(503).json({ error: 'सर्वर पर YOUTUBE_API_KEY सेट नहीं है। कृपया .env में YouTube Data API v3 Key जोड़ें।' });
  }
  try {
    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=12&q=${encodeURIComponent(q)}&key=${YOUTUBE_API_KEY}`;
    const ytRes = await fetch(url);
    const data = await ytRes.json();
    const items = (data.items || []).map(i => ({
      videoId: i.id.videoId,
      title: i.snippet.title,
      channelTitle: i.snippet.channelTitle,
      thumbnail: i.snippet.thumbnails?.medium?.url || '',
    }));
    res.json({ ok: true, items });
  } catch {
    res.status(500).json({ error: 'YouTube खोज में समस्या आ गई।' });
  }
});

// ---------------- 5. चैट, मैसेजिंग, कॉलिंग, कम्युनिटी, लाइव ----------------
app.get('/chats', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const chats = dbDriver.all(
    `SELECT c.*, cm.is_pinned, cm.is_muted, cm.is_archived
     FROM chats c JOIN chat_members cm ON cm.chat_id = c.id
     WHERE cm.user_id = ? ORDER BY cm.is_pinned DESC, c.updated_at DESC`,
    [meId]
  ).map(c => ({
    ...c,
    members: dbDriver.all('SELECT u.id, u.username, u.full_name, u.avatar_url, u.is_online, u.last_seen FROM chat_members cm JOIN users u ON u.id = cm.user_id WHERE cm.chat_id = ?', [c.id]),
    lastMessage: dbDriver.get('SELECT * FROM messages WHERE chat_id = ? AND is_deleted = 0 ORDER BY created_at DESC LIMIT 1', [c.id]) || null,
  }));
  res.json({ ok: true, chats });
});

app.post('/chats/direct', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const partnerId = req.body.partnerId;
  const existing = dbDriver.get(
    `SELECT c.id FROM chats c
     JOIN chat_members m1 ON m1.chat_id = c.id AND m1.user_id = ?
     JOIN chat_members m2 ON m2.chat_id = c.id AND m2.user_id = ?
     WHERE c.is_group = 0 LIMIT 1`,
    [meId, partnerId]
  );
  if (existing) return res.json({ ok: true, chatId: existing.id });
  const chatId = uid('chat_');
  dbDriver.run('INSERT INTO chats VALUES (?,0,"","",?,?)', [chatId, meId, Date.now()]);
  dbDriver.run('INSERT INTO chat_members (chat_id, user_id) VALUES (?,?)', [chatId, meId]);
  dbDriver.run('INSERT INTO chat_members (chat_id, user_id) VALUES (?,?)', [chatId, partnerId]);
  res.json({ ok: true, chatId });
});

app.post('/chats/group', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const { name, photoUrl = '', memberIds = [] } = req.body || {};
  const chatId = uid('grp_');
  dbDriver.run('INSERT INTO chats VALUES (?,1,?,?,?,?)', [chatId, name || 'नया ग्रुप', photoUrl, meId, Date.now()]);
  dbDriver.run("INSERT INTO chat_members (chat_id, user_id, role) VALUES (?,?,'admin')", [chatId, meId]);
  for (const mId of memberIds) {
    if (mId !== meId) dbDriver.run('INSERT OR IGNORE INTO chat_members (chat_id, user_id) VALUES (?,?)', [chatId, mId]);
  }
  res.json({ ok: true, chatId });
});

app.patch('/chats/:id/prefs', requireAuth, (req, res) => {
  const sets = [];
  const vals = [];
  for (const k of ['is_pinned', 'is_muted', 'is_archived']) {
    if (req.body[k] !== undefined) { sets.push(`${k} = ?`); vals.push(req.body[k] ? 1 : 0); }
  }
  if (sets.length) {
    vals.push(req.params.id, req.authUser.userId);
    dbDriver.run(`UPDATE chat_members SET ${sets.join(', ')} WHERE chat_id = ? AND user_id = ?`, vals);
  }
  res.json({ ok: true });
});

app.get('/chats/:id/messages', requireAuth, (req, res) => {
  const q = (req.query.q || '').trim().toLowerCase();
  const rows = dbDriver.all('SELECT * FROM messages WHERE chat_id = ? AND is_deleted = 0 ORDER BY created_at ASC', [req.params.id]);
  const filtered = q ? rows.filter(m => m.content.toLowerCase().includes(q)) : rows;
  res.json({ ok: true, messages: filtered });
});

app.post('/chats/:id/messages', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const chatId = req.params.id;
  const b = req.body || {};
  const msgId = uid('msg_');
  const now = Date.now();
  dbDriver.run(
    'INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    [msgId, chatId, meId, b.msgType || 'text', b.content || '', b.mediaUrl || '', b.fileName || '', b.replyToId || '', b.forwardedFromId || '', '{}', JSON.stringify([meId]), 0, 0, now]
  );
  dbDriver.run('UPDATE chats SET updated_at = ? WHERE id = ?', [now, chatId]);
  const msg = dbDriver.get('SELECT * FROM messages WHERE id = ?', [msgId]);

  const members = dbDriver.all('SELECT user_id FROM chat_members WHERE chat_id = ?', [chatId]);
  for (const m of members) {
    emitToUser(m.user_id, 'chat_message', msg);
    if (m.user_id !== meId) {
      sendNotification({ userId: m.user_id, actorId: meId, type: 'message', title: 'नया संदेश', body: b.content ? b.content.slice(0, 40) : 'मीडिया संदेश भेजा।', targetId: chatId });
    }
  }
  res.json({ ok: true, message: msg });
});

app.patch('/messages/:id', requireAuth, (req, res) => {
  dbDriver.run('UPDATE messages SET content = ?, is_edited = 1 WHERE id = ? AND sender_id = ?', [req.body.content || '', req.params.id, req.authUser.userId]);
  res.json({ ok: true });
});

app.delete('/messages/:id', requireAuth, (req, res) => {
  dbDriver.run('UPDATE messages SET is_deleted = 1 WHERE id = ? AND sender_id = ?', [req.params.id, req.authUser.userId]);
  res.json({ ok: true });
});

app.post('/messages/:id/react', requireAuth, (req, res) => {
  const msg = dbDriver.get('SELECT reactions_json, chat_id FROM messages WHERE id = ?', [req.params.id]);
  if (!msg) return res.status(404).json({ error: 'संदेश नहीं मिला।' });
  const r = safeJson(msg.reactions_json, {});
  r[req.authUser.userId] = req.body.emoji || '❤️';
  dbDriver.run('UPDATE messages SET reactions_json = ? WHERE id = ?', [JSON.stringify(r), req.params.id]);
  res.json({ ok: true, reactions: r });
});

app.post('/chats/:id/seen', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const msgs = dbDriver.all('SELECT id, seen_by_json FROM messages WHERE chat_id = ?', [req.params.id]);
  for (const m of msgs) {
    const arr = safeJson(m.seen_by_json, []);
    if (!arr.includes(meId)) {
      arr.push(meId);
      dbDriver.run('UPDATE messages SET seen_by_json = ? WHERE id = ?', [JSON.stringify(arr), m.id]);
    }
  }
  res.json({ ok: true });
});

// ऑडियो और वीडियो कॉल हिस्ट्री व सिग्नलिंग
app.post('/calls/start', requireAuth, (req, res) => {
  const { calleeIds = [], chatId = '', callType = 'audio', isGroup = false } = req.body || {};
  const callId = uid('call_');
  const now = Date.now();
  dbDriver.run('INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,0)', [callId, req.authUser.userId, JSON.stringify(calleeIds), chatId, callType, isGroup ? 1 : 0, 'ringing', now]);
  for (const cid of calleeIds) {
    emitToUser(cid, 'incoming_call', { callId, callerId: req.authUser.userId, callType, isGroup });
    sendNotification({ userId: cid, actorId: req.authUser.userId, type: 'incoming_call', title: callType === 'video' ? 'इनकमिंग वीडियो कॉल' : 'इनकमिंग ऑडियो कॉल', body: 'आपको कॉल कर रहे हैं...', targetId: callId });
  }
  res.json({ ok: true, callId });
});

app.post('/calls/:id/status', requireAuth, (req, res) => {
  const status = req.body.status || 'ended';
  dbDriver.run('UPDATE calls SET status = ?, ended_at = ? WHERE id = ?', [status, Date.now(), req.params.id]);
  const call = dbDriver.get('SELECT * FROM calls WHERE id = ?', [req.params.id]);
  if (call && status === 'missed') {
    for (const cid of safeJson(call.callee_ids_json, [])) {
      sendNotification({ userId: cid, actorId: call.caller_id, type: 'missed_call', title: 'मिस्ड कॉल', body: 'आपकी एक कॉल छूट गई।', targetId: call.id });
    }
  }
  res.json({ ok: true });
});

app.get('/calls/history', requireAuth, (req, res) => {
  const meId = req.authUser.userId;
  const rows = dbDriver.all('SELECT * FROM calls WHERE caller_id = ? OR callee_ids_json LIKE ? ORDER BY started_at DESC LIMIT 40', [meId, `%${meId}%`]);
  res.json({ ok: true, calls: rows });
});

// कम्युनिटी और ब्रॉडकास्ट चैनल
app.get('/communities', requireAuth, (req, res) => {
  const q = `%${(req.query.q || '').trim().toLowerCase()}%`;
  const rows = dbDriver.all('SELECT * FROM communities WHERE LOWER(name) LIKE ? ORDER BY created_at DESC', [q]);
  res.json({ ok: true, communities: rows });
});

app.post('/communities', requireAuth, (req, res) => {
  const { name, description = '', photoUrl = '', privacy = 'public', isBroadcast = false } = req.body || {};
  const id = uid('com_');
  const now = Date.now();
  dbDriver.run('INSERT INTO communities VALUES (?,?,?,?,?,?,?,?)', [id, name || 'नई कम्युनिटी', description, photoUrl, privacy, isBroadcast ? 1 : 0, req.authUser.userId, now]);
  dbDriver.run("INSERT INTO community_members VALUES (?,?,'admin',?)", [id, req.authUser.userId, now]);
  res.json({ ok: true, community: dbDriver.get('SELECT * FROM communities WHERE id = ?', [id]) });
});

app.post('/communities/:id/join', requireAuth, (req, res) => {
  dbDriver.run("INSERT OR IGNORE INTO community_members VALUES (?,?,'member',?)", [req.params.id, req.authUser.userId, Date.now()]);
  res.json({ ok: true });
});

app.get('/communities/:id/posts', requireAuth, (req, res) => {
  const posts = dbDriver.all('SELECT cp.*, u.username, u.full_name, u.avatar_url FROM community_posts cp JOIN users u ON u.id = cp.author_id WHERE cp.community_id = ? ORDER BY cp.created_at DESC', [req.params.id]);
  res.json({ ok: true, posts });
});

app.post('/communities/:id/posts', requireAuth, (req, res) => {
  dbDriver.run('INSERT INTO community_posts VALUES (?,?,?,?,?,?)', [uid('cp_'), req.params.id, req.authUser.userId, req.body.content || '', req.body.mediaUrl || '', Date.now()]);
  res.json({ ok: true });
});

// लाइव स्ट्रीमिंग
app.get('/live', requireAuth, (req, res) => {
  const streams = dbDriver.all("SELECT l.*, u.username, u.full_name, u.avatar_url FROM live_streams l JOIN users u ON u.id = l.host_id WHERE l.status = 'live' ORDER BY l.started_at DESC");
  res.json({ ok: true, streams });
});

app.post('/live/start', requireAuth, (req, res) => {
  const id = uid('live_');
  dbDriver.run("INSERT INTO live_streams VALUES (?,?,?,'live','[]',0,?,0)", [id, req.authUser.userId, req.body.title || 'लाइव वीडियो', Date.now()]);
  res.json({ ok: true, liveId: id });
});

app.post('/live/:id/end', requireAuth, (req, res) => {
  dbDriver.run("UPDATE live_streams SET status = 'ended', ended_at = ? WHERE id = ? AND host_id = ?", [Date.now(), req.params.id, req.authUser.userId]);
  res.json({ ok: true });
});

app.post('/live/:id/comment', requireAuth, (req, res) => {
  dbDriver.run('INSERT INTO live_comments VALUES (?,?,?,?,?)', [uid('lc_'), req.params.id, req.authUser.userId, req.body.content || '', Date.now()]);
  res.json({ ok: true });
});

app.get('/live/:id/comments', requireAuth, (req, res) => {
  const comments = dbDriver.all('SELECT lc.*, u.username, u.full_name FROM live_comments lc JOIN users u ON u.id = lc.user_id WHERE lc.live_id = ? ORDER BY lc.created_at ASC', [req.params.id]);
  res.json({ ok: true, comments });
});

// ---------------- 6. सर्च, नोटिफिकेशन, क्रिएटर डैशबोर्ड, रिपोर्ट और एडमिन पैनल ----------------
app.get('/search', requireAuth, (req, res) => {
  const q = `%${(req.query.q || '').trim().toLowerCase()}%`;
  const users = dbDriver.all(
    "SELECT id, public_uid, username, full_name, avatar_url, is_verified FROM users WHERE status = 'active' AND (LOWER(full_name) LIKE ? OR LOWER(username) LIKE ? OR LOWER(public_uid) LIKE ?) LIMIT 25",
    [q, q, q]
  );
  const posts = dbDriver.all(
    "SELECT p.*, u.username AS author_username, u.full_name AS author_name FROM posts p JOIN users u ON u.id = p.author_id WHERE p.is_deleted = 0 AND p.visibility = 'public' AND (LOWER(p.caption) LIKE ? OR LOWER(p.hashtags_json) LIKE ?) ORDER BY p.likes_count DESC LIMIT 30",
    [q, q]
  );
  const reels = dbDriver.all(
    'SELECT r.*, u.username AS author_username, u.full_name AS author_name FROM reels r JOIN users u ON u.id = r.author_id WHERE r.is_deleted = 0 AND LOWER(r.caption) LIKE ? ORDER BY r.views_count DESC LIMIT 20',
    [q]
  );
  res.json({ ok: true, users, posts, reels });
});

app.get('/notifications', requireAuth, (req, res) => {
  const list = dbDriver.all('SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 50', [req.authUser.userId]);
  res.json({ ok: true, notifications: list });
});

app.post('/notifications/read-all', requireAuth, (req, res) => {
  dbDriver.run('UPDATE notifications SET is_read = 1 WHERE user_id = ?', [req.authUser.userId]);
  res.json({ ok: true });
});

app.get('/creator/analytics', requireAuth, (req, res) => {
  const id = req.authUser.userId;
  const stats = dbDriver.get(
    `SELECT
      (SELECT COUNT(*) FROM follows WHERE following_id = ? AND status = 'accepted') AS followers,
      (SELECT COALESCE(SUM(likes_count),0) FROM posts WHERE author_id = ? AND is_deleted = 0) AS post_likes,
      (SELECT COALESCE(SUM(comments_count),0) FROM posts WHERE author_id = ? AND is_deleted = 0) AS post_comments,
      (SELECT COALESCE(SUM(views_count),0) FROM reels WHERE author_id = ? AND is_deleted = 0) AS reel_views`,
    [id, id, id, id]
  );
  res.json({ ok: true, stats });
});

app.post('/reports', requireAuth, (req, res) => {
  const { targetType, targetId, reason } = req.body || {};
  if (!reason?.trim()) return res.status(400).json({ error: 'कारण लिखें।' });
  dbDriver.run("INSERT INTO reports VALUES (?,?,?,?,?,'pending',?)", [uid('rep_'), req.authUser.userId, targetType || 'post', targetId || '', reason.trim(), Date.now()]);
  res.json({ ok: true, message: 'रिपोर्ट दर्ज कर ली गई है।' });
});

// सुरक्षित एडमिन पैनल APIs
app.get('/admin/overview', requireAdmin, (req, res) => {
  const summary = {
    totalUsers: dbDriver.get('SELECT COUNT(*) AS c FROM users').c,
    totalPosts: dbDriver.get('SELECT COUNT(*) AS c FROM posts WHERE is_deleted = 0').c,
    pendingReports: dbDriver.get("SELECT COUNT(*) AS c FROM reports WHERE status = 'pending'").c,
  };
  const users = dbDriver.all('SELECT id, public_uid, username, full_name, phone, status, is_verified, role, created_at FROM users ORDER BY created_at DESC LIMIT 100');
  const reports = dbDriver.all('SELECT * FROM reports ORDER BY created_at DESC LIMIT 100');
  const logs = dbDriver.all('SELECT * FROM server_logs ORDER BY id DESC LIMIT 50');
  res.json({ ok: true, summary, users, reports, logs });
});

app.post('/admin/users/:id/action', requireAdmin, (req, res) => {
  const { action } = req.body || {};
  const tid = req.params.id;
  if (action === 'suspend') dbDriver.run("UPDATE users SET status = 'suspended' WHERE id = ?", [tid]);
  else if (action === 'activate') dbDriver.run("UPDATE users SET status = 'active' WHERE id = ?", [tid]);
  else if (action === 'verify') dbDriver.run('UPDATE users SET is_verified = 1 WHERE id = ?', [tid]);
  else if (action === 'unverify') dbDriver.run('UPDATE users SET is_verified = 0 WHERE id = ?', [tid]);
  dbDriver.run('INSERT INTO admin_audit_logs (admin_id, action, target_id, created_at) VALUES (?,?,?,?)', [req.authUser.userId, action, tid, Date.now()]);
  res.json({ ok: true });
});

app.post('/admin/reports/:id/resolve', requireAdmin, (req, res) => {
  const { decision } = req.body || {};
  const rep = dbDriver.get('SELECT * FROM reports WHERE id = ?', [req.params.id]);
  if (!rep) return res.status(404).json({ error: 'रिपोर्ट नहीं मिली।' });
  if (decision === 'delete_content') {
    if (rep.target_type === 'post') dbDriver.run('UPDATE posts SET is_deleted = 1 WHERE id = ?', [rep.target_id]);
    if (rep.target_type === 'reel') dbDriver.run('UPDATE reels SET is_deleted = 1 WHERE id = ?', [rep.target_id]);
    dbDriver.run("UPDATE reports SET status = 'resolved' WHERE id = ?", [rep.id]);
  } else {
    dbDriver.run("UPDATE reports SET status = 'dismissed' WHERE id = ?", [rep.id]);
  }
  res.json({ ok: true });
});

// ---------------- HTTP + WebSocket (रियल-टाइम सिग्नलिंग व ऑनलाइन स्थिति) ----------------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const token = url.searchParams.get('token');
  let userId = null;
  try {
    userId = jwt.verify(token, JWT_SECRET).userId;
  } catch {
    ws.close();
    return;
  }

  if (!socketsByUser.has(userId)) socketsByUser.set(userId, new Set());
  socketsByUser.get(userId).add(ws);
  dbDriver.run('UPDATE users SET is_online = 1, last_seen = ? WHERE id = ?', [Date.now(), userId]);

  ws.on('message', (raw) => {
    const data = safeJson(String(raw), null);
    if (!data || !data.toUserId) return;
    emitToUser(data.toUserId, data.event || 'signal', { fromUserId: userId, ...data.payload });
  });

  ws.on('close', () => {
    const set = socketsByUser.get(userId);
    if (set) {
      set.delete(ws);
      if (set.size === 0) {
        socketsByUser.delete(userId);
        dbDriver.run('UPDATE users SET is_online = 0, last_seen = ? WHERE id = ?', [Date.now(), userId]);
      }
    }
  });
});

app.use((err, req, res, next) => {
  console.error(err);
  try { dbDriver?.run('INSERT INTO server_logs (level, route, message, created_at) VALUES (?,?,?,?)', ['error', req.path, err.message || 'Unknown', Date.now()]); } catch {}
  res.status(500).json({ error: 'सर्वर त्रुटि। कृपया पुनः प्रयास करें।' });
});

initDatabase().then(() => {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n✅ GRAMBOOK सिंगल-फ़ाइल SQLite सर्वर चालू है: http://0.0.0.0:${PORT}`);
    console.log(`🛡️ डिफ़ॉल्ट एडमिन: यूज़रनेम = admin | पासवर्ड = admin123\n`);
  });
}).catch((err) => {
  console.error('सर्वर शुरू नहीं हो सका:', err);
  process.exit(1);
});
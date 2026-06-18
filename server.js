const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs').promises;
const fsSync = require('fs'); // لبعض العمليات التي لا تتوفر promises بسهولة
const { v4: uuidv4 } = require('uuid');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const cookie = require('cookie');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: true } // يمكن تقييده لاحقاً
});

const PORT = 3000;
const DOWNLOAD_DIR = path.join(__dirname, 'downloads');
const MAX_CONCURRENT = 3;        // أقصى عمليات yt-dlp متزامنة
const MAX_QUEUE_SIZE = 500;      // أقصى حجم للطابور
const MAX_DOWNLOADS_PER_HOUR = 20; // تحميلات لكل جلسة في الساعة
const SESSION_MAX_AGE = 24 * 60 * 60 * 1000; // 24 ساعة
const SESSION_CLEANUP_INTERVAL = 60 * 60 * 1000; // تنظيف كل ساعة
const YTDLP_TIMEOUT = 15 * 60 * 1000; // 15 دقيقة

// تأكد من وجود مجلد التحميلات
if (!fsSync.existsSync(DOWNLOAD_DIR)) fsSync.mkdirSync(DOWNLOAD_DIR);

// ==================== بنى البيانات ====================
const sessions = new Map();       // sessionId -> { downloads: Map, socketId: string, lastSeen: number, history: Set }
const downloadQueue = [];        // { sessionId, id, url, format, outputTemplate, resolve, reject }
let activeDownloads = 0;
const activeStreams = new Set();  // id للملفات التي تُرسل حالياً

// ==================== دوال مساعدة ====================
function getSession(sessionId) {
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, {
      downloads: new Map(),
      socketId: null,
      lastSeen: Date.now(),
      history: new Set() // تتبع التحميلات لكل ساعة
    });
  }
  const session = sessions.get(sessionId);
  session.lastSeen = Date.now();
  return session;
}

function cleanupOldSessions() {
  const now = Date.now();
  for (const [id, session] of sessions.entries()) {
    if (now - session.lastSeen > SESSION_MAX_AGE) {
      // حذف ملفات الجلسة إن وجدت
      for (const [downloadId, info] of session.downloads) {
        fs.unlink(info.filePath).catch(() => {});
      }
      sessions.delete(id);
      console.log(`جلسة منتهية الصلاحية: ${id}`);
    }
  }
}
setInterval(cleanupOldSessions, SESSION_CLEANUP_INTERVAL);

// ==================== Express Middleware ====================
app.use(express.static('public'));
app.use(express.json());
app.use(cookieParser());
app.use((req, res, next) => {
  if (!req.cookies[SESSION_COOKIE]) {
    const sessionId = uuidv4();
    res.cookie(SESSION_COOKIE, sessionId, { httpOnly: true, maxAge: SESSION_MAX_AGE });
    req.sessionId = sessionId;
  } else {
    req.sessionId = req.cookies[SESSION_COOKIE];
  }
  getSession(req.sessionId); // تحديث lastSeen
  next();
});

const SESSION_COOKIE = 'session_id';

// Rate Limiting
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  message: { error: 'طلبات كثيرة جداً. حاول لاحقاً.' }
});
app.use('/api/download', limiter);

// ==================== Socket.IO ====================
io.on('connection', (socket) => {
  const rawCookies = socket.handshake.headers.cookie || '';
  const cookies = cookie.parse(rawCookies);
  const sessionId = cookies[SESSION_COOKIE];
  if (sessionId) {
    const session = getSession(sessionId);
    session.socketId = socket.id;
    socket.join(`session-${sessionId}`);
  }

  socket.on('disconnect', () => {
    if (sessionId && sessions.has(sessionId)) {
      sessions.get(sessionId).socketId = null;
    }
  });
});

// ==================== مسارات API ====================

// استعادة التحميلات بعد تحديث الصفحة
app.get('/api/downloads', (req, res) => {
  const session = getSession(req.sessionId);
  const list = [];
  for (const [id, info] of session.downloads) {
    list.push({
      id,
      filename: info.filename,
      fileSize: info.fileSize,
      downloadUrl: `/api/file/${id}`,
      status: info.status, // 'downloading', 'completed'
      percent: info.percent || 0
    });
  }
  res.json(list);
});

// بدء التحميل
app.post('/api/download', async (req, res) => {
  const { url, format } = req.body;
  const sessionId = req.sessionId;
  const session = getSession(sessionId);

  // التحقق من المدخلات
  if (!url || !format) return res.status(400).json({ error: 'الرابط والصيغة مطلوبان' });
  const YOUTUBE_REGEX = /^(https?:\/\/)?(www\.|m\.|music\.)?(youtube\.com|youtu\.be)\//;
  if (!YOUTUBE_REGEX.test(url)) return res.status(400).json({ error: 'رابط يوتيوب غير صالح' });
  if (!['mp3', 'mp4'].includes(format)) return res.status(400).json({ error: 'صيغة غير مدعومة' });

  // حد أقصى للتحميلات لكل جلسة في الساعة
  const now = Date.now();
  // تنظيف السجل القديم
  session.history = new Set([...session.history].filter(ts => now - ts < 3600000));
  if (session.history.size >= MAX_DOWNLOADS_PER_HOUR) {
    return res.status(429).json({ error: 'تجاوزت الحد الأقصى للتحميلات (20 كل ساعة)' });
  }

  // حد أقصى للطابور
  if (downloadQueue.length >= MAX_QUEUE_SIZE) {
    return res.status(503).json({ error: 'الطابور ممتلئ، حاول لاحقاً' });
  }

  const id = uuidv4();
  const outputTemplate = path.join(DOWNLOAD_DIR, `${id}.%(ext)s`);

  // إرسال بداية التحميل للغرفة الخاصة
  io.to(`session-${sessionId}`).emit('download-start', { id, url, format, queuePosition: downloadQueue.length + 1 });

  // إدخال الطلب في الطابور
  const queuePromise = new Promise((resolve, reject) => {
    downloadQueue.push({ sessionId, id, url, format, outputTemplate, resolve, reject });
    processQueue();
  });

  try {
    const fileInfo = await queuePromise;
    session.history.add(Date.now());
    session.downloads.set(id, { ...fileInfo, status: 'completed', percent: 100 });
    io.to(`session-${sessionId}`).emit('download-complete', {
      id,
      filename: fileInfo.filename,
      fileSize: fileInfo.fileSize,
      downloadUrl: `/api/file/${id}`
    });
    res.json({ id, status: 'queued' });
  } catch (err) {
    io.to(`session-${sessionId}`).emit('download-error', { id, error: err.message });
    res.json({ id, status: 'error', error: err.message });
  }
});

// معالجة الطابور
function processQueue() {
  if (activeDownloads >= MAX_CONCURRENT || downloadQueue.length === 0) return;
  const task = downloadQueue.shift();
  activeDownloads++;
  startDownload(task).finally(() => {
    activeDownloads--;
    processQueue();
    // تحديث موقع الطابور للباقي
    for (let i = 0; i < downloadQueue.length; i++) {
      const t = downloadQueue[i];
      io.to(`session-${t.sessionId}`).emit('queue-position', { id: t.id, position: i + 1 });
    }
  });
}

function startDownload({ sessionId, id, url, format, outputTemplate, resolve, reject }) {
  return new Promise((finalResolve) => {
    let args = ['--newline', '--progress-template', '%(progress._percent_str)s'];
    if (format === 'mp3') {
      args.push('-x', '--audio-format', 'mp3', '--audio-quality', '0', '-o', outputTemplate, url);
    } else {
      args.push('-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best', '--merge-output-format', 'mp4', '-o', outputTemplate, url);
    }

    const child = spawn('yt-dlp', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let lastProgress = -1;
    let outputBuffer = '';
    const partialFiles = new Set(); // ملفات أنشأها yt-dlp

    // مهلة زمنية
    const timeout = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error('انتهت مهلة التحميل'));
      finalResolve();
    }, YTDLP_TIMEOUT);

    child.stdout.on('data', (data) => {
      outputBuffer += data.toString();
      // استخراج جميع النسب المئوية (لنأخذ آخر واحدة)
      const matches = outputBuffer.match(/(\d+(?:\.\d+)?)%/g);
      if (matches) {
        const lastMatch = matches[matches.length - 1];
        const percent = parseFloat(lastMatch);
        if (!isNaN(percent) && percent >= 0 && percent <= 100 && percent !== lastProgress) {
          lastProgress = percent;
          io.to(`session-${sessionId}`).emit('download-progress', { id, percent: Math.round(percent) });
          // تحديث الحالة في الجلسة
          const session = sessions.get(sessionId);
          if (session) {
            const info = session.downloads.get(id);
            if (info) info.percent = Math.round(percent);
          }
        }
        outputBuffer = outputBuffer.substring(outputBuffer.lastIndexOf(lastMatch) + lastMatch.length);
      }
    });

    child.stderr.on('data', () => {}); // تجاهل stderr

    child.on('error', (err) => {
      clearTimeout(timeout);
      reject(new Error('فشل تشغيل المحمل'));
      finalResolve();
    });

    child.on('close', async (code) => {
      clearTimeout(timeout);
      if (code !== 0) {
        // حذف الملفات الجزئية التي أنشأها yt-dlp
        for (const f of partialFiles) {
          await fs.unlink(f).catch(() => {});
        }
        reject(new Error(`فشل التحميل (كود ${code})`));
        finalResolve();
        return;
      }

      // البحث عن الملف الناتج
      const files = await fs.readdir(DOWNLOAD_DIR);
      const match = files.filter(f => f.startsWith(id));
      if (match.length === 0) {
        reject(new Error('الملف الناتج غير موجود'));
        finalResolve();
        return;
      }

      const filename = match[0];
      const filePath = path.join(DOWNLOAD_DIR, filename);
      const fileSize = (await fs.stat(filePath)).size;

      const fileInfo = { filename, filePath, fileSize, format, url, createdAt: Date.now(), status: 'completed', percent: 100 };
      resolve(fileInfo);
      finalResolve();
    });
  });
}

// إرسال الملف (مع حماية من التزامن)
app.get('/api/file/:id', async (req, res) => {
  const session = getSession(req.sessionId);
  const info = session.downloads.get(req.params.id);
  if (!info) return res.status(404).send('الملف غير موجود');

  if (activeStreams.has(req.params.id)) {
    return res.status(429).send('الملف قيد التحميل حالياً');
  }

  activeStreams.add(req.params.id);
  const filePath = info.filePath;
  try {
    const stat = await fs.stat(filePath);
    res.writeHead(200, {
      'Content-Type': info.format === 'mp3' ? 'audio/mpeg' : 'video/mp4',
      'Content-Length': stat.size,
      'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(info.filename)}`
    });
    const readStream = fsSync.createReadStream(filePath);
    readStream.pipe(res);
    readStream.on('close', () => {
      activeStreams.delete(req.params.id);
    });
  } catch (err) {
    activeStreams.delete(req.params.id);
    res.status(500).send('خطأ في الملف');
  }
});

// حذف الملف
app.post('/api/delete/:id', async (req, res) => {
  const session = getSession(req.sessionId);
  const info = session.downloads.get(req.params.id);
  if (!info) return res.status(404).json({ error: 'الملف غير موجود' });
  if (activeStreams.has(req.params.id)) return res.status(409).json({ error: 'الملف قيد التحميل' });

  try {
    await fs.unlink(info.filePath);
    session.downloads.delete(req.params.id);
    io.to(`session-${req.sessionId}`).emit('download-deleted', { id: req.params.id });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'فشل حذف الملف' });
  }
});

// تنظيف تلقائي للملفات القديمة والجلسات
setInterval(async () => {
  const now = Date.now();
  for (const [sId, session] of sessions.entries()) {
    for (const [id, info] of session.downloads) {
      if (now - info.createdAt > 30 * 60 * 1000 && !activeStreams.has(id)) {
        try { await fs.unlink(info.filePath); } catch (e) {}
        session.downloads.delete(id);
        io.to(`session-${sId}`).emit('download-deleted', { id });
      }
    }
  }
}, 10 * 60 * 1000);

server.listen(PORT, '0.0.0.0', () => console.log(`الخادم يعمل على http://0.0.0.0:${PORT}`));

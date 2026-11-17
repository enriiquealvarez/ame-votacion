// AME Catering · Votación Taquizas Gourmet
// Servidor Node.js sin dependencias: votos en JSON, resultados en vivo (SSE), cierre automático.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DEADLINE = new Date(process.env.DEADLINE || '2026-11-25T23:59:59-06:00');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const MAX_PER_IP = Number(process.env.MAX_PER_IP || 25);
const FILE = path.join(DATA_DIR, 'votes.json');
const PUB = path.join(__dirname, 'public');

const MENU = [
  { id: 'cerdo', title: 'Guisados de cerdo', icon: '🐖', options: ['Cochito', 'Cochinita pibil', 'Carnitas michoacanas', 'Chicharrón al molcajete', 'Chicharrón en salsa verde', 'Chicharrón en salsa roja', 'Tiras de cerdo con papas', 'Al pastor', 'Chorizo con papas'] },
  { id: 'res', title: 'Guisados de res', icon: '🐄', options: ['Arrachera marinada', 'Sirloin', 'Barbacoa', 'Birria', 'Tiras al molcajete', 'Bistec ranchero'] },
  { id: 'esp', title: 'Especialidades', icon: '🦐', options: ['Huevo con camarón', 'Nopales con camarón', 'Puntas al albañil'] },
  { id: 'agua', title: 'Aguas frescas', icon: '🥤', options: ['Jamaica', 'Horchata', 'Horchata con fresa', 'Limonada', 'Limonada con chía', 'Menta', 'Maracuyá', 'Guanábana', 'Tascalate', 'Pozole', 'Naranjada', 'Sandía', 'Jamaica de la casa, receta secreta', 'Piña', 'Melón'] },
];

// ---------- almacenamiento ----------
fs.mkdirSync(DATA_DIR, { recursive: true });
let votes = {};
try { votes = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch { votes = {}; }
let saving = false, pending = false;
function save() {
  if (saving) { pending = true; return; }
  saving = true;
  const tmp = FILE + '.tmp';
  fs.writeFile(tmp, JSON.stringify(votes), err => {
    if (!err) fs.renameSync(tmp, FILE);
    saving = false;
    if (pending) { pending = false; save(); }
  });
}

const closed = () => Date.now() > DEADLINE.getTime();

function tally() {
  const counts = {};
  for (const c of MENU) counts[c.id] = Object.fromEntries(c.options.map(o => [o, 0]));
  for (const v of Object.values(votes)) for (const c of MENU) {
    const pick = v[c.id];
    if (pick && counts[c.id][pick] !== undefined) counts[c.id][pick]++;
  }
  return { counts, total: Object.keys(votes).length };
}

function publicState(vid) {
  const mine = vid && votes[vid] ? Object.fromEntries(MENU.map(c => [c.id, votes[vid][c.id] || null])) : null;
  return { menu: MENU, deadline: DEADLINE.toISOString(), closed: closed(), now: new Date().toISOString(), mine, ...tally() };
}

// ---------- SSE ----------
const clients = new Set();
function broadcast() {
  const msg = `data: ${JSON.stringify({ ...tally(), closed: closed() })}\n\n`;
  for (const res of clients) res.write(msg);
}
setInterval(() => { for (const res of clients) res.write(': ping\n\n'); }, 25000);
// avisa a todos al momento exacto del cierre
const msToClose = DEADLINE.getTime() - Date.now();
if (msToClose > 0 && msToClose < 2 ** 31 - 1) setTimeout(broadcast, msToClose + 500);

// ---------- utilidades ----------
function cookies(req) {
  return Object.fromEntries((req.headers.cookie || '').split(';').map(s => s.trim().split('=')).filter(p => p[0]));
}
function clientIp(req) {
  return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '';
}
const ipHash = ip => crypto.createHash('sha256').update('ame' + ip).digest('hex').slice(0, 16);
function json(res, code, obj, extra = {}) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', ...extra });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((ok, fail) => {
    let b = '';
    req.on('data', d => { b += d; if (b.length > 1e4) { fail(new Error('too big')); req.destroy(); } });
    req.on('end', () => { try { ok(JSON.parse(b || '{}')); } catch (e) { fail(e); } });
  });
}
const MIME = { '.html': 'text/html; charset=utf-8', '.jpg': 'image/jpeg', '.png': 'image/png', '.css': 'text/css', '.js': 'text/javascript', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };

// ---------- servidor ----------
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  let vid = cookies(req).vid;
  const setCookie = {};
  if (!vid || !/^[a-f0-9]{32}$/.test(vid)) {
    vid = crypto.randomBytes(16).toString('hex');
    const secure = (req.headers['x-forwarded-proto'] || '') === 'https' ? '; Secure' : '';
    setCookie['Set-Cookie'] = `vid=${vid}; Path=/; Max-Age=7776000; HttpOnly; SameSite=Lax${secure}`;
  }

  try {
    if (url.pathname === '/api/state') return json(res, 200, publicState(vid), setCookie);

    if (url.pathname === '/api/stream') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write(`data: ${JSON.stringify({ ...tally(), closed: closed() })}\n\n`);
      clients.add(res);
      req.on('close', () => clients.delete(res));
      return;
    }

    if (url.pathname === '/api/vote' && req.method === 'POST') {
      if (closed()) return json(res, 403, { error: 'La votación ya cerró.' });
      const body = await readBody(req);
      const vote = {};
      for (const c of MENU) {
        if (!c.options.includes(body[c.id])) return json(res, 400, { error: `Elige una opción en ${c.title}.` });
        vote[c.id] = body[c.id];
      }
      const ih = ipHash(clientIp(req));
      const isNew = !votes[vid];
      if (isNew && Object.values(votes).filter(v => v.ip === ih).length >= MAX_PER_IP)
        return json(res, 429, { error: 'Se alcanzó el límite de votos desde esta conexión.' });
      votes[vid] = { ...vote, ip: ih, at: new Date().toISOString() };
      save();
      broadcast();
      return json(res, 200, publicState(vid), setCookie);
    }

    if (url.pathname === '/api/check') {
      return json(res, 200, { ok: !!ADMIN_KEY && url.searchParams.get('key') === ADMIN_KEY });
    }

    if (url.pathname === '/api/export') {
      if (!ADMIN_KEY || url.searchParams.get('key') !== ADMIN_KEY) return json(res, 403, { error: 'no' });
      const rows = [['fecha', ...MENU.map(c => c.title)], ...Object.values(votes).map(v => [v.at, ...MENU.map(c => v[c.id])])];
      res.writeHead(200, { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': 'attachment; filename="votos-ame.csv"' });
      return res.end('﻿' + rows.map(r => r.map(x => `"${String(x).replace(/"/g, '""')}"`).join(',')).join('\n'));
    }

    // estáticos
    let p = url.pathname === '/' ? '/index.html' : url.pathname;
    const file = path.normalize(path.join(PUB, p));
    if (!file.startsWith(PUB) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) { res.writeHead(404); return res.end('No encontrado'); }
    const ext = path.extname(file);
    if (ext === '.html') {
      const proto = req.headers['x-forwarded-proto'] || 'http';
      const base = `${proto}://${req.headers['x-forwarded-host'] || req.headers.host}`;
      const html = fs.readFileSync(file, 'utf8').replaceAll('{{BASE}}', base);
      res.writeHead(200, { 'Content-Type': MIME[ext], 'Cache-Control': 'no-cache', ...setCookie });
      return res.end(html);
    }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': 'public, max-age=86400' });
    fs.createReadStream(file).pipe(res);
  } catch (e) {
    json(res, 400, { error: 'Solicitud inválida' });
  }
}).listen(PORT, () => console.log(`AME votación en :${PORT} · cierre ${DEADLINE.toISOString()}`));

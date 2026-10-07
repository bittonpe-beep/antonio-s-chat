const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'messages.json');
const MAX_PER_ROOM = 200;
const MAX_ROOMS = 100;

// Recent messages per room: { roomName: [{uid, name, text, ts}] }
let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { /* first run */ }

let dirty = false;
setInterval(() => {
  if (!dirty) return;
  dirty = false;
  fs.writeFile(DATA_FILE, JSON.stringify(rooms), () => {});
}, 3000);

const page = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(page);
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
  }
});

const wss = new WebSocketServer({ server, maxPayload: 8192 });
const validRoom = (r) => typeof r === 'string' && /^[a-z0-9-]{1,24}$/.test(r);
const send = (c, o) => { if (c.readyState === 1) c.send(JSON.stringify(o)); };

function announce(room) {
  let n = 0;
  wss.clients.forEach((c) => { if (c.room === room && c.readyState === 1) n++; });
  wss.clients.forEach((c) => { if (c.room === room) send(c, { type: 'online', room, n }); });
}

wss.on('connection', (ws) => {
  ws.room = null;
  ws.stamps = [];
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let d;
    try { d = JSON.parse(raw); } catch (e) { return; }

    if (d.type === 'join' && validRoom(d.room)) {
      const old = ws.room;
      ws.room = d.room;
      send(ws, { type: 'history', room: d.room, msgs: rooms[d.room] || [] });
      announce(d.room);
      if (old && old !== d.room) announce(old);
      return;
    }

    if (d.type === 'msg' && validRoom(d.room) && ws.room === d.room) {
      const text = String(d.text || '').trim().slice(0, 2000);
      const name = String(d.name || '').trim().slice(0, 30);
      const uid = String(d.uid || '').slice(0, 40);
      if (!text || !name || !uid) return;

      // Rate limit: 6 messages per 5 seconds per connection
      const now = Date.now();
      ws.stamps = ws.stamps.filter((t) => now - t < 5000);
      if (ws.stamps.length >= 6) return send(ws, { type: 'error', message: 'Slow down a little.' });
      ws.stamps.push(now);

      if (!rooms[d.room]) {
        if (Object.keys(rooms).length >= MAX_ROOMS) return send(ws, { type: 'error', message: 'Too many rooms.' });
        rooms[d.room] = [];
      }
      const msg = { uid, name, text, ts: now };
      rooms[d.room].push(msg);
      if (rooms[d.room].length > MAX_PER_ROOM) rooms[d.room].shift();
      dirty = true;
      wss.clients.forEach((c) => { if (c.room === d.room) send(c, { type: 'msg', room: d.room, msg }); });
    }
  });

  ws.on('close', () => { if (ws.room) announce(ws.room); });
});

// Keep connections alive through proxies and drop dead ones
setInterval(() => {
  wss.clients.forEach((c) => {
    if (!c.isAlive) return c.terminate();
    c.isAlive = false;
    c.ping();
  });
}, 30000);

server.listen(PORT, () => console.log('Harbor Chat listening on ' + PORT));

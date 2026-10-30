'use strict';
/* Tank Nexus co-op v3: Node owns room sessions, player movement, position ACKs,
   cooldown validation and packet routing. The browser room owner is currently
   authoritative for legacy enemy/world simulation (documented limitation). */
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || '0.0.0.0';
const WORLD_WIDTH = 9000, WORLD_HEIGHT = 6000, SPEED = 330, STEP = 1 / 30;
const ROOM_MAX = 4;
const rooms = new Map();
const clients = new Map();
const COLORS = ['#5b9fd9', '#d94a4a', '#5aaf62', '#d9a84a'];
const now = () => Date.now();
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
const finite = n => typeof n === 'number' && Number.isFinite(n);
const send = (ws, message) => {
  if (ws?.readyState !== WebSocket.OPEN || ws.bufferedAmount > 768 * 1024) return;
  try { ws.send(JSON.stringify(message)); } catch (_) {}
};
const broadcast = (room, message) => {
  for (const p of room.players.values()) send(p.ws, message);
};
const safePlayer = p => ({
  id: p.id, name: p.name, slot: p.slot, color: p.color,
  x: p.x, y: p.y, angle: p.angle, turretAngle: p.turretAngle,
  hp: p.hp, ack: p.ack
});
const publicRoom = room => ({
  code: room.code, ownerId: room.ownerId, mode: room.mode, started: room.started,
  players: Array.from(room.players.values(), safePlayer)
});
const updateRoom = room => broadcast(room, { type: 'room', room: publicRoom(room) });
function generateCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (let t = 0; t < 1000; t++) {
    let key = '';
    for (let j = 0; j < 5; j++) key += alphabet[crypto.randomInt(alphabet.length)];
    if (!rooms.has(key)) return key;
  }
  throw Error('Room codes unavailable');
}
function leave(ws) {
  const client = clients.get(ws);
  if (!client?.room) return;
  const room = client.room;
  client.room = null;
  const p = room.players.get(client.id);
  if (p) room.players.delete(p.id);
  if (!room.players.size) { rooms.delete(room.code); return; }
  if (room.ownerId === client.id) {
    room.ownerId = room.players.values().next().value.id;
    // Legacy world simulation cannot migrate safely mid-game.
    if (room.started) {
      room.started = false;
      broadcast(room, { type: 'ended', message: 'Room owner disconnected. Return to lobby.' });
    }
  }
  updateRoom(room);
}
function spawn(slot, mode) {
  return { x: (mode === 'outpost' ? 4200 : 4500) + slot * 85, y: 3000 + (slot % 2) * 65 };
}
function createPlayer(ws, client, room, name) {
  const slots = new Set([...room.players.values()].map(p => p.slot));
  let slot = 0;
  while (slots.has(slot)) slot++;
  const pos = spawn(slot, room.mode);
  const p = { id: client.id, ws, name: String(name || 'Commander').trim().slice(0, 12) || 'Commander',
    slot, color: COLORS[slot], ...pos, angle: 0, turretAngle: 0, hp: 100,
    ack: 0, lastSeq: 0, inputQueue: [], lastFire: 0, lastInputTime: now(), credits: 10, creditTime: now() };
  room.players.set(client.id, p);
  client.room = room;
  return p;
}
function error(ws, text) { send(ws, { type: 'error', message: text }); }
function handleInput(room, p, msg) {
  if (!room.started || !Number.isSafeInteger(msg.seq) || msg.seq <= p.lastSeq) return;
  // Receive/validate input separately from movement. Only the server clock moves tanks.
  const t = now();
  p.credits = Math.min(12, p.credits + Math.max(0, t - p.creditTime) * 0.037);
  p.creditTime = t;
  if (p.credits < 1 || msg.seq > p.ack + 75) return;
  p.credits--;
  p.lastSeq = msg.seq;
  let mx = finite(msg.mx) ? clamp(msg.mx, -1, 1) : 0;
  let my = finite(msg.my) ? clamp(msg.my, -1, 1) : 0;
  const length = Math.hypot(mx, my);
  if (length > 1) { mx /= length; my /= length; }
  const input = { seq: msg.seq, mx, my,
    angle: finite(msg.angle) ? clamp(msg.angle, -Math.PI * 4, Math.PI * 4) : null,
    fire: msg.fire === true };
  // Bound memory and do not accumulate stale commands when a tab is backgrounded.
  if (p.inputQueue.length >= 12) p.inputQueue.shift();
  p.inputQueue.push(input);
  p.lastInputTime = t;
}
function advancePlayer(room, p, input) {
  if (input.seq <= p.ack) return;
  p.ack = input.seq;
  let { mx, my } = input;
  if (p.hp <= 0) { mx = 0; my = 0; }
  if (Math.hypot(mx, my) > 0.04) {
    p.x = clamp(p.x + mx * SPEED * STEP, 50, WORLD_WIDTH - 50);
    p.y = clamp(p.y + my * SPEED * STEP, 50, WORLD_HEIGHT - 50);
    p.angle = Math.atan2(my, mx);
  }
  if (input.angle !== null) p.turretAngle = input.angle;
  const t = now();
  if (input.fire && p.hp > 0 && t - p.lastFire >= 180) {
    p.lastFire = t;
    const owner = room.players.get(room.ownerId);
    if (owner && owner.id !== p.id) send(owner.ws, {
      type: 'remote_fire', id: p.id, x: p.x, y: p.y, angle: p.turretAngle
    });
  }
}
const publicDir = path.resolve(__dirname);
const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, players: [...rooms.values()].reduce((a, r) => a + r.players.size, 0) }));
    return;
  }
  if (pathname !== '/' && pathname !== '/index.html') { res.writeHead(404); res.end('Not found'); return; }
  fs.readFile(path.join(publicDir, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); res.end('Game unavailable'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 512 * 1024, perMessageDeflate: false });
wss.on('connection', ws => {
  const client = { id: crypto.randomUUID(), room: null, lastSeen: now(), alive: true };
  clients.set(ws, client);
  send(ws, { type: 'hello', message: 'Tank Nexus Node.js server', id: client.id });
  ws.on('pong', () => { client.alive = true; });
  ws.on('message', (raw, isBinary) => {
    if (isBinary || raw.length > 512 * 1024) return;
    client.lastSeen = now();
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== 'object') return;
    if (msg.type === 'probe' && finite(msg.t)) { send(ws,{type:'probe_reply',t:msg.t}); return; }
    const room = client.room;
    if (msg.type === 'create') {
      if (room) return error(ws, 'Leave your current room first.');
      const code = generateCode();
      const r = { code, ownerId: client.id, mode: msg.mode === 'outpost' ? 'outpost' : 'simple',
        players: new Map(), started: false, world: null, lastWorldTime: 0 };
      rooms.set(code, r);
      const p = createPlayer(ws, client, r, msg.name);
      send(ws, { type: 'created', player: safePlayer(p), room: publicRoom(r) });
      return;
    }
    if (msg.type === 'join') {
      if (room) return error(ws, 'Leave your current room first.');
      const code = String(msg.roomCode || '').trim().toUpperCase();
      const r = rooms.get(code);
      if (!r) return error(ws, 'Room not found.');
      if (r.started) return error(ws, 'Match in progress. Wait for a new room.');
      if (r.players.size >= ROOM_MAX) return error(ws, 'Room is full.');
      const p = createPlayer(ws, client, r, msg.name);
      send(ws, { type: 'joined', player: safePlayer(p), room: publicRoom(r) });
      updateRoom(r);
      return;
    }
    if (msg.type === 'leave') { leave(ws); send(ws, { type: 'left' }); return; }
    if (!room) return;
    const p = room.players.get(client.id);
    if (!p) return;
    if (msg.type === 'start') {
      if (room.ownerId !== client.id) return error(ws, 'Only the room owner can start.');
      if (room.players.size < 2) return error(ws, 'At least 2 players are required.');
      if (room.started) return;
      room.started = true;
      for (const q of room.players.values()) {
        const pos = spawn(q.slot, room.mode);
        Object.assign(q, pos, { angle: 0, turretAngle: 0, hp: 100, ack: 0, lastSeq: 0, inputQueue: [], lastFire: 0, credits: 10, creditTime: now() });
      }
      broadcast(room, { type: 'started', room: publicRoom(room) });
      return;
    }
    if (msg.type === 'input') { handleInput(room, p, msg); return; }
    if (msg.type === 'world') {
      if (!room.started || client.id !== room.ownerId) return;
      const t = now();
      if (t - room.lastWorldTime < 90) return;
      room.lastWorldTime = t;
      // Only the original room owner may publish the legacy world snapshot.
      if (msg.state && typeof msg.state === 'object') {
        room.world = msg.state;
        for (const q of room.players.values()) if (q.id !== client.id) send(q.ws, { type: 'world', state: room.world });
      }
      return;
    }
    if (msg.type === 'host_health') {
      if (client.id !== room.ownerId || !room.started) return;
      if (finite(msg.hp)) p.hp = clamp(msg.hp, 0, 1000);
      return;
    }
    if (msg.type === 'remote_damage') {
      if (client.id !== room.ownerId || !room.started) return;
      const target = room.players.get(msg.target);
      if (target && finite(msg.amount) && msg.amount > 0 && msg.amount <= 100) {
        target.hp = Math.max(0, target.hp - msg.amount);
        if (target.hp === 0) target.respawnAt = now() + 4000;
      }
    }
  });
  ws.on('close', () => { leave(ws); clients.delete(ws); });
  ws.on('error', () => {});
});
let tickNumber = 0;
const gameTicker = setInterval(() => {
  tickNumber++;
  for (const r of rooms.values()) {
    if (!r.started) continue;
    for (const p of r.players.values()) {
      // Deterministic 30Hz movement, modest catch-up for packets arriving together.
      const limit = p.inputQueue.length > 4 ? 2 : 1;
      for (let i = 0; i < limit && p.inputQueue.length; i++) {
        const command = p.inputQueue.shift();
        if (command) advancePlayer(r, p, command);
      }
      if (p.hp <= 0 && p.respawnAt && now() >= p.respawnAt) {
        Object.assign(p, spawn(p.slot, r.mode), {hp: 100, respawnAt: 0});
      }
    }
    if (tickNumber % 2 === 0) broadcast(r, {
      type: 'players_state', players: [...r.players.values()].map(safePlayer), serverTime: now()
    });
  }
}, 1000 / 30);
const heartbeat = setInterval(() => {
  for (const [ws, c] of clients) {
    if (!c.alive) { ws.terminate(); continue; }
    c.alive = false;
    try { ws.ping(); } catch { ws.terminate(); }
  }
}, 15000);
server.on('close', () => { clearInterval(heartbeat); clearInterval(gameTicker); });
server.listen(PORT, HOST, () => console.log(`Tank Nexus: http://localhost:${PORT} | WebSocket: ws://localhost:${PORT}/ws`));

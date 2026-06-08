/**
 * VOID CHAT — Rooms Edition
 * Password-protected rooms, chats dissolve when empty
 * Pure Node.js, zero dependencies
 */

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

// ── Name generation ──────────────────────────────────────────
const ADJECTIVES = [
  'Silent','Ghost','Neon','Void','Ash','Crimson','Hollow','Flux',
  'Pale','Shadow','Wired','Static','Broken','Surge','Phantom','Bleak',
  'Drift','Ember','Null','Lost','Faded','Binary','Dark','Glitch',
  'Storm','Toxic','Wild','Rogue','Frozen','Shattered'
];
const NOUNS = [
  'Raven','Pulse','Echo','Cipher','Specter','Wraith','Nexus','Signal',
  'Vector','Node','Trace','Orbit','Shard','Prism','Veil','Rift',
  'Axon','Pixel','Dusk','Crow','Daemon','Moth','Flux','Byte',
  'Shade','Haze','Void','Fog','Static','Wire'
];

function generateName() {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const noun = NOUNS[Math.floor(Math.random() * NOUNS.length)];
  const num = Math.floor(Math.random() * 900) + 100;
  return `${adj}${noun}${num}`;
}

// ── Rooms state ──────────────────────────────────────────────
// rooms: Map<roomId, { name, passwordHash, messages[], sockets: Set }>
const rooms = new Map();
// clients: Map<socket, { name, roomId }>
const clients = new Map();

function hashPassword(pw) {
  return crypto.createHash('sha256').update(pw).digest('hex');
}

function broadcastToRoom(roomId, data, excludeSocket = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const str = JSON.stringify(data);
  for (const sock of room.sockets) {
    if (sock !== excludeSocket && sock._wsReady) {
      try { sock.write(buildFrame(str)); } catch(e) {}
    }
  }
}

function dissolveRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  // Notify remaining sockets (shouldn't be any, but just in case)
  for (const sock of room.sockets) {
    try {
      sock.write(buildFrame(JSON.stringify({ type: 'room_dissolved' })));
      sock.destroy();
    } catch(e) {}
  }
  rooms.delete(roomId);
  console.log(`[room] "${roomId}" dissolved`);
}

function leaveRoom(socket) {
  const client = clients.get(socket);
  if (!client || !client.roomId) return;
  const { name, roomId } = client;
  const room = rooms.get(roomId);
  if (!room) return;

  room.sockets.delete(socket);
  client.roomId = null;

  if (room.sockets.size === 0) {
    // Last person left — dissolve everything
    dissolveRoom(roomId);
  } else {
    // Notify others
    const leaveMsg = { type: 'system', text: `${name} dissolved into the void`, ts: Date.now() };
    room.messages.push(leaveMsg);
    broadcastToRoom(roomId, leaveMsg);
    broadcastToRoom(roomId, { type: 'online', count: room.sockets.size });
  }
}

// ── WebSocket frame parser/builder ───────────────────────────
function parseFrame(buf) {
  if (buf.length < 2) return null;
  const opcode = buf[0] & 0x0f;
  const masked = (buf[1] & 0x80) !== 0;
  let payloadLen = buf[1] & 0x7f;
  let offset = 2;
  if (payloadLen === 126) {
    if (buf.length < 4) return null;
    payloadLen = buf.readUInt16BE(2); offset = 4;
  } else if (payloadLen === 127) {
    if (buf.length < 10) return null;
    payloadLen = Number(buf.readBigUInt64BE(2)); offset = 10;
  }
  const maskOffset = offset;
  if (masked) offset += 4;
  if (buf.length < offset + payloadLen) return null;
  let payload = buf.slice(offset, offset + payloadLen);
  if (masked) {
    const mask = buf.slice(maskOffset, maskOffset + 4);
    payload = Buffer.from(payload);
    for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
  }
  return { opcode, payload, totalLength: offset + payloadLen };
}

function buildFrame(data, opcode = 0x01) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(data, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode; header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode; header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode; header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

// ── HTTP ─────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  if (req.url === '/' || req.url === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(404); res.end('Not found'); }
      else { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(data); }
    });
  } else { res.writeHead(404); res.end('Not found'); }
});

// ── WebSocket upgrade ─────────────────────────────────────────
server.on('upgrade', (req, socket) => {
  if ((req.headers['upgrade'] || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }

  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  socket._wsReady = true;
  socket.setTimeout(0);
  socket.setNoDelay(true);
  socket.setKeepAlive(true, 30000);

  const name = generateName();
  clients.set(socket, { name, roomId: null });
  console.log(`[+] ${name} connected`);

  // Send welcome — just their name, no room yet
  try {
    socket.write(buildFrame(JSON.stringify({ type: 'welcome', name })));
  } catch(e) {}

  let buffer = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 2) {
      const frame = parseFrame(buffer);
      if (!frame) break;
      buffer = buffer.slice(frame.totalLength);
      const { opcode, payload } = frame;

      if (opcode === 0x09) { try { socket.write(buildFrame(payload, 0x0a)); } catch(e) {} continue; }
      if (opcode === 0x08) { socket.destroy(); break; }
      if (opcode !== 0x01) continue;

      let parsed;
      try { parsed = JSON.parse(payload.toString('utf8')); } catch { continue; }

      const client = clients.get(socket);
      if (!client) continue;

      // ── CREATE ROOM ──
      if (parsed.type === 'create_room') {
        const roomName = (parsed.roomName || '').trim().slice(0, 40);
        const password = (parsed.password || '').trim();
        if (!roomName || !password) {
          socket.write(buildFrame(JSON.stringify({ type: 'error', text: 'Room name and password required.' })));
          continue;
        }
        // Check name not taken
        for (const [, r] of rooms) {
          if (r.name.toLowerCase() === roomName.toLowerCase()) {
            socket.write(buildFrame(JSON.stringify({ type: 'error', text: 'A room with that name already exists.' })));
            continue;
          }
        }
        const roomId = crypto.randomUUID();
        rooms.set(roomId, {
          name: roomName,
          passwordHash: hashPassword(password),
          messages: [],
          sockets: new Set()
        });
        console.log(`[room] created: "${roomName}" (${roomId})`);
        // Join the creator immediately
        joinRoom(socket, roomId);
        continue;
      }

      // ── JOIN ROOM ──
      if (parsed.type === 'join_room') {
        const roomName = (parsed.roomName || '').trim();
        const password = (parsed.password || '').trim();
        // Find room by name
        let foundId = null;
        for (const [rid, r] of rooms) {
          if (r.name.toLowerCase() === roomName.toLowerCase()) { foundId = rid; break; }
        }
        if (!foundId) {
          socket.write(buildFrame(JSON.stringify({ type: 'error', text: 'Room not found.' })));
          continue;
        }
        const room = rooms.get(foundId);
        if (room.passwordHash !== hashPassword(password)) {
          socket.write(buildFrame(JSON.stringify({ type: 'error', text: 'Wrong password.' })));
          continue;
        }
        joinRoom(socket, foundId);
        continue;
      }

      // ── LEAVE ROOM ──
      if (parsed.type === 'leave_room') {
        leaveRoom(socket);
        socket.write(buildFrame(JSON.stringify({ type: 'left_room' })));
        continue;
      }

      // ── MESSAGE ──
      if (parsed.type === 'message') {
        if (!client.roomId) continue;
        const text = (parsed.text || '').trim().slice(0, 500);
        if (!text) continue;
        const room = rooms.get(client.roomId);
        if (!room) continue;
        const msg = { type: 'message', name: client.name, text, ts: Date.now(), id: crypto.randomUUID() };
        room.messages.push(msg);
        if (room.messages.length > 200) room.messages.shift();
        broadcastToRoom(client.roomId, msg);
        console.log(`[${room.name}] ${client.name}: ${text.slice(0,60)}`);
        continue;
      }

      // ── LIST ROOMS ──
      if (parsed.type === 'list_rooms') {
        const list = [];
        for (const [rid, r] of rooms) {
          list.push({ id: rid, name: r.name, online: r.sockets.size });
        }
        socket.write(buildFrame(JSON.stringify({ type: 'room_list', rooms: list })));
        continue;
      }
    }
  });

  socket.on('close', () => {
    const client = clients.get(socket);
    if (client) {
      leaveRoom(socket);
      clients.delete(socket);
      console.log(`[-] ${client.name} disconnected`);
    }
  });
  socket.on('error', () => socket.destroy());
});

function joinRoom(socket, roomId) {
  const client = clients.get(socket);
  if (!client) return;
  // Leave current room first
  if (client.roomId) leaveRoom(socket);

  const room = rooms.get(roomId);
  if (!room) return;

  room.sockets.add(socket);
  client.roomId = roomId;

  // Send room joined + history
  try {
    socket.write(buildFrame(JSON.stringify({
      type: 'joined_room',
      roomId,
      roomName: room.name,
      history: room.messages,
      online: room.sockets.size
    })));
  } catch(e) {}

  // Announce join
  const joinMsg = { type: 'system', text: `${client.name} entered the void`, ts: Date.now() };
  room.messages.push(joinMsg);
  broadcastToRoom(roomId, joinMsg, socket);
  broadcastToRoom(roomId, { type: 'online', count: room.sockets.size });
  console.log(`[room] ${client.name} joined "${room.name}" (${room.sockets.size} online)`);
}

// Heartbeat
setInterval(() => {
  for (const [sock] of clients) {
    if (sock._wsReady) try { sock.write(buildFrame(Buffer.alloc(0), 0x09)); } catch(e) {}
  }
}, 25000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🌌 VOID CHAT [ROOMS] running on http://localhost:${PORT}\n`);
});

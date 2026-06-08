/**
 * VOID CHAT — Pure Node.js server
 * WebSocket (RFC 6455) + HTTP static file server
 * Zero external dependencies
 */

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// Railway injects PORT automatically — always use it
const PORT = process.env.PORT || 3000;

const MAX_HISTORY = 200;

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

// ── State ────────────────────────────────────────────────────
const clients = new Map(); // socket -> { name, id }
const messageHistory = [];

function addToHistory(msg) {
  messageHistory.push(msg);
  if (messageHistory.length > MAX_HISTORY) messageHistory.shift();
}

function broadcast(data, excludeSocket = null) {
  const str = JSON.stringify(data);
  for (const [sock] of clients) {
    if (sock !== excludeSocket && sock._wsReady) {
      try { sock.write(buildFrame(str)); } catch (e) {}
    }
  }
}

function broadcastAll(data) {
  broadcast(data, null);
}

// ── WebSocket handshake (RFC 6455) ────────────────────────────
function wsHandshake(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return false; }

  const accept = crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n` +
    '\r\n'
  );
  return true;
}

// ── WebSocket frame parser ────────────────────────────────────
function parseFrame(buf) {
  if (buf.length < 2) return null;
  const opcode = buf[0] & 0x0f;
  const masked = (buf[1] & 0x80) !== 0;
  let payloadLen = buf[1] & 0x7f;
  let offset = 2;

  if (payloadLen === 126) {
    if (buf.length < 4) return null;
    payloadLen = buf.readUInt16BE(2);
    offset = 4;
  } else if (payloadLen === 127) {
    if (buf.length < 10) return null;
    payloadLen = Number(buf.readBigUInt64BE(2));
    offset = 10;
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

// ── WebSocket frame builder ───────────────────────────────────
function buildFrame(data, opcode = 0x01) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(data, 'utf8');
  const len = payload.length;
  let header;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  return Buffer.concat([header, payload]);
}

// ── HTTP static file server ───────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.ico':  'image/x-icon',
};

function serveFile(res, filePath) {
  const ext = path.extname(filePath);
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404); res.end('Not found');
    } else {
      res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
      res.end(data);
    }
  });
}

// ── HTTP server ───────────────────────────────────────────────
const server = http.createServer((req, res) => {
  // Health check for Railway
  if (req.url === '/health') {
    res.writeHead(200); res.end('ok');
    return;
  }
  if (req.url === '/' || req.url === '/index.html') {
    serveFile(res, path.join(__dirname, 'index.html'));
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

// ── WebSocket upgrade ─────────────────────────────────────────
server.on('upgrade', (req, socket, head) => {
  const upgradeHeader = (req.headers['upgrade'] || '').toLowerCase();
  if (upgradeHeader !== 'websocket') {
    socket.destroy();
    return;
  }

  const ok = wsHandshake(req, socket);
  if (!ok) return;

  socket._wsReady = true;
  socket.setTimeout(0);
  socket.setNoDelay(true);
  socket.setKeepAlive(true, 30000);

  const name = generateName();
  const id = crypto.randomUUID();
  clients.set(socket, { name, id });

  console.log(`[+] ${name} connected (${clients.size} online)`);

  // Send init: their name + full history + online count
  try {
    socket.write(buildFrame(JSON.stringify({
      type: 'init',
      name,
      history: messageHistory,
      online: clients.size
    })));
  } catch(e) {}

  // Announce join to everyone else
  const joinMsg = {
    type: 'system',
    text: `${name} entered the void`,
    ts: Date.now()
  };
  addToHistory(joinMsg);
  broadcast(joinMsg, socket); // exclude self — they get it via history on reconnect
  broadcastOnlineCount();

  // Frame buffer
  let buffer = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 2) {
      const frame = parseFrame(buffer);
      if (!frame) break;
      buffer = buffer.slice(frame.totalLength);

      const { opcode, payload } = frame;

      if (opcode === 0x09) { // Ping → Pong
        try { socket.write(buildFrame(payload, 0x0a)); } catch(e) {}
        continue;
      }

      if (opcode === 0x08) { // Close
        socket.destroy();
        break;
      }

      if (opcode === 0x01) { // Text
        let parsed;
        try { parsed = JSON.parse(payload.toString('utf8')); } catch { continue; }

        if (parsed.type === 'message' && typeof parsed.text === 'string') {
          const text = parsed.text.trim().slice(0, 500);
          if (!text) continue;

          const msg = {
            type: 'message',
            name,
            text,
            ts: Date.now(),
            id: crypto.randomUUID()
          };
          addToHistory(msg);
          // Send to ALL including sender so they see their own message
          broadcastAll(msg);
          console.log(`[msg] ${name}: ${text.slice(0, 60)}`);
        }
      }
    }
  });

  socket.on('close', () => handleDisconnect(socket, name));
  socket.on('error', () => handleDisconnect(socket, name));
});

function handleDisconnect(socket, name) {
  if (!clients.has(socket)) return;
  clients.delete(socket);
  socket._wsReady = false;
  console.log(`[-] ${name} left (${clients.size} online)`);

  const leaveMsg = {
    type: 'system',
    text: `${name} dissolved into the void`,
    ts: Date.now()
  };
  addToHistory(leaveMsg);
  broadcastAll(leaveMsg);
  broadcastOnlineCount();
}

function broadcastOnlineCount() {
  broadcastAll({ type: 'online', count: clients.size });
}

// Heartbeat ping every 25s
setInterval(() => {
  for (const [sock] of clients) {
    if (sock._wsReady) {
      try { sock.write(buildFrame(Buffer.alloc(0), 0x09)); } catch(e) {}
    }
  }
}, 25000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🌌 VOID CHAT running on http://localhost:${PORT}\n`);
});

/**
 * VOID CHAT — Pure Node.js server
 * WebSocket (RFC 6455) + HTTP static file server
 * Zero external dependencies
 */

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

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
const clients = new Map(); // ws -> { name, id }
const messageHistory = [];

function addToHistory(msg) {
  messageHistory.push(msg);
  if (messageHistory.length > MAX_HISTORY) messageHistory.shift();
}

function broadcast(data, excludeWs = null) {
  const str = JSON.stringify(data);
  for (const [ws] of clients) {
    if (ws !== excludeWs && ws.readyState === 1) {
      ws.send(str);
    }
  }
}

function broadcastAll(data) {
  broadcast(data, null);
}

// ── WebSocket handshake (RFC 6455) ────────────────────────────
function wsHandshake(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

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
}

// ── WebSocket frame parser ────────────────────────────────────
function parseFrame(buf) {
  if (buf.length < 2) return null;
  const fin = (buf[0] & 0x80) !== 0;
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
    for (let i = 0; i < payload.length; i++) {
      payload[i] ^= mask[i % 4];
    }
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

// Attach send helper to raw socket
function attachWsSend(socket) {
  socket.send = function(data) {
    try {
      socket.write(buildFrame(data));
    } catch (e) {}
  };
  socket.readyState = 1;
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
  if (req.url === '/' || req.url === '/index.html') {
    serveFile(res, path.join(__dirname, 'index.html'));
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

// ── WebSocket upgrade ─────────────────────────────────────────
server.on('upgrade', (req, socket) => {
  if (req.headers['upgrade']?.toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }

  wsHandshake(req, socket);
  attachWsSend(socket);

  const name = generateName();
  const id = crypto.randomUUID();
  clients.set(socket, { name, id });

  console.log(`[+] ${name} connected (${clients.size} online)`);

  // Send init packet: name + history + online count
  socket.send(JSON.stringify({
    type: 'init',
    name,
    history: messageHistory,
    online: clients.size
  }));

  // Broadcast join notification
  const joinMsg = {
    type: 'system',
    text: `${name} entered the void`,
    ts: Date.now()
  };
  addToHistory(joinMsg);
  broadcast(joinMsg, socket);
  broadcastOnlineCount();

  // Frame buffer for partial frames
  let buffer = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 2) {
      const frame = parseFrame(buffer);
      if (!frame) break;
      buffer = buffer.slice(frame.totalLength);

      const { opcode, payload } = frame;

      // Ping → Pong
      if (opcode === 0x09) {
        socket.write(buildFrame(payload, 0x0a));
        continue;
      }

      // Close
      if (opcode === 0x08) {
        socket.destroy();
        break;
      }

      // Text
      if (opcode === 0x01) {
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
  socket.readyState = 3;
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

// Heartbeat — send ping every 25s to keep connections alive
setInterval(() => {
  for (const [ws] of clients) {
    try { ws.write(buildFrame(Buffer.alloc(0), 0x09)); } catch {}
  }
}, 25000);

server.listen(PORT, () => {
  console.log(`\n🌌 VOID CHAT running on http://localhost:${PORT}\n`);
});

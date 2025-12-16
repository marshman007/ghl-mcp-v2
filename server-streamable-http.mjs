import http from 'node:http';
import { Buffer } from 'node:buffer';
import { spawn } from 'node:child_process';

const PORT = Number(process.env.PORT || 8080);
const HOST = "0.0.0.0";
const ORIGIN_ALLOWLIST = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map(origin => origin.trim())
  .filter(Boolean);

const child = spawn('node', ['build/index.js'], {
  stdio: ['pipe', 'pipe', 'inherit'],
});

child.on('error', (err) => {
  console.error('Failed to start MCP server:', err);
  process.exit(1);
});

child.on('exit', (code, signal) => {
  console.error(`MCP server exited with code ${code} signal ${signal}`);
  process.exit(code ?? 1);
});

child.stdin.on('error', (err) => {
  console.error('Error writing to MCP server stdin:', err);
});

const pendingResponses = new Map();
let stdoutBuffer = Buffer.alloc(0);

function flushStdoutBuffer() {
  while (true) {
    const newlineIndex = stdoutBuffer.indexOf('\n');
    if (newlineIndex === -1) {
      return;
    }

    const line = stdoutBuffer
      .slice(0, newlineIndex)
      .toString('utf8')
      .replace(/\r$/, '');
    stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);

    if (!line) {
      continue;
    }

    let payload;
    try {
      payload = JSON.parse(line);
    } catch (err) {
      console.error('Invalid JSON from MCP server:', err);
      continue;
    }

    if (!Object.prototype.hasOwnProperty.call(payload, 'id')) {
      continue;
    }

    const key = JSON.stringify(payload.id);
    const resolver = pendingResponses.get(key);
    if (resolver) {
      pendingResponses.delete(key);
      resolver.resolve(payload);
    }
  }
}

child.stdout.on('data', (chunk) => {
  console.error("[RAW STDOUT]", JSON.stringify(chunk.toString("utf8")));
  stdoutBuffer = Buffer.concat([stdoutBuffer, chunk]);
  flushStdoutBuffer();
});

child.stdout.on('error', (err) => {
  console.error('Error reading MCP server stdout:', err);
});

function sendJsonRpc(message) {
  return new Promise((resolve, reject) => {
    const payloadBuffer = Buffer.from(`${JSON.stringify(message)}\n`, 'utf8');

    const hasId = Object.prototype.hasOwnProperty.call(message, 'id');
    let timeoutId;
    let key;

    if (hasId) {
      key = JSON.stringify(message.id);
      timeoutId = setTimeout(() => {
        const entry = pendingResponses.get(key);
        if (entry) {
          pendingResponses.delete(key);
          entry.reject(new Error('Timed out waiting for MCP response'));
        }
      }, 30000);
      pendingResponses.set(key, {
        resolve: (payload) => {
          clearTimeout(timeoutId);
          resolve(payload);
        },
        reject: (error) => {
          clearTimeout(timeoutId);
          reject(error);
        }
      });
    }

    try {
      child.stdin.write(payloadBuffer);
    } catch (err) {
      if (hasId && key) {
        const entry = pendingResponses.get(key);
        if (entry) {
          pendingResponses.delete(key);
          entry.reject(err);
        }
      } else {
        reject(err);
      }
      return;
    }

    if (!hasId) {
      resolve(null);
    }
  });
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function originAllowed(origin) {
  if (!origin || ORIGIN_ALLOWLIST.length === 0) {
    return true;
  }
  return ORIGIN_ALLOWLIST.includes(origin);
}

const server = http.createServer(async (req, res) => {
  if (!originAllowed(req.headers.origin)) {
    res.writeHead(403, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'Forbidden' }));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/mcp') {
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
    return;
  }

  let bodyText;
  try {
    bodyText = await readRequestBody(req);
  } catch (err) {
    console.error('Failed to read request body:', err);
    res.writeHead(500, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'Failed to read request body' }));
    return;
  }

  let payload;
  try {
    payload = JSON.parse(bodyText);
  } catch {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid JSON' }));
    return;
  }

  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'JSON-RPC payload must be an object' }));
    return;
  }

  const hasId = payload != null && Object.prototype.hasOwnProperty.call(payload, 'id');

  try {
    const response = await sendJsonRpc(payload);
    if (!hasId) {
      res.writeHead(202, { 'content-type': 'application/json' });
      res.end();
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(response));
  } catch (err) {
    console.error('Failed to handle MCP request:', err);
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'Failed to execute MCP request' }));
  }
});
server.listen(PORT, HOST, () => {
  console.error(`HTTP MCP wrapper listening on ${HOST}:${PORT}`);
});

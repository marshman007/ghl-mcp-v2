console.error("BOOT: server-streamable-http.mjs starting");

import http from 'node:http';
import { Buffer } from 'node:buffer';
import { spawn } from 'node:child_process';

const PORT = Number(process.env.PORT || 8080);
const HOST = "0.0.0.0";
const MAX_CHILD_RESTARTS = 3;
const SOCKET_DESTROY_GRACE_MS = 5000;
const CHILD_BACKOFF_STEP_MS = 1000;

let shuttingDown = false;
let child = null;
let childRestartAttempts = 0;
let childRestartTimer = null;
let forceExitTimer = null;
let serverClosed = false;
let childExited = false;
let socketsDestroyed = false;
let childSignaled = false;
const sockets = new Set();
let socketDestroyTimer = null;

const pendingResponses = new Map();
let stdoutBuffer = Buffer.alloc(0);

function rejectAllPending(error) {
  for (const [key, entry] of pendingResponses.entries()) {
    try {
      entry.reject(error);
    } catch (err) {
      console.error('Failed to reject pending response', err);
    }
  }
  pendingResponses.clear();
}

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

function spawnChildProcess() {
  if (shuttingDown) {
    return;
  }

  child = spawn('node', ['build/index.js'], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });

  stdoutBuffer = Buffer.alloc(0);

  child.stdin.on('error', (err) => {
    console.error('Error writing to MCP server stdin:', err);
  });

  child.stdout.on('data', (chunk) => {
    stdoutBuffer = Buffer.concat([stdoutBuffer, chunk]);
    flushStdoutBuffer();
  });

  child.stdout.on('error', (err) => {
    console.error('Error reading MCP server stdout:', err);
  });

  child.on('error', (err) => {
    console.error('Failed to start MCP server:', err);
    process.exit(1);
  });

  child.on('exit', (code, signal) => {
    child = null;

    if (shuttingDown) {
      console.error(`MCP server exited during shutdown with code ${code} signal ${signal}`);
      childExited = true;
      maybeExit(0);
      return;
    }

    console.error(`MCP server exited with code ${code} signal ${signal}`);
    rejectAllPending(new Error('MCP server exited unexpectedly'));

    if (childRestartAttempts >= MAX_CHILD_RESTARTS) {
      console.error('Exceeded maximum MCP restart attempts, exiting');
      process.exit(1);
    }

    childRestartAttempts += 1;
    const delay = childRestartAttempts * CHILD_BACKOFF_STEP_MS;
    console.error(`Restarting MCP server in ${delay}ms (attempt ${childRestartAttempts}/${MAX_CHILD_RESTARTS})`);

    childRestartTimer = setTimeout(() => {
      childRestartTimer = null;
      spawnChildProcess();
    }, delay);
    childRestartTimer.unref();
  });
}

function signalChildProcess() {
  if (childSignaled) {
    return;
  }
  childSignaled = true;

  if (!forceExitTimer) {
    forceExitTimer = setTimeout(() => {
      console.error('Graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, 10000);
    forceExitTimer.unref();
  }

  if (child && !child.killed) {
    child.kill('SIGTERM');
  } else {
    childExited = true;
    maybeExit();
  }
}

function destroySocketsAndSignalChild() {
  if (socketsDestroyed) {
    signalChildProcess();
    return;
  }

  socketsDestroyed = true;
  console.error('Destroying remaining sockets');

  for (const socket of sockets) {
    try {
      socket.destroy();
    } catch (err) {
      console.error('Failed to destroy socket during shutdown', err);
    }
  }

  signalChildProcess();
}

function scheduleSocketDestroy() {
  if (socketDestroyTimer || socketsDestroyed) {
    return;
  }

  const delay = sockets.size > 0 ? SOCKET_DESTROY_GRACE_MS : 0;
  socketDestroyTimer = setTimeout(() => {
    socketDestroyTimer = null;
    destroySocketsAndSignalChild();
  }, delay);
  socketDestroyTimer.unref();
}

function maybeExit(code = 0) {
  if (!shuttingDown) {
    return;
  }

  if (serverClosed && childExited) {
    if (forceExitTimer) {
      clearTimeout(forceExitTimer);
      forceExitTimer = null;
    }
    process.exit(code);
  }
}

function sendJsonRpc(message) {
  return new Promise((resolve, reject) => {
    if (shuttingDown) {
      reject(new Error('Server is shutting down'));
      return;
    }

    if (!child || child.killed) {
      reject(new Error('MCP server is not available'));
      return;
    }

    const payloadBuffer = Buffer.from(`${JSON.stringify(message)}\n`, 'utf8');
    const targetChild = child;

    const hasId = Object.prototype.hasOwnProperty.call(message, 'id');
    let timeoutId;
    let key;

    if (hasId) {
      key = JSON.stringify(message.id);
      if (pendingResponses.has(key)) {
        reject({
          jsonrpc: '2.0',
          error: {
            code: -32600,
            message: 'A request with this id is already pending'
          },
          id: message.id
        });
        return;
      }
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
      targetChild.stdin.write(payloadBuffer);
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
    const MAX_BODY_SIZE = 1024 * 1024;
    let totalLength = 0;
    let finished = false;
    const chunks = [];
    req.on('data', (chunk) => {
      if (finished) {
        return;
      }
      totalLength += chunk.length;
      if (totalLength > MAX_BODY_SIZE) {
        finished = true;
        req.destroy();
        reject(Object.assign(new Error('Request body too large'), { statusCode: 413 }));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (finished) {
        return;
      }
      finished = true;
      resolve(Buffer.concat(chunks).toString('utf8'));
    });
    req.on('error', (err) => {
      if (finished) {
        return;
      }
      finished = true;
      reject(err);
    });
  });
}

spawnChildProcess();

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok');
    return;
  }

  if (req.method === 'GET' && req.url && req.url.startsWith('/oauth/start')) {
    const clientId = process.env.GHL_CLIENT_ID;
    const redirectUri = process.env.GHL_REDIRECT_URI;
    const scopes = process.env.GHL_SCOPES;
    const authorizeUrl = process.env.GHL_AUTH_URL || 'https://marketplace.gohighlevel.com/oauth/authorize';

    if (!clientId || !redirectUri || !scopes) {
      res.writeHead(500, { 'content-type': 'text/plain' });
      res.end('Missing OAuth configuration');
      return;
    }

    let redirectLocation;
    try {
      const url = new URL(authorizeUrl);
      url.searchParams.set('response_type', 'code');
      url.searchParams.set('client_id', clientId);
      url.searchParams.set('redirect_uri', redirectUri);
      url.searchParams.set('scope', scopes);
      const state = process.env.GHL_OAUTH_STATE || Math.random().toString(36).slice(2);
      url.searchParams.set('state', state);
      redirectLocation = url.toString();
    } catch (err) {
      console.error('Invalid GHL_AUTH_URL value:', err);
      res.writeHead(500, { 'content-type': 'text/plain' });
      res.end('Invalid OAuth authorize URL');
      return;
    }

    res.writeHead(302, { Location: redirectLocation });
    res.end();
    return;
  }

  if (req.method === 'GET' && req.url && req.url.startsWith('/oauth/callback')) {
    let parsedUrl;
    try {
      parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    } catch (err) {
      console.error('Failed to parse callback URL:', err);
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid callback URL' }));
      return;
    }

    const code = parsedUrl.searchParams.get('code');
    if (!code) {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing authorization code' }));
      return;
    }

    const clientId = process.env.GHL_CLIENT_ID;
    const clientSecret = process.env.GHL_CLIENT_SECRET;
    const redirectUri = process.env.GHL_REDIRECT_URI;
    const tokenUrl = process.env.GHL_TOKEN_URL || 'https://services.leadconnectorhq.com/oauth/token';

    if (!clientId || !clientSecret || !redirectUri) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing OAuth configuration' }));
      return;
    }

    const body = new URLSearchParams();
    body.set('grant_type', 'authorization_code');
    body.set('code', code);
    body.set('redirect_uri', redirectUri);
    body.set('client_id', clientId);
    body.set('client_secret', clientSecret);

    try {
      const tokenResponse = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
          'content-type': 'application/x-www-form-urlencoded'
        },
        body: body.toString()
      });

      const responseText = await tokenResponse.text();
      if (!tokenResponse.ok) {
        console.error('Failed to exchange authorization code:', tokenResponse.status, responseText);
        res.writeHead(502, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'Failed to exchange authorization code' }));
        return;
      }

      let tokens;
      try {
        tokens = JSON.parse(responseText);
      } catch (err) {
        console.error('Token endpoint returned non-JSON response:', err);
        res.writeHead(502, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid token response format' }));
        return;
      }

      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ tokens }));
    } catch (err) {
      console.error('Error exchanging authorization code:', err);
      res.writeHead(502, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'Failed to exchange authorization code' }));
    }
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

server.on('connection', (socket) => {
  sockets.add(socket);

  socket.on('close', () => {
    sockets.delete(socket);
    if (shuttingDown && sockets.size === 0) {
      if (socketDestroyTimer) {
        clearTimeout(socketDestroyTimer);
        socketDestroyTimer = null;
      }
      destroySocketsAndSignalChild();
    }
  });
});

server.listen(PORT, HOST, () => {
  console.error(`HTTP MCP wrapper listening on ${HOST}:${PORT}`);
});

function handleSignal(signal) {
  if (shuttingDown) {
    console.error(`Received ${signal}, already shutting down`);
    return;
  }
  shuttingDown = true;

  console.error(`Received ${signal}, initiating graceful shutdown`);

  if (childRestartTimer) {
    clearTimeout(childRestartTimer);
    childRestartTimer = null;
  }

  server.close(() => {
    console.error('HTTP server closed');
    serverClosed = true;
    maybeExit();
  });

  scheduleSocketDestroy();

  if (!child) {
    childExited = true;
    maybeExit();
  }
}

process.on('SIGTERM', () => handleSignal('SIGTERM'));
process.on('SIGINT', () => handleSignal('SIGINT'));

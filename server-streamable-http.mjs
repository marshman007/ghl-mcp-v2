console.error("BOOT: server-streamable-http.mjs starting");

import http from 'node:http';
import { Buffer } from 'node:buffer';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const PORT = Number(process.env.PORT || 8080);
const HOST = "0.0.0.0";
const MAX_CHILD_RESTARTS = 3;
const SOCKET_DESTROY_GRACE_MS = 5000;
const CHILD_BACKOFF_STEP_MS = 1000;
const TOKEN_REFRESH_GRACE_MS = 2 * 60 * 1000;

// MUST be marketplace.gohighlevel.com
const AUTHORIZE_URL =
  process.env.GHL_AUTHORIZE_URL ||
  'https://marketplace.gohighlevel.com/oauth/authorize';

// MUST be services.leadconnectorhq.com
const TOKEN_URL =
  process.env.GHL_TOKEN_URL ||
  'https://services.leadconnectorhq.com/oauth/token';

const DEFAULT_GHL_HOSTS = process.env.GHL_API_HOSTS || 'services.leadconnectorhq.com';

const tokenStoreConfig = createTokenStoreConfig(process.env.TOKEN_STORE_JSON);
const TOKEN_STORE_PATH = tokenStoreConfig.path;
process.env.GHL_TOKEN_STORE_PATH = TOKEN_STORE_PATH;
process.env.GHL_API_HOSTS = DEFAULT_GHL_HOSTS;

const HEADER_INJECTOR_PATH = ensureHeaderInjectorModule();

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

let activeTokens = loadTokens();
if (activeTokens) {
  console.error('Tokens loaded on boot');
}
let refreshPromise = null;

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

function createTokenStoreConfig(rawValue) {
  if (rawValue) {
    try {
      const parsed = JSON.parse(rawValue);
      if (parsed && typeof parsed === 'object') {
        if (parsed.path && typeof parsed.path === 'string') {
          return { ...parsed, path: path.resolve(parsed.path) };
        }
        return { ...parsed, path: path.join(process.cwd(), '.ghl-token-store.json') };
      }
    } catch {
      // Ignore parse errors and fall back to default path.
    }
  }
  return { path: path.join(process.cwd(), '.ghl-token-store.json') };
}

function ensureHeaderInjectorModule() {
  const injectorPath = path.join(os.tmpdir(), 'ghl-header-injector.cjs');
  const source = `'use strict';
const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const { URL } = require('node:url');

const tokenFilePath = process.env.GHL_TOKEN_STORE_PATH;
const hostList = (process.env.GHL_API_HOSTS || 'services.leadconnectorhq.com')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);

let cachedTokens = null;
let cachedMtime = 0;

function readTokens() {
  if (!tokenFilePath) {
    return null;
  }
  let stats;
  try {
    stats = fs.statSync(tokenFilePath);
  } catch {
    cachedTokens = null;
    cachedMtime = 0;
    return null;
  }
  if (cachedTokens && cachedMtime === stats.mtimeMs) {
    return cachedTokens;
  }
  try {
    const raw = fs.readFileSync(tokenFilePath, 'utf8');
    cachedTokens = JSON.parse(raw);
    cachedMtime = stats.mtimeMs;
    return cachedTokens;
  } catch {
    cachedTokens = null;
    cachedMtime = 0;
    return null;
  }
}

function headerExists(headers, name) {
  const target = name.toLowerCase();
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === target) {
      return true;
    }
  }
  return false;
}

function shouldAttach(hostname, requestPath) {
  if (!hostname) {
    return false;
  }
  const normalizedPath = typeof requestPath === 'string' ? requestPath.toLowerCase() : '';
  if (normalizedPath.includes('/oauth/token')) {
    return false;
  }
  const lower = hostname.toLowerCase();
  if (hostList.length === 0) {
    return true;
  }
  return hostList.some((host) => lower === host || lower.endsWith('.' + host));
}

function attach(headers, hostname, requestPath) {
  if (!shouldAttach(hostname, requestPath)) {
    return;
  }
    
  const tokens = readTokens();
  const pathLabel = typeof requestPath === 'string' ? requestPath : '';
  if (!tokens || !tokens.access_token) {
    console.error('[auth] missing access token host=' + (hostname || 'unknown') + ' path=' + pathLabel); // Avoid crashing the child when auth tokens are absent
    return;
  }
  if (!tokens.locationId) {
    console.error('[auth] missing Target-User-SubAccount host=' + (hostname || 'unknown') + ' path=' + pathLabel); // Avoid crashing the child when subaccount header is absent
    return;
  }
  if (!headerExists(headers, 'authorization')) {
    headers['Authorization'] = 'Bearer ' + tokens.access_token;
  }
  if (!headerExists(headers, 'target-user-subaccount')) {
    headers['Target-User-SubAccount'] = tokens.locationId;
  }
}

function cloneOptionsFromUrl(url) {
  return {
    protocol: url.protocol,
    hostname: url.hostname,
    port: url.port,
    auth: url.username ? url.username + ':' + url.password : undefined,
    path: (url.pathname || '') + (url.search || ''),
  };
}

function patchModule(targetModule) {
  const originalRequest = targetModule.request;
  targetModule.request = function patchedRequest(options, callback) {
    let finalOptions = options;
    if (typeof finalOptions === 'string') {
      finalOptions = cloneOptionsFromUrl(new URL(finalOptions));
    } else if (finalOptions instanceof URL) {
      finalOptions = cloneOptionsFromUrl(finalOptions);
    } else if (finalOptions && typeof finalOptions === 'object') {
      finalOptions = { ...finalOptions };
    }
    if (finalOptions && typeof finalOptions === 'object') {
      finalOptions.headers = { ...(finalOptions.headers || {}) };
      const hostname = finalOptions.hostname || (typeof finalOptions.host === 'string' ? finalOptions.host.split(':')[0] : '');
      const pathValue = typeof finalOptions.path === 'string' ? finalOptions.path : '';
      attach(finalOptions.headers, hostname, pathValue);
      return originalRequest.call(this, finalOptions, callback);
    }
    return originalRequest.call(this, options, callback);
  };
  targetModule.get = function patchedGet(options, callback) {
    const req = targetModule.request(options, callback);
    req.end();
    return req;
  };
}

patchModule(http);
patchModule(https);
`;
  fs.writeFileSync(injectorPath, source, 'utf8');
  return injectorPath;
}

function appendNodeRequire(existingOptions, requiredPath) {
  const flag = `--require ${requiredPath}`;
  if (!existingOptions || existingOptions.trim() === '') {
    return flag;
  }
  if (existingOptions.includes(flag)) {
    return existingOptions;
  }
  return `${existingOptions} ${flag}`.trim();
}

function loadTokens() {
  try {
    const raw = fs.readFileSync(TOKEN_STORE_PATH, 'utf8').trim();
    if (!raw) {
      return null;
    }
    return normalizePersistedTokens(JSON.parse(raw));
  } catch (err) {
    if (err && err.code !== 'ENOENT') {
      throw err;
    }
    return null;
  }
}

function normalizePersistedTokens(data) {
  if (!data || typeof data !== 'object') {
    return null;
  }
  const { access_token, refresh_token, expires_at, locationId } = data;
  if (!access_token || !refresh_token || !expires_at || !locationId) {
    return null;
  }
  const expiresAtMs = Date.parse(expires_at);
  if (!Number.isFinite(expiresAtMs)) {
    return null;
  }
  return {
    access_token,
    refresh_token,
    expires_at: new Date(expiresAtMs).toISOString(),
    expiresAtMs,
    locationId,
  };
}

function saveTokens(tokenResponse) {
  const normalized = normalizeNewTokens(tokenResponse);
  const payload = {
    access_token: normalized.access_token,
    refresh_token: normalized.refresh_token,
    expires_at: normalized.expires_at,
    locationId: normalized.locationId,
  };
  writeTokenStore(payload);
  activeTokens = normalized;
  return normalized;
}

function normalizeNewTokens(tokenResponse) {
  if (!tokenResponse || typeof tokenResponse !== 'object') {
    throw new Error('Invalid token response');
  }
  const accessToken = tokenResponse.access_token;
  if (!accessToken) {
    throw new Error('Token response missing access_token');
  }
  const refreshToken = tokenResponse.refresh_token || activeTokens?.refresh_token;
  if (!refreshToken) {
    logMissingToken();
    throw createTokenError('Token response missing refresh_token', 401);
  }
  const locationId = tokenResponse.locationId || tokenResponse.locationID || tokenResponse.location_id || activeTokens?.locationId;
  if (!locationId) {
    logMissingTargetHeader();
    throw createTokenError('Token response missing locationId', 400);
  }
  const expiresAtMs = tokenResponse.expires_at
    ? Date.parse(tokenResponse.expires_at)
    : Date.now() + ((tokenResponse.expires_in ?? 0) * 1000);
  if (!Number.isFinite(expiresAtMs)) {
    throw new Error('Token response contains invalid expires_at');
  }
  return {
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_at: new Date(expiresAtMs).toISOString(),
    expiresAtMs,
    locationId,
  };
}

function writeTokenStore(payload) {
  const dir = path.dirname(TOKEN_STORE_PATH);
  fs.mkdirSync(dir, { recursive: true });
  const tempPath = `${TOKEN_STORE_PATH}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(payload), 'utf8');
  fs.renameSync(tempPath, TOKEN_STORE_PATH);
}

function tokensNeedRefresh(token) {
  if (!token || !token.expiresAtMs) {
    return true;
  }
  return token.expiresAtMs - Date.now() <= TOKEN_REFRESH_GRACE_MS;
}

async function ensureTokensReady() {
  if (!activeTokens) {
    logMissingToken();
    throw createTokenError('OAuth tokens not initialized. Please complete the OAuth flow.', 401);
  }
  if (!activeTokens.locationId) {
    logMissingTargetHeader();
    throw createTokenError('Missing Target-User-SubAccount. Re-authorize OAuth.', 400);
  }
  if (!tokensNeedRefresh(activeTokens)) {
    return activeTokens;
  }
  if (!activeTokens.refresh_token) {
    logMissingToken();
    throw createTokenError('Refresh token missing. Re-authorize OAuth.', 401);
  }
  if (!refreshPromise) {
    refreshPromise = (async () => {
      const updated = await refreshAccessToken(activeTokens.refresh_token);
      console.error('Token refreshed');
      return updated;
    })().finally(() => {
      refreshPromise = null;
    });
  }
  return refreshPromise;
}

async function refreshAccessToken(refreshToken) {
  const clientId = process.env.GHL_CLIENT_ID;
  const clientSecret = process.env.GHL_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw createTokenError('Missing GHL OAuth client credentials', 500);
  }
  const body = new URLSearchParams();
  body.set('grant_type', 'refresh_token');
  body.set('refresh_token', refreshToken);
  body.set('client_id', clientId);
  body.set('client_secret', clientSecret);

  let response;
  try {
    response = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    });
  } catch (err) {
    throw createTokenError('Failed to reach token endpoint during refresh', 502);
  }

  const text = await response.text();
  if (!response.ok) {
    throw createTokenError('Failed to refresh OAuth tokens', 502);
  }
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw createTokenError('Token refresh returned invalid JSON', 502);
  }
  parsed.refresh_token = parsed.refresh_token || refreshToken;
  parsed.locationId = parsed.locationId || activeTokens?.locationId;
  return saveTokens(parsed);
}

// --- Phase 2: pagination + trimming helpers (wrapper-only) ---
const PAGINATION_CONFIG = [
  { name: 'search-contacts-advanced', path: '/contacts/search', resource: 'contacts', method: 'post' },
  { name: 'get-contacts', path: '/contacts', resource: 'contacts', method: 'get' },
  { name: 'get-tags-location-id', path: '/locations/{locationId}/tags', resource: 'tags', method: 'get' },
  { name: 'get-tags-by-ids', path: '/social-media-posting/{locationId}/tags/details', resource: 'tags', method: 'get' },
  { name: 'get-custom-fields', path: '/custom-fields', resource: 'custom-fields', method: 'get' },
  { name: 'get-pipelines', path: '/pipelines', resource: 'pipelines', method: 'get' },
];

const CURSOR_ALLOWLIST = {
  contacts: [
    ['meta', 'startAfterId'],
    ['meta', 'startAfter']
  ],
  tags: [],
  'custom-fields': [],
  pipelines: []
};

function lookupTarget(payload) {
  try {
    const method = typeof payload.method === 'string' ? payload.method.toLowerCase() : null;
    const params = payload.params || {};
    // match by explicit tool name first
    if (method) {
      for (const cfg of PAGINATION_CONFIG) {
        if (cfg.name === method) return cfg;
      }
    }
    // match by pathTemplate or explicit path param if present
    const maybePath = params.path || params.pathTemplate || params.pathTemplateRaw || params.pathTemplate?.toString();
    if (typeof maybePath === 'string') {
      for (const cfg of PAGINATION_CONFIG) {
        if (cfg.path === maybePath) return cfg;
      }
    }
    return null;
  } catch (err) {
    return null;
  }
}

function capLimitInPayload(payload, cfg) {
  if (!cfg) return payload;
  // operate on a shallow clone to avoid surprising side-effects
  const cloned = JSON.parse(JSON.stringify(payload));
  const params = cloned.params || {};
  // many generated tools use top-level limit or params.requestBody.limit
  const applyLimitTo = [params, params.requestBody, params.query, params.request_body].filter(Boolean);
  let applied = false;
  for (const target of applyLimitTo) {
    if (Object.prototype.hasOwnProperty.call(target, 'limit')) {
      if (typeof target.limit === 'number') {
        if (target.limit > 100) target.limit = 100;
      }
      applied = true;
    }
  }
  if (!applied) {
    // enforce default limit = 5 when missing
    if (!Object.prototype.hasOwnProperty.call(params, 'limit')) {
      params.limit = 5;
    }
  }
  cloned.params = params;
  return cloned;
}

function pruneValue(value, keepEmptyArrayKeys, keyName) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'string') {
    if (value === '') return undefined;
    return value;
  }
  if (Array.isArray(value)) {
    const arr = value.map((v) => pruneValue(v, keepEmptyArrayKeys)).filter((v) => v !== undefined);
    if (arr.length === 0) {
      if (keyName && keepEmptyArrayKeys.has(keyName)) return [];
      return undefined;
    }
    return arr;
  }
  if (typeof value === 'object') {
    const out = {};
    for (const k of Object.keys(value)) {
      const v = pruneValue(value[k], keepEmptyArrayKeys, k);
      if (v !== undefined) out[k] = v;
    }
    if (Object.keys(out).length === 0) return undefined;
    return out;
  }
  return value;
}

function combineName(obj) {
  if (!obj) return null;
  if (typeof obj === 'string' && obj.trim() !== '') return obj.trim();
  const first = obj.firstName || obj.first_name || obj.firstname;
  const last = obj.lastName || obj.last_name || obj.lastname;
  if (first || last) return `${(first || '').trim()} ${(last || '').trim()}`.trim();
  return null;
}

function transformContactsResult(raw) {
  // find candidate items array in known locations
  const result = raw && typeof raw === 'object' ? raw : {};
  let items = null;
  if (Array.isArray(result.items)) items = result.items;
  else if (Array.isArray(result.data)) items = result.data;
  else if (Array.isArray(result.contacts)) items = result.contacts;
  else if (Array.isArray(result.results)) items = result.results;
  else if (Array.isArray(raw)) items = raw;
  else items = [];

  // extract nextCursor only from explicitly allowed fields
  function extractCursor(resource, result) {
  const allowlist = CURSOR_ALLOWLIST[resource];
  if (!allowlist || !result || typeof result !== 'object') return null;

  for (const path of allowlist) {
    let value = result;
    for (const key of path) {
      if (!value || typeof value !== 'object') {
        value = undefined;
        break;
      }
      value = value[key];
    }
    if (value !== undefined && value !== null) {
      return value;
    }
  }

  return null;
}

  const projected = items.map((it) => {
    if (!it || typeof it !== 'object') return null;
    const out = {};
    if (it.id) out.id = it.id;
    // contactName resolution
    let contactName = null;
    if (it.contactName) contactName = combineName(it.contactName);
    else if (it.name) contactName = combineName(it.name);
    if (contactName) out.contactName = contactName;
    if (it.email) out.email = it.email;
    if (it.phone) out.phone = it.phone;
    if (Array.isArray(it.tags)) out.tags = it.tags.slice();
    if (it.dateUpdated) out.dateUpdated = it.dateUpdated;
    // prune nulls/empties but keep tags even if empty
    const keepEmptyArrayKeys = new Set(['tags']);
    const pruned = pruneValue(out, keepEmptyArrayKeys);
    return pruned || null;
  }).filter(Boolean);

  const page = { hasMore: Boolean(nextCursor), nextCursor: nextCursor === undefined ? null : nextCursor };
  return { items: projected, page };
}

function transformTagsResult(raw) {
  const result = raw && typeof raw === 'object' ? raw : {};
  let items = null;
  if (Array.isArray(result.items)) items = result.items;
  else if (Array.isArray(result.data)) items = result.data;
  else if (Array.isArray(raw)) items = raw;
  else items = [];
  const projected = items.map((it) => {
    if (!it || typeof it !== 'object') return null;
    const out = {};
    if (it.id) out.id = it.id;
    if (it.name) out.name = it.name;
    const pruned = pruneValue(out, new Set());
    return pruned || null;
  }).filter(Boolean);
  const nextCursor = (result.nextCursor !== undefined ? result.nextCursor : (result.paging && result.paging.next ? result.paging.next : null));
  return { items: projected, page: { hasMore: Boolean(nextCursor), nextCursor: nextCursor === undefined ? null : nextCursor } };
}

function transformCustomFieldsResult(raw) {
  const result = raw && typeof raw === 'object' ? raw : {};
  let items = null;
  if (Array.isArray(result.items)) items = result.items;
  else if (Array.isArray(result.data)) items = result.data;
  else if (Array.isArray(raw)) items = raw;
  else items = [];
  const projected = items.map((it) => {
    if (!it || typeof it !== 'object') return null;
    const out = {};
    if (it.id) out.id = it.id;
    if (it.name) out.name = it.name;
    if (it.fieldType || it.field_type) out.fieldType = it.fieldType || it.field_type;
    const pruned = pruneValue(out, new Set());
    return pruned || null;
  }).filter(Boolean);
  const nextCursor = (result.nextCursor !== undefined ? result.nextCursor : (result.paging && result.paging.next ? result.paging.next : null));
  return { items: projected, page: { hasMore: Boolean(nextCursor), nextCursor: nextCursor === undefined ? null : nextCursor } };
}

function transformPipelinesResult(raw) {
  const result = raw && typeof raw === 'object' ? raw : {};
  let items = null;
  if (Array.isArray(result.items)) items = result.items;
  else if (Array.isArray(result.data)) items = result.data;
  else if (Array.isArray(raw)) items = raw;
  else items = [];
  const projected = items.map((it) => {
    if (!it || typeof it !== 'object') return null;
    const out = {};
    if (it.id) out.id = it.id;
    if (it.name) out.name = it.name;
    if (Array.isArray(it.stages)) {
      out.stages = it.stages.map((s) => {
        if (!s || typeof s !== 'object') return null;
        const stage = {};
        if (s.id) stage.id = s.id;
        if (s.name) stage.name = s.name;
        if (s.position !== undefined) stage.position = s.position;
        else if (s.order !== undefined) stage.position = s.order;
        const pruned = pruneValue(stage, new Set());
        return pruned || null;
      }).filter(Boolean);
    }
    const pruned = pruneValue(out, new Set(['stages']));
    return pruned || null;
  }).filter(Boolean);
  const nextCursor = (result.nextCursor !== undefined ? result.nextCursor : (result.paging && result.paging.next ? result.paging.next : null));
  return { items: projected, page: { hasMore: Boolean(nextCursor), nextCursor: nextCursor === undefined ? null : nextCursor } };
}

function transformForTarget(cfg, response) {
  try {
    const raw = (response && typeof response === 'object' && Object.prototype.hasOwnProperty.call(response, 'result')) ? response.result : response;
    const debug = String(process.env.DEBUG_TRIMMING || '').toLowerCase() === 'true';
    const toolName = cfg && cfg.name ? cfg.name : 'unknown';
    const rawSize = raw ? Buffer.byteLength(JSON.stringify(raw)) : 0;
    let transformed = null;
    if (!cfg) return response;
    if (cfg.resource === 'contacts') transformed = transformContactsResult(raw);
    else if (cfg.resource === 'tags') transformed = transformTagsResult(raw);
    else if (cfg.resource === 'custom-fields') transformed = transformCustomFieldsResult(raw);
    else if (cfg.resource === 'pipelines') transformed = transformPipelinesResult(raw);
    else return response;
    if (debug) {
      try {
        const trimmedSize = Buffer.byteLength(JSON.stringify(transformed));
        console.error('[TRIM] tool=' + toolName + ' raw_bytes=' + rawSize + ' trimmed_bytes=' + trimmedSize);
      } catch (e) {
        console.error('[TRIM] tool=' + toolName + ' sizes logged');
      }
    }
    return { jsonrpc: '2.0', result: transformed, id: response && response.id !== undefined ? response.id : null };
  } catch (err) {
    return response;
  }
}


function logMissingToken() {
  console.error('Missing token for MCP request');
}

function logMissingTargetHeader() {
  console.error('Missing Target-User-SubAccount value');
}

function createTokenError(message, statusCode) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function shouldRequireTokens(payload) {
  if (!payload || typeof payload !== 'object') {
    return false;
  }
  const method = typeof payload.method === 'string' ? payload.method.toLowerCase() : '';
  if (!method) {
    return false;
  }
  if (method === 'tools/list' || method === 'tools.list' || method === 'list_tools') {
    return false;
  }
  if (method === 'ping' || method === 'mcp/heartbeat') {
    return false;
  }
  return true;
}

function spawnChildProcess() {
  if (shuttingDown) {
    return;
  }

  const childEnv = {
    ...process.env,
    GHL_TOKEN_STORE_PATH: TOKEN_STORE_PATH,
    GHL_API_HOSTS: process.env.GHL_API_HOSTS || DEFAULT_GHL_HOSTS,
  };
  childEnv.NODE_OPTIONS = appendNodeRequire(childEnv.NODE_OPTIONS, HEADER_INJECTOR_PATH);
  process.env.NODE_OPTIONS = childEnv.NODE_OPTIONS;

  child = spawn('node', ['build/index.js'], {
    stdio: ['pipe', 'pipe', 'inherit'],
    env: childEnv,
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

// Security assumption:
// This server relies on trusted MCP transport and is not intended.
// to be exposed publicly. Authentication is enforced externally.
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

  if (!clientId || !redirectUri || !scopes) {
    res.writeHead(500, { 'content-type': 'text/plain' });
    res.end('Missing OAuth configuration');
    return;
  }

  let redirectLocation;
  try {
    const url = new URL(`https://marketplace.gohighlevel.com/oauth/authorize`);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('client_id', clientId);
    url.searchParams.set('redirect_uri', redirectUri);
    url.searchParams.set('scope', scopes);
    const state =
      process.env.GHL_OAUTH_STATE || Math.random().toString(36).slice(2);
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
    const tokenUrl = TOKEN_URL;

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

      let persistedTokens;
      try {
        persistedTokens = saveTokens(tokens);
      } catch (err) {
        const statusCode = err?.statusCode || 502;
        res.writeHead(statusCode, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: err?.message || 'Failed to persist tokens' }));
        return;
      }

      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        tokens: {
          access_token: persistedTokens.access_token,
          refresh_token: persistedTokens.refresh_token,
          expires_at: persistedTokens.expires_at,
          locationId: persistedTokens.locationId
        }
      }));
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

  const requiresTokens = shouldRequireTokens(payload);
  if (requiresTokens) {
    try {
      await ensureTokensReady();
    } catch (err) {
      const statusCode = err?.statusCode || 502;
      res.writeHead(statusCode, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: err?.message || 'Failed to prepare OAuth tokens' }));
      return;
    }
  }

  const hasId = payload != null && Object.prototype.hasOwnProperty.call(payload, 'id');

  try {
    // Determine if this call is one of the target tools/paths and adjust request/response
    const cfg = lookupTarget(payload);
    const prepared = capLimitInPayload(payload, cfg);

    const response = await sendJsonRpc(prepared);
    if (!hasId) {
      res.writeHead(202, { 'content-type': 'application/json' });
      res.end();
      return;
    }

    let out = response;
    if (cfg && response && typeof response === 'object') {
      try {
        out = transformForTarget(cfg, response);
      } catch (err) {
        // fall back to original response on any transform error
        out = response;
      }
    }

    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(out));
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
  if (signal !== 'SIGTERM' && signal !== 'SIGINT') return;

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

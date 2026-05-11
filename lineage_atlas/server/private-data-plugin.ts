import { spawn } from 'node:child_process';
import { createReadStream, existsSync, readdirSync, statSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Plugin } from 'vite';

type PrivateFile = {
  id: string;
  name: string;
  relPath: string;
  scope: 'raw' | 'generated';
  category: string;
  categoryLabel: string;
  extension: string;
  size: number;
  modifiedAt: string;
  view: 'table' | 'text' | 'image' | 'pdf' | 'binary';
  absolutePath: string;
};

type Session = {
  user: string;
  expiresAt: number;
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ATLAS_DIR = path.resolve(__dirname, '..');
const PROJECT_ROOT = path.resolve(ATLAS_DIR, '..');
const DATA_DIR = resolveProjectPath(process.env.MONOLITH_ATLAS_DATA_DIR || process.env.MONOLITHFARM_DATA_DIR, 'data');
const OUTPUT_DIR = resolveProjectPath(process.env.MONOLITH_ATLAS_OUTPUT_DIR, path.join('notebook_outputs', 'complete_ndvi'));
const SESSION_COOKIE = 'mf_atlas_session';
const SESSION_TTL_MS = 8 * 60 * 60 * 1000;
const MAX_BODY_BYTES = 8192;

const sessions = new Map<string, Session>();
const loginAttempts = new Map<string, { count: number; resetAt: number }>();
const GENERATED_FINAL = new Set([
  'decision_summary.csv',
  'event_driver_lift.csv',
  'final_hypothesis_register.csv',
  'ndvi_outlook.csv',
  'pair_classic_tests.csv',
  'pair_effect_tests.csv',
  'pair_weekly_gaps.csv',
  'weekly_correlations.csv',
]);
const GENERATED_INTERMEDIATE_PREFIXES = [
  'ndvi_',
  'pairwise_',
  'transition_model_',
  'weather_',
  'ops_',
  'miip_',
];

export function privateDataPlugin(): Plugin {
  const inventory = buildInventory();

  return {
    name: 'monolithfarm-private-data-api',
    configureServer(server) {
      server.middlewares.use((req, res, next) => handlePrivateApi(req, res, next, inventory));
    },
    configurePreviewServer(server) {
      server.middlewares.use((req, res, next) => handlePrivateApi(req, res, next, inventory));
    },
  };
}

function resolveProjectPath(configured: string | undefined, fallback: string) {
  const raw = (configured || fallback).trim();
  const candidate = path.isAbsolute(raw) ? raw : path.join(PROJECT_ROOT, raw);
  return path.resolve(candidate);
}

async function handlePrivateApi(req: any, res: any, next: any, inventory: Map<string, PrivateFile>) {
  const url = new URL(req.url || '/', 'http://127.0.0.1');
  if (!url.pathname.startsWith('/api/')) {
    next();
    return;
  }

  setSecurityHeaders(res);

  try {
    if (url.pathname === '/api/auth/session' && req.method === 'GET') {
      const session = getSession(req);
      json(res, 200, {
        authenticated: Boolean(session),
        user: session?.user || null,
        configured: isAuthConfigured(),
      });
      return;
    }

    if (url.pathname === '/api/auth/login' && req.method === 'POST') {
      if (!isSameOrigin(req)) {
        json(res, 403, { error: 'origin_forbidden' });
        return;
      }
      await login(req, res);
      return;
    }

    if (url.pathname === '/api/auth/logout' && req.method === 'POST') {
      if (!isSameOrigin(req)) {
        json(res, 403, { error: 'origin_forbidden' });
        return;
      }
      logout(req, res);
      return;
    }

    const session = getSession(req);
    if (!session) {
      json(res, 401, { error: 'authentication_required' });
      return;
    }

    if (url.pathname === '/api/private/files' && req.method === 'GET') {
      json(res, 200, {
        files: Array.from(inventory.values()).map(publicFile),
        roots: [
          { scope: 'raw', label: 'Arquivos brutos', exists: existsSync(DATA_DIR) },
          { scope: 'generated', label: 'CSVs e artefatos gerados', exists: existsSync(OUTPUT_DIR) },
        ],
      });
      return;
    }

    const fileMatch = url.pathname.match(/^\/api\/private\/files\/([^/]+)\/(rows|text|blob)$/);
    if (!fileMatch) {
      json(res, 404, { error: 'not_found' });
      return;
    }

    const file = inventory.get(decodeURIComponent(fileMatch[1]));
    if (!file || !isAllowedPrivatePath(file.absolutePath)) {
      json(res, 404, { error: 'file_not_found' });
      return;
    }

    const action = fileMatch[2];
    if (action === 'rows' && req.method === 'GET') {
      await tableRows(res, file, url);
      return;
    }
    if (action === 'text' && req.method === 'GET') {
      await textRows(res, file, url);
      return;
    }
    if (action === 'blob' && req.method === 'GET') {
      streamBlob(res, file);
      return;
    }

    json(res, 405, { error: 'method_not_allowed' });
  } catch (error) {
    json(res, 500, { error: 'private_api_error', message: error instanceof Error ? error.message : String(error) });
  }
}

async function login(req: any, res: any) {
  if (!isAuthConfigured()) {
    json(res, 503, { error: 'auth_not_configured' });
    return;
  }

  const ip = String(req.socket?.remoteAddress || 'local');
  if (isRateLimited(ip)) {
    json(res, 429, { error: 'too_many_attempts' });
    return;
  }

  const body = await readJsonBody(req);
  const username = String(body.username || '');
  const password = String(body.password || '');
  const expectedUser = process.env.MONOLITH_ATLAS_USER || 'monolito_farm';

  if (username !== expectedUser || !verifyPassword(password)) {
    registerFailedAttempt(ip);
    json(res, 401, { error: 'invalid_credentials' });
    return;
  }

  loginAttempts.delete(ip);
  const sessionId = randomBytes(32).toString('base64url');
  sessions.set(sessionId, { user: expectedUser, expiresAt: Date.now() + SESSION_TTL_MS });
  res.setHeader('Set-Cookie', serializeCookie(SESSION_COOKIE, sessionId, SESSION_TTL_MS));
  json(res, 200, { authenticated: true, user: expectedUser });
}

function logout(req: any, res: any) {
  const sessionId = cookieMap(req)[SESSION_COOKIE];
  if (sessionId) sessions.delete(sessionId);
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax`);
  json(res, 200, { authenticated: false });
}

function getSession(req: any): Session | null {
  const sessionId = cookieMap(req)[SESSION_COOKIE];
  if (!sessionId) return null;
  const session = sessions.get(sessionId);
  if (!session) return null;
  if (session.expiresAt < Date.now()) {
    sessions.delete(sessionId);
    return null;
  }
  session.expiresAt = Date.now() + SESSION_TTL_MS;
  return session;
}

function isAuthConfigured() {
  return Boolean(process.env.MONOLITH_ATLAS_PASSWORD || process.env.MONOLITH_ATLAS_PASSWORD_HASH);
}

function verifyPassword(candidate: string) {
  const expected = process.env.MONOLITH_ATLAS_PASSWORD;
  const expectedHash = process.env.MONOLITH_ATLAS_PASSWORD_HASH;
  if (expectedHash) {
    return safeEqual(sha256(candidate), expectedHash);
  }
  if (!expected) return false;
  return safeEqual(sha256(candidate), sha256(expected));
}

function safeEqual(left: string, right: string) {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}

function sha256(value: string) {
  return createHash('sha256').update(value).digest('hex');
}

function isRateLimited(ip: string) {
  const record = loginAttempts.get(ip);
  if (!record) return false;
  if (record.resetAt < Date.now()) {
    loginAttempts.delete(ip);
    return false;
  }
  return record.count >= 8;
}

function registerFailedAttempt(ip: string) {
  const now = Date.now();
  const record = loginAttempts.get(ip);
  if (!record || record.resetAt < now) {
    loginAttempts.set(ip, { count: 1, resetAt: now + 10 * 60 * 1000 });
    return;
  }
  record.count += 1;
}

function buildInventory() {
  const files = new Map<string, PrivateFile>();
  addFiles(files, DATA_DIR, 'raw');
  addFiles(files, OUTPUT_DIR, 'generated');
  return files;
}

function addFiles(files: Map<string, PrivateFile>, root: string, scope: PrivateFile['scope']) {
  if (!existsSync(root)) return;
  for (const absolutePath of walk(root)) {
    const stat = statSync(absolutePath);
    const relPath = path.relative(root, absolutePath);
    const extension = path.extname(absolutePath).toLowerCase();
    const id = `${scope}-${createHash('sha1').update(relPath).digest('hex').slice(0, 16)}`;
    const category = classifyFile(scope, relPath, extension);
    files.set(id, {
      id,
      name: path.basename(absolutePath),
      relPath: normalizePath(relPath),
      scope,
      category,
      categoryLabel: categoryLabel(category),
      extension,
      size: stat.size,
      modifiedAt: stat.mtime.toISOString(),
      view: viewForExtension(extension),
      absolutePath,
    });
  }
}

function walk(root: string): string[] {
  const result: string[] = [];
  const stack = [root];
  while (stack.length) {
    const current = stack.pop()!;
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      if (entry.name.startsWith('.')) continue;
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(absolute);
      } else if (entry.isFile()) {
        result.push(absolute);
      }
    }
  }
  return result.sort((a, b) => a.localeCompare(b));
}

function classifyFile(scope: PrivateFile['scope'], relPath: string, extension: string) {
  const name = path.basename(relPath).toLowerCase();
  const normalized = normalizePath(relPath).toLowerCase();
  if (scope === 'raw') {
    if (extension === '.csv') return 'raw_csv';
    if (extension === '.parquet') return 'raw_parquet';
    if (['.jpg', '.jpeg', '.png', '.webp', '.gif'].includes(extension)) return 'raw_image';
    if (extension === '.pdf') return 'raw_document';
    return 'raw_other';
  }
  if (normalized.startsWith('review/')) return 'generated_review';
  if (name.startsWith('lineage_') || name === 'data_audit.csv' || name === 'dataset_overview.csv' || name === 'numeric_profiles.csv' || name === 'area_inventory.csv') {
    return 'generated_lineage';
  }
  if (GENERATED_FINAL.has(name)) return 'generated_final';
  if (GENERATED_INTERMEDIATE_PREFIXES.some((prefix) => name.startsWith(prefix))) return 'generated_intermediate';
  if (extension === '.csv') return 'generated_csv';
  return 'generated_other';
}

function categoryLabel(category: string) {
  return {
    raw_csv: 'CSV bruto',
    raw_parquet: 'Parquet bruto',
    raw_image: 'Imagem bruta',
    raw_document: 'Documento bruto',
    raw_other: 'Arquivo bruto',
    generated_final: 'CSV final',
    generated_intermediate: 'Tabela intermediária',
    generated_lineage: 'Lineage/auditoria',
    generated_review: 'Revisão gerada',
    generated_csv: 'CSV gerado',
    generated_other: 'Arquivo gerado',
  }[category] || category;
}

function viewForExtension(extension: string): PrivateFile['view'] {
  if (extension === '.csv' || extension === '.parquet') return 'table';
  if (['.json', '.txt', '.md', '.log'].includes(extension)) return 'text';
  if (['.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg'].includes(extension)) return 'image';
  if (extension === '.pdf') return 'pdf';
  return 'binary';
}

function publicFile(file: PrivateFile) {
  const { absolutePath, ...publicInfo } = file;
  return publicInfo;
}

async function tableRows(res: any, file: PrivateFile, url: URL) {
  if (file.view !== 'table') {
    json(res, 415, { error: 'not_a_table' });
    return;
  }
  const offset = boundedInt(url.searchParams.get('offset'), 0, 0, 5_000_000);
  const limit = boundedInt(url.searchParams.get('limit'), 100, 1, 500);
  const query = String(url.searchParams.get('q') || '').slice(0, 160);
  const python = resolvePython();
  const script = path.join(__dirname, 'table_reader.py');
  const payload = await runPython(python, [script, '--path', file.absolutePath, '--offset', String(offset), '--limit', String(limit), '--query', query]);
  json(res, 200, JSON.parse(payload));
}

async function textRows(res: any, file: PrivateFile, url: URL) {
  if (file.view !== 'text') {
    json(res, 415, { error: 'not_text' });
    return;
  }
  const offset = boundedInt(url.searchParams.get('offset'), 0, 0, 5_000_000);
  const limit = boundedInt(url.searchParams.get('limit'), 200, 1, 1000);
  const content = await readFile(file.absolutePath, 'utf8');
  const lines = content.split(/\r?\n/);
  json(res, 200, {
    lines: lines.slice(offset, offset + limit),
    offset,
    limit,
    totalLines: lines.length,
  });
}

function streamBlob(res: any, file: PrivateFile) {
  res.statusCode = 200;
  res.setHeader('Content-Type', contentType(file.extension));
  res.setHeader('Cache-Control', 'no-store');
  createReadStream(file.absolutePath).pipe(res);
}

function resolvePython() {
  const candidates = [
    process.env.MONOLITH_ATLAS_PYTHON,
    path.join(PROJECT_ROOT, '.venv_win', 'Scripts', 'python.exe'),
    path.join(PROJECT_ROOT, '.venv', 'Scripts', 'python.exe'),
    path.join(PROJECT_ROOT, '.venv', 'bin', 'python'),
    'python',
  ].filter(Boolean) as string[];
  return candidates.find((candidate) => candidate === 'python' || existsSync(candidate)) || 'python';
}

function runPython(python: string, args: string[]) {
  return new Promise<string>((resolve, reject) => {
    const child = spawn(python, args, { cwd: PROJECT_ROOT, windowsHide: true });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => (stdout += chunk));
    child.stderr.on('data', (chunk) => (stderr += chunk));
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve(stdout);
      else reject(new Error(stderr || `Python exited with ${code}`));
    });
  });
}

function isAllowedPrivatePath(candidate: string) {
  const resolved = path.resolve(candidate);
  return [DATA_DIR, OUTPUT_DIR].some((root) => {
    const rel = path.relative(root, resolved);
    return rel && !rel.startsWith('..') && !path.isAbsolute(rel);
  });
}

function boundedInt(raw: string | null, fallback: number, min: number, max: number) {
  const value = Number.parseInt(String(raw || ''), 10);
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, value));
}

function contentType(extension: string) {
  return {
    '.csv': 'text/csv; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.txt': 'text/plain; charset=utf-8',
    '.md': 'text/markdown; charset=utf-8',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png',
    '.webp': 'image/webp',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.pdf': 'application/pdf',
  }[extension] || 'application/octet-stream';
}

function normalizePath(value: string) {
  return value.split(path.sep).join('/');
}

function setSecurityHeaders(res: any) {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'same-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
}

function json(res: any, status: number, payload: unknown) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

function serializeCookie(name: string, value: string, maxAgeMs: number) {
  const secure = process.env.MONOLITH_ATLAS_COOKIE_SECURE === '1' ? '; Secure' : '';
  return `${name}=${value}; Path=/; Max-Age=${Math.floor(maxAgeMs / 1000)}; HttpOnly; SameSite=Lax${secure}`;
}

function cookieMap(req: any) {
  const header = String(req.headers.cookie || '');
  return Object.fromEntries(
    header
      .split(';')
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const index = part.indexOf('=');
        return [part.slice(0, index), decodeURIComponent(part.slice(index + 1))];
      }),
  );
}

function isSameOrigin(req: any) {
  const origin = req.headers.origin;
  if (!origin) return true;
  const host = req.headers.host;
  try {
    return new URL(origin).host === host;
  } catch {
    return false;
  }
}

async function readJsonBody(req: any) {
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) throw new Error('request_body_too_large');
    chunks.push(Buffer.from(chunk));
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
}

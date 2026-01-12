import express, { Request, Response } from 'express';
import cors from 'cors';
import path from 'path';
import https from 'https';
import fs from 'fs';
import { spawn } from 'child_process';
import { ethers } from 'ethers';
import { SettingsManager } from './settingsManager';
import { OpenSeaHttpError, openseaRequestJson } from './scripts/openseaHttp';
import { findMatchingDocIdsFromGiga } from './scripts/gigaCompanion';
import { getNextApiKey } from './openseaKeys';

const DEFAULT_GIGA_URL = 'https://api.giga-companion.online/egg/pets-by-id';
const OPENSEA_API_BASE = 'https://api.opensea.io';
const metadataMaxSocketsRaw = Number(process.env.OPENSEA_TRAITS_METADATA_MAX_SOCKETS ?? '120');
const METADATA_AGENT = new https.Agent({
  keepAlive: true,
  maxSockets: Number.isFinite(metadataMaxSocketsRaw) && metadataMaxSocketsRaw > 0 ? Math.floor(metadataMaxSocketsRaw) : 120
});

async function openseaGetJsonWithTimeoutRetries<T>(
  url: string,
  params: { apiKey: string; timeoutMs: number; attempts?: number; baseDelayMs?: number }
): Promise<T> {
  const attempts = Math.max(1, Math.min(6, Math.floor(params.attempts ?? 3)));
  const baseDelayMs = Math.max(200, Math.min(5000, Math.floor(params.baseDelayMs ?? 700)));
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await openseaRequestJson<T>(url, { apiKey: params.apiKey, retries: 0, timeoutMs: params.timeoutMs });
    } catch (err) {
      lastErr = err;
      const msg = err instanceof Error ? err.message : String(err);
      const isTimeout = msg.includes('timed out') || msg.includes('timed out after') || msg.includes('timeout');
      const status = err instanceof OpenSeaHttpError ? err.statusCode : undefined;
      const retryableHttp = status === 429 || (typeof status === 'number' && status >= 500 && status <= 599);
      if (!isTimeout && !retryableHttp) throw err;
      const baseBackoff = baseDelayMs * (i + 1);
      const backoff =
        err instanceof OpenSeaHttpError && err.statusCode === 429
          ? err.retryAfterMs(baseBackoff)
          : baseBackoff;
      await new Promise((r) => setTimeout(r, backoff + Math.floor(Math.random() * 250)));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

function parseCsvNumbers(input: string | undefined): number[] | undefined {
  if (!input) return undefined;
  const out = new Set<number>();
  for (const part of input.split(',')) {
    const raw = part.trim();
    if (!raw) continue;
    const rangeMatch = raw.match(/^(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)$/);
    if (rangeMatch) {
      const a = Number(rangeMatch[1]);
      const b = Number(rangeMatch[2]);
      if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
      const start = Math.floor(Math.min(a, b));
      const end = Math.floor(Math.max(a, b));
      const maxExpand = 5000;
      if (end - start > maxExpand) continue;
      for (let n = start; n <= end; n++) out.add(n);
      continue;
    }
    const n = Number(raw);
    if (Number.isFinite(n)) out.add(Math.floor(n));
  }
  const nums = [...out];
  return nums.length ? nums : undefined;
}

function normalizeAddressSafe(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  try {
    return ethers.getAddress(trimmed);
  } catch {
    return undefined;
  }
}

type TraitFilter = { traitType: string; values: string[] } | { traitType: string; min?: number; max?: number };

function parseTraitFilters(raw: any): TraitFilter[] {
  const filters: TraitFilter[] = [];
  if (!Array.isArray(raw)) return filters;
  for (const t of raw) {
    const traitType = String(t?.traitType ?? '').trim();
    const rawMin = t?.min ?? t?.minValue;
    const rawMax = t?.max ?? t?.maxValue;
    const min = rawMin === undefined || rawMin === null || rawMin === '' ? undefined : Number(rawMin);
    const max = rawMax === undefined || rawMax === null || rawMax === '' ? undefined : Number(rawMax);
    const hasMin = min !== undefined && Number.isFinite(min);
    const hasMax = max !== undefined && Number.isFinite(max);
    const rawValues = t?.values ?? t?.valuesCsv ?? t?.value;
    let values: string[] = [];
    if (Array.isArray(rawValues)) {
      values = rawValues.map((v) => String(v).trim()).filter(Boolean);
    } else if (typeof rawValues === 'string') {
      values = rawValues
        .split(',')
        .map((v) => v.trim())
        .filter(Boolean);
    }
    if (!traitType) continue;
    if (hasMin || hasMax) {
      filters.push({ traitType, min: hasMin ? min : undefined, max: hasMax ? max : undefined });
      continue;
    }
    if (values.length === 1) {
      const m = values[0].match(/^(-?\\d+(?:\\.\\d+)?)\\s*[-–]\\s*(-?\\d+(?:\\.\\d+)?)$/);
      if (m) {
        const a = Number(m[1]);
        const b = Number(m[2]);
        if (Number.isFinite(a) || Number.isFinite(b)) {
          filters.push({ traitType, min: Number.isFinite(a) ? a : undefined, max: Number.isFinite(b) ? b : undefined });
          continue;
        }
      }
    }
    if (values.length === 0) continue;
    filters.push({ traitType, values });
  }
  return filters;
}

function extractTraitsMap(nft: any): Map<string, string> {
  const traits: any[] =
    nft?.nft?.traits ??
    nft?.nft?.metadata?.traits ??
    nft?.traits ??
    nft?.metadata?.traits ??
    nft?.attributes ??
    nft?.metadata?.attributes ??
    [];
  const map = new Map<string, string>();
  if (!Array.isArray(traits)) return map;
  for (const t of traits) {
    const type = t?.trait_type ?? t?.traitType ?? t?.type;
    const value = t?.value;
    if (typeof type !== 'string') continue;
    if (typeof value !== 'string' && typeof value !== 'number') continue;
    const normalizedType = type.trim();
    const normalizedValue = String(value).trim();
    if (!normalizedType || !normalizedValue) continue;
    map.set(normalizedType, normalizedValue);
    map.set(normalizedType.toLowerCase(), normalizedValue);
  }
  return map;
}

async function fetchTraitsFromMetadataUrl(url: string): Promise<Map<string, string>> {
  const timeoutMs = Math.max(1500, Math.min(20_000, Number(process.env.OPENSEA_TRAITS_METADATA_TIMEOUT_MS ?? '3000')));
  const retriesRaw = Number(process.env.OPENSEA_TRAITS_METADATA_RETRIES ?? '2');
  const retries = Number.isFinite(retriesRaw) && retriesRaw >= 0 ? Math.min(5, Math.floor(retriesRaw)) : 2;
  const maxRedirects = 3;

  const attemptOnce = async (
    u: string,
    redirectsLeft: number
  ): Promise<{ map: Map<string, string>; retryAfterMs?: number; retryable: boolean }> =>
    await new Promise((resolve) => {
      const done = (map: Map<string, string>, retryable: boolean, retryAfterMs?: number) =>
        resolve({ map, retryable, retryAfterMs });

      try {
        const parsed = new URL(u);
        const req = https.request(
          {
            method: 'GET',
            protocol: parsed.protocol,
            hostname: parsed.hostname,
            path: `${parsed.pathname}${parsed.search}`,
            timeout: timeoutMs,
            agent: METADATA_AGENT,
            headers: { accept: 'application/json', 'user-agent': 'Mozilla/5.0 (compatible; OpenSeaTraitsFetcher/1.0)' }
          },
          (res) => {
            const status = res.statusCode ?? 0;
            const loc = res.headers.location;
            if ((status === 301 || status === 302 || status === 307 || status === 308) && typeof loc === 'string' && redirectsLeft > 0) {
              res.resume();
              const nextUrl = new URL(loc, u).toString();
              void attemptOnce(nextUrl, redirectsLeft - 1).then(resolve);
              return;
            }

            const retryAfterHeader = res.headers['retry-after'];
            const retryAfterSeconds = Number(Array.isArray(retryAfterHeader) ? retryAfterHeader[0] : retryAfterHeader);
            const retryAfterMs =
              Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0 ? Math.floor(retryAfterSeconds * 1000) : undefined;

            // Retry on rate-limit and transient upstream failures.
            if (status === 429 || (status >= 500 && status <= 599)) {
              res.resume();
              done(new Map(), true, retryAfterMs);
              return;
            }

            // Non-2xx responses are not useful; don't retry most 4xx.
            if (status < 200 || status >= 300) {
              res.resume();
              done(new Map(), false);
              return;
            }

            const chunks: Buffer[] = [];
            res.on('data', (d) => chunks.push(d));
            res.on('end', () => {
              const text = Buffer.concat(chunks).toString('utf8');
              try {
                const json = JSON.parse(text);
                done(extractTraitsMap(json), false);
              } catch {
                done(new Map(), false);
              }
            });
          }
        );
        req.on('error', () => done(new Map(), true));
        req.on('timeout', () => {
          req.destroy();
          done(new Map(), true);
        });
        req.end();
      } catch {
        done(new Map(), false);
      }
    });

  for (let i = 0; i <= retries; i++) {
    const { map, retryable, retryAfterMs } = await attemptOnce(url, maxRedirects);
    if (map.size) return map;
    if (!retryable || i === retries) break;
    const base = 250 + i * 300;
    const wait = Math.min(10_000, retryAfterMs ?? base);
    const jitter = Math.floor(Math.random() * 200);
    await new Promise((r) => setTimeout(r, wait + jitter));
  }
  return new Map();
}

async function fetchCollectionBasics(apiKey: string, slug: string): Promise<{ contract?: string; chain?: string; paymentToken?: string }> {
  const url = `${OPENSEA_API_BASE}/api/v2/collections/${encodeURIComponent(slug)}`;
  const data = await openseaRequestJson<any>(url, { apiKey, retries: 3, retryDelayMs: 900, timeoutMs: 20_000 });
  const contracts = Array.isArray((data as any)?.contracts) ? (data as any).contracts : [];

  let contract: string | undefined;
  let chain: string | undefined;
  for (const c of contracts) {
    if (!contract) contract = normalizeAddressSafe((c as any)?.address ?? (c as any)?.contract ?? (c as any)?.contract_address);
    if (!chain) chain = (c as any)?.chain ?? (c as any)?.chain_identifier ?? (c as any)?.network;
    if (contract && chain) break;
  }

  const paymentTokenCandidates: any[] = [];
  if (Array.isArray((data as any)?.payment_tokens)) paymentTokenCandidates.push(...(data as any).payment_tokens);
  if (Array.isArray((data as any)?.project?.payment_tokens)) paymentTokenCandidates.push(...(data as any).project.payment_tokens);

  let paymentToken: string | undefined;
  for (const token of paymentTokenCandidates) {
    const addr = normalizeAddressSafe((token as any)?.address ?? (token as any)?.token_address ?? token);
    if (addr) {
      paymentToken = addr;
      break;
    }
  }

  return { contract, chain, paymentToken };
}

async function detectPaymentTokenForCollection(apiKey: string, slug: string, options?: { maxPages?: number }): Promise<string | undefined> {
  const maxPages = Math.max(1, Math.min(10, Math.floor(options?.maxPages ?? 2)));
  let next: string | undefined;
  let bestPayWei = 0n;
  let bestQty = 1n;
  let bestToken: string | undefined;

  for (let page = 1; page <= maxPages; page++) {
    const url = new URL(`${OPENSEA_API_BASE}/api/v2/offers/collection/${encodeURIComponent(slug)}`);
    url.searchParams.set('limit', '50');
    if (next) url.searchParams.set('next', next);

    const data = await openseaRequestJson<any>(String(url), { apiKey, retries: 3, retryDelayMs: 900, timeoutMs: 20_000 });
    const offers = Array.isArray(data?.offers) ? data.offers : [];
    next = typeof data?.next === 'string' ? data.next : undefined;

    for (const o of offers) {
      const offerItem = o?.protocol_data?.parameters?.offer?.[0];
      const payAddr = normalizeAddressSafe(offerItem?.token);
      const amountRaw = offerItem?.endAmount ?? offerItem?.startAmount;
      const qtyRaw =
        (o as any)?.protocol_data?.parameters?.consideration?.[0]?.endAmount ??
        (o as any)?.protocol_data?.parameters?.consideration?.[0]?.startAmount ??
        '1';
      if (!payAddr || amountRaw === undefined) continue;

      try {
        const payWei = BigInt(String(amountRaw));
        const qty = (() => {
          try {
            const parsed = BigInt(String(qtyRaw));
            return parsed > 0n ? parsed : 1n;
          } catch {
            return 1n;
          }
        })();

        if (payWei * bestQty > bestPayWei * qty) {
          bestPayWei = payWei;
          bestQty = qty;
          bestToken = payAddr;
        }
      } catch {
        continue;
      }
    }

    if (!next) break;
  }

  return bestToken;
}

async function fetchTokensByTraits(params: {
  apiKey: string;
  slug: string;
  filters: TraitFilter[];
  maxResults: number;
  log?: (line: string) => void;
}): Promise<{ tokens: string[]; contract?: string; chain?: string }> {
  const limit = 200; // OpenSea max per page
  const maxResults = Number.isFinite(params.maxResults) ? Math.max(1, Math.floor(params.maxResults)) : Number.POSITIVE_INFINITY;
  const tokens = new Set<string>();
  const seen = new Set<string>();
  let contract: string | undefined;
  let chain: string | undefined;
  let next: string | undefined;
  let page = 0;
  let emptyPages = 0;
  let dupPages = 0;
  const startedAt = Date.now();
  const seenCursors = new Set<string>();
  const logEveryPagesRaw = Number(process.env.OPENSEA_TRAITS_MATCH_LOG_EVERY_PAGES ?? '5');
  const logEveryPages = Number.isFinite(logEveryPagesRaw) && logEveryPagesRaw > 0 ? Math.floor(logEveryPagesRaw) : 5;
  const log = params.log;
  const includeMetaTimeoutMsRaw = Number(process.env.OPENSEA_COLLECTION_NFTS_INCLUDE_METADATA_TIMEOUT_MS ?? '20000');
  const includeMetaTimeoutMs =
    Number.isFinite(includeMetaTimeoutMsRaw) && includeMetaTimeoutMsRaw > 0 ? Math.floor(includeMetaTimeoutMsRaw) : 20_000;
  let allowIncludeMetadata = true;
  const pageDelayMsEnv = Number(process.env.OPENSEA_COLLECTION_NFTS_PAGE_DELAY_MS ?? '0');
  let pageDelayMs = Number.isFinite(pageDelayMsEnv) && pageDelayMsEnv >= 0 ? Math.floor(pageDelayMsEnv) : 0;

  const enumWants = params.filters
    .filter((f): f is { traitType: string; values: string[] } => Array.isArray((f as any)?.values))
    .map((f) => ({
      key: String(f.traitType ?? '').trim(),
      valuesRaw: (f.values ?? []).map((v) => String(v).trim()).filter(Boolean),
      allowed: new Set((f.values ?? []).map((v) => String(v).trim().toLowerCase()).filter(Boolean))
    }))
    .filter((f) => f.key.length > 0 && f.allowed.size > 0);

  const rangeWants = params.filters
    .filter((f): f is { traitType: string; min?: number; max?: number } => !Array.isArray((f as any)?.values))
    .map((f) => ({
      key: String(f.traitType ?? '').trim(),
      min: typeof f.min === 'number' && Number.isFinite(f.min) ? f.min : undefined,
      max: typeof f.max === 'number' && Number.isFinite(f.max) ? f.max : undefined
    }))
    .filter((f) => f.key.length > 0 && (f.min !== undefined || f.max !== undefined));

  const verify = async (item: { id: string; metadataUrl?: string; traits?: Map<string, string> }): Promise<boolean> => {
    if (!enumWants.length && !rangeWants.length) return true;

    // Prefer traits embedded in OpenSea responses (when include_metadata is supported),
    // and only fall back to per-token metadata_url fetch if required traits are missing.
    let traitMap = item.traits?.size ? item.traits : new Map<string, string>();
    const needsMetadataFetch = (() => {
      for (const f of enumWants) {
        const actual = traitMap.get(f.key) ?? traitMap.get(f.key.toLowerCase());
        if (!actual) return true;
      }
      for (const f of rangeWants) {
        const actual = traitMap.get(f.key) ?? traitMap.get(f.key.toLowerCase());
        if (!actual) return true;
      }
      return false;
    })();
    if (needsMetadataFetch && item.metadataUrl) {
      const fromMeta = await fetchTraitsFromMetadataUrl(item.metadataUrl);
      if (fromMeta.size) traitMap = fromMeta;
    }
    if (!traitMap.size) return false;

    for (const f of enumWants) {
      const actualRaw = traitMap.get(f.key) ?? traitMap.get(f.key.toLowerCase());
      if (!actualRaw) return false;
      const normalizedActual = String(actualRaw).trim().toLowerCase();
      if (!f.allowed.has(normalizedActual)) return false;
    }

    for (const f of rangeWants) {
      const actualRaw = traitMap.get(f.key) ?? traitMap.get(f.key.toLowerCase());
      if (!actualRaw) return false;
      const n = Number(actualRaw);
      if (!Number.isFinite(n)) return false;
      if (f.min !== undefined && n < f.min) return false;
      if (f.max !== undefined && n > f.max) return false;
    }
    return true;
  };

  const concurrencyEnv = Number(process.env.OPENSEA_TRAITS_METADATA_CONCURRENCY ?? '');
  const maxSockets = typeof (METADATA_AGENT as any)?.maxSockets === 'number' ? (METADATA_AGENT as any).maxSockets : 200;
  const concurrency =
    Number.isFinite(concurrencyEnv) && concurrencyEnv > 0
      ? Math.min(200, Math.floor(concurrencyEnv), maxSockets)
      : Math.min(60, maxSockets);

  while (tokens.size < maxResults) {
    page += 1;
    if (next) {
      if (seenCursors.has(next)) {
        log?.(`[traits/match ${params.slug}] stuck pagination: repeated cursor on page=${page} (matched=${tokens.size}, scanned=${seen.size}).`);
        break;
      }
      seenCursors.add(next);
    }

    const seenBeforePage = seen.size;
    const timeoutMsRaw = Number(process.env.OPENSEA_COLLECTION_NFTS_TIMEOUT_MS ?? '60000');
    const timeoutMs = Number.isFinite(timeoutMsRaw) && timeoutMsRaw > 0 ? Math.floor(timeoutMsRaw) : 60_000;

    const baseUrl = new URL(`${OPENSEA_API_BASE}/api/v2/collection/${encodeURIComponent(params.slug)}/nfts`);
    baseUrl.searchParams.set('limit', String(limit));
    if (next) baseUrl.searchParams.set('next', next);

    const wantsTraits = enumWants.length > 0 || rangeWants.length > 0;
    const fetchPage = async (withMeta: boolean): Promise<any> => {
      const u = new URL(String(baseUrl));
      if (withMeta) u.searchParams.set('include_metadata', 'true');
      const effectiveTimeout = withMeta ? Math.min(timeoutMs, includeMetaTimeoutMs) : timeoutMs;
      const attempts = withMeta ? 1 : 3;
      return await openseaGetJsonWithTimeoutRetries<any>(String(u), {
        apiKey: params.apiKey,
        timeoutMs: effectiveTimeout,
        attempts,
        baseDelayMs: 900
      });
    };

    let data: any;
    // Keep retrying the page on OpenSea 429 (rate limit), and optionally fall back from include_metadata.
    // This endpoint can scan 30k+ NFTs; a single transient 429 should not abort the whole match job.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const tryIncludeMeta = wantsTraits && allowIncludeMetadata;
      try {
        data = await fetchPage(tryIncludeMeta);
        break;
      } catch (err: any) {
        const msg = err instanceof Error ? err.message : String(err);
        const isTimeout = msg.includes('timed out') || msg.includes('timed out after') || msg.includes('timeout');
        const isRateLimited = err instanceof OpenSeaHttpError && err.statusCode === 429;

        if (tryIncludeMeta && (isTimeout || isRateLimited)) {
          log?.(
            `[traits/match ${params.slug}] include_metadata ${isRateLimited ? 'rate-limited' : 'timed out'}; retrying without include_metadata`
          );
          allowIncludeMetadata = false;
          continue;
        }

        if (isRateLimited) {
          const base = 5000;
          const waitMs = err instanceof OpenSeaHttpError ? err.retryAfterMs(base) : base;
          // After a 429, slow down subsequent page fetches to reduce repeated rate limits.
          pageDelayMs = Math.max(pageDelayMs, Math.min(5000, waitMs));
          log?.(`[traits/match ${params.slug}] rate-limited (429); retrying page=${page} in ${Math.ceil(waitMs / 1000)}s`);
          await new Promise((r) => setTimeout(r, waitMs + Math.floor(Math.random() * 250)));
          continue;
        }

        throw err;
      }
    }

    const nfts = Array.isArray(data?.nfts) ? data.nfts : [];
    next = typeof data?.next === 'string' ? data.next : undefined;
    if (!nfts.length) {
      emptyPages += 1;
      if (emptyPages === 1 || emptyPages % logEveryPages === 0) {
        log?.(
          `[traits/match ${params.slug}] page=${page} empty (matched=${tokens.size}, scanned=${seen.size}) next=${next ? 'yes' : 'no'}`
        );
      }
      if (!next) break;
      // Safety stop: avoid looping forever when OpenSea returns empty pages with a cursor.
      if (emptyPages >= 8) break;
      continue;
    }
    emptyPages = 0;

    const items: Array<{ id: string; metadataUrl?: string; traits?: Map<string, string> }> = [];
    for (const nft of nfts) {
      const id = String((nft as any)?.identifier ?? (nft as any)?.token_id ?? (nft as any)?.id ?? '').trim();
      if (!id || seen.has(id)) continue;
      seen.add(id);
      if (!enumWants.length && !rangeWants.length) {
        tokens.add(id);
        if (tokens.size >= maxResults) break;
      } else {
        const traitMap = extractTraitsMap(nft);
        items.push({
          id,
          metadataUrl: typeof (nft as any)?.metadata_url === 'string' ? (nft as any).metadata_url.trim() : undefined,
          traits: traitMap.size ? traitMap : undefined
        });
      }
      if (!contract) {
        contract =
          normalizeAddressSafe((nft as any)?.contract?.address) ??
          normalizeAddressSafe((nft as any)?.asset_contract?.address) ??
          normalizeAddressSafe((nft as any)?.nft?.contract_address);
      }
      if (!chain) {
        chain = (nft as any)?.contract?.chain ?? (nft as any)?.chain ?? (nft as any)?.asset_contract?.chain_identifier ?? (nft as any)?.asset_contract?.chain;
      }
    }

    const newSeen = seen.size - seenBeforePage;
    if (newSeen <= 0) {
      dupPages += 1;
      if (dupPages === 1 || dupPages % logEveryPages === 0) {
        log?.(`[traits/match ${params.slug}] page=${page} duplicate (matched=${tokens.size}, scanned=${seen.size})`);
      }
      if (dupPages >= 3) break;
    } else {
      dupPages = 0;
    }

    if (page === 1 || page % logEveryPages === 0) {
      const durSec = Math.floor((Date.now() - startedAt) / 1000);
      log?.(`[traits/match ${params.slug}] page=${page} scanned=${seen.size} matched=${tokens.size} dur=${durSec}s`);
    }

    if ((enumWants.length || rangeWants.length) && items.length) {
      let cursor = 0;
      const worker = async () => {
        while (tokens.size < maxResults) {
          const i = cursor++;
          if (i >= items.length) return;
          const item = items[i];
          try {
            if (await verify(item)) tokens.add(item.id);
          } catch {
            // ignore per-item errors
          }
        }
      };
      await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
    }

    if (tokens.size >= maxResults) break;
    if (!next) break;

    if (pageDelayMs > 0) {
      await new Promise((r) => setTimeout(r, pageDelayMs));
    }
  }

  const durSec = Math.floor((Date.now() - startedAt) / 1000);
  log?.(`[traits/match ${params.slug}] done page=${page} scanned=${seen.size} matched=${tokens.size} dur=${durSec}s`);
  return { tokens: Array.from(tokens), contract, chain };
}

export class WebServer {
  private app: express.Application;
  private port: number;
  private settingsManager: SettingsManager;
  private botInstance: any;
  private logs: Array<{ id: number; line: string }> = [];
  private logSeq: number = 0;
  private botStatus: {
    isRunning: boolean;
    collections: string[];
    floors: any;
    lastEvents: any[];
  };

  constructor(settingsManager: SettingsManager, port: number = 3000) {
    this.app = express();
    this.port = port;
    this.settingsManager = settingsManager;
    this.botStatus = {
      isRunning: false,
      collections: [],
      floors: {},
      lastEvents: []
    };

    this.setupMiddleware();
    this.setupRoutes();
  }

  private setupMiddleware(): void {
    this.app.use(cors());
    this.app.use(express.json({ limit: '20mb' }));
    // Avoid stale cached frontend assets during rapid iteration.
    this.app.use((req, res, next) => {
      if (req.path === '/' || req.path.endsWith('.html') || req.path.endsWith('.js') || req.path.endsWith('.css')) {
        res.setHeader('Cache-Control', 'no-store');
      }
      next();
    });
    this.app.use(express.static(path.join(__dirname, '../public')));
  }

  private setupRoutes(): void {
    // Serve dashboard
    this.app.get('/', (req: Request, res: Response) => {
      res.sendFile(path.join(__dirname, '../public/index.html'));
    });

    // Get current settings
    this.app.get('/api/settings', (req: Request, res: Response) => {
      res.json(this.settingsManager.getSettings());
    });

    // Update settings
    this.app.post('/api/settings', (req: Request, res: Response) => {
      try {
        this.settingsManager.saveSettings(req.body);
        res.json({ success: true, message: 'Settings saved successfully' });
      } catch (error) {
        res.status(500).json({ success: false, message: 'Failed to save settings' });
      }
    });

    // Get bot status
    this.app.get('/api/status', (req: Request, res: Response) => {
      res.json(this.botStatus);
    });

    // Get floor prices
    this.app.get('/api/floors', (req: Request, res: Response) => {
      res.json(this.botStatus.floors);
    });

    // Get recent events
    this.app.get('/api/events', (req: Request, res: Response) => {
      res.json(this.botStatus.lastEvents.slice(-50)); // Last 50 events
    });

    // Simple server log buffer for UI console
    this.app.get('/api/logs', (req: Request, res: Response) => {
      const after = Number(req.query.after ?? 0);
      const afterId = Number.isFinite(after) && after > 0 ? Math.floor(after) : 0;
      const limit = Number(req.query.limit ?? 200);
      const n = Number.isFinite(limit) && limit > 0 ? Math.min(2000, Math.floor(limit)) : 200;
      const source = afterId > 0 ? this.logs.filter((l) => l.id > afterId) : this.logs.slice(-n);
      const lines = source.map((l) => l.line);
      res.json({ lines, lastId: this.logSeq });
    });

    this.app.post('/api/logs/clear', (req: Request, res: Response) => {
      this.logs = [];
      this.logSeq = 0;
      res.json({ success: true });
    });

    // Start listening
    this.app.post('/api/start', (req: Request, res: Response) => {
      if (this.botInstance) {
        this.botInstance.startListening();
        res.json({ success: true, message: 'Bot started listening' });
      } else {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
      }
    });

    // Stop listening
    this.app.post('/api/stop', (req: Request, res: Response) => {
      if (this.botInstance) {
        this.botInstance.stopListening();
        res.json({ success: true, message: 'Bot stopped listening' });
      } else {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
      }
    });

    // Test Telegram notification
    this.app.post('/api/test-telegram', async (req: Request, res: Response) => {
      const { botToken, chatId } = req.body;
      try {
        const https = await import('https');
        const url = `https://api.telegram.org/bot${botToken}/sendMessage`;
        const data = JSON.stringify({
          chat_id: chatId,
          text: '✅ Test notification from OpenSea Floor Monitor!'
        });

        const urlObj = new URL(url);
        const options = {
          hostname: urlObj.hostname,
          path: urlObj.pathname,
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(data)
          }
        };

        const req2 = https.request(options, (res2) => {
          let responseData = '';
          res2.on('data', (chunk) => (responseData += chunk));
          res2.on('end', () => {
            if (res2.statusCode === 200) {
              res.json({ success: true, message: 'Test message sent!' });
            } else {
              res.status(400).json({ success: false, message: 'Failed to send test message' });
            }
          });
        });

        req2.on('error', (error) => {
          res.status(500).json({ success: false, message: error.message });
        });

        req2.write(data);
        req2.end();
      } catch (error: any) {
        res.status(500).json({ success: false, message: error.message });
      }
    });

    // Offer settings (env-backed defaults for UI)
    this.app.get('/api/offer-settings', (req: Request, res: Response) => {
      const settings = this.settingsManager.getSettings();
      res.json({
        offerCollectionSlug: settings.offerCollectionSlug,
        offerChain: settings.offerChain,
        offerContract: settings.offerContract,
        offerPrice: settings.offerPrice,
        offerDurationMinutes: settings.offerDurationMinutes,
        offerGigaUrl: settings.offerGigaUrl,
        offerGigaGender: settings.offerGigaGender,
        offerGigaFactions: settings.offerGigaFactions,
        offerGigaRarities: settings.offerGigaRarities,
        offerPostDelayMs: settings.offerPostDelayMs,
        offerPaymentToken: settings.offerPaymentToken,
        offerTokenIds: settings.offerTokenIds,
        offerUseTokenIdsOverride: settings.offerUseTokenIdsOverride ?? false
      });
    });

    // Fetch IDs from Giga companion API (filters only)
    this.app.post('/api/giga/ids', async (req: Request, res: Response) => {
      try {
        const settings = this.settingsManager.getSettings();
        const url = settings.offerGigaUrl ?? DEFAULT_GIGA_URL;
        const gender = String(req.body?.offerGigaGender ?? '').trim();
        const factions = parseCsvNumbers(String(req.body?.offerGigaFactions ?? ''));
        const rarities = parseCsvNumbers(String(req.body?.offerGigaRarities ?? ''));
        const ids = await findMatchingDocIdsFromGiga({
          url,
          filters: {
            complete: true,
            gender: gender || undefined,
            factions,
            rarities
          }
        });
        res.json({ success: true, ids });
      } catch (err) {
        const msg = (err as Error)?.message ?? 'Fetch IDs failed';
        console.error('[giga/ids] error:', msg);
        res.status(400).json({ success: false, message: msg });
      }
    });

    // Traits helper (for UI)
    this.app.get('/api/traits', async (req: Request, res: Response) => {
      const apiKey = getNextApiKey();
      if (!apiKey) {
        res.status(400).json({ success: false, message: 'OPENSEA_API_KEY is required' });
        return;
      }
      const slug = String(req.query.slug ?? '').trim();
      if (!slug) {
        res.status(400).json({ success: false, message: 'slug is required' });
        return;
      }
      try {
        const url = `https://api.opensea.io/api/v2/traits/${encodeURIComponent(slug)}`;
        const data = await openseaRequestJson<any>(url, { apiKey, retries: 4, retryDelayMs: 900, timeoutMs: 20_000 });
        res.json({ success: true, data });
      } catch (err: any) {
        res.status(400).json({ success: false, message: err?.message ?? 'Failed to fetch traits' });
      }
    });

    // Match token IDs for a collection by trait filters (OpenSea API)
    this.app.post('/api/traits/match', async (req: Request, res: Response) => {
      const apiKey = getNextApiKey();
      if (!apiKey) {
        res.status(400).json({ success: false, message: 'OPENSEA_API_KEY is required' });
        return;
      }
      const slug = String(req.body?.slug ?? '').trim();
      if (!slug) {
        res.status(400).json({ success: false, message: 'slug is required' });
        return;
      }
      const filters = parseTraitFilters(req.body?.traits);
      if (!filters.length) {
        res.status(400).json({ success: false, message: 'At least one trait filter is required' });
        return;
      }
      const rawMaxResults = req.body?.maxResults;
      const maxResultsRaw =
        rawMaxResults === undefined || rawMaxResults === null || rawMaxResults === '' ? undefined : Number(rawMaxResults);
      let maxResults: number;
      if (Number.isFinite(maxResultsRaw) && (maxResultsRaw as number) > 0) {
        maxResults = Math.floor(maxResultsRaw as number);
      } else {
        maxResults = Number.POSITIVE_INFINITY;
      }

      // Some deployments proxy this app under a sub-path (e.g. /opensea/) via nginx.
      // Large collections can take >60s to match, which can trigger proxy timeouts (504) if the upstream is silent.
      // To avoid that, we stream tiny whitespace keep-alive chunks and finish with a normal JSON payload.
      res.status(200);
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Cache-Control', 'no-store');
      let keepAliveTimer: NodeJS.Timeout | undefined;
      const keepAliveMsRaw = Number(process.env.TRAITS_MATCH_KEEPALIVE_MS ?? '10000');
      const keepAliveMs = Number.isFinite(keepAliveMsRaw) && keepAliveMsRaw > 0 ? Math.floor(keepAliveMsRaw) : 10_000;
      const stopKeepAlive = () => {
        if (keepAliveTimer) clearInterval(keepAliveTimer);
        keepAliveTimer = undefined;
      };
      keepAliveTimer = setInterval(() => {
        try {
          if ((res as any).writableEnded || (res as any).destroyed) return stopKeepAlive();
          res.write(' \n');
        } catch {
          stopKeepAlive();
        }
      }, keepAliveMs);

      try {
        this.addLog(`[traits/match ${slug}] start`);
        // If maxResults isn't provided, try to estimate an upper bound from the traits endpoint to avoid scanning the full collection.
        if (!Number.isFinite(maxResults)) {
          try {
            const traitsUrl = `https://api.opensea.io/api/v2/traits/${encodeURIComponent(slug)}`;
            const traits = await openseaRequestJson<any>(traitsUrl, { apiKey, retries: 2, retryDelayMs: 900, timeoutMs: 20_000 });
            const counts = traits?.counts ?? {};
            const bounds: number[] = [];
            for (const f of filters) {
              if (!Array.isArray((f as any)?.values)) continue;
              const traitType = (f as any).traitType;
              const values: string[] = (f as any).values ?? [];
              const per = counts?.[traitType];
              if (!per || typeof per !== 'object') continue;
              const perLower = new Map<string, number>();
              for (const [k, v] of Object.entries(per as any)) {
                if (typeof v === 'number' && Number.isFinite(v)) perLower.set(String(k).trim().toLowerCase(), v);
              }
              let sum = 0;
              for (const v of values) {
                const direct = (per as any)[v];
                if (typeof direct === 'number' && Number.isFinite(direct)) {
                  sum += direct;
                  continue;
                }
                const c = perLower.get(String(v).trim().toLowerCase());
                if (typeof c === 'number' && Number.isFinite(c)) sum += c;
              }
              if (sum > 0) bounds.push(sum);
            }
            if (bounds.length) {
              const upper = Math.min(...bounds);
              if (Number.isFinite(upper) && upper > 0) maxResults = upper;
            }
          } catch {
            // ignore estimation errors; fall back to full scan
          }
        }

        const basics = await fetchCollectionBasics(apiKey, slug).catch(() => ({}));
        const matches = await fetchTokensByTraits({ apiKey, slug, filters, maxResults, log: (line) => this.addLog(line) });
        const detectedPaymentToken = await detectPaymentTokenForCollection(apiKey, slug).catch(() => undefined);

        stopKeepAlive();
        res.end(
          JSON.stringify({
            success: true,
            tokens: matches.tokens,
            contract: matches.contract ?? (basics as any)?.contract,
            chain: matches.chain ?? (basics as any)?.chain,
            paymentToken: detectedPaymentToken ?? (basics as any)?.paymentToken,
            count: matches.tokens.length
          })
        );
      } catch (err: any) {
        stopKeepAlive();
        res.end(JSON.stringify({ success: false, message: err?.message ?? 'Failed to fetch matches' }));
      }
    });

    // Trait buyer controls (buys listings that match trait filters)
    this.app.post('/api/buyer/start', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      try {
        this.botInstance.startTraitBuyer(req.body ?? {});
        res.json({ success: true });
      } catch (err: any) {
        res.status(400).json({ success: false, message: err?.message ?? 'Failed to start buyer' });
      }
    });

    this.app.post('/api/buyer/stop', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      this.botInstance.stopTraitBuyer();
      res.json({ success: true });
    });

    this.app.get('/api/buyer/status', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      res.json(this.botInstance.getTraitBuyerStatus?.() ?? { isRunning: false });
    });

    // Multi-instance buyer (per tab/block)
    this.app.post('/api/buyer/instance/start', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      const key = String(req.body?.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'key is required' });
        return;
      }
      try {
        this.botInstance.startBuyerInstance(key, req.body?.settings ?? {});
        res.json({ success: true });
      } catch (err: any) {
        res.status(400).json({ success: false, message: err?.message ?? 'Failed to start buyer instance' });
      }
    });

    this.app.post('/api/buyer/instance/stop', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      const key = String(req.body?.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'key is required' });
        return;
      }
      this.botInstance.stopBuyerInstance(key);
      res.json({ success: true });
    });

    this.app.post('/api/buyer/instance/delete', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      const key = String(req.body?.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'key is required' });
        return;
      }
      this.botInstance.deleteBuyerInstance(key);
      res.json({ success: true });
    });

    this.app.get('/api/buyer/instance/status', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      const key = String(req.query.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'key is required' });
        return;
      }
      res.json(this.botInstance.getBuyerInstanceStatus(key));
    });

    this.app.get('/api/buyer/instances', (req: Request, res: Response) => {
      if (!this.botInstance) {
        res.status(500).json({ success: false, message: 'Bot instance not available' });
        return;
      }
      res.json(this.botInstance.listBuyerInstances());
    });

    this.app.post('/api/offer/run', async (req: Request, res: Response) => {
      if (process.env.ENABLE_OFFER_UI !== 'true') {
        res.status(403).json({ success: false, message: 'Set ENABLE_OFFER_UI=true to use offer UI endpoints' });
        return;
      }
      const result = await this.runOfferScript({ dryRun: false, overrideSettings: req.body ?? {} });
      if (result.exitCode === 0) {
        res.json({ success: true, output: result.output });
        return;
      }
      if (result.output.includes('OpenSea HTTP 429')) {
        res.status(429).json({ success: false, message: 'OpenSea rate limit exceeded. Increase scan delay / lower max scan.', output: result.output });
        return;
      }
      res.status(400).json({ success: false, message: 'Offer run failed', output: result.output });
    });

    // Cancel all Seaport orders (increment counter; 1 on-chain tx)
    this.app.post('/api/offers/cancel-all', async (req: Request, res: Response) => {
      if (process.env.ENABLE_OFFER_UI !== 'true') {
        res.status(403).json({ success: false, message: 'Set ENABLE_OFFER_UI=true to use offer UI endpoints' });
        return;
      }
      try {
        const result = await this.runCancelAllOffersScript();
        if (result.exitCode === 0) {
          res.json({ success: true, output: result.output });
          return;
        }
        res.status(400).json({ success: false, message: 'Cancel-all failed', output: result.output });
      } catch (err: any) {
        res.status(500).json({ success: false, message: err?.message ?? 'Cancel-all failed' });
      }
    });

    // Auto-bid settings + controls
    this.app.get('/api/autobid-settings', (req: Request, res: Response) => {
      const settings = this.settingsManager.getSettings();
      res.json({
        autoBidEnabled: settings.autoBidEnabled ?? false,
        autoBidTokenIds: settings.autoBidTokenIds ?? settings.offerTokenIds ?? '',
        autoBidIncrement: settings.autoBidIncrement ?? '0.0001',
        autoBidMaxPrice: settings.autoBidMaxPrice ?? '',
        autoBidCooldownSeconds: settings.autoBidCooldownSeconds ?? 45,
        autoBidCancelPrevious: settings.autoBidCancelPrevious ?? false,
        autoBidDurationMinutes: settings.autoBidDurationMinutes ?? 60 * 24,
        autoBidMode: settings.autoBidMode ?? 'bootstrap'
      });
    });

    this.app.post('/api/autobid-settings', (req: Request, res: Response) => {
      try {
        this.settingsManager.saveSettings(req.body);
        res.json({ success: true, message: 'Auto-bid settings saved' });
      } catch (error) {
        res.status(500).json({ success: false, message: 'Failed to save auto-bid settings' });
      }
    });

    this.app.get('/api/autobid/status', (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      const status = bot?.getAutoBidStatus ? bot.getAutoBidStatus() : { isRunning: false };
      res.json(status);
    });

    this.app.post('/api/autobid/start', async (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      if (!bot?.startAutoBid) {
        res.status(500).json({ success: false, message: 'Auto-bid not available' });
        return;
      }
      try {
        this.settingsManager.saveSettings({ autoBidEnabled: true });
        await bot.startAutoBid();
        const status = bot?.getAutoBidStatus ? bot.getAutoBidStatus() : { isRunning: false };
        if (!status.isRunning) {
          res.status(500).json({ success: false, message: 'Auto-bid failed to start (check server logs + settings)' });
          return;
        }
        res.json({ success: true, message: 'Auto-bid started' });
      } catch (error: any) {
        res.status(500).json({ success: false, message: error?.message ?? 'Failed to start auto-bid' });
      }
    });

    this.app.post('/api/autobid/stop', (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      if (!bot?.stopAutoBid) {
        res.status(500).json({ success: false, message: 'Auto-bid not available' });
        return;
      }
      this.settingsManager.saveSettings({ autoBidEnabled: false });
      bot.stopAutoBid();
      res.json({ success: true });
    });

    // Multi-instance auto-bid (per tab/block)
    this.app.get('/api/autobid/instances', (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      const items = bot?.listAutoBidInstances ? bot.listAutoBidInstances() : [];
      res.json({ success: true, instances: items });
    });

    this.app.get('/api/autobid/instance/status', (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      const key = String(req.query.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'Missing key' });
        return;
      }
      const status = bot?.getAutoBidInstanceStatus ? bot.getAutoBidInstanceStatus(key) : { isRunning: false };
      res.json({ success: true, ...status });
    });

    this.app.post('/api/autobid/instance/start', async (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      if (!bot?.startAutoBidInstance) {
        res.status(500).json({ success: false, message: 'Auto-bid instances not available' });
        return;
      }
      const key = String(req.body?.key ?? '').trim();
      const settings = req.body?.settings ?? {};
      if (!key) {
        res.status(400).json({ success: false, message: 'Missing key' });
        return;
      }
      try {
        await bot.startAutoBidInstance(key, settings);
        const status = bot?.getAutoBidInstanceStatus ? bot.getAutoBidInstanceStatus(key) : { isRunning: false };
        res.json({ success: true, message: 'Auto-bid instance started', ...status });
      } catch (err: any) {
        res.status(500).json({ success: false, message: err?.message ?? 'Failed to start auto-bid instance' });
      }
    });

    this.app.post('/api/autobid/instance/stop', (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      if (!bot?.stopAutoBidInstance) {
        res.status(500).json({ success: false, message: 'Auto-bid instances not available' });
        return;
      }
      const key = String(req.body?.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'Missing key' });
        return;
      }
      bot.stopAutoBidInstance(key);
      res.json({ success: true });
    });

    this.app.post('/api/autobid/instance/delete', (req: Request, res: Response) => {
      const bot: any = this.botInstance;
      if (!bot?.deleteAutoBidInstance) {
        res.status(500).json({ success: false, message: 'Auto-bid instances not available' });
        return;
      }
      const key = String(req.body?.key ?? '').trim();
      if (!key) {
        res.status(400).json({ success: false, message: 'Missing key' });
        return;
      }
      bot.deleteAutoBidInstance(key);
      res.json({ success: true });
    });
  }

  private async runCancelAllOffersScript(): Promise<{ output: string; exitCode: number }> {
    const jsPath = path.join(__dirname, 'scripts', 'cancelAllOffers.js');
    const tsPath = path.join(__dirname, '..', 'src', 'scripts', 'cancelAllOffers.ts');
    const hasJs = fs.existsSync(jsPath);

    const cmd = hasJs ? process.execPath : path.join(process.cwd(), 'node_modules', '.bin', 'ts-node');
    const cmdArgs = hasJs ? [jsPath] : [tsPath];

    return await new Promise((resolve, reject) => {
      const child = spawn(cmd, cmdArgs, {
        env: { ...process.env },
        stdio: ['ignore', 'pipe', 'pipe']
      });

      let output = '';
      child.stdout.on('data', (d) => {
        const s = d.toString('utf8');
        output += s;
        this.addLog(`[cancel] ${s}`);
      });
      child.stderr.on('data', (d) => {
        const s = d.toString('utf8');
        output += s;
        this.addLog(`[cancel] ${s}`);
      });

      child.on('error', reject);
      child.on('close', (code) => resolve({ output, exitCode: code ?? 1 }));
    });
  }

  private async runOfferScript({
    dryRun,
    overrideSettings
  }: {
    dryRun: boolean;
    overrideSettings: any;
  }): Promise<{ output: string; exitCode: number }> {
    const envDefaults = this.settingsManager.getSettings();
    const settings = { ...envDefaults, ...(overrideSettings ?? {}) };

    const price = settings.offerPrice;

    const args: string[] = [];
    if (!dryRun) {
      if (!price) throw new Error('Set offerPrice in Offer Settings (or OFFER_PRICE in .env).');
      args.push('--offer-weth', String(price));
    }

    if (settings.offerCollectionSlug) args.push('--slug', String(settings.offerCollectionSlug));
    if (settings.offerChain) args.push('--chain', String(settings.offerChain));
    if (settings.offerContract) args.push('--contract', String(settings.offerContract));
    if (settings.offerDurationMinutes !== undefined) args.push('--duration-minutes', String(settings.offerDurationMinutes));
    if (settings.offerGigaUrl) args.push('--giga-url', String(settings.offerGigaUrl));
    // Always pass Giga filters (empty string disables filter) so UI can override/clear env defaults.
    args.push('--giga-gender', String(settings.offerGigaGender ?? ''));
    args.push('--giga-factions', String(settings.offerGigaFactions ?? ''));
    args.push('--giga-rarities', String(settings.offerGigaRarities ?? ''));
    if (settings.offerPostDelayMs !== undefined) args.push('--post-delay-ms', String(settings.offerPostDelayMs));

    if (settings.offerPaymentToken) args.push('--payment-token', String(settings.offerPaymentToken));
    // Dry-run should always fetch fresh matches (Giga filters). The token-id override is only for real runs.
    if (!dryRun && settings.offerUseTokenIdsOverride && settings.offerTokenIds) {
      args.push('--token-ids', String(settings.offerTokenIds));
    }

    if (dryRun) {
      // Dry-run should only fetch token IDs (Giga) and should not hit OpenSea collection endpoints.
      // Avoid passing slug so the script doesn't try to resolve required_zone/fees.
      const slugIndex = args.indexOf('--slug');
      if (slugIndex !== -1) args.splice(slugIndex, 2);
      args.push('--dry-run');
    }

    const jsPath = path.join(__dirname, 'scripts', 'makeOfferByTraits.js');
    const tsPath = path.join(__dirname, '..', 'src', 'scripts', 'makeOfferByTraits.ts');
    const hasJs = fs.existsSync(jsPath);

    const cmd = hasJs ? process.execPath : path.join(process.cwd(), 'node_modules', '.bin', 'ts-node');
    const cmdArgs = hasJs ? [jsPath, ...args] : [tsPath, ...args];

    return await new Promise((resolve, reject) => {
      const child = spawn(cmd, cmdArgs, {
        env: { ...process.env },
        stdio: ['ignore', 'pipe', 'pipe']
      });

      let output = '';
      child.stdout.on('data', (d) => {
        const s = d.toString('utf8');
        output += s;
        this.addLog(`[offer] ${s}`);
      });
      child.stderr.on('data', (d) => {
        const s = d.toString('utf8');
        output += s;
        this.addLog(`[offer] ${s}`);
      });

      child.on('error', reject);
      child.on('close', (code) => {
        resolve({ output, exitCode: code ?? 1 });
      });
    });
  }

  addLog(line: string): void {
    // Normalize accidental literal "\n" sequences into real newlines
    const normalized = String(line).replace(/\\n/g, '\n');
    const chunks = normalized.split('\n');
    for (const chunk of chunks) {
      const trimmed = chunk.trimEnd();
      if (!trimmed) continue;

      // Prevent giant offer dry-run outputs from spamming the console log.
      // The full output is still returned to the Offer Output textarea.
      if (trimmed.startsWith('[offer] Matched ') && trimmed.includes('token(s):')) {
        const m = trimmed.match(/\[offer\]\s+Matched\s+(\d+)\s+token\(s\)/);
        const count = m?.[1] ?? '?';
        this.logSeq += 1;
        this.logs.push({ id: this.logSeq, line: `${new Date().toISOString()} OFFER matched ${count} token(s) (list in Offer Output)` });
        if (this.logs.length > 2000) this.logs.shift();
        continue;
      }

      this.logSeq += 1;
      this.logs.push({ id: this.logSeq, line: `${new Date().toISOString()} ${trimmed}` });
      if (this.logs.length > 2000) this.logs.shift();
    }
  }

  setBotInstance(bot: any): void {
    this.botInstance = bot;
  }

  updateBotStatus(status: Partial<typeof this.botStatus>): void {
    this.botStatus = { ...this.botStatus, ...status };
  }

  addEvent(event: any): void {
    this.botStatus.lastEvents.push({
      ...event,
      timestamp: Date.now()
    });
    // Keep only last 100 events
    if (this.botStatus.lastEvents.length > 100) {
      this.botStatus.lastEvents.shift();
    }
  }

  start(): void {
    // Check if SSL certificates exist
    const certPath = path.join(process.cwd(), 'localhost+2.pem');
    const keyPath = path.join(process.cwd(), 'localhost+2-key.pem');

    if (fs.existsSync(certPath) && fs.existsSync(keyPath)) {
      // Start HTTPS server
      const httpsOptions = {
        cert: fs.readFileSync(certPath),
        key: fs.readFileSync(keyPath)
      };

      https.createServer(httpsOptions, this.app).listen(this.port, () => {
        console.log(`\n🔒 Web Dashboard (HTTPS): https://localhost:${this.port}`);
      });
    } else {
      // Fallback to HTTP
      this.app.listen(this.port, () => {
        console.log(`\n🌐 Web Dashboard (HTTP): http://localhost:${this.port}`);
        console.log(`💡 Tip: Run 'mkcert localhost' to enable HTTPS`);
      });
    }
  }
}

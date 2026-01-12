import { OpenSeaStreamClient } from '@opensea/stream-js';
import { WebSocket } from 'ws';
import { ethers } from 'ethers';
import { Seaport } from '@opensea/seaport-js';
import { CROSS_CHAIN_SEAPORT_V1_6_ADDRESS, ItemType, OPENSEA_CONDUIT_KEY } from '@opensea/seaport-js/lib/constants';
import { SettingsManager } from './settingsManager';
import { OpenSeaHttpError, openseaRequestJson } from './scripts/openseaHttp';
import { getNextApiKey, getPrimaryApiKey } from './openseaKeys';

const OPENSEA_API_BASE = 'https://api.opensea.io';
const OPENSEA_CONDUIT_KEY_ABSTRACT = '0x61159fefdfada89302ed55f8b9e89e2d67d8258712b3a3f89aa88525877f1d5e';
const MAX_UINT256 = (2n ** 256n) - 1n;

type TokenState = {
  bestSeenWei: bigint;
  ourBidWei: bigint;
  lastBidAtMs: number;
  inFlight: boolean;
};

type BestOfferResult = {
  bestWei: bigint;
  bestMaker?: string;
  orders: any[];
};

type BasePriceMode = 'fixed' | 'collection' | 'none';

function normalizeAddress(address: string): string {
  try {
    return ethers.getAddress(address);
  } catch {
    return ethers.getAddress(address.toLowerCase());
  }
}

function normalizeBytes32(hex: string): string {
  const value = hex.trim().toLowerCase();
  if (!value.startsWith('0x') || value.length !== 66) throw new Error(`Invalid bytes32: ${hex}`);
  return value;
}

function normalizeDecimalString(input: string): string {
  return input.trim().replace(',', '.');
}

function parseTokenIds(input: string | undefined): Set<string> {
  if (!input) return new Set();
  const ids = input
    .split(/[^0-9]+/g)
    .map((x) => x.trim())
    .filter(Boolean);
  return new Set(ids);
}

function parseTokenIdFromNftId(nftId: string): string | undefined {
  // Common formats: "chain/contract/tokenId" or ".../tokenId"
  const last = nftId.split('/').filter(Boolean).pop();
  if (!last) return undefined;
  const digits = last.match(/[0-9]+/g)?.pop();
  return digits ?? undefined;
}

function fmtEth(wei: bigint): string {
  try {
    return ethers.formatEther(wei);
  } catch {
    return wei.toString();
  }
}

function fmtEthShort(wei: bigint, digits: number = 4): string {
  const raw = fmtEth(wei);
  const n = Number(raw);
  if (!Number.isFinite(n)) return raw;
  return n.toFixed(digits);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function shortErrorMessage(err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  const m = msg.match(/OpenSea request timed out after (\d+)ms/);
  if (m) return `timeout (${Math.round(Number(m[1]) / 1000)}s)`;
  // Remove super-long URLs from logs
  return msg.replace(/\(GET https:\/\/api\.opensea\.io[^)]*\)/g, '').trim();
}

function getNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : undefined;
}

function getBasePriceMode(): BasePriceMode {
  // Default to `collection` so "no offers" tokens can use the best collection offer as a base
  // without relying on a fixed hardcoded value.
  const raw = String(process.env.AUTO_BID_BASE_PRICE_MODE ?? 'collection')
    .trim()
    .toLowerCase();
  if (raw === 'none') return 'none';
  if (raw === 'collection') return 'collection';
  return 'fixed';
}

function parseBoolEnv(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined) return defaultValue;
  const v = raw.trim().toLowerCase();
  if (v === '1' || v === 'true' || v === 'yes' || v === 'y' || v === 'on') return true;
  if (v === '0' || v === 'false' || v === 'no' || v === 'n' || v === 'off') return false;
  return defaultValue;
}

async function ensureErc20Allowance(params: {
  wallet: ethers.Wallet;
  paymentToken: string;
  spender: string;
  minAmountWei: bigint;
}): Promise<void> {
  const token = normalizeAddress(params.paymentToken);
  const spender = normalizeAddress(params.spender);
  const erc20 = new ethers.Contract(
    token,
    [
      'function allowance(address owner, address spender) view returns (uint256)',
      'function approve(address spender, uint256 value) returns (bool)'
    ],
    params.wallet
  );

  const allowance: bigint = await erc20.allowance(params.wallet.address, spender);
  if (allowance >= params.minAmountWei) return;
  const tx = await erc20.approve(spender, MAX_UINT256);
  await tx.wait();
}

async function resolveCollectionRequiredZone(apiKey: string, slug: string): Promise<string | undefined> {
  const url = `${OPENSEA_API_BASE}/api/v2/collections/${encodeURIComponent(slug)}`;
  const data = await openseaRequestJson<unknown>(url, { apiKey });
  if (!data || typeof data !== 'object') return undefined;
  const z = (data as Record<string, unknown>).required_zone;
  return typeof z === 'string' && z.startsWith('0x') ? z : undefined;
}

async function resolveCollectionFees(apiKey: string, slug: string): Promise<Array<{ recipient: string; basisPoints: number }>> {
  const url = `${OPENSEA_API_BASE}/api/v2/collections/${encodeURIComponent(slug)}`;
  const data = await openseaRequestJson<unknown>(url, { apiKey });
  if (!data || typeof data !== 'object') return [];
  const fees = (data as Record<string, unknown>).fees;
  if (!Array.isArray(fees)) return [];

  const result: Array<{ recipient: string; basisPoints: number }> = [];
  for (const fee of fees) {
    if (!fee || typeof fee !== 'object') continue;
    const f = fee as Record<string, unknown>;
    if (f.required === false) continue;
    const recipient = f.recipient;
    const pct = f.fee;
    if (typeof recipient !== 'string' || !recipient.startsWith('0x')) continue;
    if (typeof pct !== 'number' || !Number.isFinite(pct) || pct <= 0) continue;
    const basisPoints = Math.round(pct * 100);
    if (basisPoints <= 0) continue;
    result.push({ recipient: normalizeAddress(recipient), basisPoints });
  }
  return result;
}

async function resolveConduitControllerFromSeaport(params: {
  provider: ethers.Provider;
  protocolAddress: string;
}): Promise<string> {
  const seaport = normalizeAddress(params.protocolAddress);
  const abi = ['function information() view returns (string version, bytes32 domainSeparator, address conduitController)'];
  const c = new ethers.Contract(seaport, abi, params.provider);
  const [, , conduitController] = (await c.information()) as [string, string, string];
  return normalizeAddress(conduitController);
}

async function resolveConduitAddress(params: {
  provider: ethers.Provider;
  conduitKey: string;
  conduitController: string;
}): Promise<string> {
  const controller = normalizeAddress(params.conduitController);
  const abi = ['function getConduit(bytes32 conduitKey) view returns (address conduit, bool exists)'];
  const cc = new ethers.Contract(controller, abi, params.provider);
  const [conduit, exists] = (await cc.getConduit(params.conduitKey)) as [string, boolean];
  if (!exists || !conduit || conduit === ethers.ZeroAddress) {
    throw new Error(`Could not resolve conduit address for key ${params.conduitKey} via controller ${controller}`);
  }
  return normalizeAddress(conduit);
}

export class AutoBidService {
  private settingsManager: SettingsManager;
  private log?: (line: string) => void;
  private client?: OpenSeaStreamClient;
  private provider?: ethers.JsonRpcProvider;
  private wallet?: ethers.Wallet;
  private seaport?: Seaport;

  private running = false;
  private tokenState = new Map<string, TokenState>();
  private taskQueue: Array<() => Promise<void>> = [];
  private processing = false;
  private pollTimer?: NodeJS.Timeout;
  private watchedTokenIds = new Set<string>();
  private bootstrapThrottleUntilMs = 0;
  private bootstrapThrottleBackoffMs = 0;

  // OpenSea POST /offers rate limiting (adaptive).
  // Allow limited concurrency while still applying interval/backoff.
  private offerPostConcurrency = 1;
  private offerPostInFlight = 0;
  private offerPostWaiters: Array<() => void> = [];
  private offerPostThrottleUntilMs = 0;
  private offerPostLastAtMs = 0;
  private offerPostIntervalMs = 0;
  private recentEventKeys = new Map<string, number>();
  private retryTimers = new Map<string, { dueMs: number; timer: NodeJS.Timeout }>();

  private requiredZone?: string;
  private collectionFees: Array<{ recipient: string; basisPoints: number }> = [];
  private conduitKey?: string;
  private conduitController?: string;
  private conduitAddress?: string;
  private paymentToken?: string;
  private contract?: string;
  private chain?: string;
  private slug?: string;
  private protocolAddress?: string;
  private primaryApiKey?: string;
  private pickApiKey(): string {
    const key = getNextApiKey() ?? this.primaryApiKey;
    if (!key) throw new Error('OPENSEA_API_KEY is required');
    return key;
  }
  private bestCollectionOfferPerItemWei: bigint = 0n;
  private collectionSweepInFlight = false;
  private collectionSweepPending = false;
  private paymentTokenDecimals: number = 18;
  private bidPriceDecimalsAllowed: number = 4;
  private bidPriceTickWei: bigint = 10n ** 14n;

  constructor(settingsManager: SettingsManager, log?: (line: string) => void) {
    this.settingsManager = settingsManager;
    this.log = log;
  }

  private logInfo(message: string): void {
    this.log?.(message);
  }

  private logToken(tokenId: string, message: string): void {
    // Keep a stable, compact, one-line-per-action format for the UI console.
    this.log?.(`${tokenId} ${message}`);
  }

  private recomputeBidTick(): void {
    const tokenDecimals = Math.max(0, Math.min(36, Math.floor(this.paymentTokenDecimals)));
    const allowed = Math.max(0, Math.min(18, Math.floor(this.bidPriceDecimalsAllowed)));
    if (tokenDecimals <= allowed) {
      this.bidPriceTickWei = 1n;
      return;
    }
    this.bidPriceTickWei = 10n ** BigInt(tokenDecimals - allowed);
  }

  private snapBidWeiUp(amountWei: bigint): bigint {
    const tick = this.bidPriceTickWei;
    if (tick <= 1n) return amountWei;
    return ((amountWei + tick - 1n) / tick) * tick;
  }

  private tryUpdateBidDecimalsAllowedFromError(err: unknown): boolean {
    if (!(err instanceof OpenSeaHttpError)) return false;
    if (err.statusCode !== 400) return false;
    const m = err.responseText.match(/(\d+)\s+decimals\s+allowed/i);
    if (!m) return false;
    const n = Number(m[1]);
    if (!Number.isFinite(n) || n < 0) return false;
    const next = Math.max(0, Math.min(18, Math.floor(n)));
    if (next === this.bidPriceDecimalsAllowed) return false;
    this.bidPriceDecimalsAllowed = next;
    this.recomputeBidTick();
    return true;
  }

  private async tryQueueOutbidForToken(params: { tokenId: string; bestSeenCandidateWei: bigint; source: string }): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    if (settings.autoBidMode === 'fixedPriceOnce') return;
    if (!this.paymentToken || !this.wallet || !this.chain || !this.contract) return;

    // Only act on watched ids.
    if (this.watchedTokenIds.size && !this.watchedTokenIds.has(params.tokenId)) return;

    let candidate = params.bestSeenCandidateWei;
    if (this.bestCollectionOfferPerItemWei > candidate) candidate = this.bestCollectionOfferPerItemWei;

    const incrementEth = normalizeDecimalString(String(settings.autoBidIncrement ?? '0.0001'));
    const incrementWei = ethers.parseEther(incrementEth);

    const maxPriceEth = settings.autoBidMaxPrice;
    if (!maxPriceEth) return;
    const maxWei = ethers.parseEther(normalizeDecimalString(String(maxPriceEth)));

    const cooldownSeconds = settings.autoBidCooldownSeconds ?? 45;
    const nowMs = Date.now();

    const current = this.tokenState.get(params.tokenId) ?? {
      bestSeenWei: 0n,
      ourBidWei: 0n,
      lastBidAtMs: 0,
      inFlight: false
    };

    if (candidate > current.bestSeenWei) current.bestSeenWei = candidate;
    const desiredWeiRaw = current.bestSeenWei + incrementWei;
    const desiredWei = this.snapBidWeiUp(desiredWeiRaw);

    if (desiredWei > maxWei) {
      this.tokenState.set(params.tokenId, current);
      this.logToken(params.tokenId, `skip desired=${fmtEthShort(desiredWei)} > max=${fmtEthShort(maxWei)}`);
      return;
    }
    if (current.ourBidWei >= desiredWei) {
      this.tokenState.set(params.tokenId, current);
      this.logToken(params.tokenId, `skip ours=${fmtEthShort(current.ourBidWei)} >= desired=${fmtEthShort(desiredWei)}`);
      return;
    }
    if (current.inFlight) {
      this.tokenState.set(params.tokenId, current);
      this.logToken(params.tokenId, 'skip inFlight');
      this.scheduleRetryAfterCooldown(params.tokenId, { minDelayMs: 2500 });
      return;
    }
    if (nowMs - current.lastBidAtMs < cooldownSeconds * 1000) {
      this.tokenState.set(params.tokenId, current);
      this.logToken(params.tokenId, 'skip cooldown');
      this.scheduleRetryAfterCooldown(params.tokenId);
      return;
    }

    current.inFlight = true;
    this.tokenState.set(params.tokenId, current);
    this.logToken(
      params.tokenId,
      `queueOutbid(${params.source}) best=${fmtEthShort(current.bestSeenWei)} -> ${fmtEthShort(desiredWei)} queue=${this.taskQueue.length}`
    );

    this.enqueue(async () => {
      try {
        if (settings.autoBidCancelPrevious && this.chain && this.contract) {
          const apiKey = this.pickApiKey();
          const url = `${OPENSEA_API_BASE}/api/v2/orders/${this.chain}/seaport/offers?asset_contract_address=${this.contract}&token_ids=${params.tokenId}&limit=50`;
          const data = await openseaRequestJson<any>(url, { apiKey, retries: 3, retryDelayMs: 900, timeoutMs: 20_000 });
          const orders = Array.isArray(data?.orders) ? data.orders : [];
          await this.cancelOrders(orders);
        }
        this.logToken(params.tokenId, `placing ${fmtEthShort(desiredWei)}`);
        await this.placeOffer(params.tokenId, desiredWei, settings.autoBidDurationMinutes ?? 60 * 24, maxWei);
        this.logToken(params.tokenId, `placed ${fmtEthShort(desiredWei)}`);
        const updated = this.tokenState.get(params.tokenId);
        if (updated) {
          updated.ourBidWei = desiredWei;
          updated.lastBidAtMs = Date.now();
        }
      } catch (err) {
        this.logToken(params.tokenId, `error ${shortErrorMessage(err)}`);
        this.noteBootstrapRateLimit(err);
        // If OpenSea throttles/timeouts, schedule a retry later (do not drop the token).
        const retryable =
          err instanceof OpenSeaHttpError
            ? err.statusCode === 429 || (err.statusCode >= 500 && err.statusCode <= 599)
            : String(err instanceof Error ? err.message : err).toLowerCase().includes('timed out');
        if (retryable) {
          const baseDelayMs =
            err instanceof OpenSeaHttpError && err.statusCode === 429 ? err.retryAfterMs(5000) : 3500;
          const jitter = Math.floor(Math.random() * 400);
          const waitMs = baseDelayMs + jitter;
          this.logToken(params.tokenId, `retry scheduled in ${Math.ceil(waitMs / 1000)}s`);
          this.scheduleRetryAfterCooldown(params.tokenId, { minDelayMs: waitMs });
        }
      } finally {
        const updated = this.tokenState.get(params.tokenId);
        if (updated) updated.inFlight = false;
      }
    });
  }

  private async handleCollectionOfferEvent(event: any): Promise<void> {
    if (!this.running) return;
    const payload = event?.payload ?? event?.event?.payload ?? undefined;
    if (!payload) return;

    const chain = String(payload?.chain ?? '').trim();
    if (this.chain && chain && chain !== this.chain) return;

    const paymentTokenAddress = payload?.payment_token?.address ?? payload?.paymentToken?.address ?? payload?.payment_token_address;
    if (!paymentTokenAddress || !this.paymentToken) return;
    try {
      if (normalizeAddress(String(paymentTokenAddress)) !== this.paymentToken) return;
    } catch {
      return;
    }

    const qtyRaw = payload?.quantity ?? 1;
    const basePriceRaw = payload?.base_price ?? payload?.basePrice ?? payload?.current_price ?? payload?.price;
    if (basePriceRaw === undefined || basePriceRaw === null) return;

    let payWei: bigint;
    let qty: bigint;
    try {
      payWei = BigInt(String(basePriceRaw));
      qty = BigInt(String(qtyRaw));
      if (qty <= 0n) qty = 1n;
    } catch {
      return;
    }

    const perItemWei = payWei / qty;
    if (perItemWei <= 0n) return;
    if (perItemWei <= this.bestCollectionOfferPerItemWei) return;

    this.bestCollectionOfferPerItemWei = perItemWei;
    this.logInfo(`collectionOffer bestPerItem=${fmtEthShort(perItemWei)} (watched=${this.watchedTokenIds.size})`);
    void this.sweepOutbidDueToCollectionOffer();
  }

  private async sweepOutbidDueToCollectionOffer(): Promise<void> {
    if (!this.running) return;
    if (this.collectionSweepInFlight) {
      this.collectionSweepPending = true;
      return;
    }
    this.collectionSweepInFlight = true;
    try {
      const ids = Array.from(this.watchedTokenIds);
      for (const tokenId of ids) {
        if (!this.running) return;
        await this.tryQueueOutbidForToken({ tokenId, bestSeenCandidateWei: 0n, source: 'collection' });
      }
    } finally {
      this.collectionSweepInFlight = false;
      if (this.collectionSweepPending) {
        this.collectionSweepPending = false;
        void this.sweepOutbidDueToCollectionOffer();
      }
    }
  }

  isRunning(): boolean {
    return this.running;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const settings = this.settingsManager.getSettings();
    const mode = settings.autoBidMode ?? 'bootstrap';

    const primaryKey = getPrimaryApiKey();
    if (!primaryKey) throw new Error('OPENSEA_API_KEY is required');
    this.primaryApiKey = primaryKey;
    const offerCollectionSlug = getNonEmptyString(settings.offerCollectionSlug) ?? getNonEmptyString(process.env.OFFER_COLLECTION_SLUG);
    const offerContract = getNonEmptyString(settings.offerContract) ?? getNonEmptyString(process.env.OFFER_CONTRACT);
    const offerChain = getNonEmptyString(settings.offerChain) ?? getNonEmptyString(process.env.OFFER_CHAIN);
    const offerPaymentToken = getNonEmptyString(settings.offerPaymentToken) ?? getNonEmptyString(process.env.OFFER_PAYMENT_TOKEN);
    if (!offerCollectionSlug) throw new Error('Offer collection slug is required (OFFER_COLLECTION_SLUG)');
    if (!offerContract) throw new Error('Offer contract is required (OFFER_CONTRACT)');
    if (!offerChain) throw new Error('Offer chain is required (OFFER_CHAIN)');
    if (!offerPaymentToken) throw new Error('Offer payment token is required (OFFER_PAYMENT_TOKEN)');

    const watched = parseTokenIds(settings.autoBidTokenIds ?? settings.offerTokenIds);
    if (watched.size === 0) throw new Error('Auto-bid token IDs are required (AUTO_BID_TOKEN_IDS / Offer Token IDs)');
    this.watchedTokenIds = watched;
    if (mode === 'fixedPriceOnce') {
      const fixedPriceStr = getNonEmptyString(settings.autoBidFixedPrice ?? '') ?? undefined;
      if (!fixedPriceStr) throw new Error('Fixed Price is required for auto-bid');
      ethers.parseEther(normalizeDecimalString(fixedPriceStr));
    } else {
      const maxPriceEth = getNonEmptyString(settings.autoBidMaxPrice);
      if (!maxPriceEth) throw new Error('Max Price is required for auto-bid');
      const incrementEth = getNonEmptyString(settings.autoBidIncrement) ?? '0.0001';
      // Validate decimals
      ethers.parseEther(normalizeDecimalString(incrementEth));
      ethers.parseEther(normalizeDecimalString(maxPriceEth));
    }

    const rpcUrl = process.env.RPC_URL;
    const privateKey = process.env.PRIVATE_KEY;
    if (!rpcUrl) throw new Error('RPC_URL is required');
    if (!privateKey) throw new Error('PRIVATE_KEY is required');

    this.slug = offerCollectionSlug;
    this.chain = offerChain;
    this.contract = normalizeAddress(offerContract);
    this.paymentToken = normalizeAddress(offerPaymentToken);
    this.protocolAddress = normalizeAddress(process.env.OPENSEA_PROTOCOL_ADDRESS ?? CROSS_CHAIN_SEAPORT_V1_6_ADDRESS);

    this.provider = new ethers.JsonRpcProvider(rpcUrl);
    const normalizedPk = privateKey.startsWith('0x') ? privateKey : `0x${privateKey}`;
    this.wallet = new ethers.Wallet(normalizedPk, this.provider);

    this.requiredZone = await resolveCollectionRequiredZone(primaryKey, this.slug);
    this.collectionFees = await resolveCollectionFees(primaryKey, this.slug);

    this.conduitKey = normalizeBytes32(
      process.env.OFFER_CONDUIT_KEY ?? (this.chain === 'abstract' ? OPENSEA_CONDUIT_KEY_ABSTRACT : OPENSEA_CONDUIT_KEY)
    );
    this.conduitController =
      process.env.OFFER_CONDUIT_CONTROLLER_ADDRESS ??
      (await resolveConduitControllerFromSeaport({ provider: this.provider, protocolAddress: this.protocolAddress }));
    this.conduitAddress = normalizeAddress(
      process.env.OFFER_CONDUIT_ADDRESS ??
        (await resolveConduitAddress({ provider: this.provider, conduitKey: this.conduitKey, conduitController: this.conduitController }))
    );

    await ensureErc20Allowance({
      wallet: this.wallet,
      paymentToken: this.paymentToken,
      spender: this.conduitAddress,
      minAmountWei: 1n
    });

    // Cache payment token decimals so we can snap bids to OpenSea's allowed increments.
    try {
      const erc20 = new ethers.Contract(this.paymentToken, ['function decimals() view returns (uint8)'], this.provider);
      const d = await erc20.decimals();
      const n = Number(d);
      if (Number.isFinite(n) && n > 0) this.paymentTokenDecimals = Math.floor(n);
    } catch {
      // keep default
    }
    this.recomputeBidTick();
    // Default offer post pacing. Can be tuned via env; increases automatically on 429.
    const intervalMsRaw = Number(process.env.OPENSEA_OFFER_POST_INTERVAL_MS ?? '250');
    this.offerPostIntervalMs = Number.isFinite(intervalMsRaw) && intervalMsRaw >= 0 ? Math.floor(intervalMsRaw) : 250;
    const postConcurrencyRaw = Number(process.env.OPENSEA_OFFER_POST_CONCURRENCY ?? '1');
    this.offerPostConcurrency =
      Number.isFinite(postConcurrencyRaw) && postConcurrencyRaw > 0
        ? Math.floor(postConcurrencyRaw)
        : 1;

    this.seaport = new Seaport(this.wallet, {
      balanceAndApprovalChecksOnOrderCreation: false,
      conduitKeyToConduit: {
        [this.conduitKey]: this.conduitAddress
      }
    });

    this.client = new OpenSeaStreamClient({
      token: primaryKey,
      connectOptions: { transport: WebSocket }
    });

    this.client.onItemReceivedOffer(this.slug, (event) => {
      void this.handleOfferEvent(event);
    });
    this.client.onItemReceivedBid(this.slug, (event) => {
      void this.handleOfferEvent(event);
    });
    this.client.onCollectionOffer(this.slug, (event) => {
      void this.handleCollectionOfferEvent(event);
    });

    this.running = true;
    this.logInfo(`autoBid started slug=${this.slug} chain=${this.chain} contract=${this.contract}`);

    const modeLabel =
      mode === 'wsOnly'
        ? 'websocket-only'
        : mode === 'collectionBootstrap'
          ? 'collection+websocket'
          : mode === 'fixedPriceOnce'
            ? 'fixed-price-once'
            : 'bootstrap+websocket';
    this.logInfo(`autoBid mode=${modeLabel}`);
    if (mode === 'fixedPriceOnce') {
      await this.bootstrapFixedPriceBids();
      this.stop();
      return;
    }
    if (mode === 'collectionBootstrap') {
      // Fast bootstrap: place collection+increment for all tokens (no per-token offer fetch).
      await this.bootstrapCollectionBids();
    } else if (mode !== 'wsOnly') {
      // Bootstrap once: fetch best offer per token and place initial outbid (no continuous polling by default).
      await this.bootstrapInitialBids();
    }

    // Optional polling fallback (DISABLED by default; set AUTO_BID_POLL_SECONDS>0 if you want it).
    const pollSeconds = Number(process.env.AUTO_BID_POLL_SECONDS ?? '0');
    if (Number.isFinite(pollSeconds) && pollSeconds > 0) {
      const runOnce = async () => {
        try {
          await this.pollBestOffers();
        } catch (err) {
          this.log?.(`autoBid: poll error: ${err instanceof Error ? err.message : String(err)}`);
        }
      };
      this.pollTimer = setInterval(() => void runOnce(), Math.floor(pollSeconds * 1000));
      this.logInfo(`autoBid polling enabled interval=${pollSeconds}s`);
    }
  }

  stop(): void {
    if (!this.running) return;
    // Mark stopped first so enqueued tasks (and POST retries) can bail out quickly.
    this.running = false;
    this.client?.disconnect();
    this.client = undefined;
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.pollTimer = undefined;
    for (const t of this.retryTimers.values()) clearTimeout(t.timer);
    this.retryTimers.clear();
    for (const wake of this.offerPostWaiters.splice(0)) wake();
    this.offerPostInFlight = 0;
    // Drop any queued outbid tasks immediately.
    this.taskQueue.length = 0;
    this.logInfo('autoBid stopped');
  }

  private enqueue(task: () => Promise<void>): void {
    if (!this.running) return;
    this.taskQueue.push(task);
    void this.processQueue();
  }

  private async acquireOfferPostPermit(): Promise<() => void> {
    if (!this.running) throw new Error('AutoBid stopped');
    while (this.offerPostInFlight >= this.offerPostConcurrency) {
      await new Promise<void>((resolve) => this.offerPostWaiters.push(resolve));
      if (!this.running) throw new Error('AutoBid stopped');
    }
    this.offerPostInFlight += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.offerPostInFlight = Math.max(0, this.offerPostInFlight - 1);
      const next = this.offerPostWaiters.shift();
      if (next) next();
    };
  }

  private async withOfferPostPermit<T>(fn: () => Promise<T>): Promise<T> {
    const release = await this.acquireOfferPostPermit();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  private async waitForOfferPostSlot(): Promise<void> {
    if (!this.running) return;
    const now = Date.now();
    const dueMs = Math.max(this.offerPostThrottleUntilMs, this.offerPostLastAtMs + Math.max(0, this.offerPostIntervalMs));
    const waitMs = Math.max(0, dueMs - now);
    if (waitMs <= 0) return;
    if (parseBoolEnv('OPENSEA_OFFER_POST_LOG_THROTTLE', false)) {
      this.logInfo(
        `offerPost throttle waitMs=${waitMs} intervalMs=${this.offerPostIntervalMs} inFlight=${this.offerPostInFlight}`
      );
    }
    await sleep(waitMs);
  }

  private noteOfferPostRateLimit(err: unknown): void {
    if (!parseBoolEnv('OPENSEA_OFFER_POST_BACKOFF_ENABLED', true)) return;
    if (!(err instanceof OpenSeaHttpError)) return;
    if (err.statusCode !== 429) return;
    const now = Date.now();
    const defaultMsRaw = Number(process.env.OPENSEA_OFFER_POST_BACKOFF_DEFAULT_MS ?? '5000');
    const defaultMs =
      Number.isFinite(defaultMsRaw) && defaultMsRaw > 0
        ? Math.floor(defaultMsRaw)
        : 5000;
    const waitMs = err.retryAfterMs(defaultMs);
    this.offerPostThrottleUntilMs = Math.max(this.offerPostThrottleUntilMs, now + waitMs);
    // Also increase steady-state interval after we hit a 429 to reduce repeated throttles.
    const maxMsRaw = Number(process.env.OPENSEA_OFFER_POST_BACKOFF_MAX_MS ?? '10000');
    const maxMs =
      Number.isFinite(maxMsRaw) && maxMsRaw >= 0
        ? Math.floor(maxMsRaw)
        : 10_000;
    this.offerPostIntervalMs = Math.max(this.offerPostIntervalMs, Math.min(maxMs, waitMs));
  }

  private async processQueue(): Promise<void> {
    if (this.processing) return;
    this.processing = true;
    try {
      while (this.taskQueue.length) {
        if (!this.running) {
          this.taskQueue.length = 0;
          break;
        }
        const t = this.taskQueue.shift();
        if (!t) continue;
        try {
          await t();
        } catch (err) {
          this.logInfo(`autoBid task error: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
    } finally {
      this.processing = false;
    }
  }

  private async handleOfferEvent(event: any): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;

    const watched = this.watchedTokenIds.size
      ? this.watchedTokenIds
      : parseTokenIds(settings.autoBidTokenIds ?? settings.offerTokenIds);
    if (watched.size === 0) return;

    const tokenId = parseTokenIdFromNftId(String(event?.payload?.item?.nft_id ?? ''));
    if (!tokenId || !watched.has(tokenId)) return;

    const maker = String(event?.payload?.maker?.address ?? '');
    const basePriceRaw = String(event?.payload?.base_price ?? '0');
    const paymentTokenAddress = String(event?.payload?.payment_token?.address ?? '');

    // Deduplicate near-identical stream events (OpenSea often emits multiple event types for the same offer).
    const dedupeKey = `${tokenId}|${maker.toLowerCase()}|${paymentTokenAddress.toLowerCase()}|${basePriceRaw}`;
    const now = Date.now();
    const last = this.recentEventKeys.get(dedupeKey);
    if (last && now - last < 5000) return;
    this.recentEventKeys.set(dedupeKey, now);
    if (this.recentEventKeys.size > 5000) {
      for (const [k, t] of this.recentEventKeys) {
        if (now - t > 60_000) this.recentEventKeys.delete(k);
      }
    }
    try {
      const offerWei = BigInt(basePriceRaw);
      this.logToken(tokenId, `event offer=${fmtEthShort(offerWei)}`);
    } catch {
      this.logToken(tokenId, 'event offer');
    }
    if (this.wallet && maker) {
      try {
        if (normalizeAddress(maker) === this.wallet.address) {
          try {
            const offerWei = BigInt(basePriceRaw);
            const current = this.tokenState.get(tokenId) ?? {
              bestSeenWei: 0n,
              ourBidWei: 0n,
              lastBidAtMs: 0,
              inFlight: false
            };
            current.bestSeenWei = offerWei > current.bestSeenWei ? offerWei : current.bestSeenWei;
            current.ourBidWei = offerWei > current.ourBidWei ? offerWei : current.ourBidWei;
            this.tokenState.set(tokenId, current);
          } catch {
            // ignore
          }
          return;
        }
      } catch {
        // ignore
      }
    }

    if (!paymentTokenAddress || !this.paymentToken) return;
    if (normalizeAddress(paymentTokenAddress) !== this.paymentToken) return;

	    let offerWei: bigint;
	    try {
	      offerWei = BigInt(basePriceRaw);
	    } catch {
	      return;
	    }

	    await this.tryQueueOutbidForToken({ tokenId, bestSeenCandidateWei: offerWei, source: 'item' });
	  }

  private scheduleRetryAfterCooldown(tokenId: string, opts?: { minDelayMs?: number }): void {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    const cooldownSeconds = settings.autoBidCooldownSeconds ?? 45;
    const current = this.tokenState.get(tokenId);
    if (!current) return;

    const minDelayMs = Math.max(0, opts?.minDelayMs ?? 0);
    const nowMs = Date.now();
    const dueMs = Math.max(current.lastBidAtMs + cooldownSeconds * 1000, nowMs + minDelayMs);
    const waitMs = Math.max(0, dueMs - nowMs);
    if (waitMs <= 0) return;

    const existing = this.retryTimers.get(tokenId);
    if (existing && existing.dueMs <= dueMs) return; // already scheduled sooner (or same)
    if (existing) {
      clearTimeout(existing.timer);
      this.retryTimers.delete(tokenId);
    }

    const jitter = Math.floor(Math.random() * 250);
    const timer = setTimeout(() => {
      this.retryTimers.delete(tokenId);
      void this.retryOutbid(tokenId);
    }, waitMs + jitter);
    this.retryTimers.set(tokenId, { dueMs, timer });
  }

  private async retryOutbid(tokenId: string): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    if (settings.autoBidMode === 'fixedPriceOnce') return;
    if (!this.wallet) return;

    // Only retry for watched ids.
    if (this.watchedTokenIds.size && !this.watchedTokenIds.has(tokenId)) return;

    // Re-check state/cooldown/in-flight.
    const cooldownSeconds = settings.autoBidCooldownSeconds ?? 45;
    const nowMs = Date.now();
    const current = this.tokenState.get(tokenId) ?? {
      bestSeenWei: 0n,
      ourBidWei: 0n,
      lastBidAtMs: 0,
      inFlight: false
    };

    if (current.inFlight) {
      // We saw an outbid while we were placing a bid. Retry shortly after the in-flight action is likely done.
      this.scheduleRetryAfterCooldown(tokenId, { minDelayMs: 2500 });
      return;
    }
    if (nowMs - current.lastBidAtMs < cooldownSeconds * 1000) {
      this.scheduleRetryAfterCooldown(tokenId);
      return;
    }

    this.enqueue(async () => {
      const innerNow = Date.now();
      const st = this.tokenState.get(tokenId) ?? current;
      if (st.inFlight) {
        this.scheduleRetryAfterCooldown(tokenId, { minDelayMs: 2500 });
        return;
      }
      if (innerNow - st.lastBidAtMs < cooldownSeconds * 1000) {
        this.scheduleRetryAfterCooldown(tokenId);
        return;
      }
      st.inFlight = true;
      this.tokenState.set(tokenId, st);
	      try {
	        const { bestWei, bestMaker, orders } = await this.fetchBestOfferForToken(tokenId);
	        let referenceWei = bestWei;
	        if (this.bestCollectionOfferPerItemWei > referenceWei) referenceWei = this.bestCollectionOfferPerItemWei;
	        if (referenceWei > st.bestSeenWei) st.bestSeenWei = referenceWei;

        // If best is ours, just update ourBidWei and stop.
        if (bestMaker) {
          try {
            if (normalizeAddress(bestMaker) === this.wallet!.address) {
              st.ourBidWei = st.bestSeenWei;
              this.tokenState.set(tokenId, st);
              this.logToken(tokenId, `skip ours=${fmtEthShort(st.bestSeenWei)}`);
              return;
            }
          } catch {
            // ignore
          }
        }

        const incrementEth = normalizeDecimalString(String(settings.autoBidIncrement ?? '0.0001'));
        const incrementWei = ethers.parseEther(incrementEth);
        const maxPriceEth = settings.autoBidMaxPrice;
        if (!maxPriceEth) return;
	        const maxWei = ethers.parseEther(normalizeDecimalString(String(maxPriceEth)));

	        const desiredWeiRaw = st.bestSeenWei + incrementWei;
	        const desiredWei = this.snapBidWeiUp(desiredWeiRaw);
	        if (desiredWei > maxWei) {
	          this.logToken(tokenId, `skip desired=${fmtEthShort(desiredWei)} > max=${fmtEthShort(maxWei)}`);
	          return;
	        }
        if (st.ourBidWei >= desiredWei) {
          this.logToken(tokenId, `skip ours=${fmtEthShort(st.ourBidWei)} >= desired=${fmtEthShort(desiredWei)}`);
          return;
        }

	        this.logToken(tokenId, `retry placing ${fmtEthShort(desiredWei)}`);
	        if (settings.autoBidCancelPrevious) await this.cancelOrders(orders);
	        await this.placeOffer(tokenId, desiredWei, settings.autoBidDurationMinutes ?? 60 * 24, maxWei);
	        st.ourBidWei = desiredWei;
	        st.lastBidAtMs = Date.now();
        this.tokenState.set(tokenId, st);
        this.logToken(tokenId, `retry placed ${fmtEthShort(desiredWei)}`);
	      } catch (err) {
	        this.logToken(tokenId, `error ${shortErrorMessage(err)}`);
	        this.noteBootstrapRateLimit(err);
	        const retryable =
	          err instanceof OpenSeaHttpError
	            ? err.statusCode === 429 || (err.statusCode >= 500 && err.statusCode <= 599)
	            : String(err instanceof Error ? err.message : err).toLowerCase().includes('timed out');
	        if (retryable) {
	          const baseDelayMs =
	            err instanceof OpenSeaHttpError && err.statusCode === 429 ? err.retryAfterMs(5000) : 3500;
	          const jitter = Math.floor(Math.random() * 400);
	          const waitMs = baseDelayMs + jitter;
	          this.logToken(tokenId, `retry scheduled in ${Math.ceil(waitMs / 1000)}s`);
	          this.scheduleRetryAfterCooldown(tokenId, { minDelayMs: waitMs });
	        }
	      } finally {
	        const updated = this.tokenState.get(tokenId);
	        if (updated) updated.inFlight = false;
	      }
    });
  }

  private async placeOffer(tokenId: string, amountWei: bigint, durationMinutes: number, maxWei?: bigint): Promise<void> {
    if (!this.wallet || !this.provider || !this.seaport) throw new Error('AutoBid not initialized');
    if (!this.chain || !this.contract || !this.paymentToken || !this.protocolAddress) throw new Error('AutoBid missing config');
    if (!this.running) throw new Error('AutoBid stopped');
    const apiKey = this.pickApiKey();
    const postUrl = `${OPENSEA_API_BASE}/api/v2/orders/${this.chain}/seaport/offers`;

    // OpenSea validation: offers must expire at least 10 minutes in the future.
    const safeDurationMinutesRaw = Number(durationMinutes);
    const safeDurationMinutes =
      Number.isFinite(safeDurationMinutesRaw) && safeDurationMinutesRaw > 0
        ? Math.max(10, Math.floor(safeDurationMinutesRaw))
        : 60 * 24;

    // Snap to allowed bid increments (OpenSea enforces tick sizes like "4 decimals allowed").
    let snappedWei = this.snapBidWeiUp(amountWei);
    if (maxWei !== undefined && snappedWei > maxWei) snappedWei = maxWei;

    const now = Math.floor(Date.now() / 1000);
    const endTime = now + Math.floor(safeDurationMinutes * 60);

    const postTimeoutRaw = Number(process.env.OPENSEA_OFFER_POST_TIMEOUT_MS ?? '20000');
    const postTimeoutMs = Number.isFinite(postTimeoutRaw) && postTimeoutRaw > 0 ? Math.floor(postTimeoutRaw) : 20_000;

    const createAndSign = async (offerWei: bigint) => {
      const { executeAllActions } = await this.seaport!.createOrder(
        {
          conduitKey: this.conduitKey,
          zone: this.requiredZone,
          restrictedByZone: this.requiredZone ? true : undefined,
          startTime: now,
          endTime,
          fees: this.collectionFees.length ? this.collectionFees : undefined,
          offer: [{ token: this.paymentToken!, amount: offerWei.toString() }],
          consideration: [
            {
              itemType: ItemType.ERC721,
              token: this.contract!,
              identifier: tokenId,
              recipient: this.wallet!.address
            }
          ]
        },
        this.wallet!.address
      );
      return await executeAllActions();
    };

    const postOrder = async (order: any) =>
      await openseaRequestJson<any>(postUrl, {
        apiKey,
        method: 'POST',
        // Fail fast; retry is handled by requeueing so the rest of the outbid queue can proceed.
        retries: 0,
        retryDelayMs: 900,
        maxRetryDelayMs: 30_000,
        timeoutMs: postTimeoutMs,
        body: {
          protocol_address: this.protocolAddress,
          parameters: (order as any).parameters,
          signature: (order as any).signature
        }
      });

    let order = await createAndSign(snappedWei);
    let postResp: any;
    postResp = await this.withOfferPostPermit(async () => {
      const postAttemptsRaw = Number(process.env.OPENSEA_OFFER_POST_RETRIES ?? '6');
      const postAttempts = Number.isFinite(postAttemptsRaw) && postAttemptsRaw > 0 ? Math.floor(postAttemptsRaw) : 6;
      let lastErr: unknown;
      for (let attempt = 0; attempt < postAttempts; attempt++) {
        if (!this.running) throw new Error('AutoBid stopped');
        try {
          if (!this.running) throw new Error('AutoBid stopped');
          const resp = await postOrder(order);
          this.offerPostLastAtMs = Date.now();
          // Gradually relax interval after success (but keep >= 0).
          const baseIntervalRaw = Number(process.env.OPENSEA_OFFER_POST_INTERVAL_MS ?? '250');
          const baseInterval = Number.isFinite(baseIntervalRaw) && baseIntervalRaw >= 0 ? Math.floor(baseIntervalRaw) : 250;
          if (this.offerPostIntervalMs > baseInterval) {
            this.offerPostIntervalMs = Math.max(baseInterval, Math.floor(this.offerPostIntervalMs * 0.9));
          }
          return resp;
        } catch (err) {
          lastErr = err;
          if (err instanceof OpenSeaHttpError && err.statusCode === 429 && parseBoolEnv('OPENSEA_OFFER_POST_LOG_429', false)) {
            const defaultMsRaw = Number(process.env.OPENSEA_OFFER_POST_BACKOFF_DEFAULT_MS ?? '5000');
            const defaultMs =
              Number.isFinite(defaultMsRaw) && defaultMsRaw > 0
                ? Math.floor(defaultMsRaw)
                : 5000;
            this.logToken(tokenId, `opensea 429 retryAfterMs=${err.retryAfterMs(defaultMs)}`);
          }
          // If decimals allowed changed, rebuild order and retry immediately.
          const updated = this.tryUpdateBidDecimalsAllowedFromError(err);
          if (updated) {
            const retryWei = this.snapBidWeiUp(snappedWei);
            if (retryWei !== snappedWei) {
              if (maxWei !== undefined && retryWei > maxWei) throw err;
              snappedWei = retryWei;
              order = await createAndSign(snappedWei);
            }
            continue;
          }

          this.noteOfferPostRateLimit(err);
          if (err instanceof OpenSeaHttpError && err.statusCode === 429) {
            // Wait for slot (Retry-After) then retry.
            continue;
          }
          if (err instanceof OpenSeaHttpError && err.statusCode >= 500 && err.statusCode <= 599) {
            // Transient OpenSea/server errors: backoff a bit then retry.
            const backoff = (800 + attempt * 600) + Math.floor(Math.random() * 250);
            await sleep(backoff);
            continue;
          }
          throw err;
        }
      }
      throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
    });

    const orderHash = postResp?.order_hash ?? postResp?.orderHash ?? postResp?.order?.order_hash ?? postResp?.order?.orderHash;
    if (orderHash) this.logToken(tokenId, `openseaAccepted orderHash=${String(orderHash).slice(0, 10)}…`);
  }

  private async cancelOrders(orders: any[]): Promise<void> {
    if (!this.wallet || !this.seaport) return;
    const ours = orders
      .filter((o) => {
        const maker = o?.maker?.address ?? o?.maker_address ?? o?.protocol_data?.parameters?.offerer;
        if (!maker) return false;
        try {
          return normalizeAddress(String(maker)) === this.wallet!.address;
        } catch {
          return false;
        }
      })
      .slice(0, 5)
      .map((o) => o?.protocol_data?.parameters)
      .filter(Boolean);

    if (!ours.length) return;
    this.logInfo(`autoBid cancelling ${ours.length} old order(s)`);
    const tx = await this.seaport.cancelOrders(ours, this.wallet.address).transact();
    this.logInfo(`autoBid cancel tx=${tx.hash}`);
    await tx.wait();
  }

  private async fetchBestOfferForToken(tokenId: string): Promise<BestOfferResult> {
    if (!this.chain || !this.contract) throw new Error('AutoBid missing chain/contract');
    const apiKey = this.pickApiKey();
    const timeoutMs = Number(process.env.AUTO_BID_OFFER_FETCH_TIMEOUT_MS ?? '20000');
    const safeTimeoutMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.floor(timeoutMs) : 20_000;
    const url = `${OPENSEA_API_BASE}/api/v2/orders/${this.chain}/seaport/offers?asset_contract_address=${this.contract}&token_ids=${tokenId}&limit=50`;
    const data = await this.openseaGetWithTimeoutRetries(url, {
      apiKey,
      retries: 6,
      retryDelayMs: 900,
      timeoutMs: safeTimeoutMs
    });
    const orders = Array.isArray(data?.orders) ? data.orders : [];

    let bestWei = 0n;
    let bestMaker: string | undefined;
    for (const o of orders) {
      // OpenSea orders response varies by endpoint/version; use Seaport protocol_data as primary source of truth.
      const payAddr =
        o?.protocol_data?.parameters?.offer?.[0]?.token ??
        o?.maker_asset_bundle?.assets?.[0]?.asset_contract?.address ??
        o?.maker_asset_bundle?.assets?.[0]?.token_address ??
        undefined;

      const price =
        o?.protocol_data?.parameters?.offer?.[0]?.endAmount ??
        o?.protocol_data?.parameters?.offer?.[0]?.startAmount ??
        o?.current_price ??
        o?.base_price ??
        undefined;

      if (!payAddr || !price) continue;
      if (!this.paymentToken) continue;
      if (normalizeAddress(String(payAddr)) !== this.paymentToken) continue;
      try {
        const wei = BigInt(String(price));
        if (wei > bestWei) {
          bestWei = wei;
          const maker =
            o?.protocol_data?.parameters?.offerer ??
            o?.maker?.address ??
            o?.maker_address ??
            o?.maker?.user?.address ??
            undefined;
          bestMaker = maker ? String(maker) : undefined;
        }
      } catch {
        // ignore
      }
    }

    return { bestWei, bestMaker, orders };
  }

  private async openseaGetWithTimeoutRetries(
    url: string,
    params: {
      apiKey: string;
      retries: number;
      retryDelayMs: number;
      timeoutMs: number;
    }
  ): Promise<any> {
    const attempts = 3;
    let lastErr: unknown;
    for (let i = 0; i < attempts; i++) {
      try {
        return await openseaRequestJson<any>(url, {
          apiKey: params.apiKey,
          retries: params.retries,
          retryDelayMs: params.retryDelayMs,
          timeoutMs: params.timeoutMs
        });
      } catch (err) {
        lastErr = err;
        const msg = err instanceof Error ? err.message : String(err);
        const isTimeout = msg.includes('request timed out') || msg.includes('timed out');
        if (!isTimeout) throw err;
        const backoff = 400 + i * 600 + Math.floor(Math.random() * 250);
        await sleep(backoff);
      }
    }
    throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
  }

  private async fetchBestCollectionOfferWei(): Promise<bigint> {
    if (!this.chain || !this.slug) throw new Error('AutoBid missing chain/slug');
    const apiKey = this.pickApiKey();
    if (!this.paymentToken) throw new Error('AutoBid missing payment token');

    const timeoutMs = Number(process.env.AUTO_BID_OFFER_FETCH_TIMEOUT_MS ?? '20000');
    const safeTimeoutMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.floor(timeoutMs) : 20_000;
    const maxPagesRaw = Number(process.env.AUTO_BID_COLLECTION_OFFER_MAX_PAGES ?? '5');
    const maxPages = Number.isFinite(maxPagesRaw) && maxPagesRaw > 0 ? Math.min(50, Math.floor(maxPagesRaw)) : 5;

    let next: string | undefined;
    // Track best per-item price across collection-wide offers.
    let bestPayWei = 0n;
    let bestQty = 1n;
    let offersSeen = 0;
    let pagesScanned = 0;

    for (let page = 1; page <= maxPages; page++) {
      const url = new URL(`${OPENSEA_API_BASE}/api/v2/offers/collection/${encodeURIComponent(this.slug)}`);
      if (next) url.searchParams.set('next', next);

      const data = await this.openseaGetWithTimeoutRetries(String(url), {
        apiKey,
        retries: 6,
        retryDelayMs: 900,
        timeoutMs: safeTimeoutMs
      });

      pagesScanned += 1;
      const offers = Array.isArray(data?.offers) ? data.offers : [];
      next = typeof data?.next === 'string' ? data.next : undefined;

      for (const o of offers) {
        if (String(o?.chain ?? '') !== this.chain) continue;

        // Only use collection-wide offers (not trait-restricted).
        // OpenSea marks trait-restricted offers in `criteria.trait` / `criteria.traits`.
        // Some responses omit `encoded_token_ids` even for collection-wide offers, so be permissive there.
        const criteria = (o as any)?.criteria;
        if (criteria) {
          const traitsField = criteria.traits;
          const hasTraitRestriction =
            criteria.trait != null ||
            (Array.isArray(traitsField) ? traitsField.length > 0 : traitsField != null);
          if (hasTraitRestriction) continue;

          const enc = criteria.encoded_token_ids;
          const encOk = enc === undefined || enc === null || enc === '' || enc === '*';
          if (!encOk) continue;
        }

        const offerItem = o?.protocol_data?.parameters?.offer?.[0];
        const payAddr = offerItem?.token;
        const amount = offerItem?.endAmount ?? offerItem?.startAmount;
        if (!payAddr || !amount) continue;

        try {
          if (normalizeAddress(String(payAddr)) !== this.paymentToken) continue;
        } catch {
          continue;
        }

        // Offers API can return bulk collection offers (quantity > 1).
        // Use best *per item* price, not the total payment amount.
        const qtyRaw =
          (o as any)?.protocol_data?.parameters?.consideration?.[0]?.endAmount ??
          (o as any)?.protocol_data?.parameters?.consideration?.[0]?.startAmount ??
          '1';

        let payWei: bigint;
        let qty: bigint;
        try {
          payWei = BigInt(String(amount));
          qty = BigInt(String(qtyRaw));
          if (qty <= 0n) qty = 1n;
        } catch {
          continue;
        }

        offersSeen += 1;
        // Compare payWei/qty > bestPayWei/bestQty via cross-multiplication.
        if (payWei * bestQty > bestPayWei * qty) {
          bestPayWei = payWei;
          bestQty = qty;
        }
      }

      if (!next) break;
    }

    if (offersSeen === 0) {
      this.logInfo(`basePrice(collection) none found (pages=${pagesScanned})`);
      return 0n;
    }
    const bestPerItemWei = bestPayWei / bestQty;
    this.logInfo(
      `basePrice(collection) bestPerItem=${fmtEthShort(bestPerItemWei)} (offers=${offersSeen}, pages=${pagesScanned})`
    );
    return bestPerItemWei;
  }

  private async maybeBootstrapThrottle(): Promise<void> {
    const now = Date.now();
    if (this.bootstrapThrottleUntilMs > now) {
      await sleep(this.bootstrapThrottleUntilMs - now);
    }
  }

  private noteBootstrapRateLimit(err: unknown): void {
    const msg = err instanceof Error ? err.message : String(err);
    const now = Date.now();
    const is429 = msg.includes('OpenSea HTTP 429');
    const isTimeout = msg.includes('timed out');
    if (!is429 && !isTimeout) return;

    const bump = is429 ? 2500 : 800;
    const next = Math.min(20_000, Math.max(this.bootstrapThrottleBackoffMs, bump) + bump);
    this.bootstrapThrottleBackoffMs = next;
    this.bootstrapThrottleUntilMs = Math.max(this.bootstrapThrottleUntilMs, now + next + Math.floor(Math.random() * 250));
  }

  private async bootstrapInitialBids(): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    if (!this.wallet) return;

    const watched = this.watchedTokenIds;
    const incrementEth = normalizeDecimalString(String(settings.autoBidIncrement ?? '0.0001'));
    const incrementWei = ethers.parseEther(incrementEth);
    const maxWei = ethers.parseEther(normalizeDecimalString(String(settings.autoBidMaxPrice)));
    const renewIfBestIsOurs = settings.autoBidRenewIfBestIsOurs === true;

    const baseMode = getBasePriceMode();
    let basePriceWei: bigint | undefined;

    if (baseMode === 'none') {
      basePriceWei = undefined;
    } else if (baseMode === 'collection') {
      try {
        const best = await this.fetchBestCollectionOfferWei();
        basePriceWei = best > 0n ? best : undefined;
        if (basePriceWei && basePriceWei > this.bestCollectionOfferPerItemWei) this.bestCollectionOfferPerItemWei = basePriceWei;
      } catch (err) {
        this.logInfo(`basePrice(collection) error: ${shortErrorMessage(err)}`);
        basePriceWei = undefined;
      }
    } else {
      const basePriceStr =
        getNonEmptyString(process.env.AUTO_BID_BASE_PRICE) ?? undefined;
      basePriceWei = basePriceStr ? ethers.parseEther(normalizeDecimalString(basePriceStr)) : undefined;
    }

    const delayMs = Number(process.env.AUTO_BID_BOOTSTRAP_DELAY_MS ?? '0');
    const safeDelayMs =
      Number.isFinite(delayMs) && delayMs >= 0
        ? Math.floor(delayMs)
        : 0;

    const concurrencyEnv = Number(process.env.AUTO_BID_BOOTSTRAP_CONCURRENCY ?? '5');
    const concurrency = Number.isFinite(concurrencyEnv) && concurrencyEnv > 0 ? Math.floor(concurrencyEnv) : 1;

    const ids = Array.from(watched);
    let cursor = 0;
    let done = 0;

    this.logInfo(
      `bootstrap start ids=${ids.length} concurrency=${concurrency} baseMode=${baseMode} base=${basePriceWei ? fmtEthShort(basePriceWei) : 'none'}`
    );

    const worker = async (workerId: number) => {
      while (this.running) {
        const i = cursor;
        cursor += 1;
        if (i >= ids.length) return;
        const tokenId = ids[i];

        try {
          await this.maybeBootstrapThrottle();
          const { bestWei, bestMaker, orders } = await this.fetchBestOfferForToken(tokenId);

          const current = this.tokenState.get(tokenId) ?? {
            bestSeenWei: 0n,
            ourBidWei: 0n,
            lastBidAtMs: 0,
            inFlight: false
          };

          let referenceWei = bestWei;
          if (basePriceWei && basePriceWei > referenceWei) referenceWei = basePriceWei;
          if (referenceWei <= 0n) {
            this.logToken(tokenId, 'bestOffer none (skip; no base)');
            continue;
          }
          current.bestSeenWei = referenceWei;
          this.tokenState.set(tokenId, current);
          this.logToken(tokenId, `bestOffer ${fmtEthShort(referenceWei)}`);

          if (bestMaker && this.wallet) {
            try {
              if (normalizeAddress(bestMaker) === this.wallet.address) {
                current.ourBidWei = bestWei > 0n ? bestWei : current.bestSeenWei;
                this.tokenState.set(tokenId, current);
                if (!renewIfBestIsOurs) {
                  this.logToken(tokenId, `skip ours=${fmtEthShort(current.ourBidWei)}`);
                  continue;
                }
                if (bestWei <= 0n) {
                  this.logToken(tokenId, 'skip renew (ours but price missing)');
                  continue;
                }
                const renewWei = this.snapBidWeiUp(bestWei);
                if (renewWei > maxWei) {
                  this.logToken(tokenId, `skip renew=${fmtEthShort(renewWei)} > max=${fmtEthShort(maxWei)}`);
                  continue;
                }
                this.logToken(tokenId, `renew placing ${fmtEthShort(renewWei)}`);
                if (settings.autoBidCancelPrevious) await this.cancelOrders(orders);
                await this.placeOffer(tokenId, renewWei, settings.autoBidDurationMinutes ?? 60 * 24, maxWei);
                current.ourBidWei = renewWei;
                current.lastBidAtMs = Date.now();
                this.tokenState.set(tokenId, current);
                this.logToken(tokenId, `renewed ${fmtEthShort(renewWei)}`);
                continue;
              }
            } catch {
              // ignore
            }
          }

	          // If there are no item offers, start from best *collection offer per item* and add the increment.
	          const desiredWeiRaw = referenceWei + incrementWei;
	          const desiredWei = this.snapBidWeiUp(desiredWeiRaw);
          if (desiredWei > maxWei) {
            this.logToken(tokenId, `skip desired=${fmtEthShort(desiredWei)} > max=${fmtEthShort(maxWei)}`);
            continue;
          }

	          this.logToken(tokenId, `placing ${fmtEthShort(desiredWei)}`);
	          if (settings.autoBidCancelPrevious) await this.cancelOrders(orders);
	          await this.placeOffer(tokenId, desiredWei, settings.autoBidDurationMinutes ?? 60 * 24, maxWei);
	          current.ourBidWei = desiredWei;
	          current.lastBidAtMs = Date.now();
	          this.tokenState.set(tokenId, current);
	          this.logToken(tokenId, `placed ${fmtEthShort(desiredWei)}`);
      } catch (err) {
        this.logToken(tokenId, `error ${shortErrorMessage(err)}`);
        this.noteBootstrapRateLimit(err);
        const retryable =
          err instanceof OpenSeaHttpError
            ? err.statusCode === 429 || (err.statusCode >= 500 && err.statusCode <= 599)
            : String(err instanceof Error ? err.message : err).toLowerCase().includes('timed out');
        if (retryable) {
          const baseDelayMs =
            err instanceof OpenSeaHttpError && err.statusCode === 429 ? err.retryAfterMs(5000) : 3500;
          const jitter = Math.floor(Math.random() * 400);
          const waitMs = baseDelayMs + jitter;
          this.logToken(tokenId, `retry scheduled in ${Math.ceil(waitMs / 1000)}s`);
          this.scheduleRetryAfterCooldown(tokenId, { minDelayMs: waitMs });
        }
      } finally {
        done += 1;
        if (done % 25 === 0 || done === ids.length) this.logInfo(`bootstrap ${done}/${ids.length}`);
        if (safeDelayMs > 0) await sleep(safeDelayMs);
      }
    }
  };

    await Promise.all(Array.from({ length: concurrency }, (_, idx) => worker(idx + 1)));
    this.logInfo('bootstrap done');
  }

  private async bootstrapCollectionBids(): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    if (!this.wallet) return;

    const watched = this.watchedTokenIds;
    const incrementEth = normalizeDecimalString(String(settings.autoBidIncrement ?? '0.0001'));
    const incrementWei = ethers.parseEther(incrementEth);
    const maxWei = ethers.parseEther(normalizeDecimalString(String(settings.autoBidMaxPrice)));

    let basePriceWei: bigint | undefined;
    try {
      const best = await this.fetchBestCollectionOfferWei();
      basePriceWei = best > 0n ? best : undefined;
      if (basePriceWei && basePriceWei > this.bestCollectionOfferPerItemWei) this.bestCollectionOfferPerItemWei = basePriceWei;
    } catch (err) {
      this.logInfo(`basePrice(collection) error: ${shortErrorMessage(err)}`);
      basePriceWei = undefined;
    }
    if (!basePriceWei || basePriceWei <= 0n) {
      this.logInfo('bootstrap(collection) skip (no base price)');
      return;
    }

    const baseWei = basePriceWei;
    const desiredWei = this.snapBidWeiUp(baseWei + incrementWei);
    if (desiredWei > maxWei) {
      this.logInfo(`bootstrap(collection) skip desired=${fmtEthShort(desiredWei)} > max=${fmtEthShort(maxWei)}`);
      return;
    }

    const delayMs = Number(process.env.AUTO_BID_BOOTSTRAP_DELAY_MS ?? '0');
    const safeDelayMs =
      Number.isFinite(delayMs) && delayMs >= 0
        ? Math.floor(delayMs)
        : 0;

    const concurrencyEnv = Number(process.env.AUTO_BID_BOOTSTRAP_CONCURRENCY ?? '5');
    const concurrency = Number.isFinite(concurrencyEnv) && concurrencyEnv > 0 ? Math.floor(concurrencyEnv) : 1;

    const ids = Array.from(watched);
    let cursor = 0;
    let done = 0;

    this.logInfo(
      `bootstrap(collection) start ids=${ids.length} concurrency=${concurrency} base=${fmtEthShort(baseWei)}`
    );

    const worker = async (workerId: number) => {
      while (this.running) {
        const i = cursor;
        cursor += 1;
        if (i >= ids.length) return;
        const tokenId = ids[i];

        try {
          await this.maybeBootstrapThrottle();
          const current = this.tokenState.get(tokenId) ?? {
            bestSeenWei: 0n,
            ourBidWei: 0n,
            lastBidAtMs: 0,
            inFlight: false
          };
          current.bestSeenWei = baseWei;
          this.tokenState.set(tokenId, current);

          this.logToken(tokenId, `placing ${fmtEthShort(desiredWei)}`);
          await this.placeOffer(tokenId, desiredWei, settings.autoBidDurationMinutes ?? 60 * 24, maxWei);
          current.ourBidWei = desiredWei;
          current.lastBidAtMs = Date.now();
          this.tokenState.set(tokenId, current);
          this.logToken(tokenId, `placed ${fmtEthShort(desiredWei)}`);
      } catch (err) {
        this.logToken(tokenId, `error ${shortErrorMessage(err)}`);
        this.noteBootstrapRateLimit(err);
        const retryable =
          err instanceof OpenSeaHttpError
            ? err.statusCode === 429 || (err.statusCode >= 500 && err.statusCode <= 599)
            : String(err instanceof Error ? err.message : err).toLowerCase().includes('timed out');
        if (retryable) {
          const baseDelayMs =
            err instanceof OpenSeaHttpError && err.statusCode === 429 ? err.retryAfterMs(5000) : 3500;
          const jitter = Math.floor(Math.random() * 400);
          const waitMs = baseDelayMs + jitter;
          this.logToken(tokenId, `retry scheduled in ${Math.ceil(waitMs / 1000)}s`);
          this.scheduleRetryAfterCooldown(tokenId, { minDelayMs: waitMs });
        }
      } finally {
        done += 1;
        if (done % 25 === 0 || done === ids.length) this.logInfo(`bootstrap(collection) ${done}/${ids.length}`);
        if (safeDelayMs > 0) await sleep(safeDelayMs);
        }
      }
    };

    await Promise.all(Array.from({ length: concurrency }, (_, idx) => worker(idx + 1)));
    this.logInfo('bootstrap(collection) done');
  }

  private async bootstrapFixedPriceBids(): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    if (!this.wallet) return;

    const watched = this.watchedTokenIds;
    const fixedPriceStr = getNonEmptyString(settings.autoBidFixedPrice ?? '') ?? undefined;
    if (!fixedPriceStr) {
      this.logInfo('bootstrap(fixed) skip (missing fixed price)');
      return;
    }
    const fixedWeiRaw = ethers.parseEther(normalizeDecimalString(fixedPriceStr));
    const fixedWei = this.snapBidWeiUp(fixedWeiRaw);

    const delayMs = Number(process.env.AUTO_BID_BOOTSTRAP_DELAY_MS ?? '0');
    const safeDelayMs =
      Number.isFinite(delayMs) && delayMs >= 0
        ? Math.floor(delayMs)
        : 0;

    const concurrencyEnv = Number(process.env.AUTO_BID_BOOTSTRAP_CONCURRENCY ?? '5');
    const concurrency = Number.isFinite(concurrencyEnv) && concurrencyEnv > 0 ? Math.floor(concurrencyEnv) : 1;

    const ids = Array.from(watched);
    let cursor = 0;
    let done = 0;

    this.logInfo(
      `bootstrap(fixed) start ids=${ids.length} concurrency=${concurrency} price=${fmtEthShort(fixedWei)}`
    );

    const worker = async (workerId: number) => {
      while (this.running) {
        const i = cursor;
        cursor += 1;
        if (i >= ids.length) return;
        const tokenId = ids[i];

        try {
          await this.maybeBootstrapThrottle();
          const current = this.tokenState.get(tokenId) ?? {
            bestSeenWei: 0n,
            ourBidWei: 0n,
            lastBidAtMs: 0,
            inFlight: false
          };
          current.bestSeenWei = fixedWei;
          this.tokenState.set(tokenId, current);

          let orders: any[] = [];
          if (settings.autoBidCancelPrevious) {
            const fetched = await this.fetchBestOfferForToken(tokenId);
            orders = fetched.orders ?? [];
          }

          this.logToken(tokenId, `placing ${fmtEthShort(fixedWei)}`);
          if (settings.autoBidCancelPrevious && orders.length) await this.cancelOrders(orders);
          await this.placeOffer(tokenId, fixedWei, settings.autoBidDurationMinutes ?? 60 * 24);
          current.ourBidWei = fixedWei;
          current.lastBidAtMs = Date.now();
          this.tokenState.set(tokenId, current);
          this.logToken(tokenId, `placed ${fmtEthShort(fixedWei)}`);
        } catch (err) {
          this.logToken(tokenId, `error ${shortErrorMessage(err)}`);
          this.noteBootstrapRateLimit(err);
          const retryable =
            err instanceof OpenSeaHttpError
              ? err.statusCode === 429 || (err.statusCode >= 500 && err.statusCode <= 599)
              : String(err instanceof Error ? err.message : err).toLowerCase().includes('timed out');
          if (retryable) {
            const baseDelayMs =
              err instanceof OpenSeaHttpError && err.statusCode === 429 ? err.retryAfterMs(5000) : 3500;
            const jitter = Math.floor(Math.random() * 400);
            const waitMs = baseDelayMs + jitter;
            this.logToken(tokenId, `retry scheduled in ${Math.ceil(waitMs / 1000)}s`);
            this.scheduleRetryAfterCooldown(tokenId, { minDelayMs: waitMs });
          }
        } finally {
          done += 1;
          if (done % 25 === 0 || done === ids.length) this.logInfo(`bootstrap(fixed) ${done}/${ids.length}`);
          if (safeDelayMs > 0) await sleep(safeDelayMs);
        }
      }
    };

    await Promise.all(Array.from({ length: concurrency }, (_, idx) => worker(idx + 1)));
    this.logInfo('bootstrap(fixed) done');
  }

  private async pollBestOffers(): Promise<void> {
    if (!this.running) return;
    const settings = this.settingsManager.getSettings();
    if (!settings.autoBidEnabled) return;
    if (!this.chain || !this.contract) return;
    const watched = parseTokenIds(settings.autoBidTokenIds ?? settings.offerTokenIds);
    if (watched.size === 0) return;

    const incrementEth = normalizeDecimalString(String(settings.autoBidIncrement ?? '0.0001'));
    const incrementWei = ethers.parseEther(incrementEth);
    const maxWei = ethers.parseEther(normalizeDecimalString(String(settings.autoBidMaxPrice)));
    const cooldownSeconds = settings.autoBidCooldownSeconds ?? 45;
    const baseMode = getBasePriceMode();
    let basePriceWei: bigint | undefined;
    let basePriceLoaded = false;
    const loadBasePrice = async (): Promise<bigint | undefined> => {
      if (baseMode === 'none') return undefined;
      if (baseMode === 'fixed') {
        if (!basePriceLoaded) {
          const basePriceStr = getNonEmptyString(process.env.AUTO_BID_BASE_PRICE) ?? undefined;
          basePriceWei = basePriceStr ? ethers.parseEther(normalizeDecimalString(basePriceStr)) : undefined;
          basePriceLoaded = true;
        }
        return basePriceWei;
      }
      if (basePriceLoaded) return basePriceWei;
      basePriceLoaded = true;
      try {
        const best = await this.fetchBestCollectionOfferWei();
        basePriceWei = best > 0n ? best : undefined;
        if (basePriceWei && basePriceWei > this.bestCollectionOfferPerItemWei) {
          this.bestCollectionOfferPerItemWei = basePriceWei;
        }
      } catch (err) {
        this.log?.(`autoBid: poll basePrice(collection) error: ${shortErrorMessage(err)}`);
        basePriceWei = undefined;
      }
      return basePriceWei;
    };

    this.log?.(`autoBid: poll tick ids=${watched.size}`);

    for (const tokenId of watched) {
      try {
        const { bestWei, bestMaker, orders } = await this.fetchBestOfferForToken(tokenId);
        if (orders.length > 0 && bestWei <= 0n) {
          this.log?.(`autoBid: tokenId=${tokenId} offers found but none in paymentToken=${this.paymentToken}`);
          continue;
        }

        const current = this.tokenState.get(tokenId) ?? {
          bestSeenWei: 0n,
          ourBidWei: 0n,
          lastBidAtMs: 0,
          inFlight: false
        };
        let referenceWei = bestWei;
        if (!orders.length) {
          const baseWei = await loadBasePrice();
          if (baseWei && baseWei > referenceWei) referenceWei = baseWei;
        }
        if (this.bestCollectionOfferPerItemWei > referenceWei) referenceWei = this.bestCollectionOfferPerItemWei;
        if (referenceWei <= 0n) {
          this.log?.(`autoBid: tokenId=${tokenId} no offers and no base`);
          continue;
        }
        if (referenceWei > current.bestSeenWei) current.bestSeenWei = referenceWei;

        if (this.wallet && bestMaker) {
          try {
            if (normalizeAddress(bestMaker) === this.wallet.address) {
              current.ourBidWei = current.bestSeenWei;
              this.tokenState.set(tokenId, current);
              this.log?.(`autoBid: tokenId=${tokenId} best is ours (${ethers.formatEther(bestWei)})`);
              continue;
            }
          } catch {
            // ignore
          }
        }

        const desiredWeiRaw = current.bestSeenWei + incrementWei;
        const desiredWei = this.snapBidWeiUp(desiredWeiRaw);
        const nowMs = Date.now();
        const cooldownOk = nowMs - current.lastBidAtMs >= cooldownSeconds * 1000;
        const shouldBid =
          desiredWei <= maxWei &&
          current.ourBidWei < desiredWei &&
          !current.inFlight &&
          cooldownOk;

        this.tokenState.set(tokenId, current);
        this.log?.(
          `autoBid: tokenId=${tokenId} best=${ethers.formatEther(current.bestSeenWei)} our=${ethers.formatEther(
            current.ourBidWei
          )} desired=${ethers.formatEther(desiredWei)} max=${ethers.formatEther(maxWei)} inFlight=${current.inFlight} cooldownOk=${cooldownOk}`
        );
        if (!shouldBid) continue;

        current.inFlight = true;
        this.tokenState.set(tokenId, current);

        this.enqueue(async () => {
          try {
            if (settings.autoBidCancelPrevious) {
              await this.cancelOrders(orders);
            }
            await this.placeOffer(tokenId, desiredWei, settings.autoBidDurationMinutes ?? 60 * 24, maxWei);
            this.log?.(
              `autoBid: polled+placed tokenId=${tokenId} best=${ethers.formatEther(bestWei)} new=${ethers.formatEther(desiredWei)}`
            );
            const updated = this.tokenState.get(tokenId);
            if (updated) {
              updated.ourBidWei = desiredWei;
              updated.lastBidAtMs = Date.now();
            }
          } finally {
            const updated = this.tokenState.get(tokenId);
            if (updated) updated.inFlight = false;
          }
        });
      } catch (err) {
        this.log?.(`autoBid: tokenId=${tokenId} poll error: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }
}

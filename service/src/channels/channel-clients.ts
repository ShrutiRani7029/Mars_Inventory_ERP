import { Injectable } from '@nestjs/common';
import { Channel } from '../common/types';
import { config } from '../config';
import { RateLimiter } from '../redis/rate-limiter.service';

export interface StockLine {
  sku: string;
  qty: number;
}

export interface PushOutcome {
  sku: string;
  ok: boolean;
  error?: string;
  /** 429 / timeout / 5xx: back off and try again. */
  retryable?: boolean;
}

export class HttpError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

/** fetch with a hard timeout. Node's fetch has none by default. */
export async function httpJson(method: string, url: string, body: unknown, timeoutMs: number, contentType = 'application/json'): Promise<any> {
  const res = await fetch(url, {
    method,
    headers: body === undefined ? {} : { 'content-type': contentType },
    body: body === undefined ? undefined : contentType === 'application/json' ? JSON.stringify(body) : String(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) throw new HttpError(res.status, `${method} ${url} -> ${res.status} ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : null;
}

const describe = (e: unknown): { error: string; retryable: boolean } => {
  if (e instanceof HttpError) return { error: e.message, retryable: e.status === 429 || e.status >= 500 };
  const name = (e as Error)?.name;
  if (name === 'TimeoutError' || name === 'AbortError') return { error: 'timeout', retryable: true };
  return { error: (e as Error)?.message ?? String(e), retryable: true }; // network errors
};

export interface StockClient {
  readonly channel: Channel;
  push(lines: StockLine[]): Promise<PushOutcome[]>;
}

/** Channel adapters against the API contract in the brief (pointed at the mock server locally). */
@Injectable()
export class StockClients {
  constructor(private readonly limiter: RateLimiter) {}

  get(channel: Channel): StockClient {
    const base = config.mockBaseUrl;
    const t = config.channelTimeoutMs;
    const limiter = this.limiter;

    /*
     * One SKU per call, several calls in flight. The token bucket sets the pace; the parallel
     * slots stop one hung call (Blinkit's ~2% timeouts) from blocking every SKU behind it.
     */
    const onePerCall = (path: (sku: string) => string, timeout: number): StockClient['push'] => async (lines) => {
      const out: PushOutcome[] = [];
      let next = 0;
      let throttled = false;
      const slot = async () => {
        while (next < lines.length) {
          const l = lines[next++];
          if (throttled) {
            out.push({ sku: l.sku, ok: false, error: 'skipped after 429', retryable: true });
            continue;
          }
          await limiter.acquire(channel);
          try {
            await httpJson('PUT', `${base}${path(l.sku)}`, { qty: l.qty }, timeout);
            out.push({ sku: l.sku, ok: true });
          } catch (e) {
            out.push({ sku: l.sku, ok: false, ...describe(e) });
            // Over the limit anyway (clock skew, another process): stop, the rest stay dirty.
            if (e instanceof HttpError && e.status === 429) throttled = true;
          }
        }
      };
      const slots = Math.max(1, Math.ceil(config.rates[channel]));
      await Promise.all(Array.from({ length: Math.min(slots, lines.length) }, slot));
      return out;
    };

    switch (channel) {
      case 'WEBSITE':
        // We own it: one bulk call.
        return {
          channel,
          push: async (lines) => {
            await limiter.acquire(channel);
            try {
              await httpJson('PUT', `${base}/website/stock`, { items: lines }, t);
              return lines.map((l) => ({ sku: l.sku, ok: true }));
            } catch (e) {
              return lines.map((l) => ({ sku: l.sku, ok: false, ...describe(e) }));
            }
          },
        };
      case 'NYKAA':
        return { channel, push: onePerCall((sku) => `/nykaa/inventory/${encodeURIComponent(sku)}`, t) };
      case 'BLINKIT':
        // ~2% of calls hang: short timeout, retry later. Safe because we always send absolute quantities.
        return { channel, push: onePerCall((sku) => `/blinkit/inventory/${encodeURIComponent(sku)}`, config.blinkitTimeoutMs) };
      case 'FLIPKART':
        return {
          channel,
          push: async (lines) => {
            const out: PushOutcome[] = [];
            for (let i = 0; i < lines.length; i += 50) {
              const chunk = lines.slice(i, i + 50); // bulk API: max 50 SKUs per call
              await limiter.acquire(channel);
              try {
                await httpJson('POST', `${base}/flipkart/inventory/bulk`, { items: chunk }, t);
                out.push(...chunk.map((l) => ({ sku: l.sku, ok: true })));
              } catch (e) {
                out.push(...chunk.map((l) => ({ sku: l.sku, ok: false, ...describe(e) })));
              }
            }
            return out;
          },
        };
    }
  }
}

import { Injectable, Logger } from '@nestjs/common';
import { StockClients, StockLine } from '../channels/channel-clients';
import { Channel } from '../common/types';
import { config } from '../config';
import { Db } from '../db/db.service';

/**
 * Should this change be sent now? Low stock is ALWAYS pushed (that is where overselling
 * happens). On one-SKU-per-call channels (Nykaa at 5 rps; Blinkit, whose REST API is for
 * urgent changes with the daily CSV as baseline) small changes on healthy SKUs are skipped
 * so the call budget goes to SKUs that matter.
 */
export function shouldPush(channel: Channel, last: number | null, desired: number): boolean {
  if (last === null || last === undefined) return true;
  if (desired === last) return false;
  if (desired <= config.lowStockThreshold || last <= config.lowStockThreshold) return true;
  const perSkuChannel = channel === 'NYKAA' || channel === 'BLINKIT';
  if (perSkuChannel && Math.min(last, desired) > config.bigStockThreshold && Math.abs(desired - last) / last < config.nykaaSuppressRatio) {
    return false;
  }
  return true;
}

interface Claimed {
  sku: string;
  desired: number;
  last_pushed_qty: number | null;
  dirty_since: Date | null;
}

/**
 * Pushes ABSOLUTE quantities (never deltas), so retries and duplicates are harmless and
 * 50 orders on one SKU coalesce into one call. Lowest stock goes first.
 */
@Injectable()
export class StockPusher {
  private readonly log = new Logger(StockPusher.name);

  constructor(private readonly db: Db, private readonly clients: StockClients) {}

  /** Claim a batch of dirty SKUs for `channel`, push them, record the result. Returns the number claimed. */
  async pushOnce(channel: Channel, limit = config.pushBatch): Promise<number> {
    // Claim + clear dirty in one short transaction. If a new order lands while we are
    // pushing, it sets dirty again and the next cycle sends the newer value.
    const { rows } = await this.db.query<Claimed>(
      `WITH picked AS (
         SELECT cs.sku, cs.channel, cs.dirty_since
           FROM channel_stock cs JOIN inventory i ON i.sku = cs.sku
          WHERE cs.channel = $1 AND cs.dirty AND cs.next_attempt_at <= now()
          ORDER BY (i.on_hand - i.reserved) ASC, cs.dirty_since ASC NULLS LAST
          LIMIT $2
          FOR UPDATE OF cs SKIP LOCKED)
       UPDATE channel_stock cs
          SET dirty = false, dirty_since = NULL
         FROM picked p, inventory i, skus s
        WHERE cs.sku = p.sku AND cs.channel = p.channel AND i.sku = p.sku AND s.sku = p.sku
       RETURNING cs.sku,
                 -- Flipkart orders reach us up to one poll late, so it would lose every last-unit
                 -- race: it stops selling a SKU a few units earlier than the real-time channels.
                 GREATEST(0, i.on_hand - i.reserved - s.safety_buffer
                             - CASE WHEN cs.channel = 'FLIPKART' THEN $3 ELSE 0 END)::int AS desired,
                 cs.last_pushed_qty,
                 p.dirty_since`,
      [channel, limit, config.polledChannelHoldback],
    );
    if (!rows.length) return 0;

    const toSend: StockLine[] = [];
    for (const r of rows) {
      if (shouldPush(channel, r.last_pushed_qty, r.desired)) toSend.push({ sku: r.sku, qty: r.desired });
    }
    if (!toSend.length) return rows.length;

    const results = await this.clients.get(channel).push(toSend);
    const since = new Map(rows.map((r) => [r.sku, r.dirty_since]));
    const qty = new Map(toSend.map((l) => [l.sku, l.qty]));

    const ok = results.filter((r) => r.ok);
    if (ok.length) {
      await this.db.query(
        `UPDATE channel_stock cs
            SET last_pushed_qty = v.qty, last_pushed_at = now(), attempts = 0, status = 'ok', last_error = NULL,
                last_lag_ms = CASE WHEN v.since IS NULL THEN NULL ELSE (extract(epoch FROM now() - v.since) * 1000)::int END
           FROM unnest($2::text[], $3::int[], $4::timestamptz[]) AS v(sku, qty, since)
          WHERE cs.channel = $1 AND cs.sku = v.sku`,
        [channel, ok.map((r) => r.sku), ok.map((r) => qty.get(r.sku)), ok.map((r) => since.get(r.sku) ?? null)],
      );
    }

    const failed = results.filter((r) => !r.ok);
    if (failed.length) {
      this.log.warn(`${channel}: ${failed.length}/${results.length} pushes failed (${failed[0].error})`);
      // Back on the dirty list with backoff. After maxPushAttempts the row shows up on the
      // ops screen as 'failed' but we keep retrying (stock must converge eventually).
      await this.db.query(
        `UPDATE channel_stock cs
            SET dirty = true,
                dirty_since = COALESCE(cs.dirty_since, v.since, now()),
                attempts = cs.attempts + 1,
                status = CASE WHEN cs.attempts + 1 >= $5 THEN 'failed' ELSE cs.status END,
                last_error = v.err,
                next_attempt_at = now() + make_interval(secs => LEAST(300, 2 * power(2, cs.attempts)) * (0.5 + random() / 2))
           FROM unnest($2::text[], $3::text[], $4::timestamptz[]) AS v(sku, err, since)
          WHERE cs.channel = $1 AND cs.sku = v.sku`,
        [channel, failed.map((r) => r.sku), failed.map((r) => r.error ?? 'error'), failed.map((r) => since.get(r.sku) ?? null), config.maxPushAttempts],
      );
    }
    return rows.length;
  }
}

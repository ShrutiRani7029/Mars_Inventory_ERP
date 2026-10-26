import { Injectable, Logger } from '@nestjs/common';
import { config } from '../config';
import { Db } from '../db/db.service';

/**
 * Safety buffer = units we hide from channels to cover what we cannot see yet:
 *   1. sales on any channel during the push delay, and
 *   2. Flipkart sales we have not polled yet (orders reach us up to one poll interval late).
 *
 *   buffer = max(MIN, ceil(1.5 x (v_all x push delay + v_flipkart x poll interval)))
 *   v = units reserved per minute over the last 5 minutes, per SKU.
 *
 * Hot SKUs get a bigger buffer during a sale; slow SKUs keep the minimum.
 * Ops can pin a value with skus.buffer_locked = true.
 */
@Injectable()
export class BufferTuner {
  private readonly log = new Logger(BufferTuner.name);

  constructor(private readonly db: Db) {}

  async tune(): Promise<number> {
    const r = await this.db.query(
      `WITH velocity AS (
         SELECT sku,
                SUM(delta_reserved)::numeric / 5 AS per_min,
                COALESCE(SUM(delta_reserved) FILTER (WHERE actor = 'FLIPKART'), 0)::numeric / 5 AS fk_per_min
           FROM inventory_ledger
          WHERE reason = 'RESERVE' AND created_at > now() - interval '5 minutes'
          GROUP BY sku),
       target AS (
         SELECT s.sku, GREATEST($1, CEIL(1.5 * (COALESCE(v.per_min, 0) * $2 + COALESCE(v.fk_per_min, 0) * $3)))::int AS buf
           FROM skus s LEFT JOIN velocity v ON v.sku = s.sku
          WHERE NOT s.buffer_locked)
       UPDATE skus s SET safety_buffer = t.buf
         FROM target t
        WHERE s.sku = t.sku AND s.safety_buffer <> t.buf
       RETURNING s.sku`,
      [config.minSafetyBuffer, config.expectedPushDelayMin, config.flipkartPollMs / 60_000],
    );
    if (r.rowCount) {
      // A changed buffer changes what channels should show.
      await this.db.query(`UPDATE channel_stock SET dirty = true, dirty_since = COALESCE(dirty_since, now()) WHERE sku = ANY($1)`, [r.rows.map((x) => x.sku)]);
      this.log.log(`safety buffer changed for ${r.rowCount} SKU(s)`);
    }
    return r.rowCount ?? 0;
  }
}

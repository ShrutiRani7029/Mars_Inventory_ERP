import { Injectable, Logger } from '@nestjs/common';
import { httpJson } from '../channels/channel-clients';
import { config } from '../config';
import { Db } from '../db/db.service';
import { IngestService } from './ingest.service';

const POLLER_LOCK_ID = 7_201_001;

/**
 * Flipkart has no webhooks. We poll "orders updated since cursor - overlap" and push them
 * through the same dedupe path as webhooks, so the overlap can never create duplicates.
 * The cursor only moves forward after the events are committed: if Flipkart is down we
 * simply ask again from the same point next time, so nothing is skipped.
 */
@Injectable()
export class FlipkartPoller {
  private readonly log = new Logger(FlipkartPoller.name);

  constructor(private readonly db: Db, private readonly ingest: IngestService) {}

  async pollOnce(): Promise<{ fetched: number; inserted: number } | null> {
    return this.db.tx(async (c) => {
      // Only one poller instance at a time, even if we run several worker tasks.
      const lock = await c.query(`SELECT pg_try_advisory_xact_lock($1) AS ok`, [POLLER_LOCK_ID]);
      if (!lock.rows[0].ok) return null;

      const cur = await c.query(`SELECT cursor FROM poll_cursors WHERE channel = 'FLIPKART' FOR UPDATE`);
      const cursor: Date = cur.rows[0]?.cursor ?? new Date(0);
      const since = new Date(Math.max(0, cursor.getTime() - config.flipkartOverlapMs));

      const res = await httpJson('GET', `${config.mockBaseUrl}/flipkart/orders?updatedAfter=${encodeURIComponent(since.toISOString())}`, undefined, config.channelTimeoutMs);
      const orders: any[] = res?.orders ?? [];

      let inserted = 0;
      let newest = cursor;
      for (const o of orders) {
        const r = await this.ingest.ingestRaw('FLIPKART', o, c);
        if (r.status === 'accepted') inserted++;
        const t = new Date(o.updatedAt);
        if (!Number.isNaN(t.getTime()) && t > newest) newest = t;
      }
      await c.query(
        `INSERT INTO poll_cursors (channel, cursor, updated_at) VALUES ('FLIPKART', $1, now())
         ON CONFLICT (channel) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
        [newest],
      );
      if (inserted) this.log.log(`flipkart: fetched ${orders.length}, new ${inserted}`);
      return { fetched: orders.length, inserted };
    });
  }
}

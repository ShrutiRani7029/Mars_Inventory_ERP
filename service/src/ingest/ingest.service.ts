import { Injectable, Logger } from '@nestjs/common';
import { createHmac, timingSafeEqual } from 'crypto';
import { NORMALIZERS, rawHash } from '../channels/normalizers';
import { Channel, OrderEvent } from '../common/types';
import { config } from '../config';
import { Db, Tx } from '../db/db.service';

export interface IngestResult {
  id: number | null;
  duplicate: boolean;
  status: 'accepted' | 'duplicate' | 'rejected';
}

export function sign(secret: string, body: string | Buffer): string {
  return createHmac('sha256', secret).update(body).digest('hex');
}

/**
 * Webhook / poll entry point. Deliberately does almost nothing: normalise, store, return.
 * The raw event is durable in Postgres BEFORE we acknowledge, so nothing is lost if workers are down.
 */
@Injectable()
export class IngestService {
  private readonly log = new Logger(IngestService.name);

  constructor(private readonly db: Db) {}

  /** Run on the caller's transaction when given one (Flipkart poller), else on the pool. */
  private q(c?: Tx) {
    return (text: string, params: unknown[]) => (c ? c.query(text, params as any[]) : this.db.query(text, params));
  }

  verifySignature(secret: string, rawBody: Buffer | undefined, signature: string | undefined): boolean {
    if (!config.verifySignatures) return true;
    if (!rawBody || !signature) return false;
    const expected = Buffer.from(sign(secret, rawBody), 'hex');
    const got = Buffer.from(signature, 'hex');
    return expected.length === got.length && timingSafeEqual(expected, got);
  }

  /** Normalise + store. A payload we cannot parse is stored as `dead` so ops can see it (never silently dropped). */
  async ingestRaw(channel: Channel, raw: unknown, c?: Tx): Promise<IngestResult> {
    let evt: OrderEvent;
    try {
      evt = NORMALIZERS[channel](raw);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.log.warn(`rejected ${channel} payload: ${msg}`);
      await this.q(c)(
        `INSERT INTO inbound_events (channel, dedupe_key, payload, status, last_error)
         VALUES ($1, $2, $3, 'dead', $4) ON CONFLICT (channel, dedupe_key) DO NOTHING`,
        [channel, rawHash(raw), JSON.stringify(raw), `parse error: ${msg}`],
      );
      return { id: null, duplicate: false, status: 'rejected' };
    }
    return this.ingestEvent(evt, raw, c);
  }

  async ingestEvent(evt: OrderEvent, raw: unknown, c?: Tx): Promise<IngestResult> {
    const r = await this.q(c)(
      `INSERT INTO inbound_events (channel, dedupe_key, event_type, external_order_id, event_time, payload, normalized)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (channel, dedupe_key) DO NOTHING
       RETURNING id`,
      [evt.channel, evt.dedupe_key, evt.event, evt.external_order_id, evt.event_time, JSON.stringify(raw), JSON.stringify(evt)],
    );
    if (!r.rowCount) return { id: null, duplicate: true, status: 'duplicate' };
    return { id: Number(r.rows[0].id), duplicate: false, status: 'accepted' };
  }
}

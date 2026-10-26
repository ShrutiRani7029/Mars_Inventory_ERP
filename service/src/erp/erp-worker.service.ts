import { Inject, Injectable, Logger } from '@nestjs/common';
import { HttpError } from '../channels/channel-clients';
import { config } from '../config';
import { Db } from '../db/db.service';
import { CircuitBreaker } from './circuit-breaker';
import { ErpClient, isRetryableErp } from './erp-client';

export const ERP_CLIENT = 'ERP_CLIENT';

interface OutboxRow {
  id: number;
  order_id: number;
  kind: 'CREATE' | 'CANCEL';
  attempts: number;
}

/**
 * Delivers the outbox to Business Central. Delivery is at-least-once; it is made
 * effectively-once by the idempotency key (externalDocumentNumber = MRS-<order id>) and by
 * looking the document up before creating it.
 */
@Injectable()
export class ErpWorker {
  private readonly log = new Logger(ErpWorker.name);
  readonly breaker = new CircuitBreaker(config.erpBreakerThreshold, config.erpBreakerOpenMs);

  constructor(private readonly db: Db, @Inject(ERP_CLIENT) private readonly erp: ErpClient) {}

  static docNo(orderId: number) {
    return `MRS-${orderId}`;
  }

  /** Returns false when idle (nothing due, or breaker open). */
  async processNext(): Promise<boolean> {
    if (!this.breaker.allow()) return false;

    // Lease one row. A row stuck in 'processing' (worker died) becomes claimable after the lease.
    const { rows } = await this.db.query<OutboxRow>(
      `UPDATE erp_outbox SET status = 'processing', locked_until = now() + make_interval(secs => $1::double precision / 1000)
        WHERE id = (
          SELECT id FROM erp_outbox
           WHERE (status = 'pending' AND next_attempt_at <= now())
              OR (status = 'processing' AND locked_until < now())
           ORDER BY next_attempt_at, id
           FOR UPDATE SKIP LOCKED LIMIT 1)
        RETURNING id, order_id, kind, attempts`,
      [config.erpLeaseMs],
    );
    if (!rows.length) {
      this.breaker.success(); // nothing to probe with; don't stay half-open forever
      return false;
    }
    const job = rows[0];
    const ext = ErpWorker.docNo(Number(job.order_id));

    try {
      if (job.kind === 'CREATE') {
        const doc = (await this.erp.findByExternalDoc(ext)) ?? (await this.erp.create(await this.buildOrder(Number(job.order_id), ext)));
        await this.done(job.id, doc);
      } else {
        const create = await this.db.query(`SELECT status, erp_doc_no FROM erp_outbox WHERE order_id = $1 AND kind = 'CREATE'`, [job.order_id]);
        const c = create.rows[0];
        if (!c || c.status === 'skipped') {
          await this.done(job.id, null);
        } else if (c.status === 'dead') {
          throw new HttpError(422, 'CREATE for this order is dead; retry it first from the ops console');
        } else if (c.status !== 'done') {
          // CREATE still in flight: wait for it, without burning an attempt.
          await this.db.query(`UPDATE erp_outbox SET status = 'pending', next_attempt_at = now() + interval '30 seconds' WHERE id = $1`, [job.id]);
        } else {
          await this.erp.cancel(c.erp_doc_no);
          await this.done(job.id, c.erp_doc_no);
        }
      }
      this.breaker.success();
      return true;
    } catch (err) {
      const retryable = isRetryableErp(err);
      if (retryable) this.breaker.failure();
      await this.fail(job, err, retryable);
      return true;
    }
  }

  private async buildOrder(orderId: number, ext: string) {
    const { rows: [o] } = await this.db.query(`SELECT * FROM orders WHERE id = $1`, [orderId]);
    const { rows: items } = await this.db.query(`SELECT sku, qty, unit_price FROM order_items WHERE order_id = $1 ORDER BY sku`, [orderId]);
    return {
      externalDocumentNumber: ext,
      channel: o.channel,
      channelOrderId: o.external_order_id,
      paymentMode: o.payment_mode,
      shipToPincode: o.shipping_pincode,
      lines: items.map((i) => ({ sku: i.sku, qty: i.qty, unitPrice: Number(i.unit_price) })),
    };
  }

  private async done(id: number, doc: string | null) {
    await this.db.query(
      `UPDATE erp_outbox SET status = 'done', erp_doc_no = $2, sent_at = now(), attempts = attempts + 1, last_error = NULL, locked_until = NULL WHERE id = $1`,
      [id, doc],
    );
  }

  private async fail(job: OutboxRow, err: unknown, retryable: boolean) {
    const msg = err instanceof Error ? err.message : String(err);
    const { rows } = await this.db.query(
      `UPDATE erp_outbox
          SET attempts = attempts + 1, last_error = $2, locked_until = NULL,
              status = CASE WHEN NOT $3 OR attempts + 1 >= $4 THEN 'dead' ELSE 'pending' END,
              next_attempt_at = now() + make_interval(secs => LEAST(900, 5 * power(2, attempts)) * (0.5 + random() / 2))
        WHERE id = $1 RETURNING status`,
      [job.id, msg, retryable, config.maxErpAttempts],
    );
    const level = rows[0]?.status === 'dead' ? 'error' : 'warn';
    this.log[level](`ERP ${job.kind} order ${job.order_id} failed (attempt ${job.attempts + 1}, breaker ${this.breaker.state}): ${msg}`);
  }
}

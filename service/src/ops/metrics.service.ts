import { Injectable } from '@nestjs/common';
import { Db } from '../db/db.service';

/** The 5 day-one signals from DESIGN.md §7, computed straight from Postgres. */
@Injectable()
export class MetricsService {
  constructor(private readonly db: Db) {}

  async snapshot() {
    const [orders, push, erp, dead, inbound] = await Promise.all([
      this.db.query(
        `SELECT count(*)::int AS total,
                count(*) FILTER (WHERE status = 'OVERSOLD')::int AS oversold,
                count(*) FILTER (WHERE status = 'RESERVED')::int AS reserved,
                count(*) FILTER (WHERE status = 'CANCELLED')::int AS cancelled
           FROM orders WHERE created_at > now() - interval '24 hours' AND status <> 'NEW'`,
      ),
      this.db.query(
        `SELECT channel,
                count(*) FILTER (WHERE dirty)::int AS backlog,
                COALESCE(EXTRACT(epoch FROM now() - min(dirty_since) FILTER (WHERE dirty)) * 1000, 0)::int AS oldest_dirty_ms,
                COALESCE(percentile_disc(0.95) WITHIN GROUP (ORDER BY last_lag_ms)
                         FILTER (WHERE last_pushed_at > now() - interval '10 minutes'), 0)::int AS p95_lag_ms,
                count(*) FILTER (WHERE status = 'failed')::int AS failed
           FROM channel_stock GROUP BY channel ORDER BY channel`,
      ),
      this.db.query(
        `SELECT count(*) FILTER (WHERE status IN ('pending','processing'))::int AS backlog,
                COALESCE(EXTRACT(epoch FROM now() - min(created_at) FILTER (WHERE status IN ('pending','processing'))), 0)::int AS oldest_age_s,
                count(*) FILTER (WHERE status = 'done')::int AS delivered,
                count(*) FILTER (WHERE status = 'done' AND kind = 'CREATE')::int AS created,
                count(*) FILTER (WHERE status = 'dead')::int AS dead
           FROM erp_outbox`,
      ),
      this.db.query(`SELECT count(*)::int AS n FROM inbound_events WHERE status = 'dead'`),
      this.db.query(`SELECT count(*)::int AS n FROM inbound_events WHERE status = 'pending'`),
    ]);
    const o = orders.rows[0];
    const decided = o.reserved + o.oversold + o.cancelled;
    return {
      oversell_rate_24h: decided ? +(o.oversold / decided).toFixed(5) : 0,
      orders_24h: { total: o.total, reserved: o.reserved, oversold: o.oversold, cancelled: o.cancelled },
      stock_push: push.rows,
      erp_outbox: erp.rows[0],
      dead_letters: { inbound_events: dead.rows[0].n, erp: erp.rows[0].dead, push_failed: push.rows.reduce((a, r) => a + r.failed, 0) },
      inbound_pending: inbound.rows[0].n,
    };
  }

  /** Prometheus text format, for CloudWatch agent / Grafana scraping. */
  async prometheus(): Promise<string> {
    const m = await this.snapshot();
    const lines = [
      `mars_oversell_rate_24h ${m.oversell_rate_24h}`,
      `mars_erp_outbox_backlog ${m.erp_outbox.backlog}`,
      `mars_erp_outbox_oldest_age_seconds ${m.erp_outbox.oldest_age_s}`,
      `mars_dead_letters{kind="inbound"} ${m.dead_letters.inbound_events}`,
      `mars_dead_letters{kind="erp"} ${m.dead_letters.erp}`,
      `mars_inbound_pending ${m.inbound_pending}`,
      ...m.stock_push.flatMap((r: any) => [
        `mars_push_backlog{channel="${r.channel}"} ${r.backlog}`,
        `mars_push_oldest_dirty_ms{channel="${r.channel}"} ${r.oldest_dirty_ms}`,
        `mars_push_p95_lag_ms{channel="${r.channel}"} ${r.p95_lag_ms}`,
        `mars_push_failed{channel="${r.channel}"} ${r.failed}`,
      ]),
    ];
    return lines.join('\n') + '\n';
  }

  /**
   * Invariant checks used by the sale simulation and handy for ops:
   *  1. ledger replays to exactly the current inventory,
   *  2. reserved stock equals the sum of open (RESERVED) orders,
   *  3. no order was sent to the ERP twice (enforced by UNIQUE, checked anyway).
   */
  async consistency() {
    const ledger = await this.db.query(
      `SELECT i.sku, i.on_hand, i.reserved, COALESCE(l.oh, 0)::int AS ledger_on_hand, COALESCE(l.rs, 0)::int AS ledger_reserved
         FROM inventory i
         LEFT JOIN (SELECT sku, SUM(delta_on_hand) oh, SUM(delta_reserved) rs FROM inventory_ledger GROUP BY sku) l ON l.sku = i.sku
        WHERE i.on_hand <> COALESCE(l.oh, 0) OR i.reserved <> COALESCE(l.rs, 0)`,
    );
    const reserved = await this.db.query(
      `SELECT i.sku, i.reserved, COALESCE(o.q, 0)::int AS open_order_qty
         FROM inventory i
         LEFT JOIN (SELECT oi.sku, SUM(oi.qty) q FROM order_items oi JOIN orders o ON o.id = oi.order_id
                     WHERE o.status = 'RESERVED' GROUP BY oi.sku) o ON o.sku = i.sku
        WHERE i.reserved <> COALESCE(o.q, 0)`,
    );
    const dupErp = await this.db.query(`SELECT erp_doc_no, count(*)::int n FROM erp_outbox WHERE kind = 'CREATE' AND erp_doc_no IS NOT NULL GROUP BY 1 HAVING count(*) > 1`);
    return {
      ok: !ledger.rowCount && !reserved.rowCount && !dupErp.rowCount,
      ledger_mismatches: ledger.rows,
      reserved_mismatches: reserved.rows,
      duplicate_erp_docs: dupErp.rows,
    };
  }
}

import { BadRequestException, Body, Controller, Get, Header, Headers, NotFoundException, Param, Post, Query, Res } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { Response } from 'express';
import { CHANNELS } from '../common/types';
import { Db } from '../db/db.service';
import { markDirty, moveStock } from '../inventory/stock';
import { BlinkitCsv } from '../push/blinkit-csv.service';
import { MetricsService } from './metrics.service';
import { OPS_HTML } from './ops.html';

/**
 * Ops API + console. In production this sits behind the company SSO (ALB OIDC auth);
 * auth is intentionally out of scope for v1 of this repo.
 */
@Controller()
export class OpsController {
  constructor(private readonly db: Db, private readonly metrics: MetricsService, private readonly csv: BlinkitCsv) {}

  @Get('health')
  async health() {
    await this.db.query('SELECT 1');
    return { ok: true };
  }

  @Get()
  root(@Res() res: Response) {
    res.redirect('/ops');
  }

  @Get('ops')
  @Header('content-type', 'text/html; charset=utf-8')
  console() {
    return OPS_HTML;
  }

  @Get('metrics')
  @Header('content-type', 'text/plain; version=0.0.4')
  prom() {
    return this.metrics.prometheus();
  }

  @Get('ops/metrics')
  snapshot() {
    return this.metrics.snapshot();
  }

  @Get('ops/consistency')
  consistency() {
    return this.metrics.consistency();
  }

  /** Requirement 6: every failed sync, in one list. */
  @Get('ops/failures')
  async failures() {
    const [events, erp, push] = await Promise.all([
      this.db.query(
        `SELECT id, channel, event_type, external_order_id, attempts, last_error, received_at
           FROM inbound_events WHERE status = 'dead' ORDER BY id DESC LIMIT 200`,
      ),
      this.db.query(
        `SELECT e.id, e.kind, e.order_id, o.channel, o.external_order_id, e.attempts, e.last_error, e.created_at
           FROM erp_outbox e JOIN orders o ON o.id = e.order_id WHERE e.status = 'dead' ORDER BY e.id DESC LIMIT 200`,
      ),
      this.db.query(
        `SELECT sku, channel, attempts, last_error, last_pushed_qty, next_attempt_at
           FROM channel_stock WHERE status = 'failed' ORDER BY next_attempt_at LIMIT 200`,
      ),
    ]);
    return { inbound_events: events.rows, erp: erp.rows, stock_push: push.rows };
  }

  @Post('ops/retry/event/:id')
  async retryEvent(@Param('id') id: string) {
    const r = await this.db.query(
      `UPDATE inbound_events SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL
        WHERE id = $1 AND status = 'dead' AND normalized IS NOT NULL RETURNING id`,
      [id],
    );
    if (!r.rowCount) throw new BadRequestException('not a retryable dead event (unparseable payloads must be fixed at the source)');
    return { retried: Number(id) };
  }

  @Post('ops/retry/erp/:id')
  async retryErp(@Param('id') id: string) {
    const r = await this.db.query(
      `UPDATE erp_outbox SET status = 'pending', next_attempt_at = now(), last_error = NULL WHERE id = $1 AND status = 'dead' RETURNING id`,
      [id],
    );
    if (!r.rowCount) throw new NotFoundException('no dead ERP job with that id');
    return { retried: Number(id) };
  }

  @Post('ops/retry/push/:sku/:channel')
  async retryPush(@Param('sku') sku: string, @Param('channel') channel: string) {
    const r = await this.db.query(
      `UPDATE channel_stock SET dirty = true, dirty_since = COALESCE(dirty_since, now()), attempts = 0, status = 'ok', next_attempt_at = now()
        WHERE sku = $1 AND channel = $2 RETURNING sku`,
      [sku, channel.toUpperCase()],
    );
    if (!r.rowCount) throw new NotFoundException();
    return { retried: `${sku}/${channel}` };
  }

  /** Requirement: "for any SKU, why did stock change, and when?" */
  @Get('ops/sku/:sku')
  async sku(@Param('sku') sku: string, @Query('limit') limit = '200') {
    const inv = await this.db.query(
      `SELECT i.sku, s.name, i.on_hand, i.reserved, i.on_hand - i.reserved AS available, s.safety_buffer, i.updated_at
         FROM inventory i JOIN skus s ON s.sku = i.sku WHERE i.sku = $1`,
      [sku],
    );
    if (!inv.rowCount) throw new NotFoundException('unknown SKU');
    const [channels, ledger] = await Promise.all([
      this.db.query(`SELECT channel, last_pushed_qty, last_pushed_at, dirty, status, last_error FROM channel_stock WHERE sku = $1 ORDER BY channel`, [sku]),
      this.db.query(
        `SELECT l.created_at, l.reason, l.delta_on_hand, l.delta_reserved, l.on_hand_after, l.reserved_after,
                l.actor, l.note, o.channel, o.external_order_id
           FROM inventory_ledger l LEFT JOIN orders o ON o.id = l.order_id
          WHERE l.sku = $1 ORDER BY l.id DESC LIMIT $2`,
        [sku, Math.min(1000, Number(limit) || 200)],
      ),
    ]);
    return { inventory: inv.rows[0], channels: channels.rows, ledger: ledger.rows };
  }

  @Get('ops/orders')
  async orders(@Query('status') status = 'OVERSOLD') {
    const { rows } = await this.db.query(
      `SELECT o.id, o.channel, o.external_order_id, o.status, o.created_at,
              (SELECT json_agg(json_build_object('sku', sku, 'qty', qty)) FROM order_items WHERE order_id = o.id) AS items
         FROM orders o WHERE o.status = $1 ORDER BY o.id DESC LIMIT 200`,
      [status.toUpperCase()],
    );
    return rows;
  }

  /** Stock count correction / goods received. Idempotent per Idempotency-Key header. */
  @Post('ops/inventory/adjust')
  async adjust(@Body() b: { sku: string; delta: number; note?: string; actor?: string }, @Headers('idempotency-key') key?: string) {
    if (!b?.sku || !Number.isInteger(b.delta) || b.delta === 0) throw new BadRequestException('sku and non-zero integer delta required');
    return this.db.tx(async (c) => {
      const dup = await c.query(`SELECT 1 FROM inventory_ledger WHERE idempotency_key = $1`, [`ADJUST:${key}`]);
      if (key && dup.rowCount) return { ok: true, duplicate: true };
      const ok = await moveStock(c, {
        sku: b.sku,
        deltaOnHand: b.delta,
        deltaReserved: 0,
        reason: 'ADJUST',
        idempotencyKey: `ADJUST:${key ?? randomUUID()}`,
        actor: b.actor ?? 'ops',
        note: b.note,
      });
      if (!ok) throw new BadRequestException('adjustment would make available stock negative (or unknown SKU)');
      await markDirty(c, [b.sku]);
      return { ok: true };
    });
  }

  /** Add a SKU (e.g. after an order failed with "unknown SKU"), then retry the event. */
  @Post('ops/skus')
  async addSku(@Body() b: { sku: string; name?: string; on_hand?: number }) {
    if (!b?.sku) throw new BadRequestException('sku required');
    return this.db.tx(async (c) => {
      await c.query(`INSERT INTO skus (sku, name) VALUES ($1, $2)`, [b.sku, b.name ?? b.sku]);
      await c.query(`INSERT INTO inventory (sku, on_hand) VALUES ($1, 0)`, [b.sku]);
      await c.query(`INSERT INTO channel_stock (sku, channel) SELECT $1, unnest($2::text[])`, [b.sku, CHANNELS]);
      if (b.on_hand) {
        await moveStock(c, { sku: b.sku, deltaOnHand: b.on_hand, deltaReserved: 0, reason: 'INITIAL', idempotencyKey: `INITIAL:${b.sku}`, actor: 'ops' });
      }
      return { ok: true };
    });
  }

  @Get('ops/blinkit/daily.csv')
  @Header('content-type', 'text/csv')
  async blinkitCsv() {
    return (await this.csv.build()).csv;
  }

  @Post('ops/blinkit/upload-csv')
  async uploadCsv() {
    return { uploaded: await this.csv.upload() };
  }
}

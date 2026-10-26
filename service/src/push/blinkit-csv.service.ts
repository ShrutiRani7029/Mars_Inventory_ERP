import { Injectable, Logger } from '@nestjs/common';
import { httpJson } from '../channels/channel-clients';
import { config } from '../config';
import { Db } from '../db/db.service';

/**
 * Blinkit's daily CSV upload is the self-healing baseline: whatever the flaky REST API
 * missed during the day is corrected here from the source of truth.
 */
@Injectable()
export class BlinkitCsv {
  private readonly log = new Logger(BlinkitCsv.name);
  private lastRunDay = '';

  constructor(private readonly db: Db) {}

  async build(): Promise<{ csv: string; rows: { sku: string; qty: number }[] }> {
    const { rows } = await this.db.query(
      `SELECT i.sku, GREATEST(0, i.on_hand - i.reserved - s.safety_buffer)::int AS qty
         FROM inventory i JOIN skus s ON s.sku = i.sku WHERE s.active ORDER BY i.sku`,
    );
    const csv = ['item_code,quantity', ...rows.map((r) => `${r.sku},${r.qty}`)].join('\n');
    return { csv, rows };
  }

  async upload(): Promise<number> {
    const { csv, rows } = await this.build();
    await httpJson('POST', `${config.mockBaseUrl}/blinkit/inventory/csv`, csv, 60_000, 'text/csv');
    await this.db.query(
      `UPDATE channel_stock cs SET last_pushed_qty = v.qty, last_pushed_at = now(), status = 'ok', attempts = 0, last_error = NULL
         FROM unnest($1::text[], $2::int[]) AS v(sku, qty)
        WHERE cs.channel = 'BLINKIT' AND cs.sku = v.sku`,
      [rows.map((r) => r.sku), rows.map((r) => r.qty)],
    );
    this.log.log(`blinkit daily CSV uploaded (${rows.length} SKUs)`);
    return rows.length;
  }

  /** Called every minute by the worker; uploads once a day at the configured IST hour. */
  async maybeRunDaily(now = new Date()): Promise<void> {
    const ist = new Date(now.getTime() + 5.5 * 3600_000);
    const day = ist.toISOString().slice(0, 10);
    if (ist.getUTCHours() !== config.blinkitCsvHourIst || this.lastRunDay === day) return;
    this.lastRunDay = day;
    await this.upload();
  }
}

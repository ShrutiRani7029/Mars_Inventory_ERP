import { Tx } from '../db/db.service';

export type LedgerReason = 'INITIAL' | 'RESERVE' | 'RELEASE' | 'SHIP' | 'RETURN' | 'ADJUST';

export interface StockMove {
  sku: string;
  deltaOnHand: number;
  deltaReserved: number;
  reason: LedgerReason;
  idempotencyKey: string;
  actor: string;
  orderId?: number | null;
  note?: string;
}

/**
 * The ONLY way stock changes. One conditional UPDATE (row-locked by Postgres) plus one
 * ledger row, inside the caller's transaction.
 *
 * Returns false when the move would make on_hand / reserved / available negative.
 * The WHERE clause is what makes "two channels sell the last unit" safe: the second
 * transaction waits on the row lock, re-evaluates the condition and updates 0 rows.
 */
export async function moveStock(c: Tx, m: StockMove): Promise<boolean> {
  const r = await c.query(
    `UPDATE inventory
        SET on_hand = on_hand + $2, reserved = reserved + $3, version = version + 1, updated_at = now()
      WHERE sku = $1
        AND on_hand + $2 >= 0
        AND reserved + $3 >= 0
        AND (on_hand + $2) - (reserved + $3) >= 0
      RETURNING on_hand, reserved`,
    [m.sku, m.deltaOnHand, m.deltaReserved],
  );
  if (r.rowCount === 0) return false;
  // UNIQUE(idempotency_key): a replay that slipped past every other guard fails here
  // and rolls back the whole transaction instead of moving stock twice.
  await c.query(
    `INSERT INTO inventory_ledger
       (sku, delta_on_hand, delta_reserved, on_hand_after, reserved_after, reason, order_id, idempotency_key, actor, note)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [m.sku, m.deltaOnHand, m.deltaReserved, r.rows[0].on_hand, r.rows[0].reserved, m.reason, m.orderId ?? null, m.idempotencyKey, m.actor, m.note ?? null],
  );
  return true;
}

/**
 * Flag SKUs so every channel's pusher picks them up. Rows are locked in a fixed order
 * so two transactions touching overlapping SKUs cannot deadlock.
 */
export async function markDirty(c: Tx, skus: string[]): Promise<void> {
  if (!skus.length) return;
  await c.query(
    `UPDATE channel_stock cs
        SET dirty = true, dirty_since = COALESCE(cs.dirty_since, now())
       FROM (SELECT sku, channel FROM channel_stock
              WHERE sku = ANY($1) ORDER BY sku, channel FOR UPDATE) l
      WHERE cs.sku = l.sku AND cs.channel = l.channel`,
    [skus],
  );
}

export type Channel = 'WEBSITE' | 'NYKAA' | 'FLIPKART' | 'BLINKIT';
export const CHANNELS: Channel[] = ['WEBSITE', 'NYKAA', 'FLIPKART', 'BLINKIT'];

export type OrderEventType = 'ORDER_CREATED' | 'ORDER_CANCELLED' | 'ORDER_SHIPPED' | 'ORDER_RETURNED';

export type OrderStatus = 'NEW' | 'RESERVED' | 'OVERSOLD' | 'CANCELLED' | 'SHIPPED' | 'RETURNED';

export interface OrderItem {
  sku: string;
  qty: number;
  unit_price: number;
}

/** The common order format every channel payload is converted into. */
export interface OrderEvent {
  channel: Channel;
  external_order_id: string;
  event: OrderEventType;
  event_time: string;
  items: OrderItem[];
  payment_mode?: string;
  shipping_pincode?: string;
  total_amount?: number;
  /** Stable key for this delivery: channel event id when present, else a content hash. */
  dedupe_key: string;
}

/** An error that will never succeed on retry (bad payload, unknown SKU). Goes straight to dead. */
export class NonRetryableError extends Error {
  readonly nonRetryable = true;
}

export const isNonRetryable = (e: unknown): boolean =>
  !!e && typeof e === 'object' && (e as { nonRetryable?: boolean }).nonRetryable === true;

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Exponential backoff with full jitter, capped. */
export function backoffMs(attempt: number, baseMs = 2000, capMs = 15 * 60_000): number {
  const exp = Math.min(capMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(exp / 2 + Math.random() * (exp / 2));
}

import { createHash } from 'crypto';
import { Channel, NonRetryableError, OrderEvent, OrderEventType, OrderItem } from '../common/types';

/*
 * Each channel sends a different payload. These shapes are ASSUMPTIONS (the brief gives
 * no contracts); they are listed in DESIGN.md. Every converter returns the common format.
 */

const hash = (s: string) => createHash('sha256').update(s).digest('hex').slice(0, 32);

function req<T>(v: T | undefined | null, field: string): T {
  if (v === undefined || v === null || v === '') throw new NonRetryableError(`missing field: ${field}`);
  return v;
}

function toIso(v: unknown, field: string): string {
  const d = new Date(req(v as string, field));
  if (Number.isNaN(d.getTime())) throw new NonRetryableError(`bad timestamp in ${field}`);
  return d.toISOString();
}

/** Merge repeated SKU lines and validate quantities. */
function items(raw: unknown, map: (x: any) => OrderItem, required: boolean): OrderItem[] {
  if (!Array.isArray(raw)) {
    if (required) throw new NonRetryableError('missing items');
    return [];
  }
  const merged = new Map<string, OrderItem>();
  for (const x of raw) {
    const it = map(x);
    if (!it.sku || !Number.isInteger(it.qty) || it.qty <= 0) throw new NonRetryableError(`bad item: ${JSON.stringify(x)}`);
    const prev = merged.get(it.sku);
    merged.set(it.sku, prev ? { ...prev, qty: prev.qty + it.qty } : it);
  }
  if (required && merged.size === 0) throw new NonRetryableError('order has no items');
  return [...merged.values()];
}

function finish(e: Omit<OrderEvent, 'dedupe_key'>, channelEventId?: string): OrderEvent {
  const dedupe_key = channelEventId ? `evt:${channelEventId}` : `h:${hash(`${e.external_order_id}|${e.event}|${e.event_time}`)}`;
  return { ...e, dedupe_key };
}

const WEBSITE_EVENTS: Record<string, OrderEventType> = {
  'order.created': 'ORDER_CREATED',
  'order.cancelled': 'ORDER_CANCELLED',
  'order.returned': 'ORDER_RETURNED',
};

/** Own website (Shopify-like): { id, topic, created_at, order: { name, line_items[...] } } */
export function normalizeWebsite(p: any): OrderEvent {
  const event = WEBSITE_EVENTS[req(p?.topic, 'topic')];
  if (!event) throw new NonRetryableError(`unknown website topic ${p.topic}`);
  const o = req(p.order, 'order');
  return finish(
    {
      channel: 'WEBSITE',
      external_order_id: String(req(o.name, 'order.name')),
      event,
      event_time: toIso(p.created_at, 'created_at'),
      items: items(o.line_items, (x) => ({ sku: x.sku, qty: Number(x.quantity), unit_price: Number(x.price ?? 0) }), event === 'ORDER_CREATED'),
      payment_mode: o.payment_mode,
      shipping_pincode: o.shipping_pincode,
      total_amount: o.total != null ? Number(o.total) : undefined,
    },
    p.id != null ? String(p.id) : undefined,
  );
}

const NYKAA_EVENTS: Record<string, OrderEventType> = {
  ORDER_CREATED: 'ORDER_CREATED',
  ORDER_CANCELLED: 'ORDER_CANCELLED',
  ORDER_RETURNED: 'ORDER_RETURNED',
};

/** Nykaa: { event_id?, event_type, event_ts, order: { order_no, items:[{sku_code, qty, price}] } }. May repeat / reorder. */
export function normalizeNykaa(p: any): OrderEvent {
  const event = NYKAA_EVENTS[req(p?.event_type, 'event_type')];
  if (!event) throw new NonRetryableError(`unknown nykaa event ${p.event_type}`);
  const o = req(p.order, 'order');
  return finish(
    {
      channel: 'NYKAA',
      external_order_id: String(req(o.order_no, 'order.order_no')),
      event,
      event_time: toIso(p.event_ts, 'event_ts'),
      items: items(o.items, (x) => ({ sku: x.sku_code, qty: Number(x.qty), unit_price: Number(x.price ?? 0) }), event === 'ORDER_CREATED'),
      payment_mode: o.payment_mode,
      shipping_pincode: o.pincode,
      total_amount: o.amount != null ? Number(o.amount) : undefined,
    },
    p.event_id ? String(p.event_id) : undefined,
  );
}

const BLINKIT_EVENTS: Record<string, OrderEventType> = {
  ORDER_PLACED: 'ORDER_CREATED',
  ORDER_CANCELLED: 'ORDER_CANCELLED',
  ORDER_RETURNED: 'ORDER_RETURNED',
};

/** Blinkit: { type, order_id, timestamp, items:[{item_code, quantity, selling_price}] } */
export function normalizeBlinkit(p: any): OrderEvent {
  const event = BLINKIT_EVENTS[req(p?.type, 'type')];
  if (!event) throw new NonRetryableError(`unknown blinkit type ${p.type}`);
  return finish({
    channel: 'BLINKIT',
    external_order_id: String(req(p.order_id, 'order_id')),
    event,
    event_time: toIso(p.timestamp, 'timestamp'),
    items: items(p.items, (x) => ({ sku: x.item_code, qty: Number(x.quantity), unit_price: Number(x.selling_price ?? 0) }), event === 'ORDER_CREATED'),
    payment_mode: p.payment_mode,
    shipping_pincode: p.pincode,
    total_amount: p.total != null ? Number(p.total) : undefined,
  });
}

const FLIPKART_STATUS: Record<string, OrderEventType> = {
  APPROVED: 'ORDER_CREATED',
  CANCELLED: 'ORDER_CANCELLED',
  RETURNED: 'ORDER_RETURNED',
};

/** Flipkart (polled): { orderId, status, updatedAt, orderItems:[{sku, quantity, sellingPrice}] } */
export function normalizeFlipkart(p: any): OrderEvent {
  const event = FLIPKART_STATUS[req(p?.status, 'status')];
  if (!event) throw new NonRetryableError(`unknown flipkart status ${p.status}`);
  const orderId = String(req(p.orderId, 'orderId'));
  return finish(
    {
      channel: 'FLIPKART',
      external_order_id: orderId,
      event,
      event_time: toIso(p.updatedAt, 'updatedAt'),
      items: items(p.orderItems, (x) => ({ sku: x.sku, qty: Number(x.quantity), unit_price: Number(x.sellingPrice ?? 0) }), event === 'ORDER_CREATED'),
      payment_mode: p.paymentType,
      shipping_pincode: p.pincode,
      total_amount: p.totalPrice != null ? Number(p.totalPrice) : undefined,
    },
    `${orderId}:${p.status}`, // a status change happens once per order on Flipkart
  );
}

export const NORMALIZERS: Record<Channel, (p: any) => OrderEvent> = {
  WEBSITE: normalizeWebsite,
  NYKAA: normalizeNykaa,
  BLINKIT: normalizeBlinkit,
  FLIPKART: normalizeFlipkart,
};

export const rawHash = (raw: unknown) => `raw:${hash(JSON.stringify(raw))}`;

import { normalizeBlinkit, normalizeFlipkart, normalizeNykaa, normalizeWebsite } from '../src/channels/normalizers';

describe('channel payload -> common order format', () => {
  it('Nykaa: maps fields, merges repeated SKU lines, stable content-hash dedupe key', () => {
    const p = {
      event_type: 'ORDER_CREATED',
      event_ts: '2026-10-23T11:04:52+05:30',
      order: { order_no: 'NYK-20261023-884213', items: [{ sku_code: 'MRS-LIP-104', qty: 1, price: 349 }, { sku_code: 'MRS-LIP-104', qty: 1, price: 349 }, { sku_code: 'MRS-KJL-011', qty: 1, price: 199 }], payment_mode: 'PREPAID', pincode: '110034', amount: 897 },
    };
    const e = normalizeNykaa(p);
    expect(e).toMatchObject({
      channel: 'NYKAA',
      external_order_id: 'NYK-20261023-884213',
      event: 'ORDER_CREATED',
      event_time: '2026-10-23T05:34:52.000Z',
      items: [{ sku: 'MRS-LIP-104', qty: 2, unit_price: 349 }, { sku: 'MRS-KJL-011', qty: 1, unit_price: 199 }],
      shipping_pincode: '110034',
      total_amount: 897,
    });
    expect(normalizeNykaa(p).dedupe_key).toBe(e.dedupe_key);
    expect(normalizeNykaa({ ...p, event_id: 'abc' }).dedupe_key).toBe('evt:abc');
  });

  it('Website, Blinkit and Flipkart shapes', () => {
    expect(normalizeWebsite({ id: 9, topic: 'order.cancelled', created_at: '2026-10-23T10:00:00Z', order: { name: 'W-1' } })).toMatchObject({ event: 'ORDER_CANCELLED', items: [], dedupe_key: 'evt:9' });
    expect(normalizeBlinkit({ type: 'ORDER_PLACED', order_id: 'B-1', timestamp: '2026-10-23T10:00:00Z', items: [{ item_code: 'X', quantity: 2 }] })).toMatchObject({ channel: 'BLINKIT', event: 'ORDER_CREATED' });
    expect(normalizeFlipkart({ orderId: 'F-1', status: 'APPROVED', updatedAt: '2026-10-23T10:00:00Z', orderItems: [{ sku: 'X', quantity: 1 }] }).dedupe_key).toBe('evt:F-1:APPROVED');
  });

  it('rejects bad payloads as non-retryable', () => {
    expect(() => normalizeBlinkit({ type: 'ORDER_PLACED', order_id: 'B', timestamp: 'x', items: [] })).toThrow(/timestamp/);
    expect(() => normalizeNykaa({ event_type: 'ORDER_CREATED', event_ts: '2026-10-23T10:00:00Z', order: { order_no: 'N', items: [{ sku_code: 'X', qty: 0 }] } })).toThrow(/bad item/);
  });
});

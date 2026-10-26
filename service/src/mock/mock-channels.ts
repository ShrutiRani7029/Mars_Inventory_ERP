/*
 * Mock Website / Nykaa / Flipkart / Blinkit / Business Central, built against the API
 * contract table in the brief (no real endpoints are ever called):
 *   Nykaa    - 5 rps on stock updates (429 above), webhooks sometimes duplicated / out of order
 *   Flipkart - polling only, bulk stock API max 50 SKUs, 10 rps (429 above)
 *   Blinkit  - ~2% of REST calls hang (client times out), daily CSV upload
 *   ERP      - can be put into "Sunday maintenance" (503); ~1% of creates commit but answer slowly
 * It also contains a sale-day order generator that buys only what each channel DISPLAYS,
 * which is exactly how real overselling happens.
 */
import express from 'express';
import { Channel, CHANNELS, sleep } from '../common/types';
import { config } from '../config';
import { sign } from '../ingest/ingest.service';

const app = express();
app.use(express.json({ limit: '5mb' }));
app.use(express.text({ type: 'text/csv', limit: '5mb' }));

// ---------- state ----------
const displayed: Record<Channel, Map<string, number>> = { WEBSITE: new Map(), NYKAA: new Map(), FLIPKART: new Map(), BLINKIT: new Map() };
const calls: Record<string, { ok: number; rejected429: number; timeouts: number; bad: number }> = {};
const hit = (k: string, f: 'ok' | 'rejected429' | 'timeouts' | 'bad') => ((calls[k] ??= { ok: 0, rejected429: 0, timeouts: 0, bad: 0 })[f] += 1);

const windows: Record<string, number[]> = {};
function overLimit(key: string, rps: number): boolean {
  const now = Date.now();
  const w = (windows[key] = (windows[key] ?? []).filter((t) => now - t < 1000));
  if (w.length >= rps) return true;
  w.push(now);
  return false;
}

interface FkOrder { orderId: string; status: string; updatedAt: string; orderItems: { sku: string; quantity: number; sellingPrice: number }[]; paymentType: string; pincode: string; totalPrice: number }
const flipkartFeed = new Map<string, FkOrder>();

interface ErpDoc { number: string; externalDocumentNumber: string; cancelled: boolean; body: unknown }
const erpDocs = new Map<string, ErpDoc>();
const erp = { down: false, creates: 0, conflicts409: 0, rejectedWhileDown: 0, cancels: 0, slowCommits: 0 };
let docSeq = 100000;

// ---------- stock endpoints ----------
app.put('/website/stock', (req, res) => {
  for (const l of req.body.items ?? []) displayed.WEBSITE.set(l.sku, l.qty);
  hit('WEBSITE', 'ok');
  res.json({ ok: true });
});

app.put('/nykaa/inventory/:sku', (req, res) => {
  if (overLimit('nykaa', 5)) return hit('NYKAA', 'rejected429'), res.status(429).json({ error: 'rate limit 5 rps' });
  displayed.NYKAA.set(req.params.sku, Number(req.body.qty));
  hit('NYKAA', 'ok');
  res.json({ ok: true });
});

app.post('/flipkart/inventory/bulk', (req, res) => {
  if (overLimit('flipkart', 10)) return hit('FLIPKART', 'rejected429'), res.status(429).json({ error: 'rate limit 10 rps' });
  const items = req.body.items ?? [];
  if (items.length > 50) return hit('FLIPKART', 'bad'), res.status(400).json({ error: 'max 50 SKUs per call' });
  for (const l of items) displayed.FLIPKART.set(l.sku, l.qty);
  hit('FLIPKART', 'ok');
  res.json({ ok: true, updated: items.length });
});

app.put('/blinkit/inventory/:sku', async (req, res) => {
  if (Math.random() < 0.02) {
    hit('BLINKIT', 'timeouts');
    await sleep(8000); // longer than our 5 s client timeout
  } else hit('BLINKIT', 'ok');
  displayed.BLINKIT.set(req.params.sku, Number(req.body.qty));
  if (!res.headersSent) res.json({ ok: true });
});

app.post('/blinkit/inventory/csv', (req, res) => {
  const lines = String(req.body).trim().split('\n').slice(1);
  for (const line of lines) {
    const [sku, qty] = line.split(',');
    displayed.BLINKIT.set(sku, Number(qty));
  }
  hit('BLINKIT_CSV', 'ok');
  res.json({ ok: true, rows: lines.length });
});

// ---------- Flipkart order polling ----------
app.get('/flipkart/orders', (req, res) => {
  const after = new Date(String(req.query.updatedAfter ?? 0)).getTime();
  const orders = [...flipkartFeed.values()].filter((o) => new Date(o.updatedAt).getTime() > after);
  res.json({ orders });
});

// ---------- Business Central ----------
app.use('/erp', (_req, res, next) => {
  if (erp.down) return erp.rejectedWhileDown++, res.status(503).json({ error: 'scheduled maintenance' });
  next();
});
app.get('/erp/salesOrders', (req, res) => {
  const d = erpDocs.get(String(req.query.externalDocumentNumber));
  res.json({ value: d ? [{ number: d.number }] : [] });
});
app.post('/erp/salesOrders', async (req, res) => {
  const ext = req.body.externalDocumentNumber;
  if (erpDocs.has(ext)) return erp.conflicts409++, res.status(409).json({ error: 'duplicate externalDocumentNumber' });
  const doc = { number: `SO-${++docSeq}`, externalDocumentNumber: ext, cancelled: false, body: req.body };
  erpDocs.set(ext, doc);
  erp.creates++;
  if (Math.random() < 0.01) {
    erp.slowCommits++;
    await sleep(config.erpTimeoutMs + 2000); // committed, but the caller times out
  }
  if (!res.headersSent) res.status(201).json({ number: doc.number });
});
app.post('/erp/salesOrders/:no/cancel', (req, res) => {
  const d = [...erpDocs.values()].find((x) => x.number === req.params.no);
  if (!d) return res.status(404).json({ error: 'not found' });
  d.cancelled = true;
  erp.cancels++;
  res.json({ ok: true });
});

// ---------- control ----------
app.post('/_control/erp', (req, res) => {
  erp.down = !!req.body.down;
  res.json(erp);
});
app.post('/_control/load', (req, res) => {
  const qty: Record<string, number> = req.body.qtyBySku ?? {};
  // Initial catalogue upload: only fills SKUs a channel has not received a real push for.
  for (const ch of CHANNELS) for (const [sku, q] of Object.entries(qty)) if (!displayed[ch].has(sku)) displayed[ch].set(sku, q);
  res.json({ loaded: Object.keys(qty).length });
});
app.get('/_control/displayed', (_req, res) => {
  res.json(Object.fromEntries(CHANNELS.map((c) => [c, Object.fromEntries(displayed[c])])));
});
app.get('/_control/stats', (_req, res) => {
  res.json({ sim, calls, erp: { ...erp, docs: erpDocs.size, cancelledDocs: [...erpDocs.values()].filter((d) => d.cancelled).length }, flipkartFeed: flipkartFeed.size });
});

// ---------- sale-day order generator ----------
interface SimOpts { orders: number; perMinute: number; hotSkus: string[]; hotShare: number; cancelRate: number; nykaaDupRate: number; outOfOrderRate: number; erpDownSeconds: number }
const sim = { running: false, generated: 0, byChannel: {} as Record<string, number>, skippedNoStock: 0, duplicatesSent: 0, cancelsSent: 0, outOfOrder: 0, webhookErrors: 0 };
const WEIGHTS: [Channel, number][] = [['WEBSITE', 0.25], ['NYKAA', 0.35], ['FLIPKART', 0.25], ['BLINKIT', 0.15]];
const pick = <T>(a: T[]) => a[Math.floor(Math.random() * a.length)];

async function sendWebhook(path: string, secret: string, payload: unknown) {
  const body = JSON.stringify(payload);
  try {
    const r = await fetch(`${config.apiBaseUrl}/webhooks/${path}`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-signature': sign(secret, body) }, body });
    if (!r.ok) sim.webhookErrors++;
  } catch {
    sim.webhookErrors++;
  }
}

function payloadFor(ch: Channel, id: string, kind: 'create' | 'cancel', items: { sku: string; qty: number }[], when: Date) {
  const ts = when.toISOString();
  const total = items.reduce((a, i) => a + i.qty * 349, 0);
  switch (ch) {
    case 'WEBSITE':
      return { id: `${id}-${kind}`, topic: kind === 'create' ? 'order.created' : 'order.cancelled', created_at: ts,
        order: { name: id, line_items: items.map((i) => ({ sku: i.sku, quantity: i.qty, price: 349 })), payment_mode: 'PREPAID', shipping_pincode: '110034', total } };
    case 'NYKAA': // no event_id: dedupe falls back to content hash, like a real retry
      return { event_type: kind === 'create' ? 'ORDER_CREATED' : 'ORDER_CANCELLED', event_ts: ts,
        order: { order_no: id, items: items.map((i) => ({ sku_code: i.sku, qty: i.qty, price: 349 })), payment_mode: 'COD', pincode: '400001', amount: total } };
    case 'BLINKIT':
      return { type: kind === 'create' ? 'ORDER_PLACED' : 'ORDER_CANCELLED', order_id: id, timestamp: ts,
        items: items.map((i) => ({ item_code: i.sku, quantity: i.qty, selling_price: 349 })), pincode: '560001', total };
    default:
      return null;
  }
}

async function deliver(ch: Channel, id: string, kind: 'create' | 'cancel', items: { sku: string; qty: number }[], when: Date) {
  if (ch === 'FLIPKART') {
    flipkartFeed.set(id, { orderId: id, status: kind === 'create' ? 'APPROVED' : 'CANCELLED', updatedAt: when.toISOString(),
      orderItems: items.map((i) => ({ sku: i.sku, quantity: i.qty, sellingPrice: 349 })), paymentType: 'PREPAID', pincode: '122001', totalPrice: 349 });
    return;
  }
  const path = ch.toLowerCase();
  await sendWebhook(path, config.webhookSecrets[ch], payloadFor(ch, id, kind, items, when));
}

async function runSim(o: SimOpts) {
  Object.assign(sim, { running: true, generated: 0, byChannel: {}, skippedNoStock: 0, duplicatesSent: 0, cancelsSent: 0, outOfOrder: 0, webhookErrors: 0 });
  if (o.erpDownSeconds > 0) {
    erp.down = true;
    setTimeout(() => (erp.down = false), o.erpDownSeconds * 1000);
  }
  const gap = 60_000 / o.perMinute;
  const pending: Promise<unknown>[] = [];
  const started = Date.now();
  for (let n = 0; n < o.orders; n++) {
    const wait = started + n * gap - Date.now();
    if (wait > 0) await sleep(wait);

    const r = Math.random();
    let acc = 0;
    const ch = WEIGHTS.find(([, w]) => (acc += w) >= r)?.[0] ?? 'WEBSITE';
    const shown = displayed[ch];
    const lines: { sku: string; qty: number }[] = [];
    const want = 1 + Math.floor(Math.random() * 3);
    for (let k = 0; k < want; k++) {
      const pool = Math.random() < o.hotShare && o.hotSkus.length ? o.hotSkus : [...shown.keys()];
      const sku = pick(pool);
      const qty = Math.random() < 0.8 ? 1 : 2;
      if (!sku || lines.some((l) => l.sku === sku) || (shown.get(sku) ?? 0) < qty) continue; // shopper sees "out of stock"
      shown.set(sku, (shown.get(sku) ?? 0) - qty); // the marketplace decrements its own count
      lines.push({ sku, qty });
    }
    if (!lines.length) {
      sim.skippedNoStock++;
      continue;
    }
    const id = `${ch.slice(0, 3)}-${Date.now()}-${n}`;
    sim.generated++;
    sim.byChannel[ch] = (sim.byChannel[ch] ?? 0) + 1;
    const now = new Date();

    const willCancel = Math.random() < o.cancelRate;
    const outOfOrder = ch === 'NYKAA' && willCancel && Math.random() < o.outOfOrderRate;
    pending.push(
      (async () => {
        if (outOfOrder) {
          sim.outOfOrder++;
          sim.cancelsSent++;
          await deliver(ch, id, 'cancel', lines, new Date(now.getTime() + 5000));
          await sleep(500);
          await deliver(ch, id, 'create', lines, now);
          return;
        }
        await deliver(ch, id, 'create', lines, now);
        if (ch === 'NYKAA' && Math.random() < o.nykaaDupRate) {
          sim.duplicatesSent++;
          await sleep(Math.random() * 3000);
          await deliver(ch, id, 'create', lines, now); // same payload again
        }
        if (willCancel) {
          await sleep(2000 + Math.random() * 15000);
          sim.cancelsSent++;
          await deliver(ch, id, 'cancel', lines, new Date());
        }
      })(),
    );
  }
  await Promise.all(pending);
  sim.running = false;
}

app.post('/_control/simulate', (req, res) => {
  if (sim.running) return res.status(409).json({ error: 'already running' });
  const b = req.body ?? {};
  const opts: SimOpts = {
    orders: b.orders ?? 1000,
    perMinute: b.perMinute ?? 300,
    hotSkus: b.hotSkus ?? [],
    hotShare: b.hotShare ?? 0.4,
    cancelRate: b.cancelRate ?? 0.06,
    nykaaDupRate: b.nykaaDupRate ?? 0.15,
    outOfOrderRate: b.outOfOrderRate ?? 0.3,
    erpDownSeconds: b.erpDownSeconds ?? 0,
  };
  runSim(opts).catch((e) => console.error('sim failed', e));
  res.json({ started: opts });
});

app.listen(config.mockPort, () => console.log(`mock channels + ERP on :${config.mockPort}`));

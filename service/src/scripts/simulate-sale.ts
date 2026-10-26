/*
 * Sale-day simulation, end to end through the real api + worker + mock channels.
 *   npm run simulate -- --orders 1500 --per-minute 300 --erp-down 60
 * Prints oversell rate, ERP delivery (exactly once), consistency checks and how far each
 * channel's displayed stock is from the truth after things settle.
 */
import { config } from '../config';
import { sleep } from '../common/types';

const arg = (name: string, d: number) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? Number(process.argv[i + 1]) : d;
};

const api = (p: string) => fetch(`${config.apiBaseUrl}${p}`).then((r) => r.json() as Promise<any>);
const mock = (p: string, body?: unknown) =>
  fetch(`${config.mockBaseUrl}${p}`, body === undefined ? undefined : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(
    (r) => r.json() as Promise<any>,
  );

async function desiredBySku(): Promise<Record<string, number>> {
  const csv = await fetch(`${config.apiBaseUrl}/ops/blinkit/daily.csv`).then((r) => r.text());
  return Object.fromEntries(csv.trim().split('\n').slice(1).map((l) => [l.split(',')[0], Number(l.split(',')[1])]));
}

async function main() {
  const orders = arg('orders', 1500);
  const perMinute = arg('per-minute', 300);
  const erpDown = arg('erp-down', 60);

  const desired = await desiredBySku();
  await mock('/_control/load', { qtyBySku: desired });
  const hotSkus = Object.entries(desired).filter(([, q]) => q <= 15).map(([s]) => s);
  console.log(`catalogue ${Object.keys(desired).length} SKUs, ${hotSkus.length} hot/low-stock. ERP down for first ${erpDown}s.`);

  await mock('/_control/simulate', { orders, perMinute, hotSkus, erpDownSeconds: erpDown });
  const t0 = Date.now();
  for (;;) {
    await sleep(5000);
    const s = await mock('/_control/stats');
    const m = await api('/ops/metrics');
    const lag = m.stock_push.map((r: any) => `${r.channel[0]}:${r.backlog}/${Math.round(r.oldest_dirty_ms / 1000)}s`).join(' ');
    console.log(`t+${Math.round((Date.now() - t0) / 1000)}s generated=${s.sim.generated} pending=${m.inbound_pending} erp_backlog=${m.erp_outbox.backlog} erp_down=${s.erp.down} push[backlog/oldest]=${lag}`);
    const settled = !s.sim.running && m.inbound_pending === 0 && m.erp_outbox.backlog === 0 && m.stock_push.every((r: any) => r.backlog === 0);
    if (settled || Date.now() - t0 > 20 * 60_000) break;
  }
  // Let the Flipkart poller and last pushes catch up.
  await sleep(Math.min(config.flipkartPollMs, 30_000) + 5000);

  const [m, s, cons, finalDesired, shown, oversold] = await Promise.all([
    api('/ops/metrics'),
    mock('/_control/stats'),
    api('/ops/consistency'),
    desiredBySku(),
    mock('/_control/displayed'),
    api('/ops/orders?status=OVERSOLD'),
  ]);
  // "over" = channel shows MORE than we can sell (oversell risk); "lowOver" = same, on low-stock SKUs (the dangerous ones)
  const drift: Record<string, { over: number; lowOver: number }> = {};
  for (const ch of Object.keys(shown)) {
    const over = Object.entries(finalDesired).filter(([sku, q]) => (shown[ch][sku] ?? 0) > q);
    drift[ch] = { over: over.length, lowOver: over.filter(([, q]) => q <= config.lowStockThreshold).length };
  }
  const oversoldBy: Record<string, number> = {};
  for (const r of oversold) oversoldBy[r.channel] = (oversoldBy[r.channel] ?? 0) + 1;
  const o = m.orders_24h;

  console.log('\n================ SALE SIMULATION REPORT ================');
  console.log(`orders generated            ${s.sim.generated}  ${JSON.stringify(s.sim.byChannel)}`);
  console.log(`duplicates / out-of-order   ${s.sim.duplicatesSent} duplicate Nykaa webhooks, ${s.sim.outOfOrder} cancel-before-create`);
  console.log(`orders in system            ${o.total} (reserved ${o.reserved}, cancelled ${o.cancelled}, oversold ${o.oversold})`);
  console.log(`oversell rate               ${(m.oversell_rate_24h * 100).toFixed(2)}%   (target <= 0.50%)  by channel ${JSON.stringify(oversoldBy)}`);
  console.log(`ERP                         ${s.erp.docs} docs in ERP vs ${m.erp_outbox.created} CREATE jobs done -> ${s.erp.docs === m.erp_outbox.created ? 'EXACTLY ONCE' : 'MISMATCH'}`);
  console.log(`ERP faults survived         ${s.erp.rejectedWhileDown} calls during maintenance, ${s.erp.slowCommits} commit-then-timeout, ${s.erp.conflicts409} duplicate creates rejected`);
  console.log(`ERP outbox                  dead ${m.erp_outbox.dead}, backlog ${m.erp_outbox.backlog}`);
  console.log(`channel API calls           ${JSON.stringify(s.calls)}`);
  console.log(`stock push p95 lag (ms)     ${m.stock_push.map((r: any) => `${r.channel}=${r.p95_lag_ms}`).join(' ')}`);
  console.log(`channel shows > sellable    ${JSON.stringify(drift)}  (lowOver must be 0; Nykaa skips <10% changes on SKUs with >50 units)`);
  console.log(`consistency checks          ${cons.ok ? 'PASS' : 'FAIL ' + JSON.stringify(cons).slice(0, 500)}`);
  console.log('=========================================================');
  process.exit(cons.ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

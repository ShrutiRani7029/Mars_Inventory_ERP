# Order & Inventory Sync Service: Design

**Author:** Salim Siddiqui · **For:** Mars Cosmetics Engineering assessment · **Diagram:** [`docs/architecture.png`](docs/architecture.png) (source: [`docs/architecture.mmd`](docs/architecture.mmd))

**AI usage note:** Claude was used to draft and structure this document and to write the reference implementation in [`service/`](service/) and its tests; all design decisions were reviewed by me and I can defend them on the call.

**Reference implementation:** the design is built and tested in [`service/`](service/) (NestJS + Postgres + Redis, with mock channels that reproduce every quirk in the brief). A simulated sale (about 1,000 orders at 300/min, ERP down for 90 s, duplicate and out-of-order webhooks, Blinkit timeouts) gives: **ERP exactly once (931 docs = 931 jobs), 0 lost orders, ledger consistency PASS, push p95 lag 2–8 s per channel, oversell 0.51%.** That last figure comes from a deliberately harsh test: 40% of demand on 20 SKUs holding only 3–15 units each. See the [README](README.md).

## 0. Core idea

**PostgreSQL is the single source of truth for stock.** Every stock change is one atomic SQL statement plus an append-only ledger row. Channels only ever receive a *derived, slightly conservative* number. Everything else (queues, workers, Redis) is plumbing around that.

## 1. Architecture

See the diagram. Everything runs on **ECS Fargate** (NestJS, one image, different entrypoints), region `ap-south-1`.

| Component | Runs on | Job |
|---|---|---|
| `ingest-api` | Fargate, 2 tasks behind ALB | Webhook endpoints. Verify signature, write raw event, return 200 fast. No business logic. |
| `flipkart-poller` | Fargate, 1 task (leader via DB lock) | Polls every 2 min with a cursor and overlap window. |
| `core-worker` | Fargate, auto-scaled on queue depth | Normalise → dedupe → reserve/release stock → write outbox. |
| `stock-pusher` | Fargate, one consumer group per channel | Coalesce dirty SKUs, respect each channel's rate limit, push absolute quantities. |
| `erp-worker` | Fargate, 1–2 tasks | Sends confirmed orders to Business Central, with circuit breaker. |
| `ops-console` | Served by `api` (single HTML page, no build step) | Failed syncs, retry buttons, per-SKU audit trail, day-one metrics. |
| PostgreSQL (RDS) | Multi-AZ | State, ledger, outbox. |
| Redis (ElastiCache) | Single small node | Token buckets, locks, dirty-SKU set. **Rebuildable**; losing it loses nothing durable. |
| Work queues | Postgres tables, claimed with `FOR UPDATE SKIP LOCKED` | `inbound_events`, `channel_stock` (dirty rows), `erp_outbox`. Enqueued **in the same transaction** as the state change, so there is no dual-write. Rows that keep failing are marked `dead` and appear on the ops screen (§8). |

Flow: channel → `ingest-api`/poller → `inbound_events` (raw, deduped) → `core-worker` claims it → order + reservation + ledger + dirty flags + ERP outbox row, **one transaction** → pushers / erp-worker claim their rows → channel / Business Central.

## 2. Data model

| Table | Key columns / constraints |
|---|---|
| `skus` | `sku` PK, `safety_buffer`, `active` |
| `inventory` | `sku` PK/FK, `on_hand`, `reserved`, `version`. **CHECK (on_hand − reserved >= 0)** |
| `inventory_ledger` (append-only) | `id`, `sku`, `delta_on_hand`, `delta_reserved`, `on_hand_after`, `reserved_after`, `reason` (INITIAL, RESERVE, RELEASE, SHIP, RETURN, ADJUST), `order_id`, `actor`, `created_at`. **UNIQUE idempotency_key** (e.g. `RESERVE:<order>:<sku>`) |
| `inbound_events` | `id`, `channel`, `dedupe_key`, `payload` (raw), `normalized`, `status` (pending/done/dead), `attempts`, `next_attempt_at`, `last_error`. **UNIQUE (channel, dedupe_key)** |
| `orders` | `id`, `channel`, `external_order_id`, `status` (RESERVED, OVERSOLD, CANCELLED, SHIPPED, RETURNED), `last_event_time`, totals. **UNIQUE (channel, external_order_id)** |
| `order_items` | `order_id`, `sku`, `qty`, `unit_price`. PK (order_id, sku) |
| `channel_stock` | `sku`, `channel`, `last_pushed_qty`, `dirty`, `dirty_since`, `attempts`, `next_attempt_at`, `status` (ok/failed). PK (sku, channel) |
| `erp_outbox` | `id`, `order_id`, `kind` (CREATE/CANCEL), `status`, `attempts`, `next_attempt_at`, `locked_until`, `erp_doc_no`, `last_error`. **UNIQUE (order_id, kind)** |
| `poll_cursors` | `channel` PK, `cursor`, `updated_at` |

**Avoiding duplicate orders (three layers):**
1. `inbound_events` `UNIQUE (channel, dedupe_key)`. The key is the channel's event id if present, else `hash(external_order_id + event + event_time)`. A second delivery hits `ON CONFLICT DO NOTHING` and is dropped.
2. `orders` `UNIQUE (channel, external_order_id)`. Even if layer 1 is bypassed, the order cannot exist twice.
3. `inventory_ledger` `UNIQUE (idempotency_key)`. A replayed message can never reserve stock twice.

## 3. Order ingestion

- **Webhooks (Website, Nykaa, Blinkit):** verify signature → insert raw event → enqueue → 200. Response time stays small even at 300/min, and the raw event is stored *before* we ack, so nothing is lost if workers are down.
- **Flipkart polling:** poller asks for orders since `cursor − 5 min` (overlap window), inserts through the same dedupe path, then advances the cursor only after commit. Duplicates from the overlap are harmless.
- **Duplicates (Nykaa):** handled by the three layers above.
- **Out of order:** each order is a small state machine, with `last_event_time` guarding updates. If CANCEL arrives *before* CREATE, we create the order directly in `CANCELLED` state **without reserving** (a tombstone). A late CREATE after a CANCEL is ignored. Stale events (older `event_time` than stored) are logged and skipped.
- **Channel down:** channels are isolated by separate queues, workers and circuit breakers, so a Blinkit outage never slows Nykaa. For Flipkart the cursor simply stays put, so nothing is skipped when it returns. For webhook channels we cannot pull missed orders, so a **daily reconciliation job** compares the channel's order list with ours (a gap-filler, flagged in assumptions).

## 4. Inventory consistency: the last unit

Two channels sell the last unit of `MRS-LIP-104` in the same second.

1. Both orders get through `ingest-api` independently and land in `core-worker`.
2. Each worker runs, **in one transaction**:
   ```sql
   -- SKUs always processed in sorted order (no deadlocks on multi-item orders)
   UPDATE inventory
      SET reserved = reserved + :qty, version = version + 1
    WHERE sku = :sku AND on_hand - reserved >= :qty;   -- 0 rows => no stock
   ```
   then inserts the ledger row, the order row and the outbox rows.
3. Postgres takes a **row lock** on that SKU, so the two updates are serialised. The first sees `available = 1` and succeeds. The second waits, then sees `available = 0`, updates **0 rows**, and the whole transaction rolls back for that order.
4. Loser handling: order is stored as `OVERSOLD` (no reservation), an alert/row appears on the ops screen, and we **cancel it on the channel** (API where available, else ops does it manually). The customer is told immediately rather than at dispatch.
5. The commit of the winner marks `channel_stock` dirty for all channels with `desired_qty = 0`, so all four channels show "out of stock" within the push window.

**Honest limit:** marketplaces have already confirmed the order to the customer, so we *cannot* reject it, only cancel afterwards. The DB guarantees we never reserve more than we have; it cannot stop two marketplaces from selling during the push delay. That is why the target (<0.5% cancellations) is met by **reducing the window**, not by locks:
- **Safety buffer.** We push `max(0, available − buffer)`, where `buffer = max(1, ceil(1.5 × (v_all × push_delay + v_flipkart × poll_interval)))` and `v` is units per minute over the last 5 min, retuned every minute. Hot SKUs get larger buffers; slow SKUs keep 1. The Flipkart term covers its orders that we have not polled yet.
- **Flipkart holdback.** Flipkart orders reach us up to 2 min late, so Flipkart loses every last-unit race. It therefore stops selling a SKU 3 units earlier than the real-time channels. In the simulation every remaining oversell was a Flipkart order, and this rule cut the oversell rate from 1.4% to 0.5%.
- **Low-stock priority.** SKUs below a threshold jump the push queue and are pushed on *every* change.
- **Sale-day low-stock quotas** (v1.1 if time): when `available` is tiny, split the remaining units across channels instead of showing the same number everywhere.

**Pseudo-code**
```
reserve(order):
  BEGIN
  for item in sorted(order.items by sku):
     n = UPDATE inventory SET reserved = reserved + item.qty
          WHERE sku = item.sku AND on_hand - reserved >= item.qty
     if n == 0: ROLLBACK; mark order OVERSOLD (new txn); alert; return
     INSERT ledger(order.id, item.sku, +qty reserved, 'RESERVE')   -- UNIQUE guards replays
  INSERT orders(status = RESERVED), erp_outbox(order.id), mark channel_stock dirty
  COMMIT
```
Cancel/return mirrors it: `reserved = reserved − qty` (or `on_hand + qty` for a physical return after warehouse receipt), ledger reason `RELEASE`/`RETURN`, same uniqueness guard, so a repeated cancel event is a no-op.

## 5. Stock push (60 s target)

- **Absolute quantities, not deltas.** Retries and duplicates are therefore idempotent, and "latest value wins" lets us **coalesce**: 50 orders on one SKU become one push.
- **Dirty-set model.** A commit marks `(sku, channel)` dirty. Each channel's pusher loop drains its dirty set every ~5 s, ordered by **lowest available first** (most oversell risk).
- **Per-channel token buckets in Redis**, set to ~80% of the stated limit for headroom.

| Channel | Method | Capacity per 60 s | Note |
|---|---|---|---|
| Website | direct DB/API | unlimited | immediate |
| Flipkart | bulk, 50 SKUs × 10 rps (use ~8) | 24,000 SKUs | whole catalogue (1,200) fits in 24 calls; 429 → exponential backoff + jitter, bucket slows |
| Nykaa | 1 SKU/call, 5 rps (use 4) | ~240 SKUs | **the bottleneck** |
| Blinkit | REST for urgent, daily CSV baseline | limited by ~2% timeouts | retries are safe (absolute qty) |

**Nykaa is the hard one.** Peak ≈ 300 orders/min × ~2 items ≈ 600 SKU changes/min, but only ~240 can be pushed. Mitigation: (a) coalescing (sale traffic is concentrated on a few hot SKUs), (b) priority by lowest stock, (c) **suppress pushes that don't matter** (Nykaa and Blinkit REST): a SKU with plenty of stock (>50) is pushed only if the change is ≥10%, and (d) alert if the backlog age exceeds 60 s. Blinkit needs this too: in the simulation it received about 500 changes/min against about 300 calls/min, and its p95 lag dropped from 195 s to 8 s once suppression was on. Low-stock/at-risk SKUs, the ones that cause oversell, always meet the 60 s target; healthy SKUs are best effort. If Nykaa has a bulk endpoint in reality, the bottleneck disappears (noted for the call).

Blinkit's daily CSV is generated from Postgres as the nightly source of truth, and acts as a self-healing baseline for anything REST missed.

## 6. Failure handling

- **Retries:** failed rows go back to `pending` with exponential backoff and jitter (`next_attempt_at`). After N attempts (8 for events, 15 for ERP) they become `dead`, our DLQ. Errors that can never succeed (unparseable payload, unknown SKU) go to `dead` immediately.
- **Poison messages:** the `dead` row keeps its error and raw payload. Ops can fix the cause (e.g. add the SKU) and **retry from the console**; one click re-queues it, and idempotency keys make that safe. Any dead row triggers an alert.
- **ERP exactly once:** we deliver at-least-once, and make it effectively once with an **idempotency key = our `order_id`** sent as the Business Central `externalDocumentNumber`. Before creating we look the document up first; a duplicate create returns the existing doc. `erp_outbox.order_id` is UNIQUE so we never even enqueue twice.
- **ERP Sunday downtime:** a circuit breaker opens after N consecutive failures/timeouts. Rows wait in `erp_outbox` (durable, no retention limit) and the worker sends one probe a minute. On recovery it drains at a controlled rate (~5 orders/s). Worst case: 2 h × 300/min = 36k orders, drained in ~2 h. Orders are *never* lost because Postgres holds them durably. Other channels are unaffected (stock logic does not depend on ERP).
- **Blinkit timeouts (~2%):** 5 s per-call timeout and several calls in flight, so one hung call blocks nothing else. Retried with backoff, which is safe because pushes are absolute and idempotent. Failed SKUs stay dirty, and the daily CSV corrects any drift.
- **Worker crash mid-transaction:** nothing partial is visible (single transaction); the message reappears and is replayed safely thanks to the unique keys.

## 7. Observability (day one)

1. **Oversell/cancel rate** (`OVERSOLD` orders ÷ orders), alert at >0.3% on sale day (target 0.5%).
2. **Stock push lag p95 per channel** (dirty → acked), alert > 60 s (Nykaa especially).
3. **ERP outbox backlog and oldest-item age**, alert > 15 min outside Sunday maintenance windows.
4. **Dead-letter count** (dead events, dead ERP jobs, failed pushes), alert > 0.
5. **Reconciliation drift:** hourly job comparing our `desired_qty` to the channel's reported stock where readable; alert on drift beyond threshold. Plus Postgres CPU / connections as a basic infra signal.

## 8. Trade-offs

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Where stock is locked | Postgres atomic conditional `UPDATE` | Redis lock/counter as primary; distributed locks | One durable source of truth, row locks serialise correctly, no split-brain between Redis and DB. 300 orders/min is trivial for Postgres. |
| Queue | Postgres tables + `SKIP LOCKED` | SQS; BullMQ on Redis | At peak this is about 5 orders/s, which is trivial for Postgres. The enqueue is in the same transaction as the state change (no dual-write, no lost or phantom messages), and there is one less component to justify. A Redis queue would tie durability to a cache. SQS comes in if volume grows about 100×. |
| Push model | Absolute qty, coalesced dirty set | Delta/event-per-order pushes | Idempotent, retry-safe, fewer calls, and the only way to fit Nykaa's 5 rps. |
| Architecture | Modular NestJS monolith with separate entrypoints | Microservices per channel | 3 developers, 6 weeks. Isolation comes from queues and breakers, not separate repos. |
| Oversell protection | Safety buffer + priority push | Reserving stock per channel (static split) | A static split strands stock and lowers sales; a buffer costs a few units only on hot SKUs. |
| ERP exactly-once | Outbox + idempotency key + lookup | Distributed transactions / hoping it's once | Not possible across systems; at-least-once + idempotency is the standard answer. |

**Budget (estimate, ap-south-1):** RDS Postgres Multi-AZ `db.t4g.large` ~₹14k, Fargate (~6 small tasks) ~₹10k, ElastiCache small ~₹3k, ALB + NAT + data ~₹6k, CloudWatch/logs ~₹3k, buffer ~₹5k, giving roughly **₹40k of the ₹50k** budget. Sale days scale Fargate tasks, not the database.

## 9. Assumptions

- Webhooks are signed with HMAC-SHA256 of the raw body (`x-signature`); the payload shapes in `service/src/channels/normalizers.ts` can be mapped to the common format. Business Central calls us back when an order is dispatched (that moves `reserved` to shipped). Channels expose a cancel API (else ops cancels manually).
- Nykaa has no bulk stock endpoint; if it does, §5 improves massively.
- Stock `available` on a channel can be read via API for reconciliation (Nykaa/Flipkart yes; Blinkit may not, so there only the CSV baseline applies).
- Business Central accepts an external document number and can be queried by it for dedupe.
- Warehouse-confirmed returns increase `on_hand`; customer-initiated cancellations before dispatch only release the reservation.
- One warehouse; INR only; prepaid and COD both reserve stock at order time.
- "Confirmed order" = order that was successfully reserved (not OVERSOLD/CANCELLED).
- Traffic estimates: ~2 line items per order; sale peak 300 orders/min lasts about an hour.
- Webhook-only channels (Nykaa, Blinkit) lack a "list orders" fallback, so a daily reconciliation export is requested from the brand.

## 10. Deliberately left out of v1

- Second warehouse and smart order routing (see stretch).
- Real-time return inspection / grading workflow (returns only restore stock after warehouse "received" signal).
- Dynamic per-channel stock quotas (only buffer + low-stock priority).
- Price/catalogue sync and listing management: out of scope, stock and orders only.
- Auto-scaling ML/forecasting for buffers (use a simple moving-average velocity).
- Multi-region DR (Multi-AZ only) and a full-blown event-sourcing platform; the ledger gives us the audit trail we need.

## 11. Stretch A: second warehouse (Bengaluru)

- `inventory` key becomes `(sku, warehouse_id)`; the ledger gets `warehouse_id`.
- Channel-visible stock = **sum of available across warehouses** (minus buffer); channels don't know about warehouses.
- Reservation adds an **allocation step**: choose a warehouse by pincode proximity, falling back to the other if empty. A multi-line order should ship from one warehouse where possible, else is split (more ERP lines).
- ERP order carries the `warehouse_id`; Business Central location codes map to it.
- The "last unit" race stays correct per warehouse row; fallback loops over candidates inside the same transaction.

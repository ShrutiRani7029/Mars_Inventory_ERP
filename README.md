# Mars Cosmetics: Order & Inventory Sync Service

Keeps orders and stock in sync across **Website, Nykaa, Flipkart and Blinkit** and pushes every confirmed order to **Microsoft Business Central** exactly once.

| Deliverable | Where |
|---|---|
| Design doc (all 8 questions, assumptions, trade-offs, AI note, pseudo-code, stretch A) | [`DESIGN.md`](DESIGN.md) |
| Architecture diagram | [`docs/architecture.png`](docs/architecture.png) · source [`docs/architecture.mmd`](docs/architecture.mmd) |
| Working implementation (NestJS + PostgreSQL + Redis) | [`service/`](service/) |
| Tests (run against real Postgres) | [`service/test/`](service/test/) |
| Ops console screenshot | [`docs/ops-console.png`](docs/ops-console.png) |

![architecture](docs/architecture.png)

## The idea in one paragraph

**PostgreSQL is the single source of truth for stock.** An order reserves stock with one conditional `UPDATE inventory SET reserved = reserved + q WHERE sku = $1 AND on_hand - reserved >= q`, plus an append-only ledger row, in the same transaction that records the order and queues its ERP job. When two channels sell the last unit in the same second, Postgres row-locks the SKU and the loser's `UPDATE` matches 0 rows, so we can never reserve stock we don't have. Duplicates are stopped by unique keys at three levels. Channels get *absolute* quantities (minus a safety buffer), so retries are harmless. The ERP gets an idempotency key, so retries can't create a second sales order.

## Results from a simulated sale

`npm run simulate` drives the real api and worker through mock channels that behave as the brief says: Nykaa sends duplicate and out-of-order webhooks and enforces 5 rps; Flipkart is poll-only with a 50-SKU bulk API at 10 rps; about 2% of Blinkit calls hang; and the ERP goes down for maintenance. Shoppers only buy what each channel *displays*, which is how real overselling happens.

```
orders generated            993  {"FLIPKART":251,"NYKAA":351,"WEBSITE":232,"BLINKIT":159}
duplicates / out-of-order   44 duplicate Nykaa webhooks, 8 cancel-before-create
orders in system            987 (reserved 923, cancelled 59, oversold 5)
oversell rate               0.51%   (target <= 0.50%)  by channel {"FLIPKART":5}
ERP                         931 docs in ERP vs 931 CREATE jobs done -> EXACTLY ONCE
ERP faults survived         16 calls during maintenance, 7 commit-then-timeout
ERP outbox                  dead 0, backlog 0
channel API calls           Nykaa 293 ok / 0 x 429 · Blinkit 290 ok / 4 timeouts survived
stock push p95 lag (ms)     BLINKIT=8010 FLIPKART=3897 NYKAA=6237 WEBSITE=1901   (target 60,000)
consistency checks          PASS  (ledger replays to inventory; reserved == open orders)
```

The test is deliberately harsh: about 300 orders/min starting cold, with **40% of all demand on 20 SKUs that hold only 3–15 units each**, and the ERP down for the first 90 s. Two findings from earlier runs changed the design: (1) Blinkit REST needs the same "push only what matters" rule as Nykaa, which cut its p95 lag from 195 s to 8 s; (2) Flipkart orders reach us up to 2 min late, so Flipkart stops selling a SKU a few units early. That cut oversells from 1.4% to 0.5%. Both are in DESIGN.md §4–5.

## Run it

**Docker** (Postgres, Redis, api, worker, mock channels):

```bash
docker compose up --build
open http://localhost:3000/ops                     # ops console
docker compose exec api node dist/scripts/simulate-sale.js --orders 1200 --per-minute 300 --erp-down 90
```

**Without Docker** (needs Postgres 14+ and Redis running locally):

```bash
cd service
npm ci && npm run build
export DATABASE_URL=postgres://mars:mars@localhost:5432/mars
npm run migrate && npm run seed          # seed WIPES data and loads 1,200 SKUs
npm run start:mock & npm run start:api & npm run start:worker &
npm run simulate -- --orders 1200 --per-minute 300 --erp-down 90
```

**Tests** (need a Postgres database, default `postgres://mars:mars@localhost:5432/mars_test`):

```bash
cd service && npm test      # 21 tests
```

| Test | Proves |
|---|---|
| 12 orders for the last unit, 12 workers racing | exactly 1 `RESERVED`, 11 `OVERSOLD`, `reserved = 1`, one ledger row |
| 200 concurrent mixed orders | never over-reserves, stock is used up (not stranded), ledger replays exactly |
| multi-item order, one line short | all-or-nothing, no partial reservation |
| same Nykaa webhook twice / re-sent with new timestamp | stored once / reserved once |
| CANCEL arrives before CREATE | tombstone; late CREATE ignored; nothing sent to ERP |
| double cancel, ship → return | stock released once; full audit trail `INITIAL → RESERVE → SHIP → RETURN` |
| ERP commits then times out | retry looks the doc up first: no duplicate |
| ERP down (Sunday) | breaker opens, at most 3 attempts per job, 0 dead, all delivered after recovery |
| 7 orders on one SKU | coalesced into one absolute-quantity push |
| unknown SKU / unparseable payload | dead immediately, visible to ops, nothing moved |

## Code map (`service/src`)

| Requirement | Code |
|---|---|
| 1. Common order format | `channels/normalizers.ts` (one converter per channel; payload shapes are assumptions) |
| 1. Webhooks / polling | `ingest/ingest.controller.ts` (HMAC check, store raw, 200) · `ingest/flipkart-poller.service.ts` |
| 2. Reserve on arrival | `inventory/stock.ts` (`moveStock`: the conditional UPDATE + ledger) · `inventory/order-processor.service.ts` |
| 3. Channels within 60 s | `push/stock-pusher.service.ts` · `channels/channel-clients.ts` · `redis/rate-limiter.service.ts` (token bucket, Lua) · `push/buffer-tuner.service.ts` · `push/blinkit-csv.service.ts` |
| 4. Cancel / return | `order-processor.service.ts` (`onCancelled`, `onReturned`, tombstones) |
| 5. ERP exactly once | `erp/erp-worker.service.ts` (outbox, lease, lookup-before-create) · `erp/circuit-breaker.ts` |
| 6. Ops screen + retry | `ops/ops.controller.ts` · `ops/ops.html.ts` (served at `/ops`) |
| Audit trail | `inventory_ledger` + `GET /ops/sku/:sku` |
| Day-one metrics | `ops/metrics.service.ts` (`/ops/metrics`, Prometheus `/metrics`, `/ops/consistency`) |
| Schema | `migrations/001_init.sql` |
| Mock channels + ERP + sale generator | `mock/mock-channels.ts` (local only; no real endpoint is ever called) |

## Scope notes

- Channel and ERP payload shapes, webhook HMAC auth and the ERP "dispatched" callback are assumptions; they are listed in DESIGN.md §9.
- Queues are Postgres tables claimed with `FOR UPDATE SKIP LOCKED`, not SQS. At about 5 orders/s, this avoids an extra component and the dual-write problem (see the trade-offs table).
- Ops console auth would sit at the ALB (company SSO) and is not in this repo. Terraform/AWS deployment is not included; there is no hosted demo, because this runs locally with `docker compose up`.
- **AI usage:** Claude was used to draft and structure the design doc and to write the implementation and tests. Every decision was reviewed by the author.

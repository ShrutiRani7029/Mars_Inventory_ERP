-- Order & Inventory Sync Service: initial schema.
-- PostgreSQL is the single source of truth for stock. Every stock change is an
-- atomic UPDATE on `inventory` plus an append-only `inventory_ledger` row in the
-- same transaction.

CREATE TABLE skus (
  sku            text PRIMARY KEY,
  name           text NOT NULL,
  safety_buffer  int  NOT NULL DEFAULT 1 CHECK (safety_buffer >= 0),
  buffer_locked  boolean NOT NULL DEFAULT false,   -- ops override: tuner leaves it alone
  active         boolean NOT NULL DEFAULT true
);

CREATE TABLE inventory (
  sku        text PRIMARY KEY REFERENCES skus(sku),
  on_hand    int  NOT NULL CHECK (on_hand >= 0),
  reserved   int  NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  version    bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT available_non_negative CHECK (on_hand - reserved >= 0)
);

-- Append-only audit trail: "why did stock change, and when?"
CREATE TABLE inventory_ledger (
  id               bigserial PRIMARY KEY,
  sku              text NOT NULL REFERENCES skus(sku),
  delta_on_hand    int  NOT NULL DEFAULT 0,
  delta_reserved   int  NOT NULL DEFAULT 0,
  on_hand_after    int  NOT NULL,
  reserved_after   int  NOT NULL,
  reason           text NOT NULL CHECK (reason IN ('INITIAL','RESERVE','RELEASE','SHIP','RETURN','ADJUST')),
  order_id         bigint,
  idempotency_key  text NOT NULL UNIQUE,          -- replayed events can never move stock twice
  actor            text NOT NULL,
  note             text,
  created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX inventory_ledger_sku_time ON inventory_ledger (sku, created_at DESC);

-- Raw inbound events. Also the work queue for the order processor (SKIP LOCKED).
CREATE TABLE inbound_events (
  id                bigserial PRIMARY KEY,
  channel           text NOT NULL,
  dedupe_key        text NOT NULL,
  event_type        text,
  external_order_id text,
  event_time        timestamptz,
  payload           jsonb NOT NULL,               -- exactly what the channel sent
  normalized        jsonb,                        -- common order format (null if unparseable)
  status            text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead')),
  outcome           text,
  attempts          int  NOT NULL DEFAULT 0,
  next_attempt_at   timestamptz NOT NULL DEFAULT now(),
  last_error        text,
  received_at       timestamptz NOT NULL DEFAULT now(),
  processed_at      timestamptz,
  CONSTRAINT inbound_events_dedupe UNIQUE (channel, dedupe_key)   -- layer 1 of duplicate protection
);
CREATE INDEX inbound_events_pending ON inbound_events (next_attempt_at, id) WHERE status = 'pending';

CREATE TABLE orders (
  id                bigserial PRIMARY KEY,
  channel           text NOT NULL,
  external_order_id text NOT NULL,
  status            text NOT NULL CHECK (status IN ('NEW','RESERVED','OVERSOLD','CANCELLED','SHIPPED','RETURNED')),
  last_event_time   timestamptz,
  payment_mode      text,
  shipping_pincode  text,
  total_amount      numeric(12,2),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT orders_channel_external UNIQUE (channel, external_order_id)  -- layer 2
);
CREATE INDEX orders_status_created ON orders (status, created_at);

CREATE TABLE order_items (
  order_id   bigint NOT NULL REFERENCES orders(id),
  sku        text   NOT NULL,
  qty        int    NOT NULL CHECK (qty > 0),
  unit_price numeric(12,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (order_id, sku)
);

-- What each channel should show. Dirty rows are the stock-push work queue.
CREATE TABLE channel_stock (
  sku              text NOT NULL REFERENCES skus(sku),
  channel          text NOT NULL,
  last_pushed_qty  int,
  last_pushed_at   timestamptz,
  last_lag_ms      int,
  dirty            boolean NOT NULL DEFAULT true,
  dirty_since      timestamptz DEFAULT now(),
  attempts         int NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  status           text NOT NULL DEFAULT 'ok' CHECK (status IN ('ok','failed')),
  last_error       text,
  PRIMARY KEY (sku, channel)
);
CREATE INDEX channel_stock_dirty ON channel_stock (channel, next_attempt_at) WHERE dirty;

-- Transactional outbox for Business Central.
CREATE TABLE erp_outbox (
  id               bigserial PRIMARY KEY,
  order_id         bigint NOT NULL REFERENCES orders(id),
  kind             text NOT NULL CHECK (kind IN ('CREATE','CANCEL')),
  status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','done','dead','skipped')),
  attempts         int  NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  locked_until     timestamptz,
  erp_doc_no       text,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  sent_at          timestamptz,
  CONSTRAINT erp_outbox_once UNIQUE (order_id, kind)   -- never enqueued twice
);
CREATE INDEX erp_outbox_ready ON erp_outbox (next_attempt_at) WHERE status IN ('pending','processing');

CREATE TABLE poll_cursors (
  channel    text PRIMARY KEY,
  cursor     timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

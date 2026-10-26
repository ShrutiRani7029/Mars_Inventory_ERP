import { Channel } from './common/types';

const env = (key: string, fallback: string): string => process.env[key] ?? fallback;
const num = (key: string, fallback: number): number => Number(process.env[key] ?? fallback);

export const config = {
  databaseUrl: env('DATABASE_URL', 'postgres://mars:mars@localhost:5432/mars'),
  dbPoolMax: num('DB_POOL_MAX', 10),
  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),
  apiPort: num('API_PORT', 3000),
  mockPort: num('MOCK_PORT', 4000),
  mockBaseUrl: env('MOCK_BASE_URL', 'http://localhost:4000'),
  apiBaseUrl: env('API_BASE_URL', 'http://localhost:3000'),

  // Webhook auth: HMAC-SHA256 of the raw body in `x-signature` (assumption, see DESIGN.md).
  verifySignatures: env('VERIFY_SIGNATURES', 'true') === 'true',
  webhookSecrets: {
    WEBSITE: env('WEBSITE_WEBHOOK_SECRET', 'website-secret'),
    NYKAA: env('NYKAA_WEBHOOK_SECRET', 'nykaa-secret'),
    BLINKIT: env('BLINKIT_WEBHOOK_SECRET', 'blinkit-secret'),
    FLIPKART: '',
  } as Record<Channel, string>,
  erpWebhookSecret: env('ERP_WEBHOOK_SECRET', 'erp-secret'),

  workerConcurrency: num('WORKER_CONCURRENCY', 4),
  maxEventAttempts: num('MAX_EVENT_ATTEMPTS', 8),

  // Stock push. Rates are ~80% of each channel's published limit.
  pushIntervalMs: num('PUSH_INTERVAL_MS', 2000),
  pushBatch: num('PUSH_BATCH', 200),
  maxPushAttempts: num('MAX_PUSH_ATTEMPTS', 10),
  rates: {
    WEBSITE: num('RATE_WEBSITE', 50),
    NYKAA: num('RATE_NYKAA', 4),       // limit 5 rps, 1 SKU per call
    FLIPKART: num('RATE_FLIPKART', 8), // limit 10 rps, 50 SKUs per call
    BLINKIT: num('RATE_BLINKIT', 5),
  } as Record<Channel, number>,
  blinkitTimeoutMs: num('BLINKIT_TIMEOUT_MS', 5000),
  channelTimeoutMs: num('CHANNEL_TIMEOUT_MS', 5000),
  lowStockThreshold: num('LOW_STOCK_THRESHOLD', 10),
  bigStockThreshold: num('BIG_STOCK_THRESHOLD', 50),
  nykaaSuppressRatio: num('NYKAA_SUPPRESS_RATIO', 0.1),
  minSafetyBuffer: num('MIN_SAFETY_BUFFER', 1),
  polledChannelHoldback: num('POLLED_CHANNEL_HOLDBACK', 3), // extra units hidden from Flipkart only
  bufferTuneIntervalMs: num('BUFFER_TUNE_INTERVAL_MS', 60_000),
  expectedPushDelayMin: num('EXPECTED_PUSH_DELAY_MIN', 1),

  // Flipkart polling
  flipkartPollMs: num('FLIPKART_POLL_MS', 120_000),
  flipkartOverlapMs: num('FLIPKART_OVERLAP_MS', 5 * 60_000),

  // ERP
  erpTimeoutMs: num('ERP_TIMEOUT_MS', 10_000),
  maxErpAttempts: num('MAX_ERP_ATTEMPTS', 15),
  erpBreakerThreshold: num('ERP_BREAKER_THRESHOLD', 5),
  erpBreakerOpenMs: num('ERP_BREAKER_OPEN_MS', 60_000),
  erpLeaseMs: num('ERP_LEASE_MS', 60_000),

  // Blinkit daily CSV baseline (IST hour)
  blinkitCsvHourIst: num('BLINKIT_CSV_HOUR_IST', 2),
};

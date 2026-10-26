import { Global, Module } from '@nestjs/common';
import { StockClients } from './channels/channel-clients';
import { Db } from './db/db.service';
import { HttpErpClient } from './erp/erp-client';
import { ERP_CLIENT, ErpWorker } from './erp/erp-worker.service';
import { FlipkartPoller } from './ingest/flipkart-poller.service';
import { IngestService } from './ingest/ingest.service';
import { OrderProcessor } from './inventory/order-processor.service';
import { MetricsService } from './ops/metrics.service';
import { BlinkitCsv } from './push/blinkit-csv.service';
import { BufferTuner } from './push/buffer-tuner.service';
import { StockPusher } from './push/stock-pusher.service';
import { RateLimiter } from './redis/rate-limiter.service';

const providers = [
  Db,
  RateLimiter,
  StockClients,
  IngestService,
  OrderProcessor,
  StockPusher,
  BufferTuner,
  BlinkitCsv,
  FlipkartPoller,
  ErpWorker,
  MetricsService,
  { provide: ERP_CLIENT, useClass: HttpErpClient },
];

/** One codebase, one image. `api` and `worker` just start different parts of it. */
@Global()
@Module({ providers, exports: providers })
export class CoreModule {}

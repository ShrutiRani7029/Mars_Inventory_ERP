import 'reflect-metadata';
import { Injectable, Logger, Module, OnApplicationShutdown } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CHANNELS, Channel, sleep } from './common/types';
import { config } from './config';
import { CoreModule } from './core.module';
import { ErpWorker } from './erp/erp-worker.service';
import { FlipkartPoller } from './ingest/flipkart-poller.service';
import { OrderProcessor } from './inventory/order-processor.service';
import { BlinkitCsv } from './push/blinkit-csv.service';
import { BufferTuner } from './push/buffer-tuner.service';
import { StockPusher } from './push/stock-pusher.service';

/** Channels limited to one SKU per call get small batches so low-stock SKUs never wait long. */
const batchFor = (ch: Channel) => (ch === 'NYKAA' || ch === 'BLINKIT' ? Math.max(10, config.rates[ch] * 5) : config.pushBatch);

@Injectable()
export class WorkerRunner implements OnApplicationShutdown {
  private readonly log = new Logger('Worker');
  private stopping = false;
  private loops: Promise<void>[] = [];

  constructor(
    private readonly orders: OrderProcessor,
    private readonly pusher: StockPusher,
    private readonly erp: ErpWorker,
    private readonly poller: FlipkartPoller,
    private readonly tuner: BufferTuner,
    private readonly csv: BlinkitCsv,
  ) {}

  /**
   * Run `step` repeatedly. `step` returns true when it did work (run again at once) or false
   * when idle (sleep). Errors never kill a loop: each channel/ERP loop is isolated.
   */
  private loop(name: string, step: () => Promise<boolean>, idleMs: number) {
    const run = async () => {
      while (!this.stopping) {
        try {
          if (!(await step())) await sleep(idleMs);
        } catch (e) {
          this.log.error(`${name}: ${(e as Error).message}`);
          await sleep(Math.min(idleMs * 5, 10_000));
        }
      }
    };
    this.loops.push(run());
  }

  start() {
    for (let i = 0; i < config.workerConcurrency; i++) this.loop(`orders-${i}`, () => this.orders.processNext(), 200);
    for (const ch of CHANNELS) this.loop(`push-${ch}`, async () => (await this.pusher.pushOnce(ch, batchFor(ch))) > 0, config.pushIntervalMs);
    this.loop('erp', () => this.erp.processNext(), 1000);
    this.loop('flipkart-poll', async () => (await this.poller.pollOnce(), false), config.flipkartPollMs);
    this.loop('buffer-tuner', async () => (await this.tuner.tune(), false), config.bufferTuneIntervalMs);
    this.loop('blinkit-csv', async () => (await this.csv.maybeRunDaily(), false), 60_000);
    this.log.log(`started: ${config.workerConcurrency} order loops, ${CHANNELS.length} pushers, erp, flipkart poller (${config.flipkartPollMs} ms)`);
  }

  async onApplicationShutdown() {
    this.stopping = true;
    await Promise.race([Promise.all(this.loops), sleep(15_000)]);
  }
}

@Module({ imports: [CoreModule], providers: [WorkerRunner] })
class WorkerModule {}

async function main() {
  const app = await NestFactory.createApplicationContext(WorkerModule);
  app.enableShutdownHooks();
  app.get(WorkerRunner).start();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

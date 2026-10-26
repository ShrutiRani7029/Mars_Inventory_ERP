import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { config } from './config';
import { CoreModule } from './core.module';
import { IngestController } from './ingest/ingest.controller';
import { OpsController } from './ops/ops.controller';

@Module({ imports: [CoreModule], controllers: [IngestController, OpsController] })
export class ApiModule {}

async function main() {
  // rawBody: needed to verify webhook HMAC signatures over the exact bytes received.
  const app = await NestFactory.create(ApiModule, { rawBody: true });
  app.enableShutdownHooks();
  await app.listen(config.apiPort);
  console.log(`api listening on :${config.apiPort}  (ops console: /ops)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

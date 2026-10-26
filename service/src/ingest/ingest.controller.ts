import { Body, Controller, Headers, HttpCode, NotFoundException, Param, Post, RawBodyRequest, Req, UnauthorizedException } from '@nestjs/common';
import { Request } from 'express';
import { Channel } from '../common/types';
import { config } from '../config';
import { Db } from '../db/db.service';
import { IngestService } from './ingest.service';

const WEBHOOK_CHANNELS: Record<string, Channel> = { website: 'WEBSITE', nykaa: 'NYKAA', blinkit: 'BLINKIT' };

@Controller('webhooks')
export class IngestController {
  constructor(private readonly ingest: IngestService, private readonly db: Db) {}

  /** Business Central tells us an order was dispatched (assumed callback). */
  @Post('erp/dispatched')
  @HttpCode(200)
  async erpDispatched(@Req() req: RawBodyRequest<Request>, @Headers('x-signature') sig: string, @Body() body: { externalDocumentNumber?: string }) {
    if (!this.ingest.verifySignature(config.erpWebhookSecret, req.rawBody, sig)) throw new UnauthorizedException();
    const id = Number(String(body.externalDocumentNumber ?? '').replace(/^MRS-/, ''));
    const { rows } = await this.db.query(`SELECT channel, external_order_id FROM orders WHERE id = $1`, [id]);
    if (!rows.length) throw new NotFoundException('unknown order');
    return this.ingest.ingestEvent(
      { channel: rows[0].channel, external_order_id: rows[0].external_order_id, event: 'ORDER_SHIPPED', event_time: new Date().toISOString(), items: [], dedupe_key: `erp-ship:${id}` },
      body,
    );
  }

  @Post(':channel')
  @HttpCode(200)
  async handle(@Param('channel') name: string, @Req() req: RawBodyRequest<Request>, @Headers('x-signature') sig: string, @Body() body: unknown) {
    const channel = WEBHOOK_CHANNELS[name.toLowerCase()];
    if (!channel) throw new NotFoundException(`no webhook for channel ${name}`); // Flipkart is polled
    if (!this.ingest.verifySignature(config.webhookSecrets[channel], req.rawBody, sig)) throw new UnauthorizedException('bad signature');
    // 200 for accepted AND duplicate: the channel must stop retrying either way.
    return this.ingest.ingestRaw(channel, body);
  }
}

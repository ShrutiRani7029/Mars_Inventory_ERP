import { Injectable } from '@nestjs/common';
import { HttpError, httpJson } from '../channels/channel-clients';
import { config } from '../config';

export interface ErpSalesOrder {
  externalDocumentNumber: string; // our idempotency key: MRS-<order id>
  channel: string;
  channelOrderId: string;
  paymentMode?: string | null;
  shipToPincode?: string | null;
  lines: { sku: string; qty: number; unitPrice: number }[];
}

export interface ErpClient {
  findByExternalDoc(externalDocumentNumber: string): Promise<string | null>;
  create(order: ErpSalesOrder): Promise<string>;
  cancel(docNo: string): Promise<void>;
}

/** Business Central adapter (pointed at the mock locally). */
@Injectable()
export class HttpErpClient implements ErpClient {
  private readonly base = `${config.mockBaseUrl}/erp`;

  async findByExternalDoc(ext: string): Promise<string | null> {
    const r = await httpJson('GET', `${this.base}/salesOrders?externalDocumentNumber=${encodeURIComponent(ext)}`, undefined, config.erpTimeoutMs);
    return r?.value?.[0]?.number ?? null;
  }

  async create(order: ErpSalesOrder): Promise<string> {
    try {
      const r = await httpJson('POST', `${this.base}/salesOrders`, order, config.erpTimeoutMs);
      return r.number;
    } catch (e) {
      // Someone (a previous attempt that timed out) already created it: fetch the existing doc.
      if (e instanceof HttpError && e.status === 409) {
        const existing = await this.findByExternalDoc(order.externalDocumentNumber);
        if (existing) return existing;
      }
      throw e;
    }
  }

  async cancel(docNo: string): Promise<void> {
    await httpJson('POST', `${this.base}/salesOrders/${encodeURIComponent(docNo)}/cancel`, {}, config.erpTimeoutMs);
  }
}

export const isRetryableErp = (e: unknown) =>
  !(e instanceof HttpError) || e.status === 408 || e.status === 429 || e.status >= 500;

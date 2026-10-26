import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolClient, QueryResultRow } from 'pg';
import { config } from '../config';

export type Tx = PoolClient;

@Injectable()
export class Db implements OnModuleDestroy {
  readonly pool = new Pool({ connectionString: config.databaseUrl, max: config.dbPoolMax });

  query<T extends QueryResultRow = any>(text: string, params?: unknown[]) {
    return this.pool.query<T>(text, params as any[]);
  }

  /** Run `fn` in one transaction. Anything thrown rolls the whole thing back. */
  async tx<T>(fn: (c: Tx) => Promise<T>): Promise<T> {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      const result = await fn(c);
      await c.query('COMMIT');
      return result;
    } catch (err) {
      await c.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      c.release();
    }
  }

  async onModuleDestroy() {
    await this.pool.end();
  }
}

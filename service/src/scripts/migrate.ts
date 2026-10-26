import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import { Client } from 'pg';
import { config } from '../config';

/** Minimal forward-only migrator: applies migrations/*.sql in name order, once each. */
export async function migrate(url = config.databaseUrl) {
  const dir = join(__dirname, '..', '..', 'migrations');
  const c = new Client({ connectionString: url });
  await c.connect();
  try {
    await c.query(`CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz DEFAULT now())`);
    for (const f of readdirSync(dir).filter((x) => x.endsWith('.sql')).sort()) {
      const done = await c.query(`SELECT 1 FROM schema_migrations WHERE name = $1`, [f]);
      if (done.rowCount) continue;
      await c.query('BEGIN');
      await c.query(readFileSync(join(dir, f), 'utf8'));
      await c.query(`INSERT INTO schema_migrations (name) VALUES ($1)`, [f]);
      await c.query('COMMIT');
      console.log(`applied ${f}`);
    }
  } finally {
    await c.end();
  }
}

if (require.main === module) {
  migrate().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}

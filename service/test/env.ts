process.env.DATABASE_URL ??= 'postgres://mars:mars@localhost:5432/mars_test';
process.env.DB_POOL_MAX = '30';
process.env.ERP_BREAKER_THRESHOLD = '3';
process.env.ERP_BREAKER_OPEN_MS = '300';
process.env.MIN_SAFETY_BUFFER = '1';

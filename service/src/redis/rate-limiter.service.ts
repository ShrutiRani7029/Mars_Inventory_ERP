import { Injectable, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';
import { config } from '../config';
import { Channel, sleep } from '../common/types';

/*
 * Token bucket shared by every pusher process (that is why it lives in Redis, not memory).
 * Returns 0 if a token was taken, else the ms to wait. Redis holds no durable state:
 * if it is flushed, buckets simply refill.
 */
const TOKEN_BUCKET_LUA = `
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local d = redis.call('HMGET', key, 't', 'ts')
local tokens = tonumber(d[1]) or burst
local ts = tonumber(d[2]) or now
tokens = math.min(burst, tokens + math.max(0, now - ts) / 1000 * rate)
local wait = 0
if tokens >= 1 then tokens = tokens - 1 else wait = math.ceil((1 - tokens) / rate * 1000) end
redis.call('HSET', key, 't', tokens, 'ts', now)
redis.call('PEXPIRE', key, 60000)
return wait`;

@Injectable()
export class RateLimiter implements OnModuleDestroy {
  readonly redis = new Redis(config.redisUrl, { lazyConnect: true, maxRetriesPerRequest: 2 });

  constructor() {
    this.redis.defineCommand('takeToken', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
  }

  /**
   * Block until one call to `channel` is allowed. Burst is 1: calls are evenly spaced,
   * because the channels count requests in a sliding 1-second window.
   */
  async acquire(channel: Channel): Promise<void> {
    const rate = config.rates[channel];
    for (;;) {
      const wait = Number(await (this.redis as any).takeToken(`tb:${channel}`, rate, 1, Date.now()));
      if (wait <= 0) return;
      await sleep(wait);
    }
  }

  async onModuleDestroy() {
    this.redis.disconnect();
  }
}

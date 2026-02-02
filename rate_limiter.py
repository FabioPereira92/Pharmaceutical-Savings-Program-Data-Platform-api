from __future__ import annotations
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


@dataclass
class RateLimitDecision:
    allowed: bool
    remaining: int


class InMemoryTokenBucket:
    def __init__(self):
        self._buckets = defaultdict(lambda: {"tokens": 0, "last": time.time(), "limit": 0})

    def allow(self, key: str, limit: int, period: int) -> RateLimitDecision:
        now = time.time()
        b = self._buckets[key]
        # initialize bucket for key if limit changed or first time
        if b["limit"] != limit:
            b["limit"] = limit
            b["tokens"] = limit
            b["last"] = now

        elapsed = now - b["last"]
        refill = int(elapsed / period) * limit
        if refill > 0:
            b["tokens"] = min(limit, b["tokens"] + refill)
            b["last"] = now

        if b["tokens"] > 0:
            b["tokens"] -= 1
            return RateLimitDecision(True, b["tokens"])
        return RateLimitDecision(False, 0)


class RedisTokenBucket:
    """
    Minimal Redis token bucket using a Lua script for atomicity.
    Keyed per API key. Stores tokens and last_refill.
    """
    LUA = r"""
local key_tokens = KEYS[1]
local key_last = KEYS[2]
local limit = tonumber(ARGV[1])
local period = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local tokens = tonumber(redis.call("GET", key_tokens) or limit)
local last = tonumber(redis.call("GET", key_last) or now)

local elapsed = now - last
local refill = math.floor(elapsed / period) * limit
if refill > 0 then
  tokens = math.min(limit, tokens + refill)
  last = now
end

if tokens > 0 then
  tokens = tokens - 1
  redis.call("SET", key_tokens, tokens, "EX", period*2)
  redis.call("SET", key_last, last, "EX", period*2)
  return {1, tokens}
else
  redis.call("SET", key_tokens, tokens, "EX", period*2)
  redis.call("SET", key_last, last, "EX", period*2)
  return {0, 0}
end
"""

    def __init__(self, redis_url: str):
        if redis is None:
            raise RuntimeError("redis library not installed")
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self._sha = self.r.script_load(self.LUA)

    def allow(self, key: str, limit: int, period: int) -> RateLimitDecision:
        now = int(time.time())
        kt = f"rl:{key}:tokens"
        kl = f"rl:{key}:last"
        allowed, remaining = self.r.evalsha(self._sha, 2, kt, kl, limit, period, now)
        return RateLimitDecision(bool(int(allowed)), int(remaining))


def build_limiter(redis_url: Optional[str]):
    if redis_url:
        try:
            return RedisTokenBucket(redis_url)
        except Exception:
            # Fall back to in-memory if Redis unavailable
            return InMemoryTokenBucket()
    return InMemoryTokenBucket()


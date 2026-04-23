import time
import redis  
from fastapi import HTTPException, Request, Depends
from typing import Callable

class RedisTokenBucket:
    def __init__(self, redis_client: redis.Redis, key: str, rate: float, capacity: int):
        """
        Initialize the Redis-backed token bucket.
        
        :param redis_client: Redis client instance.
        :param key: The key to use in Redis for storing the token bucket.
        :param rate: The rate at which tokens are added to the bucket (tokens per second).
        :param capacity: The maximum number of tokens the bucket can hold.
        """
        self.redis = redis_client
        self.key = key
        self.rate = rate
        self.capacity = capacity

    def _get_tokens(self):
        """Retrieve the current number of tokens and the last checked time from Redis."""
        data = self.redis.hmget(self.key, "tokens", "last_checked")
        tokens = float(data[0]) if data[0] is not None else self.capacity
        last_checked = float(data[1]) if data[1] is not None else time.time()
        return tokens, last_checked

    def _set_tokens(self, tokens, last_checked):
        """Update the number of tokens and the last checked time in Redis."""
        self.redis.hset(self.key, mapping={"tokens": tokens, "last_checked": last_checked})

    def allow_request(self) -> bool:
        """Determine if a request is allowed based on the current token count."""
        tokens, last_checked = self._get_tokens()
        now = time.time()
        time_passed = now - last_checked

        tokens = min(self.capacity, tokens + time_passed * self.rate)

        if tokens >= 1:
            tokens -= 1
            self._set_tokens(tokens, now)
            return True
        else:
            self._set_tokens(tokens, last_checked)
            return False

def rate_limiter(
    redis_client: redis.Redis, rate: float, capacity: int
) -> Callable[[Request], None]:
    bucket = RedisTokenBucket(redis_client, "rate_limiter", rate, capacity)

    def limiter(request: Request):
        if not bucket.allow_request():
            raise HTTPException(status_code=429, detail="Too Many Requests")

    return limiter


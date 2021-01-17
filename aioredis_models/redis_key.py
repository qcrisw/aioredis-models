from aioredis import Redis


class RedisKey:
    def __init__(self, redis: Redis, key: str):
        self._redis = redis
        self._key = key

    async def delete(self):
        return await self._redis.delete(self._key)

    async def exists(self):
        return await self._redis.exists(self._key) > 0

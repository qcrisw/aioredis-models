from typing import Set
from .redis_key import RedisKey


class RedisSet(RedisKey):
    async def get_all(self, encoding='utf-8') -> Set:
        return await self._redis.smembers(self._key, encoding=encoding)

    async def add(self, value: str):
        if value is not None:
            return await self._redis.sadd(self._key, value)

    async def remove(self, value: str):
        return await self._redis.srem(self._key, value)

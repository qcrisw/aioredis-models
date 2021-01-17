from typing import Set
from .redis_key import RedisKey


class RedisHash(RedisKey):
    async def field_exists(self, field: str) -> bool:
        return await self._redis.hexists(self._key, field)

    async def fields(self, encoding='utf-8') -> Set:
        return set(await self._redis.hkeys(self._key, encoding=encoding))

    async def get_all(self, encoding='utf-8') -> dict:
        return await self._redis.hgetall(self._key, encoding=encoding)

    async def get(self, field: str, encoding='utf-8'):
        return await self._redis.hget(self._key, field, encoding=encoding)

    async def set_all(self, values: dict):
        if values:
            return await self._redis.hmset_dict(self._key, values)

    async def set(self, field: str, value: str):
        if value:
            return await self._redis.hset(self._key, field, value)

    async def remove(self, field: str) -> int:
        return await self._redis.hdel(self._key, field)

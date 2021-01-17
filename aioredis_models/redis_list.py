from functools import partial
from typing import List
from .redis_key import RedisKey


class RedisList(RedisKey):
    async def get_range(self, start: int=0, stop: int=-1, encoding='utf-8') -> List:
        return await self._redis.lrange(self._key, start, stop, encoding=encoding)

    async def push(self, *value: List[str], reverse: bool=False):
        value = list(filter(None, value))
        if not value:
            return
        func = self._redis.rpush if reverse else self._redis.lpush
        return await func(self._key, *value)

    async def pop(self, reverse: bool=False, block: bool=False, timeout: int=0, encoding='utf-8'):
        if reverse and block:
            func = partial(self._redis.brpop, timeout=timeout)
        elif reverse and not block:
            func = self._redis.rpop
        elif block:
            func = partial(self._redis.blpop, timeout=timeout)
        else:
            func = self._redis.lpop

        return await func(self._key, encoding=encoding)

    async def move(self, destination_key: str, block: bool=False, timeout: int=0, encoding='utf-8'):
        func = partial(self._redis.brpoplpush, timeout=timeout) if block else self._redis.rpoplpush
        return await func(self._key, destination_key, encoding=encoding)

    async def requeue(self, block: bool=False, timeout: int=0, encoding='utf-8'):
        return await self.move(self._key, block=block, timeout=timeout, encoding=encoding)

    async def remove(self, value: str, count: int=0):
        return await self._redis.lrem(self._key, count, value)

    async def find_index(self, value, start: int=0, stop: int=-1, encoding='utf-8') -> int:
        items = await self.get_range(start=start, stop=stop, encoding=encoding) or []
        try:
            return items.index(value) + start
        except ValueError:
            return None

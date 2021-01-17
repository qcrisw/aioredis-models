from typing import Set
# Aliasing this to avoid confusion with the `set` function below.
from builtins import set as builtin_set
from aioredis import Redis
from .redis_key import RedisKey
from .redis_set import RedisSet


class RedisDoubleHash:
    def __init__(self, redis: Redis, key: str, inverse_key: str):
        self._redis = redis
        self._key = key
        self._inverse_key = inverse_key

    async def fields(self) -> Set:
        return builtin_set(
            self._extract_field_name(redis_key, self._key) \
                for redis_key in await self._fields_generic()
        )

    async def fields_inverted(self) -> Set:
        return builtin_set(
            self._extract_field_name(redis_key, self._inverse_key) \
                for redis_key in await self._fields_generic(inverse=True)
        )

    async def get(self, field: str) -> Set:
        return await self._get_field_value(self._key, field)

    async def get_inverted(self, field: str) -> Set:
        return await self._get_field_value(self._inverse_key, field)

    async def set(self, field: str, value: str):
        if value is None:
            return
        await self._set_field(self._key, field, value)
        await self._set_field(self._inverse_key, value, field)

    async def unset(self, field: str, value: str):
        if value is None:
            return
        await self._unset_field(self._key, field, value)
        await self._unset_field(self._inverse_key, value, field)

    async def set_inverted(self, field: str, value: str):
        if value is None:
            return
        return await self.set(field=value, value=field)

    async def remove(self, field: str):
        return await self._remove_generic(self._key, self._inverse_key, field)

    async def remove_inverted(self, field: str):
        return await self._remove_generic(self._inverse_key, self._key, field)

    async def delete(self):
        await self._sub_delete(False)
        await self._sub_delete(True)

    async def _fields_generic(self, inverse: bool=False) -> Set:
        return await self._redis.keys(self._get_field_name(
            self._inverse_key if inverse else self._key,
            '*'
        ), encoding='utf-8')

    async def _get_field_value(self, key: str, field: str) -> Set:
        sub_set = self._get_redis_set(key, field)
        return await sub_set.get_all()

    async def _set_field(self, key: str, field: str, value: str):
        sub_set = self._get_redis_set(key, field)
        return await sub_set.add(value)

    async def _unset_field(self, key: str, field: str, value: str):
        sub_set = self._get_redis_set(key, field)
        return await sub_set.remove(value)

    async def _sub_delete(self, inverse: bool):
        for key in await self._fields_generic(inverse):
            await RedisKey(self._redis, key).delete()

    async def _remove_generic(self, key: str, inverse_key: str, field: str):
        sub_set = self._get_redis_set(key, field)
        values = await sub_set.get_all()
        await sub_set.delete()
        for value in values:
            value_set = RedisSet(self._redis, self._get_field_name(inverse_key, value))
            await value_set.remove(field)

    def _get_redis_set(self, key: str, field: str) -> RedisSet:
        return RedisSet(self._redis, self._get_field_name(key, field))

    @staticmethod
    def _get_field_name(key: str, field: str) -> str:
        return f'{key}:{field}'

    @staticmethod
    def _extract_field_name(redis_key: str, key: str) -> str:
        return redis_key.removeprefix(key + ':')

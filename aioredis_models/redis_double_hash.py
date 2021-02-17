"""
This module contains the following classes:
- RedisDoubleHash: Represents a two-way hash map stored in Redis.
"""

from typing import Set
# Aliasing this to avoid confusion with the `set` function below.
from builtins import set as builtin_set
from aioredis import Redis
from .redis_key import RedisKey
from .redis_set import RedisSet


class RedisDoubleHash:
    """
    Represents a two-way hash map stored in Redis. Each field can be associated with multiple
    values and each value can be associated with multiple fields. The structure allows for
    getting all the values for a field and all the fields for a given value. The values are
    thus referred to as inverted fields.
    """

    def __init__(self, redis: Redis, key: str, inverse_key: str):
        """
        Creates an instance of `RedisDoubleHash`.

        Args:
            redis (Redis): The Redis instance to use to connect to Redis.
            key (str): The key to use for forward hash map.
            inverse_key (str): The key to use for inverted hash map.
        """

        self._redis = redis
        self._key = key
        self._inverse_key = inverse_key

    async def fields(self) -> Set:
        """
        Gets all the fields in the hash map.

        Returns:
            Set: The fields in the hash map.
        """

        return builtin_set(
            self._extract_field_name(redis_key, self._key) \
                for redis_key in await self._fields_generic()
        )

    async def fields_inverted(self) -> Set:
        """
        Gets all the inverted fields (values) in the hash map.

        Returns:
            Set: The inverted fields (values) in the hash map.
        """

        return builtin_set(
            self._extract_field_name(redis_key, self._inverse_key) \
                for redis_key in await self._fields_generic(inverse=True)
        )

    async def get(self, field: str) -> Set:
        """
        Gets the inverted fields associated with the given field.

        Args:
            field (str): The field to get.

        Returns:
            Set: The set of all inverted fields associated with the given field.
        """

        return await self._get_field_value(self._key, field)

    async def get_inverted(self, field: str) -> Set:
        """
        Gets the fields associated with the given inverted field (value).

        Args:
            field (str): The inverted field to get.

        Returns:
            Set: The set of all fields associated with the given inverted field.
        """

        return await self._get_field_value(self._inverse_key, field)

    async def set(self, field: str, value: str):
        """
        Associates the given value with the given field.

        Args:
            field (str): The name of the field.
            value (str): The value to associate.
        """

        if value is None:
            return
        await self._set_field(self._key, field, value)
        await self._set_field(self._inverse_key, value, field)

    async def unset(self, field: str, value: str):
        """
        Dissociates the given value with the given field.

        Args:
            field (str): The name of the field.
            value (str): The name of the value to dissociate.
        """

        if value is None:
            return
        await self._unset_field(self._key, field, value)
        await self._unset_field(self._inverse_key, value, field)

    async def set_inverted(self, field: str, value: str):
        """
        Associates the given value with the inverted field.

        Args:
            field (str): The name of the inverted field.
            value (str): The value to associate.
        """

        if value is None:
            return
        return await self.set(field=value, value=field)

    async def remove(self, field: str):
        """
        Removes the given field from both sides of the hash map.

        Args:
            field (str): The field to remove.
        """

        return await self._remove_generic(self._key, self._inverse_key, field)

    async def remove_inverted(self, field: str):
        """
        Removes the given inverted field from both sides of the hash map.

        Args:
            field (str): The inverted field to remove.
        """

        return await self._remove_generic(self._inverse_key, self._key, field)

    async def delete(self):
        """
        Deletes all mappings from both sides of the hash map.
        """

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

"""
This module contains the following classes:
- RedisHash: Represents a hash map stored in Redis.
"""

from typing import Set, Any, Awaitable, AsyncIterator
from .redis_key import RedisKey


class RedisHash(RedisKey):
    """
    Represents a hash map stored in Redis.
    """

    async def length(self) -> Awaitable[int]:
        """
        Gets the length of the hash map.

        Returns:
            Awaitable[int]: The length of the hash map.
        """

        return await self._redis.hlen(self._key)

    async def field_length(self, field: str) -> Awaitable[int]:
        """
        Gets the length of the given field.

        Args:
            field (str): The field to check.

        Returns:
            Awaitable[int]: The length of the given field.
        """

        return await self._redis.hstrlen(self._key, field)

    async def field_exists(self, field: str) -> Awaitable[bool]:
        """
        Checks whether the given field exists.

        Args:
            field (str): The field to check.

        Returns:
            Awaitable[bool]: Whether the field exists or not.
        """

        return await self._redis.hexists(self._key, field)

    async def fields(self, encoding='utf-8') -> Awaitable[Set]:
        """
        Gets all the fields in the hash map.

        Args:
            encoding (str, optional): The encoding to use for decoding the field keys. Defaults to
                'utf-8'.

        Returns:
            Awaitable[Set]: The set of fields in the hash map.
        """

        return set(await self._redis.hkeys(self._key, encoding=encoding))

    async def get_all(self, encoding='utf-8') -> Awaitable[dict]:
        """
        Gets the entire hash map.

        Args:
            encoding (str, optional): The encoding to use for decoding the keys and values.
                Defaults to 'utf-8'.

        Returns:
            Awaitable[dict]: The hash map.
        """

        return await self._redis.hgetall(self._key, encoding=encoding)

    async def get(self, field: str, encoding='utf-8') -> Awaitable[Any]:
        """
        Gets the value of the given field in the hash map.

        Args:
            field (str): The field to get.
            encoding (str, optional): The encoding to use for decoding the values. Defaults to
                'utf-8'.

        Returns:
            Awaitable[Any]: The value of the field.
        """

        return await self._redis.hget(self._key, field, encoding=encoding)

    async def enumerate(
        self,
        field_pattern: str=None,
        batch_size: int=None,
        encoding: str='utf-8'
    )-> AsyncIterator[Any]:
        """
        Enumerates over the items of the hash using HSCAN command.

        Args:
            field_pattern (str, optional): A string to filter fields with, if needed.
                Defaults to None.
            batch_size (int, optional): The maximum number of items to get with each scan.
                Defaults to None.

        Returns:
            AsyncIterator[Any]: An iterator that can be used to iterate over the result.
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = await self._redis.hscan(
                self._key, cursor=cursor, match=field_pattern, count=batch_size
            )
            for key, value in data:
                yield (key, value) if not encoding else (
                    key.decode(encoding),
                    value.decode(encoding)
                )

    async def set_all(self, values: dict):
        """
        Sets the entire hash map to the given `dict`.

        Args:
            values (dict): A `dict` containing the key/value map to set.
        """

        if values:
            return await self._redis.hmset_dict(self._key, values)

    async def set(self, field: str, value: str):
        """
        Set the value of the given field.

        Args:
            field (str): The field whose value is to be set.
            value (str): The value to set for the given field.
        """

        if value:
            return await self._redis.hset(self._key, field, value)

    async def remove(self, field: str) -> Awaitable[int]:
        """
        Removes the given field from the hash map.

        Args:
            field (str): The field to remove.

        Returns:
            Awaitable[int]: The number of field removed from the hash map.
        """

        return await self._redis.hdel(self._key, field)

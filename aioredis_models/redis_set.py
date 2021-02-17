"""
This module contains the following classes:
- RedisSet: Represents a set stored in Redis.
"""

from typing import Set
from .redis_key import RedisKey


class RedisSet(RedisKey):
    """
    Represents a set stored in Redis.
    """

    async def size(self) -> int:
        """
        Gets the size of the set.

        Returns:
            int: The size of the set.
        """

        return await self._redis.scard(self._key)

    async def get_all(self, encoding='utf-8') -> Set:
        """
        Gets all the members of the set.

        Args:
            encoding (str, optional): The encoding to use when decoding set members. Defaults to
                'utf-8'.

        Returns:
            Set: The members of the set.
        """

        return await self._redis.smembers(self._key, encoding=encoding)

    async def add(self, value: str) -> int:
        """
        Adds an item to the set.

        Args:
            value (str): The item to add.

        Returns:
            int: The number of items that were added to the set.
        """

        if value is not None:
            return await self._redis.sadd(self._key, value)

    async def remove(self, value: str) -> int:
        """
        Removes an item from the set.

        Args:
            value (str): The item to remove.

        Returns:
            int: The number of elements that were removed from the set.
        """

        return await self._redis.srem(self._key, value)

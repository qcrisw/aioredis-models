"""
This module contains the following classes:
- RedisSet: Represents a set stored in Redis.
"""

from typing import Awaitable, Set
from .redis_key import RedisKey
from .asyncio_utils import noop


class RedisSet(RedisKey):
    """
    Represents a set stored in Redis.
    """

    def size(self) -> Awaitable[int]:
        """
        Gets the size of the set.

        Returns:
            Awaitable[int]: The size of the set.
        """

        return self.get_connection().scard(self._key)

    def get_all(self, encoding='utf-8') -> Awaitable[Set]:
        """
        Gets all the members of the set.

        Args:
            encoding (str, optional): The encoding to use when decoding set members. Defaults to
                'utf-8'.

        Returns:
            Awaitable[Set]: The members of the set.
        """

        return self.get_connection().smembers(self._key, encoding=encoding)

    def add(self, value: str) -> Awaitable[int]:
        """
        Adds an item to the set.

        Args:
            value (str): The item to add.

        Returns:
            Awaitable[int]: The number of items that were added to the set.
        """

        return noop() if value is None else self.get_connection().sadd(self._key, value)

    def remove(self, value: str) -> Awaitable[int]:
        """
        Removes an item from the set.

        Args:
            value (str): The item to remove.

        Returns:
            Awaitable[int]: The number of elements that were removed from the set.
        """

        return self.get_connection().srem(self._key, value)

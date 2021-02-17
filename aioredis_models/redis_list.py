"""This module contains the following classes:
- RedisList: Represents a list stored in Redis.
"""

from functools import partial
from typing import List, Tuple, Any
from .redis_key import RedisKey


class RedisList(RedisKey):
    """
    Represents a list store in Redis.
    """

    async def length(self) -> int:
        """
        Gets the length of the list.

        Returns:
            int: The length of the list.
        """

        return await self._redis.llen(self._key)

    async def get_range(self, start: int=0, stop: int=-1, encoding='utf-8') -> List:
        """
        Gets the given sub-sequence of the list.

        Args:
            start (int, optional): The start index of the range get. Defaults to 0.
            stop (int, optional): The stop index of the range to get. Negative indices are offsets
                from the length of the sequence. Defaults to -1.
            encoding (str, optional): The encoding to use for decoding the values. Defaults
                to 'utf-8'.

        Returns:
            List: The retrieved range as a list.
        """

        return await self._redis.lrange(self._key, start, stop, encoding=encoding)

    async def push(self, *value: Tuple, reverse: bool=False) -> int:
        """
        Pushes the given values into the list.

        Args:
            value (Tuple): The values to push into the list.
            reverse (bool, optional): Whether to push the values at the end of the list. Defaults
                to `False`.

        Returns:
            int: The length of the list after the push operation.
        """

        value = list(filter(None, value))
        if not value:
            return
        func = self._redis.rpush if reverse else self._redis.lpush
        return await func(self._key, *value)

    async def pop(
        self,
        reverse: bool=False,
        block: bool=False,
        timeout_seconds: int=0,
        encoding='utf-8'
    ) -> Any:
        """
        Pops a value from the list.

        Args:
            reverse (bool, optional): Whether to pop the value from the end of
                the list. Defaults to `False`.
            block (bool, optional): Whether to block until an item is available to pop. Defaults to
                `False`.
            timeout_seconds (int, optional): The amount of time in seconds to wait before giving up.
                Defaults to 0, which indicates no timeout.
            encoding (str, optional): The encoding to use for decoding the popped value. Defaults
                to 'utf-8'.

        Returns:
            Any: The value popped from the list, if any.
        """

        if reverse and block:
            func = partial(self._redis.brpop, timeout=timeout_seconds)
        elif reverse and not block:
            func = self._redis.rpop
        elif block:
            func = partial(self._redis.blpop, timeout=timeout_seconds)
        else:
            func = self._redis.lpop

        return await func(self._key, encoding=encoding)

    async def move(
        self,
        destination_key: str,
        block: bool=False,
        timeout_seconds: int=0,
        encoding='utf-8'
    ) -> Any:
        """Moves a value from the end of one list to the beginning of another.

        Args:
            destination_key (str): The key of the list to move popped item to.
            block (bool, optional): Whether to block until an item is available to pop. Defaults to
                `False`.
            timeout_seconds (int, optional): The amount of time in seconds to wait before giving
                up. Defaults to 0.
            encoding (str, optional): The encoding to use for decoding the popped value. Defaults
                to 'utf-8'.

        Returns:
            Any: The value popped from the list, if any.
        """

        func = partial(
            self._redis.brpoplpush,
            timeout=timeout_seconds
        ) if block else self._redis.rpoplpush
        return await func(self._key, destination_key, encoding=encoding)

    async def requeue(self, block: bool=False, timeout_seconds: int=0, encoding='utf-8') -> Any:
        """
        Removes a value from the beginning of the list and pushes it to the end of the same list.

        Args:
            block (bool, optional): Whether to block until an item is available. Defaults to
                `False`.
            timeout_seconds (int, optional): The amount of time to wait before giving up. Defaults
                to 0.
            encoding (str, optional): The encoding to use for decoding the popped value. Defaults
                to 'utf-8'.

        Returns:
            Any: The value popped from the list, if any.
        """

        return await self.move(
            self._key,
            block=block,
            timeout_seconds=timeout_seconds,
            encoding=encoding
        )

    async def remove(self, value: str, count: int=0) -> int:
        """
        Removes occurrences of the given value from the list.

        Args:
            value (str): The value to remove.
            count (int, optional): The number of occurrences to remove. Defaults to 0, which
                removes all.

        Returns:
            int: The number of items that were removed.
        """

        return await self._redis.lrem(self._key, count, value)

    async def find_index(self, value: Any, start: int=0, stop: int=-1, encoding='utf-8') -> int:
        """
        Finds the index of the given value, if any.

        Args:
            value (Any): The value to look for.
            start (int, optional): The index to start looking from. Defaults to 0.
            stop (int, optional): The index to stop looking at. Negative indices are offsets from
                the length of the sequence. Defaults to -1.
            encoding (str, optional): The encoding to use for decoding values. Defaults to 'utf-8'.

        Returns:
            int: The index of the provided value. `None` if not found.
        """

        items = await self.get_range(start=start, stop=stop, encoding=encoding) or []
        try:
            return items.index(value) + start
        except ValueError:
            return None

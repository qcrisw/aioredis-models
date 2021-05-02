"""This module contains the following classes:
- RedisList: Represents a list stored in Redis.
"""

from functools import partial
from typing import List, Tuple, Any, Awaitable, AsyncIterator
from .redis_key import RedisKey
from .asyncio_utils import noop


class RedisList(RedisKey):
    """
    Represents a list store in Redis.
    """

    def length(self) -> Awaitable[int]:
        """
        Gets the length of the list.

        Returns:
            Awaitable[int]: The length of the list.
        """

        return self.get_connection().llen(self._key)

    def get_range(self, start: int=0, stop: int=-1, encoding='utf-8') -> Awaitable[List]:
        """
        Gets the given sub-sequence of the list.

        Args:
            start (int, optional): The start index of the range get. Defaults to 0.
            stop (int, optional): The stop index of the range to get. Negative indices are offsets
                from the length of the sequence. Defaults to -1.
            encoding (str, optional): The encoding to use for decoding the values. Defaults
                to 'utf-8'.

        Returns:
            Awaitable[List]: The retrieved range as a list.
        """

        return self.get_connection().lrange(self._key, start, stop, encoding=encoding)

    async def enumerate(
        self,
        start: int=0,
        stop: int=None,
        batch_size: int=0,
        encoding='utf-8'
    ) -> AsyncIterator[Any]:
        """
        Enumerates the items of this list in batches. This operation is not atomic and cannot be
        performed transactionally.

        Args:
            start (int, optional): The index to start from. Defaults to 0.
            stop (int, optional): The index to stop at. A value of None indicates no stop index.
                Defaults to None.
            batch_size (int, optional): The number of items to get in each batch.
                A value of 0 or None indicates a batch size equal to the full length of the list.
                Defaults to 0.
            encoding (str, optional): The encoding to use for the items. Defaults to 'utf-8'.

        Returns:
            AsyncIterator[Any]: An iterator that can be used to iterate over the result.
        """
        current_start = start
        while True:
            possible_stop = current_start + batch_size - 1
            current_stop = min(possible_stop if stop is None else stop, possible_stop) \
                if batch_size else (-1 if stop is None else stop)
            len_result = 0
            for item in await self.get_range(current_start, current_stop, encoding=encoding):
                len_result += 1
                yield item
            current_start = current_start + len_result
            if current_stop == -1 or \
                current_start <= current_stop or \
                (stop is not None and current_stop >= stop):
                break

    def push(self, *value: Tuple, reverse: bool=False) -> Awaitable[int]:
        """
        Pushes the given values into the list.

        Args:
            value (Tuple): The values to push into the list.
            reverse (bool, optional): Whether to push the values at the end of the list. Defaults
                to `False`.

        Returns:
            Awaitable[int]: The length of the list after the push operation.
        """

        value = list(filter(None, value))
        if not value:
            return noop()
        func = self.get_connection().rpush if reverse else self.get_connection().lpush
        return func(self._key, *value)

    def pop(
        self,
        reverse: bool=False,
        block: bool=False,
        timeout_seconds: int=0,
        encoding='utf-8'
    ) -> Awaitable[Any]:
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
            Awaitable[Any]: The value popped from the list, if any.
        """

        if reverse and block:
            func = partial(self.get_connection().brpop, timeout=timeout_seconds)
        elif reverse:
            func = self.get_connection().rpop
        elif block:
            func = partial(self.get_connection().blpop, timeout=timeout_seconds)
        else:
            func = self.get_connection().lpop

        return func(self._key, encoding=encoding)

    def enqueue(self, *value: Tuple) -> Awaitable[int]:
        """
        Enqueues the given values into the list.

        Args:
            value (Tuple): The values to enqueue.

        Returns:
            Awaitable[int]: The length of the list after the push operation.
        """

        return self.push(*value)

    def dequeue(
        self,
        block: bool=False,
        timeout_seconds: int=0,
        encoding='utf-8'
    ) -> Awaitable[Any]:
        """
        Dequeues an item from the list.

        Args:
            block (bool, optional): Whether to block until an item is available to dequeue.
                Defaults to `False`.
            timeout_seconds (int, optional): The amount of time in seconds to wait before giving
                up. Defaults to 0, which indicates no timeout.
            encoding (str, optional): The encoding to use for decoding the dequeued value.
                Defaults to 'utf-8'.

        Returns:
            Awaitable[Any]: The value dequeued from the list, if any.
        """

        return self.pop(
            reverse=True,
            block=block,
            timeout_seconds=timeout_seconds,
            encoding=encoding
        )

    def move(
        self,
        destination_key: str,
        block: bool=False,
        timeout_seconds: int=0,
        encoding='utf-8'
    ) -> Awaitable[Any]:
        """
        Moves a value from the end of one list to the beginning of another.

        Args:
            destination_key (str): The key of the list to move popped item to.
            block (bool, optional): Whether to block until an item is available to pop. Defaults to
                `False`.
            timeout_seconds (int, optional): The amount of time in seconds to wait before giving
                up. Defaults to 0.
            encoding (str, optional): The encoding to use for decoding the popped value. Defaults
                to 'utf-8'.

        Returns:
            Awaitable[Any]: The value popped from the list, if any.
        """

        func = partial(
            self.get_connection().brpoplpush,
            timeout=timeout_seconds
        ) if block else self.get_connection().rpoplpush
        return func(self._key, destination_key, encoding=encoding)

    def requeue(
        self,
        block: bool=False,
        timeout_seconds: int=0,
        encoding='utf-8'
    ) -> Awaitable[Any]:
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
            Awaitable[Any]: The value popped from the list, if any.
        """

        return self.move(
            self._key,
            block=block,
            timeout_seconds=timeout_seconds,
            encoding=encoding
        )

    def remove(self, value: str, count: int=0) -> Awaitable[int]:
        """
        Removes occurrences of the given value from the list.

        Args:
            value (str): The value to remove.
            count (int, optional): The number of occurrences to remove. Defaults to 0, which
                removes all.

        Returns:
            Awaitable[int]: The number of items that were removed.
        """

        return self.get_connection().lrem(self._key, count, value)

    async def find_index(
        self,
        value: Any,
        start: int=0,
        stop: int=-1,
        batch_size: int=0,
        encoding='utf-8',
    ) -> int:
        """
        Finds the index of the given value, if any. This operation is not atomic and cannot be
        performed transactionally.

        Args:
            value (Any): The value to look for.
            start (int, optional): The index to start looking from. Defaults to 0.
            stop (int, optional): The index to stop at. A value of None indicates no stop index.
                Defaults to None.
            batch_size (int, optional): The number of items to get in each batch.
                A value of 0 or None indicates a batch size equal to the full length of the list.
                Defaults to 0.
            encoding (str, optional): The encoding to use for decoding values. Defaults to 'utf-8'.

        Returns:
            Awaitable[int]: The index of the provided value. `None` if not found.
        """

        index = start
        async for item in self.enumerate(
            start=start,
            stop=stop,
            batch_size=batch_size,
            encoding=encoding
        ):
            if item == value:
                return index
            index += 1
        return None

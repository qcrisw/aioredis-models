"""
This module contains the following classes:
- RedisTransaction: Represents a Redis transaction.
"""

from typing import Any, Callable, List, Tuple
from asyncio import Future, gather


class RedisTransaction:
    """
    Represents a Redis transaction that can also be used as a context manager.
    """

    _tasks: List[Future]
    _result_callback: Callable[[Tuple[Any, ...]], Any]

    def __init__(self, redis_client):
        """
        Creates an instance of `RedisTransaction`.

        Args:
            redis_client (RedisClient): The instance to use for connecting to Redis.
        """
        self._redis_client = redis_client
        self._tasks = []
        self._result_callback = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        if not exc:
            await self._redis_client.execute_transaction()
            result = await gather(*self._tasks)
            if self._result_callback:
                self._result_callback(*result)
        else:
            self._redis_client.discard_transaction()
            for task in self._tasks:
                task.cancel()

    def add_operation(self, *tasks: Future):
        """
        Adds the given operation(s) to the transaction.

        Args:
            tasks (tuple[Future, ...]): The tasks to add to the transaction. Should be Redis
            operations that support transactions.
        """
        self._tasks.extend(tasks)

    def set_result_callback(self, callback: Callable[[Tuple[Any, ...]], Any]):
        """
        Sets a callback function to be called with the result of the transaction when it completes.

        Args:
            callback (Callable[[Tuple[Any, ...]], Any]): The function to call. Accepts positional
                arguments containing results of operations in the same order as they were added
                to the transaction using calls to `add_operation`.
        """
        self._result_callback = callback

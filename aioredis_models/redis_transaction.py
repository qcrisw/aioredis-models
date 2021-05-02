"""
This module contains the following classes:
- RedisTransaction: Represents a Redis transaction.
"""

from typing import List
from asyncio import Future, gather


class RedisTransaction:
    """
    Represents a Redis transaction that can also be used as a context manager.
    """

    _tasks: List[Future]

    def __init__(self, redis_client):
        """
        Creates an instance of `RedisTransaction`.

        Args:
            redis_client (RedisClient): The instance to use for connecting to Redis.
        """
        self._redis_client = redis_client
        self._tasks = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        if not exc:
            await self._redis_client.execute_transaction()
            await gather(*self._tasks)
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

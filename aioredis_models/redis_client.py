"""
This module contains the following classes:
- RedisClient: A class that wraps the Redis client from aioredis and simplifies support for
    transactions.
"""

from typing import Awaitable, List, Union
from aioredis import Redis
from aioredis.commands import MultiExec
from .redis_transaction import RedisTransaction


class RedisClient:
    """
    A class that wraps the Redis client from aioredis and simplifies support for transactions.
    """
    _multi_exec: MultiExec = None

    def __init__(self, redis: Redis):
        """
        Creates a new instance of `RedisClient`.

        Args:
            redis (Redis): The Redis instance to use to connect to Redis.
        """
        self._redis = redis

    def get_connection(self) -> Union[Redis, MultiExec]:  # pylint:disable=unsubscriptable-object
        """
        Gets the Redis connection currently in use.

        Returns:
            Union[Redis, MultiExec]: The Redis instance to use to connect to Redis. This can be
                a `MultiExec` instance if a transaction is in progress.
        """
        return self._multi_exec or self._redis

    def is_in_transaction(self) -> bool:
        """
        Returns a value indicating whether a transaction is in progress.

        Returns:
            bool: Whether a transaction is in progress.
        """
        return self._multi_exec is not None

    def begin_transaction(self) -> RedisTransaction:
        """
        Begins a transaction.

        Returns:
            RedisTransaction: A `RedisTransaction` instance that can be used to interact with the
                transaction.
        """
        assert not self.is_in_transaction()

        self._multi_exec = self._redis.multi_exec()
        return RedisTransaction(self)

    def discard_transaction(self):
        """
        Discards the current transaction. The client is responsible for canceling pending tasks.
        """

        assert self._multi_exec
        self._multi_exec = None

    def execute_transaction(self) -> Awaitable[List]:
        """
        Executes the transaction. The client is responsible for awaiting pending tasks.

        Returns:
            Awaitable[List]: The result of the transaction operations.
        """

        assert self._multi_exec

        result = self._multi_exec.execute()
        self._multi_exec = None
        return result

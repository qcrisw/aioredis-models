"""
This module contains the following classes:
- RedisModel: Base class for all Redis models.
"""

from typing import Union
from aioredis import Redis
from .redis_client import RedisClient


class RedisModel:
    """
    Base class for all Redis models.
    """

    def __init__(
        self,
        redis: Union[Redis, RedisClient]  # pylint:disable=unsubscriptable-object
    ):
        """
        Creates a new instance of `RedisModel`.

        Args:
            redis (Union[Redis, RedisClient]): The Redis instance to use to connect to Redis.
                This can be an instance of `RedisClient` to allow controlling transactions
                externally.
        """

        self._redis =  redis \
            if isinstance(redis, RedisClient) else RedisClient(redis)

        self.begin_transaction = self._redis.begin_transaction
        self.is_in_transaction = self._redis.is_in_transaction
        self.discard_transaction = self._redis.discard_transaction
        self.execute_transaction = self._redis.execute_transaction
        self.get_connection = self._redis.get_connection

"""
This module contains the following classes:
- RedisKey: represents a generic Redis key.
"""

from typing import Awaitable, Union
from aioredis import Redis
from .redis_model import RedisModel
from .redis_client import RedisClient


class RedisKey(RedisModel):
    """
    Represents a Redis key of any type. Acts as the class for all data structures.
    """

    def __init__(
        self,
        redis: Union[Redis, RedisClient],  # pylint:disable=unsubscriptable-object
        key: str
    ):
        """
        Creates an instance ot `RedisKey`.

        Args:
            redis (Union[Redis, RedisClient]): The Redis instance to use to connect to Redis.
                This can be an instance of `RedisClient` to allow controlling transactions
                externally.
            key (str): The Redis key to use.
        """

        super().__init__(redis)
        self._key = key

    def delete(self) -> Awaitable[int]:
        """
        Deletes the key from Redis.

        Returns:
            Awaitable[int]: The number of items that were deleted.
        """

        return self.get_connection().delete(self._key)

    async def exists(self) -> Awaitable[bool]:
        """
        Checks if the key exists in Redis or not. This operation cannot be performed
        transactionally.

        Returns:
            Awaitable[bool]: A flag indicating whether the key exists or not.
        """

        return await self.get_connection().exists(self._key) > 0

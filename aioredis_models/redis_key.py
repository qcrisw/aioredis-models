"""
This module contains the following classes:
- RedisKey: represents a generic Redis key.
"""

from aioredis import Redis


class RedisKey:
    """
    Represents a Redis key of any type. Acts as the class for all data structures.
    """

    def __init__(self, redis: Redis, key: str):
        """
        Creates an instance ot `RedisKey`.

        Args:
            redis (Redis): The Redis instance to use to connect to Redis.
            key (str): The Redis key to use.
        """

        self._redis = redis
        self._key = key

    async def delete(self):
        """
        Deletes the key from Redis.
        """

        return await self._redis.delete(self._key)

    async def exists(self) -> bool:
        """
        Checks if the key exists in Redis or not.

        Returns:
            bool: A flag indicating whether the key exists or not.
        """

        return await self._redis.exists(self._key) > 0

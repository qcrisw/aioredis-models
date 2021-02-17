"""
This module contains the following classes:
- RedisString: Represents a string stored in Redis.
"""

from typing import Union
from .redis_key import RedisKey


class RedisString(RedisKey):
    """
    Represents a string stored in Redis.
    """

    async def length(self) -> int:
        """
        Gets the length of the string.

        Returns:
            int: The length of the string.
        """

        return await self._redis.strlen(self._key)

    async def get(self) -> str:
        """
        Gets the stored value of the string.

        Returns:
            str: The value of the string.
        """

        return await self._redis.get(self._key)

    async def set(
        self,
        value: Union[str, int, float],  # pylint:disable=unsubscriptable-object
        timeout_seconds: Union[int, float]=None,  # pylint:disable=unsubscriptable-object
        if_exists_equals: bool=None
    ):
        """
        Sets the string to a given value.

        Args:
            value (Union[str, int, float]): The value to set.
            timeout_seconds (Union[int, float], optional): The amount of time in seconds after which
                the key should expire. Defaults to `None`.
            if_exists_equals (bool, optional): If `True`, will only set this value if key already
                exists;
                if `False`, will only set this value if key does not already exist;
                if `None`, the value will always be set regardless of whether it already exists or
                not. Defaults to `None`.
        """

        if if_exists_equals is True:
            exist = 'SET_IF_EXIST'
        elif if_exists_equals is False:
            exist = 'SET_IF_NOT_EXIST'
        else:
            exist = None
        return await self._redis.set(
            self._key,
            value,
            pexpire=round(timeout_seconds * 1000) if timeout_seconds else None,
            exist=exist
        )

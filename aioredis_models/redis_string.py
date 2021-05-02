"""
This module contains the following classes:
- RedisString: Represents a string stored in Redis.
"""

from typing import Awaitable, Union
from .redis_key import RedisKey


class RedisString(RedisKey):
    """
    Represents a string stored in Redis.
    """

    def length(self) -> Awaitable[int]:
        """
        Gets the length of the string.

        Returns:
            Awaitable[int]: The length of the string.
        """

        return self.get_connection().strlen(self._key)

    def get(self, encoding='utf-8') -> Awaitable[str]:
        """
        Gets the stored value of the string.

        Returns:
            Awaitable[str]: The value of the string.
        """

        return self.get_connection().get(self._key, encoding=encoding)

    def set(
        self,
        value: Union[str, int, float],  # pylint:disable=unsubscriptable-object
        timeout_seconds: Union[int, float]=None,  # pylint:disable=unsubscriptable-object
        if_exists_equals: bool=None
    ) -> Awaitable:
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

        Returns:
            Awaitable: The result of the operation.
        """

        if if_exists_equals is True:
            exist = 'SET_IF_EXIST'
        elif if_exists_equals is False:
            exist = 'SET_IF_NOT_EXIST'
        else:
            exist = None
        return self.get_connection().set(
            self._key,
            value,
            pexpire=round(timeout_seconds * 1000) if timeout_seconds else None,
            exist=exist
        )

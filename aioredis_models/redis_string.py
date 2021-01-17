from typing import Union, Any
from .redis_key import RedisKey


class RedisString(RedisKey):
    async def get(self) -> Any:
        return await self._redis.get(self._key)

    async def set(
        self,
        value: Union[str, int, float],  # pylint:disable=unsubscriptable-object
        timeout_seconds: Union[int, float]=None,  # pylint:disable=unsubscriptable-object
        if_exists_equals: bool=None
    ):
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

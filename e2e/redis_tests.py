from os import environ as env
import unittest
from aioredis import Redis, create_redis_pool

class RedisTests(unittest.IsolatedAsyncioTestCase):
    _redis: Redis = None

    async def asyncSetUp(self):
        self._redis = await create_redis_pool(env['REDIS_URL'])

    async def asyncTearDown(self):
        self._redis.close()
        await self._redis.wait_closed()

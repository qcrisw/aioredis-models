from aioredis_models import RedisList
from .redis_tests import RedisTests

class RedisListTests(RedisTests):
    _key = 'test-key'
    _redis_list: RedisList = None

    async def asyncSetUp(self):
        await super().asyncSetUp()

        self._redis_list = RedisList(self._redis, self._key)
        await self._redis_list.delete()

    async def test_enumerate_gets_result(self):
        values = ['test', 'this', 'now', 'here', '!']
        await self._redis_list.push(*values, reverse=True)

        result = [item async for item in self._redis_list.enumerate(batch_size=2)]

        self.assertEqual(result, values)

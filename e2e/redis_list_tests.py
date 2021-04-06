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

    async def test_find_index_with_start_stop_batch_size_finds_result(self):
        values = ['this', 'is', 'a', 'very', 'valuable', 'test']
        await self._redis_list.push(*values, reverse=True)

        result = await self._redis_list.find_index('very', batch_size=2)

        self.assertEqual(result, values.index('very'))

    async def test_find_index_with_start_stop_batch_size_does_not_find_result_not_in_range(self):
        values = ['this', 'is', 'a', 'very', 'valuable', 'test']
        await self._redis_list.push(*values, reverse=True)

        result = await self._redis_list.find_index('valuable', start=1, stop=3, batch_size=2)

        self.assertIsNone(result)

    async def test_find_index_with_no_stop_finds_result(self):
        values = ['this', 'is', 'a', 'very', 'valuable', 'test']
        await self._redis_list.push(*values, reverse=True)

        result = await self._redis_list.find_index('valuable', start=1, batch_size=2)

        self.assertEqual(result, values.index('valuable'))

    async def test_find_index_with_no_stop_does_not_find_missing_result(self):
        values = ['this', 'is', 'a', 'very', 'valuable', 'test']
        await self._redis_list.push(*values, reverse=True)

        result = await self._redis_list.find_index('code', start=1, batch_size=2)

        self.assertIsNone(result)

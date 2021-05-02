from aioredis_models import RedisHash
from .redis_tests import RedisTests

class RedisHashTests(RedisTests):
    _key = 'test-key'
    _redis_hash: RedisHash = None

    async def asyncSetUp(self):
        await super().asyncSetUp()

        self._redis_hash = RedisHash(self._redis, self._key)
        await self._redis_hash.delete()

    async def test_enumerate_gets_result(self):
        values = {
            'foo': 'bar',
            'baz': 'bat',
            'boo': 'hoo',
            'snow': 'ball',
            'ball': 'crawl'
        }
        await self._redis_hash.set_all(values)

        result = {item async for item in self._redis_hash.enumerate(batch_size=2)}

        self.assertEqual(result, set(values.items()))

    async def test_fields_gets_fields(self):
        values = {
            'foo': 'bar',
            'baz': 'bat',
            'boo': 'hoo',
            'snow': 'ball',
            'ball': 'crawl'
        }
        await self._redis_hash.set_all(values)

        result = await self._redis_hash.fields()

        self.assertEqual(result, list(values.keys()))

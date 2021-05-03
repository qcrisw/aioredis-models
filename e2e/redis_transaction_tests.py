from typing import List
from aioredis_models import RedisList, RedisString, RedisKey, RedisClient
from .redis_tests import RedisTests


class RedisTransactionTests(RedisTests):
    _redis_keys: List[RedisKey] = []

    def add_redis_item(self, redis_key: RedisKey):
        self._redis_keys.append(redis_key)
        return redis_key

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self._redis_keys = []

    async def asyncTearDown(self):
        for redis_key in self._redis_keys:
            await redis_key.delete()
        await super().asyncTearDown()

    async def test_simple_transaction(self):
        key = 'something'
        redis_list = self.add_redis_item(RedisList(self._redis, key))

        tx_result = []
        async with redis_list.begin_transaction() as transaction:
            transaction.add_operation(
                redis_list.enqueue('foo'),
                redis_list.get_range(),
                redis_list.enqueue('bar'),
                redis_list.length()
            )
            transaction.set_result_callback(lambda *args: tx_result.extend(args))

        result = await redis_list.get_range()

        self.assertEqual(result, ['bar', 'foo'])
        self.assertEqual(tx_result, [1, ['foo'], 2, 2])

    async def test_empty_transaction(self):
        key1 = 'something'
        redis_str = self.add_redis_item(RedisString(self._redis, key1))

        async with redis_str.begin_transaction():
            pass

    async def test_external_transaction(self):
        key1 = 'something'
        key2 = 'other-thing'
        redis_client = RedisClient(self._redis)
        redis_list = self.add_redis_item(RedisList(redis_client, key1))
        redis_str = self.add_redis_item(RedisString(redis_client, key2))

        async with redis_client.begin_transaction() as transaction:
            transaction.add_operation(
                redis_list.enqueue('test'),
                redis_str.set('special-value'),
                redis_list.enqueue('me')
            )

        items = await redis_list.get_range()
        value = await redis_str.get()

        self.assertEqual(items, ['me', 'test'])
        self.assertEqual(value, 'special-value')

    async def test_discard_transaction(self):
        key = 'something'
        redis_list = self.add_redis_item(RedisList(self._redis, key))

        try:
            async with redis_list.begin_transaction() as transaction:
                transaction.add_operation(
                    redis_list.enqueue('foo'),
                    redis_list.enqueue('bar')
                )
                raise Exception()
        except Exception:
            pass

        await redis_list.enqueue('outstanding', 'task')

        result = await redis_list.get_range()

        self.assertEqual(result, ['task', 'outstanding'])

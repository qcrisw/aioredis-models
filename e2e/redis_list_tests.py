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

    async def test_enqueue_pushes_values_to_beginning_of_list(self):
        values = ['these', 'are', 'some', 'values']

        result = await self._redis_list.enqueue(*values[0:3])

        self.assertEqual(3, result)
        self.assertEqual(values[2::-1], await self._redis_list.get_range())

        result = await self._redis_list.enqueue(values[3])

        self.assertEqual(4, result)
        self.assertEqual(values[3:] + values[2::-1], await self._redis_list.get_range())

    async def test_dequeue_dequeues_value_from_end(self):
        values = ['these', 'are', 'some', 'values']
        await self._redis_list.enqueue(*values)

        for i in range(len(values)):
            result = await self._redis_list.dequeue()

            self.assertEqual(values[i], result)

    async def test_transaction_performs_operation(self):
        values = ['foo', 'bar']
        async with self._redis_list.begin_transaction() as transaction:
            transaction.add_operation(*(
                self._redis_list.push(value) for value in values
            ))

        result = await self._redis_list.get_range()

        self.assertEqual(result, values[::-1])

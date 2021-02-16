import unittest
from unittest.mock import MagicMock, AsyncMock
from aioredis_models.redis_list import RedisList


class RedisListTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_list = RedisList(MagicMock(), MagicMock())

        self.assertIsInstance(redis_list, RedisList)

    async def test_length_returns_length(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.length()

        redis.llen.assert_called_once_with(key)
        self.assertEqual(result, redis.llen.return_value)

    async def test_get_range_passes_correct_defaults(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.get_range()

        redis.lrange.assert_awaited_once_with(key, 0, -1, encoding='utf-8')
        self.assertEqual(result, redis.lrange.return_value)

    async def test_get_range_works_correctly(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        start = MagicMock()
        stop = MagicMock()
        encoding = MagicMock()

        result = await redis_list.get_range(start, stop, encoding=encoding)

        redis.lrange.assert_awaited_once_with(key, start, stop, encoding=encoding)
        self.assertEqual(result, redis.lrange.return_value)

    async def test_push_with_none_value_does_nothing(self):
        key = MagicMock()
        redis_list = RedisList(None, key)

        result = await redis_list.push(None)

        self.assertIsNone(result)

    async def test_push_with_none_value_reverse_does_nothing(self):
        key = MagicMock()
        redis_list = RedisList(None, key)

        result = await redis_list.push(None, reverse=True)

        self.assertIsNone(result)

    async def test_push_with_values_lpushes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        values = [MagicMock() for _ in range(8)]

        result = await redis_list.push(*values)

        redis.lpush.assert_awaited_once_with(key, *values)
        self.assertEqual(result, redis.lpush.return_value)

    async def test_push_with_values_and_reverse_rpushes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        values = [MagicMock() for _ in range(8)]

        result = await redis_list.push(*values, reverse=True)

        redis.rpush.assert_awaited_once_with(key, *values)
        self.assertEqual(result, redis.rpush.return_value)

    async def test_pop_with_reverse_and_block_brpops(self):
        redis = AsyncMock()
        key = MagicMock()
        timeout = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(reverse=True, block=True, timeout=timeout, encoding=encoding)

        redis.brpop.assert_awaited_once_with(key, timeout=timeout, encoding=encoding)
        self.assertEqual(result, redis.brpop.return_value)

    async def test_pop_with_reverse_and_block_and_default_timeout_brpops_with_zero_timeout(self):
        redis = AsyncMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(reverse=True, block=True, encoding=encoding)

        redis.brpop.assert_awaited_once_with(key, timeout=0, encoding=encoding)
        self.assertEqual(result, redis.brpop.return_value)

    async def test_pop_with_reverse_default_no_block_rpops(self):
        redis = AsyncMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(reverse=True, encoding=encoding)

        redis.rpop.assert_awaited_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.rpop.return_value)

    async def test_pop_with_reverse_not_block_rpops(self):
        redis = AsyncMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(reverse=True, block=False, encoding=encoding)

        redis.rpop.assert_awaited_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.rpop.return_value)

    async def test_pop_with_reverse_and_block_blpops(self):
        redis = AsyncMock()
        key = MagicMock()
        timeout = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(block=True, timeout=timeout, encoding=encoding)

        redis.blpop.assert_awaited_once_with(key, timeout=timeout, encoding=encoding)
        self.assertEqual(result, redis.blpop.return_value)

    async def test_pop_with_reverse_and_block_and_default_timeout_blpops_with_zero_timeout(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(block=True)

        redis.blpop.assert_awaited_once_with(key, timeout=0, encoding='utf-8')
        self.assertEqual(result, redis.blpop.return_value)

    async def test_pop_with_reverse_default_no_block_lpops(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop()

        redis.lpop.assert_awaited_once_with(key, encoding='utf-8')
        self.assertEqual(result, redis.lpop.return_value)

    async def test_pop_with_reverse_not_block_lpops(self):
        redis = AsyncMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.pop(block=False, encoding=encoding)

        redis.lpop.assert_awaited_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.lpop.return_value)

    async def test_move_with_block_brpoplpushes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        destination_key = MagicMock()
        timeout = MagicMock()
        encoding = MagicMock()

        result = await redis_list.move(destination_key, block=True, timeout=timeout, encoding=encoding)

        redis.brpoplpush.assert_awaited_once_with(key, destination_key, timeout=timeout, encoding=encoding)
        self.assertEqual(result, redis.brpoplpush.return_value)

    async def test_move_with_block_default_timeout_brpoplpushes_with_default_timeout(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        destination_key = MagicMock()

        result = await redis_list.move(destination_key, block=True)

        redis.brpoplpush.assert_awaited_once_with(key, destination_key, timeout=0, encoding='utf-8')
        self.assertEqual(result, redis.brpoplpush.return_value)

    async def test_move_without_block_rpoplpushes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        destination_key = MagicMock()
        encoding = MagicMock()

        result = await redis_list.move(destination_key, block=False, encoding=encoding)

        redis.rpoplpush.assert_awaited_once_with(key, destination_key, encoding=encoding)
        self.assertEqual(result, redis.rpoplpush.return_value)

    async def test_requeue_with_block_brpoplpushes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        timeout = MagicMock()
        encoding = MagicMock()

        result = await redis_list.requeue(block=True, timeout=timeout, encoding=encoding)

        redis.brpoplpush.assert_awaited_once_with(key, key, timeout=timeout, encoding=encoding)
        self.assertEqual(result, redis.brpoplpush.return_value)

    async def test_requeue_with_block_default_timeout_brpoplpushes_with_default_timeout(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.requeue(block=True)

        redis.brpoplpush.assert_awaited_once_with(key, key, timeout=0, encoding='utf-8')
        self.assertEqual(result, redis.brpoplpush.return_value)

    async def test_requeue_without_block_rpoplpushes(self):
        redis = AsyncMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.requeue(block=False, encoding=encoding)

        redis.rpoplpush.assert_awaited_once_with(key, key, encoding=encoding)
        self.assertEqual(result, redis.rpoplpush.return_value)

    async def test_remove_removes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        value = MagicMock()
        count = MagicMock()

        result = await redis_list.remove(value, count)

        redis.lrem.assert_called_once_with(key, count, value)
        self.assertEqual(result, redis.lrem.return_value)

    async def test_remove_with_no_count_removes_with_zero_count(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_list = RedisList(redis, key)
        value = MagicMock()

        result = await redis_list.remove(value)

        redis.lrem.assert_called_once_with(key, 0, value)
        self.assertEqual(result, redis.lrem.return_value)

    async def test_find_index_uses_correct_defaults(self):
        redis = AsyncMock()
        redis.lrange.return_value = ['test', 'this', 'for', 'me']
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.find_index('this')

        redis.lrange.assert_awaited_once_with(key, 0, -1, encoding='utf-8')
        self.assertEqual(result, 1)

    async def test_find_index_when_value_present_returns_index(self):
        redis = AsyncMock()
        redis.lrange.return_value = ['test', 'this', 'for', 'me']
        key = MagicMock()
        redis_list = RedisList(redis, key)
        start = 0
        stop = MagicMock()
        encoding = MagicMock()

        result = await redis_list.find_index('for', start=start, stop=stop, encoding=encoding)

        redis.lrange.assert_awaited_once_with(key, start, stop, encoding=encoding)
        self.assertEqual(result, 2)

    async def test_find_index_when_value_not_present_returns_none(self):
        redis = AsyncMock()
        redis.lrange.return_value = ['test', 'this']
        key = MagicMock()
        redis_list = RedisList(redis, key)

        result = await redis_list.find_index('me')

        self.assertIsNone(result)

    async def test_find_index_with_non_zero_start_adds_start_to_index(self):
        redis = AsyncMock()
        redis.lrange.return_value = ['test', 'this', 'for', 'me']
        key = MagicMock()
        redis_list = RedisList(redis, key)
        start = 5

        result = await redis_list.find_index('me', start=start)

        self.assertEqual(result, 8)

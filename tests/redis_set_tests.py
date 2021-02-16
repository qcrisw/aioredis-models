import unittest
from unittest.mock import MagicMock, AsyncMock
from aioredis_models.redis_set import RedisSet


class RedisSetTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_list = RedisSet(MagicMock(), MagicMock())

        self.assertIsInstance(redis_list, RedisSet)

    async def test_size_returns_size(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)

        result = await redis_set.size()

        redis.scard.assert_called_once_with(key)
        self.assertEqual(result, redis.scard.return_value)

    async def test_get_all_gets_all(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)

        result = await redis_set.get_all()

        redis.smembers.assert_awaited_once_with(key, encoding='utf-8')
        self.assertEqual(result, redis.smembers.return_value)

    async def test_get_all_with_encoding_uses_encoding(self):
        redis = AsyncMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_set = RedisSet(redis, key)

        result = await redis_set.get_all(encoding=encoding)

        redis.smembers.assert_awaited_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.smembers.return_value)

    async def test_add_adds(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)
        value = MagicMock()

        result = await redis_set.add(value)

        redis.sadd.assert_awaited_once_with(key, value)
        self.assertEqual(result, redis.sadd.return_value)

    async def test_add_with_none_value_does_not_add(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)

        result = await redis_set.add(None)

        redis.sadd.assert_not_awaited()
        self.assertIsNone(result)

    async def test_remove_removes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)
        value = MagicMock()

        result = await redis_set.remove(value)

        redis.srem.assert_awaited_once_with(key, value)
        self.assertEqual(result, redis.srem.return_value)

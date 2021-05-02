import unittest
from unittest.mock import MagicMock, AsyncMock
from aioredis_models.redis_set import RedisSet


class RedisSetTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_list = RedisSet(MagicMock(), MagicMock())

        self.assertIsInstance(redis_list, RedisSet)

    def test_size_returns_size(self):
        redis = MagicMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)

        result = redis_set.size()

        redis.scard.assert_called_once_with(key)
        self.assertEqual(result, redis.scard.return_value)

    def test_get_all_gets_all(self):
        redis = MagicMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)

        result = redis_set.get_all()

        redis.smembers.assert_called_once_with(key, encoding='utf-8')
        self.assertEqual(result, redis.smembers.return_value)

    def test_get_all_with_encoding_uses_encoding(self):
        redis = MagicMock()
        key = MagicMock()
        encoding = MagicMock()
        redis_set = RedisSet(redis, key)

        result = redis_set.get_all(encoding=encoding)

        redis.smembers.assert_called_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.smembers.return_value)

    def test_add_adds(self):
        redis = MagicMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)
        value = MagicMock()

        result = redis_set.add(value)

        redis.sadd.assert_called_once_with(key, value)
        self.assertEqual(result, redis.sadd.return_value)

    async def test_add_with_none_value_does_not_add(self):
        redis = AsyncMock()
        redis.sadd = None
        key = MagicMock()
        redis_set = RedisSet(redis, key)

        result = await redis_set.add(None)

        self.assertIsNone(result)

    def test_remove_removes(self):
        redis = MagicMock()
        key = MagicMock()
        redis_set = RedisSet(redis, key)
        value = MagicMock()

        result = redis_set.remove(value)

        redis.srem.assert_called_once_with(key, value)
        self.assertEqual(result, redis.srem.return_value)

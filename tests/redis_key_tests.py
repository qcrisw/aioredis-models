import unittest
from unittest.mock import MagicMock, AsyncMock
from aioredis_models.redis_key import RedisKey


class RedisKeyTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_key = RedisKey(MagicMock(), MagicMock())

        self.assertIsInstance(redis_key, RedisKey)

    async def test_delete_deletes(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_key = RedisKey(redis, key)

        result = await redis_key.delete()

        redis.delete.assert_awaited_once_with(key)
        self.assertEqual(result, redis.delete.return_value)

    async def test_exists_when_key_in_redis_returns_true(self):
        redis = AsyncMock()
        redis.exists.return_value = 1
        key = MagicMock()
        redis_key = RedisKey(redis, key)

        result = await redis_key.exists()

        redis.exists.assert_awaited_once_with(key)
        self.assertTrue(result)

    async def test_exists_when_key_not_in_redis_returns_false(self):
        redis = AsyncMock()
        redis.exists.return_value = 0
        key = MagicMock()
        redis_key = RedisKey(redis, key)

        result = await redis_key.exists()

        redis.exists.assert_awaited_once_with(key)
        self.assertFalse(result)

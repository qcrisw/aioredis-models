import unittest
from unittest.mock import MagicMock, AsyncMock
from aioredis_models.redis_string import RedisString


class RedisStringTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_string = RedisString(MagicMock(), MagicMock())

        self.assertIsInstance(redis_string, RedisString)

    async def test_length_returns_length(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_string = RedisString(redis, key)

        result = await redis_string.length()

        redis.strlen.assert_called_once_with(key)
        self.assertEqual(result, redis.strlen.return_value)

    async def test_get_gets(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_string = RedisString(redis, key)
        encoding = MagicMock()

        result = await redis_string.get(encoding=encoding)

        self.assertEqual(result, redis.get.return_value)
        redis.get.assert_awaited_once_with(key, encoding=encoding)

    async def test_get_with_default_encoding_uses_utf8(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_string = RedisString(redis, key)

        result = await redis_string.get()

        self.assertEqual(result, redis.get.return_value)
        redis.get.assert_awaited_once_with(key, encoding='utf-8')

    async def test_set_with_if_exist_equals_true_sets_correctly(self):
        redis = AsyncMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.2239
        redis_string = RedisString(redis, key)

        result = await redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=True)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_awaited_once_with(key, value, pexpire=1224, exist='SET_IF_EXIST')

    async def test_set_with_if_exist_equals_false_sets_correctly(self):
        redis = AsyncMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.2239
        redis_string = RedisString(redis, key)

        result = await redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=False)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_awaited_once_with(key, value, pexpire=1224, exist='SET_IF_NOT_EXIST')

    async def test_set_with_if_exist_equals_none_sets_correctly(self):
        redis = AsyncMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.2239
        redis_string = RedisString(redis, key)

        result = await redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=None)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_awaited_once_with(key, value, pexpire=1224, exist=None)

    async def test_set_rounds_timeout(self):
        redis = AsyncMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.22349
        redis_string = RedisString(redis, key)

        result = await redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=None)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_awaited_once_with(key, value, pexpire=1223, exist=None)

    async def test_set_uses_correct_defaults(self):
        redis = AsyncMock()
        key = MagicMock()
        value = MagicMock()
        redis_string = RedisString(redis, key)

        result = await redis_string.set(value)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_awaited_once_with(key, value, pexpire=None, exist=None)

import unittest
from unittest.mock import MagicMock
from aioredis_models.redis_string import RedisString


class RedisStringTests(unittest.TestCase):
    def test_init_succeeds(self):
        redis_string = RedisString(MagicMock(), MagicMock())

        self.assertIsInstance(redis_string, RedisString)

    def test_length_returns_length(self):
        redis = MagicMock()
        key = MagicMock()
        redis_string = RedisString(redis, key)

        result = redis_string.length()

        redis.strlen.assert_called_once_with(key)
        self.assertEqual(result, redis.strlen.return_value)

    def test_get_gets(self):
        redis = MagicMock()
        key = MagicMock()
        redis_string = RedisString(redis, key)
        encoding = MagicMock()

        result = redis_string.get(encoding=encoding)

        self.assertEqual(result, redis.get.return_value)
        redis.get.assert_called_once_with(key, encoding=encoding)

    def test_get_with_default_encoding_uses_utf8(self):
        redis = MagicMock()
        key = MagicMock()
        redis_string = RedisString(redis, key)

        result = redis_string.get()

        self.assertEqual(result, redis.get.return_value)
        redis.get.assert_called_once_with(key, encoding='utf-8')

    def test_set_with_if_exist_equals_true_sets_correctly(self):
        redis = MagicMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.2239
        redis_string = RedisString(redis, key)

        result = redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=True)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_called_once_with(key, value, pexpire=1224, exist='SET_IF_EXIST')

    def test_set_with_if_exist_equals_false_sets_correctly(self):
        redis = MagicMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.2239
        redis_string = RedisString(redis, key)

        result = redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=False)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_called_once_with(key, value, pexpire=1224, exist='SET_IF_NOT_EXIST')

    def test_set_with_if_exist_equals_none_sets_correctly(self):
        redis = MagicMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.2239
        redis_string = RedisString(redis, key)

        result = redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=None)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_called_once_with(key, value, pexpire=1224, exist=None)

    def test_set_rounds_timeout(self):
        redis = MagicMock()
        key = MagicMock()
        value = MagicMock()
        timeout_seconds = 1.22349
        redis_string = RedisString(redis, key)

        result = redis_string.set(value, timeout_seconds=timeout_seconds, if_exists_equals=None)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_called_once_with(key, value, pexpire=1223, exist=None)

    def test_set_uses_correct_defaults(self):
        redis = MagicMock()
        key = MagicMock()
        value = MagicMock()
        redis_string = RedisString(redis, key)

        result = redis_string.set(value)

        self.assertEqual(result, redis.set.return_value)
        redis.set.assert_called_once_with(key, value, pexpire=None, exist=None)

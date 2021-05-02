import unittest
from unittest.mock import MagicMock, patch
from aioredis_models.redis_client import RedisClient


class RedisClientTests(unittest.TestCase):
    @staticmethod
    def test_init_succeeds():
        RedisClient(MagicMock())

    def test_get_connection_when_no_transaction_started_returns_redis(self):
        redis = MagicMock()
        client = RedisClient(redis)

        result = client.get_connection()

        self.assertEqual(result, redis)

    @patch('aioredis_models.redis_client.RedisTransaction', MagicMock())
    def test_get_connection_when_transaction_started_returns_multi_exec(self):
        redis = MagicMock()
        client = RedisClient(redis)
        client.begin_transaction()

        result = client.get_connection()

        self.assertEqual(result, redis.multi_exec.return_value)

    def test_is_in_transaction_when_no_transaction_started_returns_false(self):
        client = RedisClient(MagicMock())

        result = client.is_in_transaction()

        self.assertFalse(result)

    @patch('aioredis_models.redis_client.RedisTransaction', MagicMock())
    def test_is_in_transaction_when_transaction_started_returns_true(self):
        client = RedisClient(MagicMock())
        client.begin_transaction()

        result = client.is_in_transaction()

        self.assertTrue(result)

    @patch('aioredis_models.redis_client.RedisTransaction')
    def test_begin_transaction_returns_transaction(self, redis_transaction_init):
        redis = MagicMock()
        client = RedisClient(redis)

        result = client.begin_transaction()

        self.assertEqual(result, redis_transaction_init.return_value)
        self.assertEqual(client.get_connection(), redis.multi_exec.return_value)
        redis.multi_exec.assert_called_once_with()
        redis_transaction_init.assert_called_once_with(client)

    @patch('aioredis_models.redis_client.RedisTransaction', MagicMock())
    def test_begin_transaction_when_already_in_transaction_raises_exception(self):
        client = RedisClient(MagicMock())
        client.begin_transaction()

        self.assertRaises(AssertionError, client.begin_transaction)

    def test_discard_transaction_when_no_transaction_raises_exception(self):
        redis = MagicMock()
        client = RedisClient(redis)

        self.assertRaises(AssertionError, client.discard_transaction)

    @patch('aioredis_models.redis_client.RedisTransaction', MagicMock())
    def test_discard_transaction_when_is_in_transaction_discards_transaction(self):
        redis = MagicMock()
        client = RedisClient(redis)
        client.begin_transaction()

        client.discard_transaction()

        self.assertFalse(client.is_in_transaction())
        self.assertEqual(redis, client.get_connection())

    def test_execute_transaction_when_no_transaction_raises_exception(self):
        client = RedisClient(MagicMock())

        self.assertRaises(AssertionError, client.execute_transaction)

    @patch('aioredis_models.redis_client.RedisTransaction', MagicMock())
    def test_execute_transaction_when_is_in_transaction_executes_transaction(self):
        redis = MagicMock()
        client = RedisClient(redis)
        client.begin_transaction()

        result = client.execute_transaction()

        self.assertEqual(result, redis.multi_exec.return_value.execute.return_value)
        redis.multi_exec.return_value.execute.assert_called_once_with()

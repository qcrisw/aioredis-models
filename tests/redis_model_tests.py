import unittest
from unittest.mock import MagicMock, patch
from aioredis_models.redis_model import RedisModel


class RedisModelTests(unittest.TestCase):
    @patch('aioredis_models.redis_model.RedisClient')
    @patch('aioredis_models.redis_model.isinstance')
    def test_init_with_redis_creates_instance(self, isinstance_mock, redis_client_init):
        redis = MagicMock()
        client = redis_client_init.return_value
        isinstance_mock.return_value = False
        model = RedisModel(redis)

        self.assertEqual(model.begin_transaction, client.begin_transaction)
        self.assertEqual(model.is_in_transaction, client.is_in_transaction)
        self.assertEqual(model.discard_transaction, client.discard_transaction)
        self.assertEqual(model.execute_transaction, client.execute_transaction)
        self.assertEqual(model.get_connection, client.get_connection)
        isinstance_mock.assert_called_once_with(redis, redis_client_init)
        redis_client_init.assert_called_once_with(redis)

    @patch('aioredis_models.redis_model.RedisClient')
    @patch('aioredis_models.redis_model.isinstance')
    def test_init_with_redis_client_creates_instance(self, isinstance_mock, redis_client_init):
        client = MagicMock()
        isinstance_mock.return_value = True
        model = RedisModel(client)

        self.assertEqual(model.begin_transaction, client.begin_transaction)
        self.assertEqual(model.is_in_transaction, client.is_in_transaction)
        self.assertEqual(model.discard_transaction, client.discard_transaction)
        self.assertEqual(model.execute_transaction, client.execute_transaction)
        self.assertEqual(model.get_connection, client.get_connection)
        isinstance_mock.assert_called_once_with(client, redis_client_init)
        redis_client_init.assert_not_called()

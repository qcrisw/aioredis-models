from functools import partial
import unittest
from unittest.mock import MagicMock, AsyncMock, call, patch
from aioredis_models.redis_double_hash import RedisDoubleHash


def assert_transaction(transaction_ctx, return_value, *_, **__):
    transaction_ctx.__aenter__.assert_awaited_once()
    transaction_ctx.__aexit__.assert_not_awaited()
    return return_value

def assert_pre_transaction(transaction_ctx, return_value, *_, **__):
    transaction_ctx.__aenter__.assert_not_awaited()
    transaction_ctx.__aexit__.assert_not_awaited()
    return return_value

class RedisDoubleHashTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_hash = RedisDoubleHash(MagicMock(), MagicMock(), MagicMock())

        self.assertIsInstance(redis_hash, RedisDoubleHash)

    async def test_fields_returns_fields(self):
        redis = AsyncMock()
        key = 'some-key'
        fields = ['foo', 'bar', 'baz', 'bin']
        redis.keys.return_value = [f'{key}:{field}' for field in fields]
        inverse_key = MagicMock()
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)

        result = await redis_double_hash.fields()

        redis.keys.assert_awaited_once_with(key + ':*', encoding='utf-8')
        self.assertEqual(result, set(fields))

    async def test_fields_inverted_returns_fields_inverted(self):
        redis = AsyncMock()
        inverse_key = 'some-inverted-key'
        fields = ['foo', 'bar', 'baz', 'bin']
        redis.keys.return_value = [f'{inverse_key}:{field}' for field in fields]
        key = MagicMock()
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)

        result = await redis_double_hash.fields_inverted()

        redis.keys.assert_awaited_once_with(inverse_key + ':*', encoding='utf-8')
        self.assertEqual(result, set(fields))

    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.isinstance')
    def test_get_works_correctly(self, isinstance_mock, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = True
        key = 'some-key'
        inverse_key = MagicMock()
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'

        result = redis_double_hash.get(field)

        redis_set_init.assert_called_once_with(redis, f'{key}:{field}')
        redis_set_init.return_value.get_all.assert_called_once_with()
        self.assertEqual(result, redis_set_init.return_value.get_all.return_value)

    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.RedisClient')
    @patch('aioredis_models.redis_model.isinstance')
    def test_get_inverted_works_correctly(self, isinstance_mock, redis_client_init, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = False
        key = MagicMock()
        inverse_key = 'some-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'

        result = redis_double_hash.get_inverted(field)

        redis_client_init.assert_called_once_with(redis)
        redis_set_init.assert_called_once_with(redis_client_init.return_value, f'{inverse_key}:{field}')
        redis_set_init.return_value.get_all.assert_called_once_with()
        self.assertEqual(result, redis_set_init.return_value.get_all.return_value)

    @staticmethod
    async def test_set_with_none_value_does_nothing():
        redis_double_hash = RedisDoubleHash(None, MagicMock(), MagicMock())

        await redis_double_hash.set(MagicMock(), None)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.isinstance')
    async def test_set_with_value_sets_value(isinstance_mock, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = True
        transaction_ctx = AsyncMock()
        transaction = MagicMock()
        transaction.add_operation.side_effect = partial(assert_transaction, transaction_ctx, True)
        transaction_ctx.__aenter__.return_value = transaction
        redis.begin_transaction.return_value = transaction_ctx
        redis_sets = [MagicMock(), MagicMock()]
        for redis_set in redis_sets:
            redis_set.add.side_effect = partial(assert_transaction, transaction_ctx, redis_set.add.return_value)
        redis_set_init.side_effect = redis_sets
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'
        value = 'some-value'

        await redis_double_hash.set(field, value)

        redis_set_init.assert_has_calls([
            call(redis, f'{key}:{field}'),
            call(redis, f'{inverse_key}:{value}')
        ])
        redis_sets[0].add.assert_called_once_with(value)
        redis_sets[1].add.assert_called_once_with(field)
        transaction_ctx.__aenter__.assert_awaited_once()
        transaction_ctx.__aexit__.assert_awaited_once()
        transaction.add_operation.assert_called_once_with(
            redis_sets[0].add.return_value,
            redis_sets[1].add.return_value
        )

    async def test_set_inverted_with_none_value_does_nothing(self):
        redis_double_hash = RedisDoubleHash(None, MagicMock(), MagicMock())

        await redis_double_hash.set_inverted(MagicMock(), None)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.isinstance')
    async def test_set_inverted_with_value_sets_inverted_value(isinstance_mock, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = True
        transaction_ctx = AsyncMock()
        transaction = MagicMock()
        transaction.add_operation.side_effect = partial(assert_transaction, transaction_ctx, True)
        transaction_ctx.__aenter__.return_value = transaction
        redis.begin_transaction.return_value = transaction_ctx
        redis_sets = [MagicMock(), MagicMock()]
        for redis_set in redis_sets:
            redis_set.add.side_effect = partial(assert_transaction, transaction_ctx, redis_set.add.return_value)
        redis_set_init.side_effect = redis_sets
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'
        value = 'some-value'

        await redis_double_hash.set_inverted(field, value)

        redis_set_init.assert_has_calls([
            call(redis, f'{key}:{value}'),
            call(redis, f'{inverse_key}:{field}')
        ])
        redis_sets[0].add.assert_called_once_with(field)
        redis_sets[1].add.assert_called_once_with(value)
        transaction_ctx.__aenter__.assert_awaited_once()
        transaction_ctx.__aexit__.assert_awaited_once()
        transaction.add_operation.assert_called_once_with(
            redis_sets[0].add.return_value,
            redis_sets[1].add.return_value
        )

    @staticmethod
    async def test_unset_with_none_value_does_nothing():
        redis_double_hash = RedisDoubleHash(None, MagicMock(), MagicMock())

        await redis_double_hash.unset(MagicMock(), None)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.isinstance')
    async def test_unset_with_value_unsets_value(isinstance_mock, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = True
        transaction_ctx = AsyncMock()
        transaction = MagicMock()
        transaction.add_operation.side_effect = partial(assert_transaction, transaction_ctx, True)
        transaction_ctx.__aenter__.return_value = transaction
        redis.begin_transaction.return_value = transaction_ctx
        redis_sets = [MagicMock(), MagicMock()]
        for redis_set in redis_sets:
            redis_set.remove.side_effect = partial(assert_transaction, transaction_ctx, redis_set.remove.return_value)
        redis_set_init.side_effect = redis_sets
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'
        value = 'some-value'

        await redis_double_hash.unset(field, value)

        redis_set_init.assert_has_calls([
            call(redis, f'{key}:{field}'),
            call(redis, f'{inverse_key}:{value}')
        ])
        redis_sets[0].remove.assert_called_once_with(value)
        redis_sets[1].remove.assert_called_once_with(field)
        transaction_ctx.__aenter__.assert_awaited_once()
        transaction_ctx.__aexit__.assert_awaited_once()
        transaction.add_operation.assert_called_once_with(
            redis_sets[0].remove.return_value,
            redis_sets[1].remove.return_value
        )

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.isinstance')
    async def test_remove_removes_field(isinstance_mock, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = True
        transaction_ctx = AsyncMock()
        transaction = MagicMock()
        transaction.add_operation.side_effect = partial(assert_transaction, transaction_ctx, True)
        transaction_ctx.__aenter__.return_value = transaction
        redis.begin_transaction.return_value = transaction_ctx
        mock_field_set = MagicMock()
        values = ['value1', 'value2']
        mock_field_set.get_all = AsyncMock()
        mock_field_set.get_all.side_effect = partial(assert_pre_transaction, transaction_ctx, values)
        mock_field_set.delete = MagicMock()
        mock_field_set.delete.side_effect = partial(assert_transaction, transaction_ctx, mock_field_set.delete.return_value)
        mock_value_sets = [MagicMock(), MagicMock()]
        for redis_set in mock_value_sets:
            redis_set.remove = MagicMock()
            redis_set.remove.side_effect = partial(assert_transaction, transaction_ctx, redis_set.remove.return_value)
        redis_sets = [mock_field_set, *mock_value_sets]
        redis_set_init.side_effect = redis_sets

        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'

        await redis_double_hash.remove(field)

        redis_set_init.assert_has_calls([call(redis, f'{key}:{field}')] + [
            call(redis, f'{inverse_key}:{value}') for value in values
        ])
        redis_sets[0].get_all.assert_awaited_once_with()
        redis_sets[0].delete.assert_called_once_with()
        transaction_ctx.__aenter__.assert_awaited_once()
        transaction_ctx.__aexit__.assert_awaited_once()
        for redis_set in redis_sets[1:]:
            redis_set.remove.assert_called_once_with(field)
        transaction.add_operation.assert_has_calls([
            call(mock_field_set.delete.return_value)
        ] + [
            call(redis_set.remove.return_value) for redis_set in redis_sets[1:]
        ])

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    @patch('aioredis_models.redis_model.isinstance')
    async def test_remove_inverted_removes_value(isinstance_mock, redis_set_init):
        redis = MagicMock()
        isinstance_mock.return_value = True
        transaction_ctx = AsyncMock()
        transaction = MagicMock()
        transaction.add_operation.side_effect = partial(assert_transaction, transaction_ctx, True)
        transaction_ctx.__aenter__.return_value = transaction
        redis.begin_transaction.return_value = transaction_ctx
        mock_field_set = MagicMock()
        fields = ['field1', 'field2']
        mock_field_set.get_all = AsyncMock()
        mock_field_set.get_all.side_effect = partial(assert_pre_transaction, transaction_ctx, fields)
        mock_field_set.delete = MagicMock()
        mock_field_set.delete.side_effect = partial(assert_transaction, transaction_ctx, mock_field_set.delete.return_value)
        mock_value_sets = [MagicMock(), MagicMock()]
        for redis_set in mock_value_sets:
            redis_set.remove = MagicMock()
            redis_set.remove.side_effect = partial(assert_transaction, transaction_ctx, redis_set.remove.return_value)
        redis_sets = [mock_field_set, *mock_value_sets]
        redis_set_init.side_effect = redis_sets

        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        value = 'some-value'

        await redis_double_hash.remove_inverted(value)

        redis_set_init.assert_has_calls([call(redis, f'{inverse_key}:{value}')] + [
            call(redis, f'{key}:{field}') for field in fields
        ])
        redis_sets[0].get_all.assert_awaited_once_with()
        redis_sets[0].delete.assert_called_once_with()
        transaction_ctx.__aenter__.assert_awaited_once()
        transaction_ctx.__aexit__.assert_awaited_once()
        for redis_set in redis_sets[1:]:
            redis_set.remove.assert_called_once_with(value)
        transaction.add_operation.assert_has_calls([
            call(mock_field_set.delete.return_value)
        ] + [
            call(redis_set.remove.return_value) for redis_set in redis_sets[1:]
        ])

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisKey')
    @patch('aioredis_models.redis_model.isinstance')
    async def test_delete_deletes_both_sides(isinstance_mock, redis_key_init):
        fields = [MagicMock() for _ in range(12)]
        values = [MagicMock() for _ in range(19)]
        redis = MagicMock()
        connections = [MagicMock(), MagicMock()] + ([MagicMock()] * len(fields + values))
        transaction_ctxs = [AsyncMock(), AsyncMock()]
        transactions = [MagicMock(), MagicMock()]
        for index in range(len(transactions)):
            transaction_ctxs[index].__aenter__.return_value = transactions[index]
            transactions[index].add_operation.side_effect = partial(assert_transaction, transaction_ctxs[index], MagicMock())
        transactions[0].set_result_callback.side_effect = \
            lambda callback: assert_transaction(transaction_ctxs[0], True) and callback(fields, values)
        transactions[1].set_result_callback = None
        for connection in connections[:2]:
            connection.keys.side_effect = partial(assert_transaction, transaction_ctxs[0], connection.keys.return_value)
        redis.get_connection.side_effect = connections
        redis.begin_transaction.side_effect = transaction_ctxs
        isinstance_mock.return_value = True
        redis_keys = [MagicMock() for _ in range(len(fields + values))]
        for redis_key in redis_keys:
            redis_key.delete = MagicMock()
            redis_key.delete.side_effect = partial(assert_transaction, transaction_ctxs[1], redis_key.delete.return_value)
        redis_key_init.side_effect = redis_keys
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)

        await redis_double_hash.delete()

        connections[0].keys.assert_called_once_with(key + ':*', encoding='utf-8')
        connections[1].keys.assert_called_once_with(inverse_key + ':*', encoding='utf-8')
        redis_key_init.assert_has_calls([
            call(connections[2], key) for key in fields + values
        ], any_order=True)
        for redis_key in redis_keys:
            redis_key.delete.assert_called_once_with()
        transactions[0].add_operation.assert_called_once_with(*(
            connection.keys.return_value for connection in connections[:2]
        ))
        transactions[1].add_operation.assert_called_once_with(*(
            redis_keys[index].delete.return_value for index in range(len(redis_keys))
        ))

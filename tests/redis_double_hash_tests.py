import unittest
from unittest.mock import MagicMock, AsyncMock, call, patch
from aioredis_models.redis_double_hash import RedisDoubleHash


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
    async def test_get_works_correctly(self, redis_set_init):
        redis_set_init.return_value.get_all = AsyncMock()
        redis = AsyncMock()
        key = 'some-key'
        inverse_key = MagicMock()
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'

        result = await redis_double_hash.get(field)

        redis_set_init.assert_called_once_with(redis, f'{key}:{field}')
        redis_set_init.return_value.get_all.assert_awaited_once_with()
        self.assertEqual(result, redis_set_init.return_value.get_all.return_value)

    @patch('aioredis_models.redis_double_hash.RedisSet')
    async def test_get_inverted_works_correctly(self, redis_set_init):
        redis_set_init.return_value.get_all = AsyncMock()
        redis = AsyncMock()
        key = MagicMock()
        inverse_key = 'some-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'

        result = await redis_double_hash.get_inverted(field)

        redis_set_init.assert_called_once_with(redis, f'{inverse_key}:{field}')
        redis_set_init.return_value.get_all.assert_awaited_once_with()
        self.assertEqual(result, redis_set_init.return_value.get_all.return_value)

    @staticmethod
    async def test_set_with_none_value_does_nothing():
        redis_double_hash = RedisDoubleHash(None, MagicMock(), MagicMock())

        await redis_double_hash.set(MagicMock(), None)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    async def test_set_with_value_sets_value(redis_set_init):
        redis_sets = [MagicMock(), MagicMock()]
        for redis_set in redis_sets:
            redis_set.add = AsyncMock()
        redis_set_init.side_effect = redis_sets
        redis = AsyncMock()
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
        redis_sets[0].add.assert_awaited_once_with(value)
        redis_sets[1].add.assert_awaited_once_with(field)

    async def test_set_inverted_with_none_value_does_nothing(self):
        redis_double_hash = RedisDoubleHash(None, MagicMock(), MagicMock())

        await redis_double_hash.set_inverted(MagicMock(), None)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    async def test_set_inverted_with_value_sets_inverted_value(redis_set_init):
        redis_sets = [MagicMock(), MagicMock()]
        for redis_set in redis_sets:
            redis_set.add = AsyncMock()
        redis_set_init.side_effect = redis_sets
        redis = AsyncMock()
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
        redis_sets[0].add.assert_awaited_once_with(field)
        redis_sets[1].add.assert_awaited_once_with(value)

    @staticmethod
    async def test_unset_with_none_value_does_nothing():
        redis_double_hash = RedisDoubleHash(None, MagicMock(), MagicMock())

        await redis_double_hash.unset(MagicMock(), None)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    async def test_unset_with_value_unsets_value(redis_set_init):
        redis_sets = [MagicMock(), MagicMock()]
        for redis_set in redis_sets:
            redis_set.remove = AsyncMock()
        redis_set_init.side_effect = redis_sets
        redis = AsyncMock()
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
        redis_sets[0].remove.assert_awaited_once_with(value)
        redis_sets[1].remove.assert_awaited_once_with(field)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    async def test_remove_removes_field(redis_set_init):
        mock_field_set = MagicMock()
        values = ['value1', 'value2']
        mock_field_set.get_all = AsyncMock(return_value=values)
        mock_field_set.delete = AsyncMock()
        mock_value_sets = [MagicMock(), MagicMock()]
        for redis_set in mock_value_sets:
            redis_set.remove = AsyncMock()
        redis_sets = [mock_field_set, *mock_value_sets]
        redis_set_init.side_effect = redis_sets
        redis = AsyncMock()
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        field = 'some-field'

        await redis_double_hash.remove(field)

        redis_set_init.assert_has_calls([call(redis, f'{key}:{field}')] + [
            call(redis, f'{inverse_key}:{value}') for value in values
        ])
        redis_sets[0].get_all.assert_awaited_once_with()
        redis_sets[0].delete.assert_awaited_once_with()
        for index in range(len(values)):
            redis_sets[index+1].remove.assert_awaited_once_with(field)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisSet')
    async def test_remove_inverted_removes_value(redis_set_init):
        mock_value_set = MagicMock()
        fields = ['field1', 'field2']
        mock_value_set.get_all = AsyncMock(return_value=fields)
        mock_value_set.delete = AsyncMock()
        mock_field_sets = [MagicMock(), MagicMock()]
        for redis_set in mock_field_sets:
            redis_set.remove = AsyncMock()
        redis_sets = [mock_value_set, *mock_field_sets]
        redis_set_init.side_effect = redis_sets
        redis = AsyncMock()
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)
        value = 'some-value'

        await redis_double_hash.remove_inverted(value)

        redis_set_init.assert_has_calls([call(redis, f'{inverse_key}:{value}')] + [
            call(redis, f'{key}:{field}') for field in fields
        ])
        redis_sets[0].get_all.assert_awaited_once_with()
        redis_sets[0].delete.assert_awaited_once_with()
        for index in range(len(fields)):
            redis_sets[index+1].remove.assert_awaited_once_with(value)

    @staticmethod
    @patch('aioredis_models.redis_double_hash.RedisKey')
    async def test_delete_deletes_both_sides(redis_key_init):
        fields = [MagicMock() for _ in range(12)]
        values = [MagicMock() for _ in range(19)]
        redis = MagicMock()
        redis.keys = AsyncMock(side_effect=[fields, values])
        redis_keys = [MagicMock() for _ in range(len(fields + values))]
        for redis_key in redis_keys:
            redis_key.delete = AsyncMock()
        redis_key_init.side_effect = redis_keys
        key = 'some-key'
        inverse_key = 'some-inverse-key'
        redis_double_hash = RedisDoubleHash(redis, key, inverse_key)

        await redis_double_hash.delete()

        redis.keys.assert_has_awaits([
            call(key + ':*', encoding='utf-8'),
            call(inverse_key + ':*', encoding='utf-8')
        ])
        redis_key_init.assert_has_calls([
            call(redis, key) for key in fields + values
        ])
        for redis_key in redis_keys:
            redis_key.delete.assert_awaited_once_with()

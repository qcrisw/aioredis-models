import unittest
from unittest.mock import MagicMock, AsyncMock, call
from aioredis_models.redis_hash import RedisHash


class RedisHashTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_hash = RedisHash(MagicMock(), MagicMock())

        self.assertIsInstance(redis_hash, RedisHash)

    def test_length_returns_length(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)

        result = redis_hash.length()

        redis.hlen.assert_called_once_with(key)
        self.assertEqual(result, redis.hlen.return_value)

    def test_field_length_returns_field_length(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        result = redis_hash.field_length(field)

        redis.hstrlen.assert_called_once_with(key, field)
        self.assertEqual(result, redis.hstrlen.return_value)

    def test_field_exists_works_correctly(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        result = redis_hash.field_exists(field)

        redis.hexists.assert_called_once_with(key, field)
        self.assertEqual(result, redis.hexists.return_value)

    def test_fields_works_correctly(self):
        fields = [MagicMock() for _ in range(7)]
        redis = MagicMock()
        redis.hkeys.return_value = fields
        key = MagicMock()
        encoding = MagicMock()
        redis_hash = RedisHash(redis, key)

        result = redis_hash.fields(encoding=encoding)

        redis.hkeys.assert_called_once_with(key, encoding=encoding)
        self.assertEqual(result, fields)

    def test_fields_with_default_encoding_uses_utf8(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)

        redis_hash.fields()

        redis.hkeys.assert_called_once_with(key, encoding='utf-8')

    def test_get_all_works_correctly(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        encoding = MagicMock()

        result = redis_hash.get_all(encoding=encoding)

        redis.hgetall.assert_called_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.hgetall.return_value)

    def test_get_all_with_default_encoding_uses_utf8(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)

        result = redis_hash.get_all()

        redis.hgetall.assert_called_once_with(key, encoding='utf-8')
        self.assertEqual(result, redis.hgetall.return_value)

    def test_get_gets_field(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()
        encoding = MagicMock()

        result = redis_hash.get(field, encoding=encoding)

        redis.hget.assert_called_once_with(key, field, encoding=encoding)
        self.assertEqual(result, redis.hget.return_value)

    def test_get_with_default_encoding_uses_utf8(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        redis_hash.get(field)

        redis.hget.assert_called_once_with(key, field, encoding='utf-8')

    async def test_enumerate_with_none_encoding_enumerates_items(self):
        redis = AsyncMock()
        items = [
            (MagicMock(), MagicMock()) for _ in range(6)
        ]
        scans = [
            (1,
                items[:2]),
            (2, items[2:5]),
            (0, items[5:])
        ]
        redis.hscan.side_effect = scans
        key = MagicMock()
        field_pattern = MagicMock()
        batch_size = MagicMock()
        redis_hash= RedisHash(redis, key)

        result = [item async for item in redis_hash.enumerate(
            field_pattern=field_pattern,
            batch_size=batch_size,
            encoding=None
        )]

        self.assertEqual(result, items)
        redis.hscan.assert_has_awaits([
            call(key, cursor=cursor, match=field_pattern, count=batch_size) \
                for cursor in ['0', 1, 2]
        ])

    async def test_enumerate_with_default_encoding_enumerates_items_with_utf8(self):
        redis = AsyncMock()
        items = [
            (MagicMock(), MagicMock()) for _ in range(6)
        ]
        scans = [
            (1,
                items[:2]),
            (2, items[2:5]),
            (0, items[5:])
        ]
        redis.hscan.side_effect = scans
        key = MagicMock()
        field_pattern = MagicMock()
        batch_size = MagicMock()
        redis_hash= RedisHash(redis, key)

        result = [item async for item in redis_hash.enumerate(
            field_pattern=field_pattern,
            batch_size=batch_size
        )]

        self.assertEqual(result, [
            (field.decode.return_value, value.decode.return_value) for field, value in items
        ])
        for field, value in items:
            field.decode.assert_called_once_with('utf-8')
            value.decode.assert_called_once_with('utf-8')
        redis.hscan.assert_has_awaits([
            call(key, cursor=cursor, match=field_pattern, count=batch_size) \
                for cursor in ['0', 1, 2]
        ])

    async def test_enumerate_with_provided_encoding_enumerates_items_with_encoding(self):
        redis = AsyncMock()
        items = [
            (MagicMock(), MagicMock()) for _ in range(6)
        ]
        scans = [
            (1,
                items[:2]),
            (2, items[2:5]),
            (0, items[5:])
        ]
        redis.hscan.side_effect = scans
        key = MagicMock()
        field_pattern = MagicMock()
        batch_size = MagicMock()
        encoding=MagicMock()
        redis_hash= RedisHash(redis, key)

        result = [item async for item in redis_hash.enumerate(
            field_pattern=field_pattern,
            batch_size=batch_size,
            encoding=encoding
        )]

        self.assertEqual(result, [
            (field.decode.return_value, value.decode.return_value) for field, value in items
        ])
        for field, value in items:
            field.decode.assert_called_once_with(encoding)
            value.decode.assert_called_once_with(encoding)
        redis.hscan.assert_has_awaits([
            call(key, cursor=cursor, match=field_pattern, count=batch_size) \
                for cursor in ['0', 1, 2]
        ])

    async def test_set_all_with_none_value_does_nothing(self):
        key = MagicMock()
        redis_hash = RedisHash(None, key)

        await redis_hash.set_all(None)

    def test_set_all_with_value_sets_all(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        values = MagicMock()

        result = redis_hash.set_all(values)

        redis.hmset_dict.assert_called_once_with(key, values)
        self.assertEqual(result, redis.hmset_dict.return_value)

    async def test_set_with_none_value_does_nothing(self):
        key = MagicMock()
        redis_hash = RedisHash(None, key)

        await redis_hash.set(MagicMock(), None)

    def test_set_with_value_set(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()
        value = MagicMock()

        result = redis_hash.set(field, value)

        redis.hset.assert_called_once_with(key, field, value)
        self.assertEqual(result, redis.hset.return_value)

    def test_remove_removes_field(self):
        redis = MagicMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        result = redis_hash.remove(field)

        redis.hdel.assert_called_once_with(key, field)
        self.assertEqual(result, redis.hdel.return_value)

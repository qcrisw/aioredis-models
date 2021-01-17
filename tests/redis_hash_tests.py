import unittest
from unittest.mock import MagicMock, AsyncMock
from aioredis_models.redis_hash import RedisHash


class RedisHashTests(unittest.IsolatedAsyncioTestCase):
    def test_init_succeeds(self):
        redis_hash = RedisHash(MagicMock(), MagicMock())

        self.assertIsInstance(redis_hash, RedisHash)

    async def test_field_exists_works_correctly(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        result = await redis_hash.field_exists(field)

        redis.hexists.assert_awaited_once_with(key, field)
        self.assertEqual(result, redis.hexists.return_value)

    async def test_field_works_correctly(self):
        fields = [MagicMock() for _ in range(7)]
        redis = AsyncMock()
        redis.hkeys.return_value = fields
        key = MagicMock()
        encoding = MagicMock()
        redis_hash = RedisHash(redis, key)

        result = await redis_hash.fields(encoding=encoding)

        redis.hkeys.assert_awaited_once_with(key, encoding=encoding)
        self.assertEqual(result, set(fields))

    async def test_field_with_default_encoding_uses_utf8(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)

        await redis_hash.fields()

        redis.hkeys.assert_awaited_once_with(key, encoding='utf-8')

    async def test_get_all_works_correctly(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        encoding = MagicMock()

        result = await redis_hash.get_all(encoding=encoding)

        redis.hgetall.assert_awaited_once_with(key, encoding=encoding)
        self.assertEqual(result, redis.hgetall.return_value)

    async def test_get_all_with_default_encoding_uses_utf8(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)

        result = await redis_hash.get_all()

        redis.hgetall.assert_awaited_once_with(key, encoding='utf-8')
        self.assertEqual(result, redis.hgetall.return_value)

    async def test_get_gets_field(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()
        encoding = MagicMock()

        result = await redis_hash.get(field, encoding=encoding)

        redis.hget.assert_awaited_once_with(key, field, encoding=encoding)
        self.assertEqual(result, redis.hget.return_value)

    async def test_get_with_default_encoding_uses_utf8(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        await redis_hash.get(field)

        redis.hget.assert_awaited_once_with(key, field, encoding='utf-8')

    async def test_set_all_with_none_value_does_nothing(self):
        key = MagicMock()
        redis_hash = RedisHash(None, key)

        await redis_hash.set_all(None)

    async def test_set_all_with_value_sets_all(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        values = MagicMock()

        result = await redis_hash.set_all(values)

        redis.hmset_dict.assert_awaited_once_with(key, values)
        self.assertEqual(result, redis.hmset_dict.return_value)

    async def test_set_with_none_value_does_nothing(self):
        key = MagicMock()
        redis_hash = RedisHash(None, key)

        await redis_hash.set(MagicMock(), None)

    async def test_set_with_value_set(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()
        value = MagicMock()

        result = await redis_hash.set(field, value)

        redis.hset.assert_awaited_once_with(key, field, value)
        self.assertEqual(result, redis.hset.return_value)

    async def test_remove_removes_field(self):
        redis = AsyncMock()
        key = MagicMock()
        redis_hash = RedisHash(redis, key)
        field = MagicMock()

        result = await redis_hash.remove(field)

        redis.hdel.assert_awaited_once_with(key, field)
        self.assertEqual(result, redis.hdel.return_value)

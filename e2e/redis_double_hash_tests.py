from aioredis_models import RedisDoubleHash
from .redis_tests import RedisTests


class RedisDoubleHashTests(RedisTests):
    _key = 'test-key'
    _inverse_key = 'key-test'
    _redis_double_hash: RedisDoubleHash = None

    async def asyncSetUp(self):
        await super().asyncSetUp()

        self._redis_double_hash = RedisDoubleHash(self._redis, self._key, self._inverse_key)
        await self._redis_double_hash.delete()

    async def test_fields_gets_fields(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')
        await self._redis_double_hash.set('biz', 'boo')

        result = await self._redis_double_hash.fields()

        self.assertEqual(result, {'foo', 'biz'})

    async def test_fields_inverted_gets_inverted_fields(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')
        await self._redis_double_hash.set('biz', 'boo')

        result = await self._redis_double_hash.fields_inverted()

        self.assertEqual(result, {'bar', 'bat', 'boo'})

    async def test_set_sets_mapping(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')

        foo = await self._redis_double_hash.get('foo')
        bar = await self._redis_double_hash.get_inverted('bar')
        bat = await self._redis_double_hash.get_inverted('bat')

        self.assertEqual(set(foo), {'bar', 'bat'})
        self.assertEqual(bar, ['foo'])
        self.assertEqual(bat, ['foo'])

    async def test_set_inverted_sets_mapping(self):
        await self._redis_double_hash.set_inverted('foo', 'bar')
        await self._redis_double_hash.set_inverted('foo', 'bat')

        bar = await self._redis_double_hash.get('bar')
        bat = await self._redis_double_hash.get('bat')
        foo = await self._redis_double_hash.get_inverted('foo')

        self.assertEqual(set(foo), {'bar', 'bat'})
        self.assertEqual(bar, ['foo'])
        self.assertEqual(bat, ['foo'])

    async def test_unset_unsets_mapping(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')

        await self._redis_double_hash.unset('foo', 'bat')

        foo = await self._redis_double_hash.get('foo')
        bar = await self._redis_double_hash.get_inverted('bar')
        bat = await self._redis_double_hash.get_inverted('bat')

        self.assertEqual(foo, ['bar'])
        self.assertEqual(bar, ['foo'])
        self.assertEqual(bat, [])

    async def test_remove_removes_mapping(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')
        await self._redis_double_hash.set('bar', 'foo')

        await self._redis_double_hash.remove('foo')

        foo = await self._redis_double_hash.get('foo')
        bar_fwd = await self._redis_double_hash.get('bar')
        bar_bwd = await self._redis_double_hash.get_inverted('bar')
        bat = await self._redis_double_hash.get_inverted('bat')

        self.assertEqual(foo, [])
        self.assertEqual(bar_fwd, ['foo'])
        self.assertEqual(bar_bwd, [])
        self.assertEqual(bat, [])

    async def test_remove_inverted_removes_mapping(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')
        await self._redis_double_hash.set('biz', 'bat')

        await self._redis_double_hash.remove_inverted('bat')

        foo = await self._redis_double_hash.get('foo')
        biz = await self._redis_double_hash.get('biz')
        bar = await self._redis_double_hash.get_inverted('bar')
        bat = await self._redis_double_hash.get_inverted('bat')

        self.assertEqual(foo, ['bar'])
        self.assertEqual(biz, [])
        self.assertEqual(bar, ['foo'])
        self.assertEqual(bat, [])

    async def test_delete_deletes_all(self):
        await self._redis_double_hash.set('foo', 'bar')
        await self._redis_double_hash.set('foo', 'bat')

        await self._redis_double_hash.delete()

        foo = await self._redis_double_hash.get('foo')
        bar = await self._redis_double_hash.get_inverted('bar')
        bat = await self._redis_double_hash.get_inverted('bat')

        self.assertFalse(foo)
        self.assertFalse(bar)
        self.assertFalse(bat)

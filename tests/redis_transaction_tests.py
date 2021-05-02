import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from aioredis_models.redis_transaction import RedisTransaction


class RedisTransactionTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def test_init_succeeds():
        rtx = RedisTransaction(MagicMock())

    async def test_aenter_returns_self(self):
        rtx = RedisTransaction(MagicMock())

        result = await rtx.__aenter__()

        self.assertEqual(result, rtx)

    @staticmethod
    @patch('aioredis_models.redis_transaction.gather', new_callable=AsyncMock)
    async def test_aexit_with_no_expection_executes_transaction(gather_mock):
        client = AsyncMock()
        rtx = RedisTransaction(client)
        tasks = [MagicMock() for _ in range(4)]

        rtx.add_operation(*tasks)
        await rtx.__aexit__(None, None, None)

        client.execute_transaction.assert_awaited_once_with()
        gather_mock.assert_awaited_once_with(*tasks)

    @staticmethod
    @patch('aioredis_models.redis_transaction.gather', new_callable=AsyncMock)
    async def test_aexit_with_expection_discards_transaction(gather_mock):
        client = MagicMock()
        client.execute_transaction = None
        rtx = RedisTransaction(client)
        tasks = [MagicMock() for _ in range(19)]

        rtx.add_operation(*tasks)
        await rtx.__aexit__(None, MagicMock(), None)

        client.discard_transaction.assert_called_once_with()
        for task in tasks:
            task.cancel.assert_called_once_with()
        gather_mock.assert_not_awaited()

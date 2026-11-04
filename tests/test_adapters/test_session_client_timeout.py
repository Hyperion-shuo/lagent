"""Exercise the real HTTP forwarding deadline without calling a model API."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import unittest

from aiohttp import web
from aiohttp.test_utils import TestServer

from lagent.adapters.proxy import SessionClient


class SessionClientTimeoutTests(unittest.IsolatedAsyncioTestCase):
    def test_invalid_request_timeout_is_rejected(self):
        for timeout in (0, -1, float('nan'), float('inf'), True, '300'):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, 'finite positive'):
                SessionClient(real_api_key='test', real_base_url='http://127.0.0.1', request_timeout=timeout)

    def test_default_keeps_aiohttp_timeout_policy(self):
        client = SessionClient(real_api_key='test', real_base_url='http://127.0.0.1')
        self.assertIsNone(client.request_timeout)

    async def test_real_upstream_deadline(self):
        async def delayed(request):
            await asyncio.sleep(0.08)
            return web.json_response({'ok': True})

        app = web.Application()
        app.router.add_get('/delay', delayed)
        upstream = TestServer(app)
        await upstream.start_server()
        try:
            for timeout, expired in ((None, False), (1.0, False), (0.01, True)):
                with self.subTest(timeout=timeout):
                    client = SessionClient(real_api_key='test', real_base_url=str(upstream.make_url('')).rstrip('/'),
                                           request_timeout=timeout)
                    request = SimpleNamespace(method='GET', match_info={'path': 'delay'}, headers={}, query_string='',
                                              read=AsyncMock(return_value=b''))
                    if expired:
                        with self.assertRaises(asyncio.TimeoutError):
                            await client._handle_request(request)
                    else:
                        response = await client._handle_request(request)
                        self.assertEqual(response.status, 200)
                        self.assertIn(b'"ok": true', response.body)
        finally:
            await upstream.close()


if __name__ == '__main__':
    unittest.main()

import asyncio
import json
import shlex
import subprocess
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from lagent.serving.sandbox.providers.dbox_client import DBoxSandboxClient, DBoxTransportError


class PIDTests(unittest.IsolatedAsyncioTestCase):
    async def client(self, handler):
        c = DBoxSandboxClient('https://dbox.test/v1/sandboxes/test/proxy/44772/',
                              auth_headers=lambda: {'OPEN-SANDBOX-API-KEY': 'private'},
                              execd_token='exec-private', sandbox_id='test', provider=None)
        await c._client.aclose()
        c._client = httpx.AsyncClient(base_url=c.url, transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(c.aclose)
        return c

    async def test_direct_pid_alive_and_absent(self):
        async def handler(req):
            command = json.loads(req.content)['command']
            self.assertNotIn('18000', command)
            self.assertNotIn('private', command)
            code = shlex.split(command)[2]
            result = subprocess.run(['python3', '-c', code], capture_output=True, text=True, check=True)
            events = [{'type': 'stdout', 'text': result.stdout}, {'type': 'execution_complete', 'exit_code': 0}]
            return httpx.Response(200, text='\n'.join('data: '+json.dumps(e) for e in events))
        c = await self.client(handler)
        import os
        self.assertTrue(await c.is_pid_running(os.getpid()))
        self.assertFalse(await c.is_pid_running(2147483647))

    async def test_bad_stream_is_never_dead_pid(self):
        cases = [[], [{'type': 'stdout', 'text': 'DBOX_PID:0'}],
                 [{'type': 'execution_complete'}],
                 [{'type': 'stdout', 'text': 'DBOX_PID:0'}, {'type': 'error'}, {'type': 'execution_complete'}],
                 [{'type': 'stdout', 'text': 'DBOX_PID:0'}, {'type': 'execution_complete', 'exit_code': 1}],
                 [{'type': 'stdout', 'text': 'DBOX_PID:0\nDBOX_PID:1'}, {'type': 'execution_complete'}]]
        for events in cases:
            with self.subTest(events=events):
                c = await self.client(lambda req: httpx.Response(200, text='\n'.join(map(json.dumps, events))))
                with self.assertRaises(DBoxTransportError): await c.is_pid_running(123)

    async def test_transport_error_and_cancellation(self):
        for status in (409, 404, 500):
            c = await self.client(lambda req: httpx.Response(status))
            with self.assertRaisesRegex(DBoxTransportError, str(status)): await c.is_pid_running(123)
        def timeout(req): raise httpx.ReadTimeout('private')
        c = await self.client(timeout)
        with self.assertRaisesRegex(DBoxTransportError, 'ReadTimeout') as error: await c.is_pid_running(123)
        self.assertNotIn('private', str(error.exception))
        with patch.object(c, '_execd_stdout', AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError): await c.is_pid_running(123)

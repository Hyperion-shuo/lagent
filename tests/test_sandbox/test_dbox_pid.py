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

    async def test_completion_does_not_wait_for_eof_and_retains_timings(self):
        class Stream(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                yield b'data: {"type":"stdout","text":"DBOX_PID:1"}\n'
                yield b'data: {"type":"execution_complete"}\n'
                await asyncio.sleep(60)
            async def aclose(self): self.closed = True
        streams = []
        def handler(req):
            stream = Stream(); streams.append(stream)
            return httpx.Response(200, stream=stream)
        c = await self.client(handler)
        for _ in range(4):
            self.assertTrue(await asyncio.wait_for(c.is_pid_running(123), 1))
        self.assertTrue(all(s.closed for s in streams))
        self.assertEqual(len(c.pid_check_diagnostics), 3)
        d = c.pid_check_diagnostics[-1]
        self.assertEqual(d['http_status'], 200)
        self.assertEqual(d['last_event_type'], 'execution_complete')
        self.assertLessEqual(d['headers_seconds'], d['execution_complete_seconds'])
        self.assertNotIn('private', json.dumps(c.pid_check_diagnostics))

    async def test_stalled_stream_keeps_stage_and_never_returns_false(self):
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'data: {"type":"ping"}\n'
                await asyncio.sleep(60)
        c = await self.client(lambda req: httpx.Response(200, stream=Stream()))
        real_timeout = asyncio.timeout
        with patch('lagent.serving.sandbox.providers.dbox_client.asyncio.timeout',
                   side_effect=lambda seconds: real_timeout(.02)):
            with self.assertRaisesRegex(DBoxTransportError, 'deadline'):
                await c.is_pid_running(123)
        d = c.pid_check_diagnostics[-1]
        self.assertFalse(d['ok'])
        self.assertEqual(d['last_event_type'], 'ping')
        self.assertNotIn('execution_complete_seconds', d)

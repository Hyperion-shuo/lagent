"""Real HTTP regression for mini's OpenAI base URL and SessionClient trace."""
import json
import sys
from types import ModuleType
import unittest
from unittest.mock import patch
import urllib.request

from aiohttp import web
from aiohttp.test_utils import TestServer

from lagent.adapters.mini_swe_agent import MiniSWEAgentAdapter
from lagent.adapters.proxy import SessionClient


class MiniProxyPathTests(unittest.IsolatedAsyncioTestCase):
    async def test_mini_openai_path_is_recognized_injected_and_recorded(self):
        received = []

        async def upstream(request):
            received.append((request.path, await request.json()))
            return web.json_response({"choices": [{"message": {"role": "assistant", "content": "done"}}]})

        app = web.Application()
        app.router.add_post('/v1/chat/completions', upstream)
        server = TestServer(app)
        await server.start_server()
        proxy = SessionClient(real_api_key='model-test-key', real_base_url=str(server.make_url('')).rstrip('/'),
                              session_id='mini-path-regression', extra_body={'max_tokens': 8192, 'top_p': 1.0})
        await proxy.start()
        modules = {}
        for name in ('minisweagent', 'minisweagent.agents', 'minisweagent.config', 'minisweagent.environments',
                     'minisweagent.models', 'minisweagent.utils', 'minisweagent.utils.serialize'):
            modules[name] = ModuleType(name)
            modules[name].__path__ = []

        class MiniStub:
            messages = []

            def __init__(self, model):
                self.model = model

            def run(self, task, **kwargs):
                # Mirror the OpenAI SDK's relative chat/completions route;
                # api_base must already include /v1.
                url = self.model['model_kwargs']['api_base'].rstrip('/') + '/chat/completions'
                payload = json.dumps({'model': 'test', 'messages': [{'role': 'user', 'content': task}]}).encode()
                request = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
                with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=5) as response:
                    content = json.loads(response.read())['choices'][0]['message']['content']
                return {'submission': content}

        modules['minisweagent.config'].get_config_from_spec = lambda spec: {}
        modules['minisweagent.utils.serialize'].recursive_merge = lambda *values: values[1]
        modules['minisweagent.models'].get_model = lambda *, config: config
        modules['minisweagent.environments'].get_environment = lambda config, **kwargs: object()
        modules['minisweagent.agents'].get_agent = lambda model, environment, config, **kwargs: MiniStub(model)
        try:
            with patch.dict(sys.modules, modules):
                adapter = MiniSWEAgentAdapter(model='test', proxy=proxy)
                self.assertEqual(await adapter.run_external_async('hello'), 'done')
            self.assertEqual(len(received), 1)
            path, payload = received[0]
            self.assertEqual(path, '/v1/chat/completions')
            self.assertEqual(payload['session_id'], 'mini-path-regression')
            self.assertEqual(payload['max_tokens'], 8192)
            self.assertEqual(payload['top_p'], 1.0)
            self.assertEqual(len(proxy.get_messages()), 1)
        finally:
            await proxy.stop()
            await server.close()


if __name__ == '__main__':
    unittest.main()

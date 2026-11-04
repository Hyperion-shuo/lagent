"""Authenticated execd transport to the loopback remote-exec protocol.

The original remote-exec server remains responsible for commands, detached
processes and file serialization. It listens only on loopback. Neither IAM nor
execd credentials are forwarded to commands, files or task runtime environments.
"""

from __future__ import annotations

import base64
import json
import shlex

import httpx

from .base import SandboxClient


class DBoxTransportError(httpx.HTTPError, RuntimeError):
    """Sanitized data-plane failure compatible with existing PID polling retries."""


class DBoxSandboxClient(SandboxClient):
    """Preserve SandboxClient operations through an authenticated execd call."""

    def __init__(self, url: str, *, auth_headers, execd_token: str, sandbox_id: str, provider):
        super().__init__(url)
        self._auth_headers = auth_headers
        self._execd_token = execd_token
        self.sandbox_id = sandbox_id
        self.provider = provider

    async def _remote_request(self, path: str, payload: dict | None = None, timeout: float = 60) -> dict:
        encoded = base64.b64encode(json.dumps(payload).encode()).decode() if payload is not None else ""
        code = (
            "import base64,json,urllib.request; "
            "payload=base64.b64decode(" + repr(encoded) + ") if " + repr(encoded) + " else None; "
            "request=urllib.request.Request(" + repr("http://127.0.0.1:18000/" + path)
            + ",data=payload,headers={'Content-Type':'application/json'}); "
            "response=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout="
            + repr(timeout) + "); "
            "print('DBOX_RESPONSE:'+base64.b64encode(response.read()).decode())"
        )
        headers = self._auth_headers()
        headers["X-EXECD-ACCESS-TOKEN"] = self._execd_token
        stdout = []
        complete = False
        failed = False
        try:
            async with self._client.stream(
                "POST", "command", headers=headers,
                json={"command": "python3 -c " + shlex.quote(code)}, timeout=timeout + 15,
            ) as response:
                if response.status_code != 200:
                    raise DBoxTransportError(f"DBox execd returned HTTP {response.status_code}")
                async for line in response.aiter_lines():
                    if not line.strip() or line.startswith(":"):
                        continue
                    if line.startswith("data:"):
                        line = line[5:].lstrip()
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if event.get("type") == "stdout":
                        stdout.append(event.get("text", ""))
                    elif event.get("type") in {"stderr", "error", "execution_error"}:
                        failed = True
                    elif event.get("type") == "execution_complete":
                        complete = True
        except DBoxTransportError:
            raise
        except Exception as exc:
            raise DBoxTransportError(f"DBox authenticated transport failed ({type(exc).__name__})") from None
        finally:
            headers.clear()
        text = "".join(stdout).strip()
        if failed or not complete or not text.startswith("DBOX_RESPONSE:"):
            raise DBoxTransportError("DBox execution response incomplete or failed")
        try:
            result = json.loads(base64.b64decode(text[len("DBOX_RESPONSE:"):], validate=True))
            if not isinstance(result, dict):
                raise ValueError("not a response object")
            return result
        except Exception:
            raise RuntimeError("DBox execution returned invalid response data") from None

    async def execute(self, command: str, cwd: str = "/root", timeout_sec: int = 60, detach: bool = False) -> dict:
        return await self._remote_request(
            "exec", {"command": command, "cwd": cwd, "timeout_sec": int(timeout_sec), "detach": detach},
            timeout=int(timeout_sec) + 10,
        )

    async def upload_bytes(self, remote_path: str, content: bytes) -> dict:
        # execd invokes a shell: keep each command well below ARG_MAX. Assemble
        # chunks in a private staging path and rename only after all succeed.
        if len(content) <= 24000:
            return await self._remote_request(
                "upload", {"target_path": remote_path, "content_b64": base64.b64encode(content).decode()},
            )
        from uuid import uuid4
        staging = "/tmp/dbox-transfer-" + uuid4().hex
        try:
            await self.execute("umask 077; : > " + shlex.quote(staging), cwd="/tmp")
            for offset in range(0, len(content), 24000):
                chunk = base64.b64encode(content[offset:offset + 24000]).decode()
                code = "import base64;open(" + repr(staging) + ",'ab').write(base64.b64decode(" + repr(chunk) + "))"
                result = await self.execute("python3 -c " + shlex.quote(code), cwd="/tmp")
                if result.get("return_code") != 0:
                    raise RuntimeError("DBox chunk upload failed")
            result = await self.execute("mv -- " + shlex.quote(staging) + " " + shlex.quote(remote_path), cwd="/tmp")
            if result.get("return_code") != 0:
                raise RuntimeError("DBox upload commit failed")
            return {"ok": True}
        finally:
            await self.execute("rm -f -- " + shlex.quote(staging), cwd="/tmp")

    async def download_file(self, remote_path: str) -> bytes:
        result = await self._remote_request("download", {"source_path": remote_path})
        if result.get("ok") is not True:
            raise RuntimeError("DBox file download failed")
        return base64.b64decode(result["content_b64"], validate=True)

    async def health_check(self) -> dict:
        try:
            return await self._remote_request("health", timeout=5)
        except Exception as exc:
            return {"ok": False, "error_type": type(exc).__name__}

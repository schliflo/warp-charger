"""Fake WARP device for client tests (aiohttp TestServer on 127.0.0.1 only)."""

import asyncio
import contextlib
import hashlib
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from warp_charger import WarpClient

REALM = "esp32-lib"
NONCE = "dcd98b7102dd2f0e8b11d0f600bfb0c093"
OPAQUE = "5ccc069c403ebaf9f0171e9517f40e41"


def msg(topic: str, payload: Any) -> str:
    """One wire message including its trailing newline."""
    return json.dumps({"topic": topic, "payload": payload}) + "\n"


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def check_digest(request: web.Request, user: str, password: str) -> bool:
    """Minimal server-side RFC 2617 qop=auth check."""
    header = request.headers.get("Authorization", "")
    if not header.startswith("Digest "):
        return False
    params = {k: v.strip('"') for k, v in re.findall(r'(\w+)=("[^"]*"|[^,\s]+)', header[7:])}
    try:
        ha1 = md5(f"{params['username']}:{REALM}:{password}")
        ha2 = md5(f"{request.method}:{params['uri']}")
        expected = md5(
            f"{ha1}:{params['nonce']}:{params['nc']}:{params['cnonce']}:{params['qop']}:{ha2}"
        )
    except KeyError:
        return False
    return (
        params["username"] == user
        and params["uri"] == request.raw_path
        and params["response"] == expected
    )


type Script = Callable[["FakeDevice", web.WebSocketResponse, web.Request], Awaitable[None]]


class FakeDevice:
    """Scriptable WARP stand-in: /ws dump + pushes, GET/PUT with optional Digest auth."""

    def __init__(self) -> None:
        self.dump_frames: list[str] = [
            msg("info/keep_alive", {"uptime": 1}) + msg("info/name", {"uid": "CGy"}),
            msg("evse/state", {"charger_state": 0}),
            msg("meters/0/value", {"v": 1}) + msg("nfc/seen_tags", []) + "\n",
        ]
        self.scripts: list[Script] = []  # popped per ws connection, then default
        self.sockets: list[web.WebSocketResponse] = []
        self.requests: list[web.Request] = []
        self.ws_requests = 0
        self.ws_reject_status = 503
        self.ws_reject_count = 0
        self.auth: tuple[str, str] | None = None
        self.get_data: dict[str, Any] = {}
        self.get_raw: dict[str, str] = {}  # served as-is, not JSON
        self.put_status: dict[str, tuple[int, str]] = {}
        self.puts: list[tuple[str, str, str | None]] = []  # (path, body, content-type)
        self.put_gate: asyncio.Event | None = None
        self.put_active = 0
        self.put_max_active = 0
        self.connected_event = asyncio.Event()

    # ---- helpers for tests ----

    def reject_next(self, count: int, status: int = 503) -> None:
        """Answer the next ``count`` /ws upgrades with an HTTP error."""
        self.ws_reject_count = count
        self.ws_reject_status = status

    async def push(self, topic: str, payload: Any) -> None:
        for ws in list(self.sockets):
            await ws.send_str(msg(topic, payload))

    async def close_all(self, code: int = 1000) -> None:
        for ws in list(self.sockets):
            await ws.close(code=code)

    def abort_all(self) -> None:
        """Drop the TCP connections without a close frame."""
        for request in list(self.requests):
            if request.transport is not None:
                request.transport.close()

    async def send_dump(self, ws: web.WebSocketResponse) -> None:
        for frame in self.dump_frames:
            await ws.send_str(frame)

    # ---- handlers ----

    def _unauthorized(self) -> web.Response:
        challenge = f'Digest realm="{REALM}", qop="auth", nonce="{NONCE}", opaque="{OPAQUE}"'
        return web.Response(status=401, headers={"WWW-Authenticate": challenge})

    def _authorized(self, request: web.Request) -> bool:
        return self.auth is None or check_digest(request, *self.auth)

    async def ws_handler(self, request: web.Request) -> web.StreamResponse:
        self.ws_requests += 1
        if not self._authorized(request):
            return self._unauthorized()
        if self.ws_reject_count > 0:
            self.ws_reject_count -= 1
            return web.Response(status=self.ws_reject_status)
        ws = web.WebSocketResponse(heartbeat=None)
        await ws.prepare(request)
        self.sockets.append(ws)
        self.requests.append(request)
        self.connected_event.set()
        try:
            script = self.scripts.pop(0) if self.scripts else default_script
            await script(self, ws, request)
        finally:
            if ws in self.sockets:
                self.sockets.remove(ws)
            if request in self.requests:
                self.requests.remove(request)
        return ws

    async def http_handler(self, request: web.Request) -> web.StreamResponse:
        if not self._authorized(request):
            return self._unauthorized()
        path = request.match_info["path"]
        if request.method == "GET":
            if path in self.get_raw:
                return web.Response(text=self.get_raw[path])
            if path not in self.get_data:
                return web.Response(status=404)
            return web.json_response(self.get_data[path])
        body = await request.text()
        self.puts.append((path, body, request.headers.get("Content-Type")))
        self.put_active += 1
        self.put_max_active = max(self.put_max_active, self.put_active)
        try:
            if self.put_gate is not None:
                await self.put_gate.wait()
        finally:
            self.put_active -= 1
        status, text = self.put_status.get(path, (200, ""))
        return web.Response(status=status, text=text)


async def default_script(device: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
    """Send the dump, then idle until the client goes away."""
    await device.send_dump(ws)
    async for _msg in ws:
        pass


def gated_script(gate: asyncio.Event | None = None, code: int | None = 1000) -> Script:
    """Dump, wait for ``gate``, then close with ``code`` (None: keep open)."""

    async def script(device: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await device.send_dump(ws)
        if gate is not None:
            await gate.wait()
        if code is not None:
            await ws.close(code=code)
        else:
            async for _msg in ws:
                pass

    return script


@pytest.fixture(autouse=True)
def _allow_localhost_sockets(request: pytest.FixtureRequest) -> None:
    """The root env loads pytest-socket (via the HA plugin); lib tests need loopback."""
    with contextlib.suppress(pytest.FixtureLookupError):
        request.getfixturevalue("socket_enabled")


@pytest.fixture
def until() -> Callable[..., Awaitable[None]]:
    """``await until(predicate)``: poll until true, fail after ``timeout`` seconds."""

    async def wait(predicate: Callable[[], object], timeout: float = 3.0) -> None:  # noqa: ASYNC109
        async with asyncio.timeout(timeout):
            while not predicate():  # noqa: ASYNC110
                await asyncio.sleep(0.002)

    return wait


@pytest.fixture
async def device() -> AsyncIterator[FakeDevice]:
    yield FakeDevice()


@pytest.fixture
async def server(device: FakeDevice) -> AsyncIterator[TestServer]:
    app = web.Application()
    app.router.add_get("/ws", device.ws_handler)
    app.router.add_route("*", "/{path:.*}", device.http_handler)
    srv = TestServer(app, host="127.0.0.1")
    await srv.start_server()
    yield srv
    await srv.close()


@pytest.fixture
def base_url(server: TestServer) -> str:
    return str(server.make_url("")).rstrip("/")


@pytest.fixture
async def session() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as sess:
        yield sess


@pytest.fixture
async def make_client(
    session: aiohttp.ClientSession, base_url: str
) -> AsyncIterator[Callable[..., WarpClient]]:
    """Factory with tiny tunables; clients are stopped at teardown."""
    clients: list[tuple[WarpClient, asyncio.Task[None] | None]] = []

    def factory(*, start: bool = True, sess: aiohttp.ClientSession | None = None, **kw: Any):
        opts: dict[str, Any] = {
            "reconnect_delay": 0.01,
            "max_reconnect_delay": 0.05,
            "jitter": 0.0,
            "going_away_delay": 0.02,
            "receive_timeout": 5.0,
        }
        opts.update(kw)
        client = WarpClient(sess or session, base_url, **opts)
        task = asyncio.create_task(client.run()) if start else None
        clients.append((client, task))
        return client

    yield factory

    for client, task in clients:
        await client.stop()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

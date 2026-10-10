import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
import pytest

from conftest import FakeDevice
from warp_charger import (
    WarpAuthError,
    WarpClient,
    WarpCommandError,
    WarpConnectionError,
)

type Factory = Callable[..., WarpClient]
type Until = Callable[..., Awaitable[None]]


@pytest.fixture
def client(make_client: Factory) -> WarpClient:
    return make_client(start=False, request_timeout=2)


@pytest.fixture
def dead_client(session: aiohttp.ClientSession) -> WarpClient:
    """Client pointing at a closed port."""
    return WarpClient(session, "http://127.0.0.1:1", request_timeout=2)


# ---- get ----


async def test_get_parses_json(device: FakeDevice, client: WarpClient) -> None:
    device.get_data["info/name"] = {"uid": "CGy", "n": [1, 2]}
    assert await client.get("info/name") == {"uid": "CGy", "n": [1, 2]}
    assert await client.get("/info/name") == {"uid": "CGy", "n": [1, 2]}


async def test_get_unknown_path(device: FakeDevice, client: WarpClient) -> None:
    with pytest.raises(WarpConnectionError, match="404"):
        await client.get("nope")


async def test_get_401(device: FakeDevice, client: WarpClient) -> None:
    device.auth = ("u", "p")
    with pytest.raises(WarpAuthError):
        await client.get("info/name")


async def test_get_network_error(dead_client: WarpClient) -> None:
    with pytest.raises(WarpConnectionError):
        await dead_client.get("info/name")


async def test_get_invalid_json(device: FakeDevice, client: WarpClient) -> None:
    device.get_raw["x"] = "<html>not json"
    with pytest.raises(WarpConnectionError):
        await client.get("x")


# ---- put ----


async def test_put_sends_json_body(device: FakeDevice, client: WarpClient) -> None:
    await client.put("evse/external_current", {"current": 6000})
    await client.put("evse/start_charging")
    await client.put("evse/flag", False)
    assert device.puts == [
        ("evse/external_current", '{"current": 6000}', "application/json"),
        ("evse/start_charging", "null", "application/json"),
        ("evse/flag", "false", "application/json"),
    ]
    assert json.loads(device.puts[1][1]) is None


async def test_put_400_is_command_error(device: FakeDevice, client: WarpClient) -> None:
    device.put_status["evse/x"] = (400, "current must be 0 or 6000..32000")
    with pytest.raises(WarpCommandError) as exc:
        await client.put("evse/x", {})
    assert str(exc.value) == "current must be 0 or 6000..32000"


async def test_put_401(device: FakeDevice, client: WarpClient) -> None:
    device.put_status["x"] = (401, "")
    with pytest.raises(WarpAuthError):
        await client.put("x")


@pytest.mark.parametrize("status", [404, 500, 503])
async def test_put_other_status(device: FakeDevice, client: WarpClient, status: int) -> None:
    device.put_status["x"] = (status, "whatever")
    with pytest.raises(WarpConnectionError, match=str(status)):
        await client.put("x")


async def test_put_connection_refused(dead_client: WarpClient) -> None:
    with pytest.raises(WarpConnectionError):
        await dead_client.put("x")


async def test_put_timeout(device: FakeDevice, make_client: Factory) -> None:
    device.put_gate = asyncio.Event()
    client = make_client(start=False, request_timeout=0.1)
    try:
        with pytest.raises(WarpConnectionError):
            await client.put("x")
    finally:
        device.put_gate.set()


async def test_puts_are_serialized(device: FakeDevice, client: WarpClient, until: Until) -> None:
    device.put_gate = asyncio.Event()
    first = asyncio.create_task(client.put("a"))
    second = asyncio.create_task(client.put("b"))
    await until(lambda: device.put_active == 1)
    await asyncio.sleep(0.05)
    assert [p[0] for p in device.puts] == ["a"]  # "b" is waiting for the lock
    device.put_gate.set()
    await asyncio.gather(first, second)
    assert [p[0] for p in device.puts] == ["a", "b"]
    assert device.put_max_active == 1


async def test_failed_put_releases_lock(device: FakeDevice, client: WarpClient) -> None:
    device.put_status["bad"] = (400, "nope")
    with pytest.raises(WarpCommandError):
        await client.put("bad")
    await asyncio.wait_for(client.put("good"), 1)


def test_url_helpers(session: aiohttp.ClientSession) -> None:
    c: Any = WarpClient(session, "https://warp.local:8443/")
    assert c.base_url == "https://warp.local:8443"
    assert c.ws_url == "wss://warp.local:8443/ws"

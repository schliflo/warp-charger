import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

import aiohttp
import pytest

from conftest import FakeDevice
from warp_charger import WarpAuthError, WarpClient

type Factory = Callable[..., WarpClient]


@pytest.fixture
async def auth_device(device: FakeDevice) -> FakeDevice:
    device.auth = ("admin", "s3cret")
    return device


def digest_session(user: str, password: str) -> aiohttp.ClientSession:
    return aiohttp.ClientSession(middlewares=(aiohttp.DigestAuthMiddleware(user, password),))


@pytest.fixture
async def good_session() -> AsyncIterator[aiohttp.ClientSession]:
    async with digest_session("admin", "s3cret") as sess:
        yield sess


@pytest.fixture
async def bad_session() -> AsyncIterator[aiohttp.ClientSession]:
    async with digest_session("admin", "wrong") as sess:
        yield sess


async def test_get_and_put_with_digest(
    auth_device: FakeDevice, make_client: Factory, good_session: aiohttp.ClientSession
) -> None:
    auth_device.get_data["info/name"] = {"uid": "CGy"}
    client = make_client(start=False, sess=good_session)
    assert await client.get("info/name") == {"uid": "CGy"}
    await client.put("evse/start_charging")
    await client.put("evse/external_current", {"current": 8000})
    assert [p[1] for p in auth_device.puts] == ["null", '{"current": 8000}']


async def test_ws_upgrade_with_digest(
    auth_device: FakeDevice,
    make_client: Factory,
    good_session: aiohttp.ClientSession,
    until: Callable[..., Awaitable[None]],
) -> None:
    client = make_client(sess=good_session)
    await client.wait_ready(3)
    assert client.states["info/name"] == {"uid": "CGy"}
    await auth_device.push("evse/state", {"charger_state": 3})
    await until(lambda: client.states["evse/state"] == {"charger_state": 3})
    # Commands after the WS handshake must still be authenticated (same digest origin).
    await client.put("evse/start_charging")
    assert auth_device.puts[-1][1] == "null"


async def test_wrong_password(
    auth_device: FakeDevice, make_client: Factory, bad_session: aiohttp.ClientSession
) -> None:
    client = make_client(start=False, sess=bad_session)
    with pytest.raises(WarpAuthError):
        await client.get("info/name")
    with pytest.raises(WarpAuthError):
        await client.put("x")
    assert auth_device.puts == []


async def test_no_credentials(
    auth_device: FakeDevice, make_client: Factory, session: aiohttp.ClientSession
) -> None:
    client = make_client(start=False, sess=session)
    with pytest.raises(WarpAuthError):
        await client.get("info/name")


async def test_ws_401_ends_run_and_fails_wait_ready(
    auth_device: FakeDevice, make_client: Factory, bad_session: aiohttp.ClientSession
) -> None:
    client = make_client(start=False, sess=bad_session)
    task = asyncio.create_task(client.run())
    with pytest.raises(WarpAuthError):
        await client.wait_ready(3)
    with pytest.raises(WarpAuthError):
        await task
    assert not client.connected
    with pytest.raises(WarpAuthError):  # stays failed
        await client.wait_ready(0.1)
    assert auth_device.ws_requests <= 4  # loop ended, no retry storm


async def test_ws_401_without_middleware(
    auth_device: FakeDevice, make_client: Factory, session: aiohttp.ClientSession
) -> None:
    client = make_client(start=False, sess=session)
    task = asyncio.create_task(client.run())
    with pytest.raises(WarpAuthError):
        await task

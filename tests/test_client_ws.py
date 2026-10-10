import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from conftest import FakeDevice, gated_script, msg
from warp_charger import WarpClient, WarpConnectionError, ws_url

type Until = Callable[..., Awaitable[None]]
type Factory = Callable[..., WarpClient]


@pytest.mark.parametrize(
    ("base", "expected"),
    [
        ("http://1.2.3.4", "ws://1.2.3.4/ws"),
        ("http://1.2.3.4/", "ws://1.2.3.4/ws"),
        ("https://warp.local:8443", "wss://warp.local:8443/ws"),
        ("http://host:8080/prefix/", "ws://host:8080/prefix/ws"),
    ],
)
def test_ws_url(base: str, expected: str) -> None:
    assert ws_url(base) == expected


async def test_dump_split_across_frames(
    device: FakeDevice, server: Any, make_client: Factory, until: Until
) -> None:
    release = asyncio.Event()
    events: list[tuple[str, Any]] = []

    async def script(dev: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await ws.send_str(msg("info/keep_alive", {"uptime": 1}) + msg("a", 1))
        await ws.send_str(msg("b", 2))
        await release.wait()
        await ws.send_str(msg("c", 3) + msg("d", 4) + "\n")  # multi-message end frame
        await dev.push("a", 10)  # live update after the dump
        async for _msg in ws:
            pass

    device.scripts.append(script)
    client = make_client(start=False)
    client.subscribe_all(lambda t, p: events.append((t, p)))
    conn: list[bool] = []
    client.subscribe_connection(conn.append)
    task = asyncio.create_task(client.run())

    await until(lambda: device.sockets)
    await asyncio.sleep(0.05)
    assert events == []
    assert dict(client.states) == {}
    assert not client.connected
    assert conn == []

    release.set()
    await client.wait_ready(2)
    await until(lambda: ("a", 10) in events)
    # dump dispatched once per topic in dump order, before the live update
    assert events == [
        ("info/keep_alive", {"uptime": 1}),
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("d", 4),
        ("a", 10),
    ]
    assert dict(client.states) == {
        "info/keep_alive": {"uptime": 1},
        "a": 10,
        "b": 2,
        "c": 3,
        "d": 4,
    }
    assert conn == [True]
    await client.stop()
    await task


async def test_states_swapped_atomically_on_reconnect(
    device: FakeDevice, server: Any, make_client: Factory, until: Until
) -> None:
    release = asyncio.Event()
    first_closed = asyncio.Event()

    async def first(dev: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await ws.send_str(msg("keep", 1) + msg("gone", 1) + "\n")
        await first_closed.wait()
        await ws.close(code=1000)

    async def second(dev: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await ws.send_str(msg("keep", 2))
        await release.wait()
        await ws.send_str(msg("new", 2) + "\n")
        async for _msg in ws:
            pass

    device.scripts += [first, second]
    seen: list[tuple[str, Any]] = []
    client = make_client()
    await client.wait_ready(2)
    client.subscribe_all(lambda t, p: seen.append((t, p)))
    assert dict(client.states) == {"keep": 1, "gone": 1}

    first_closed.set()
    await until(lambda: not client.connected)
    await until(lambda: len(device.sockets) == 1)
    await asyncio.sleep(0.05)
    # old state is kept, partial new dump is neither visible nor dispatched
    assert dict(client.states) == {"keep": 1, "gone": 1}
    assert seen == []

    release.set()
    await until(lambda: client.connected)
    assert dict(client.states) == {"keep": 2, "new": 2}
    assert seen == [("keep", 2), ("new", 2)]


async def test_subscribe_unsubscribe_and_listeners(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    client = make_client(start=False)
    got: list[Any] = []
    other: list[Any] = []
    unsub = client.subscribe("evse/state", got.append)
    client.subscribe("other", other.append)
    conn: list[bool] = []
    client.subscribe_connection(conn.append)
    task = asyncio.create_task(client.run())
    await client.wait_ready(2)
    assert got == [{"charger_state": 0}]
    assert other == []

    await device.push("evse/state", {"charger_state": 3})
    await until(lambda: len(got) == 2)
    assert got[1] == {"charger_state": 3}
    assert client.states["evse/state"] == {"charger_state": 3}

    unsub()
    unsub()  # idempotent
    await device.push("evse/state", {"charger_state": 1})
    await until(lambda: client.states["evse/state"] == {"charger_state": 1})
    assert len(got) == 2

    await client.stop()
    await task
    assert conn == [True, False]
    assert not client.connected
    assert client.states["evse/state"] == {"charger_state": 1}  # states are kept


async def test_callback_exceptions_are_isolated(
    device: FakeDevice, make_client: Factory, until: Until, caplog: pytest.LogCaptureFixture
) -> None:
    client = make_client(start=False)
    good: list[Any] = []

    def boom(*_: Any) -> None:
        raise RuntimeError("boom")

    client.subscribe("evse/state", boom)
    client.subscribe_all(boom)
    client.subscribe_connection(boom)
    client.subscribe("evse/state", good.append)
    with caplog.at_level(logging.ERROR, logger="warp_charger.client"):
        task = asyncio.create_task(client.run())
        await client.wait_ready(2)
        await device.push("evse/state", {"charger_state": 2})
        await until(lambda: len(good) == 2)
    assert client.connected
    assert "boom" in caplog.text
    await client.stop()
    await task


async def test_reconnect_after_server_close(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    client = make_client(start=False)
    conn: list[bool] = []
    client.subscribe_connection(conn.append)
    task = asyncio.create_task(client.run())
    await client.wait_ready(2)
    await device.close_all(1000)
    await until(lambda: conn == [True, False, True])
    assert device.ws_requests == 2
    await device.push("x", 1)
    await until(lambda: client.states.get("x") == 1)
    await client.stop()
    await task


async def test_reconnect_after_abrupt_drop(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    client = make_client()
    await client.wait_ready(2)
    device.abort_all()
    await until(lambda: device.ws_requests == 2 and client.connected)


async def test_logs_once_per_outage(
    device: FakeDevice, make_client: Factory, until: Until, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="warp_charger.client")
    device.reject_next(5)
    client = make_client()
    await client.wait_ready(3)
    records = [r for r in caplog.records if r.name == "warp_charger.client"]
    assert [r.levelno for r in records] == [logging.WARNING, logging.INFO]


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def record_sleeps(client: WarpClient, delays: list[float]) -> None:
    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0.001)

    client._sleep = fake_sleep  # type: ignore[method-assign]


async def test_backoff_grows_to_max(device: FakeDevice, make_client: Factory, until: Until) -> None:
    device.reject_next(100)
    delays: list[float] = []
    client = make_client(start=False, reconnect_delay=1, max_reconnect_delay=8)
    record_sleeps(client, delays)
    task = asyncio.create_task(client.run())
    await until(lambda: len(delays) >= 6)
    assert delays[:6] == [1, 2, 4, 8, 8, 8]
    await client.stop()
    await task


async def test_backoff_jitter(
    device: FakeDevice, make_client: Factory, until: Until, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("warp_charger.client.random.uniform", lambda _a, b: b)
    device.reject_next(100)
    delays: list[float] = []
    client = make_client(start=False, reconnect_delay=1, max_reconnect_delay=60, jitter=0.2)
    record_sleeps(client, delays)
    task = asyncio.create_task(client.run())
    await until(lambda: len(delays) >= 2)
    assert delays[:2] == pytest.approx([1.2, 2.4])
    await client.stop()
    await task


async def test_backoff_reset_only_after_stable_connection(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    clock = FakeClock()
    gate = asyncio.Event()
    device.reject_next(2)
    device.scripts += [gated_script(None), gated_script(None), gated_script(gate)]
    delays: list[float] = []
    client = make_client(
        start=False, reconnect_delay=1, max_reconnect_delay=16, stable_after=60, clock=clock
    )
    record_sleeps(client, delays)
    task = asyncio.create_task(client.run())
    # 2 rejects, then 2 short-lived connections (no reset), then one that lives >= 60 s
    await until(lambda: len(delays) == 4 and client.connected)
    assert delays == [1, 2, 4, 8]
    clock.now += 61
    gate.set()
    await until(lambda: len(delays) == 5)
    assert delays == [1, 2, 4, 8, 1]
    await client.stop()
    await task


async def test_going_away_uses_fixed_delay_and_is_no_kick(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    async def reboot(dev: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await dev.send_dump(ws)
        dev.reject_next(1)
        await ws.close(code=1001)

    device.scripts.append(reboot)
    delays: list[float] = []
    client = make_client(start=False, reconnect_delay=1, going_away_delay=7, kick_threshold=1)
    record_sleeps(client, delays)
    task = asyncio.create_task(client.run())
    await until(lambda: len(delays) == 2)
    assert delays == [7, 1]  # fixed 5 s-style delay; failed attempt starts the normal backoff
    assert not client.kicked_repeatedly
    await client.stop()
    await task


async def test_kick_counting(device: FakeDevice, make_client: Factory, until: Until) -> None:
    clock = FakeClock()
    gates = [asyncio.Event() for _ in range(3)]
    device.scripts += [gated_script(g, 1000) for g in gates]
    client = make_client(clock=clock, kick_threshold=3, kick_window=600)
    for i, gate in enumerate(gates):
        await until(lambda i=i: device.ws_requests == i + 1 and client.connected)
        assert not client.kicked_repeatedly
        clock.now += 10
        gate.set()
        await until(lambda i=i: device.ws_requests == i + 2)
    await until(lambda: client.connected)
    assert client.kicked_repeatedly
    clock.now += 585  # oldest kick (t=1010) leaves the window
    assert not client.kicked_repeatedly
    clock.now = 5000
    assert not client.kicked_repeatedly


async def test_kicks_expire_individually(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    clock = FakeClock()
    client = make_client(clock=clock, kick_window=600)
    for i in range(3):
        await until(lambda i=i: device.ws_requests == i + 1 and client.connected)
        if i == 2:
            clock.now += 400
        await device.close_all(1000)
    await until(lambda: device.ws_requests == 4 and client.connected)
    assert client.kicked_repeatedly
    clock.now += 250  # first two kicks are 650 s old, only one left
    assert not client.kicked_repeatedly


async def test_failed_connects_and_stop_are_not_kicks(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    device.reject_next(5)
    client = make_client(kick_threshold=1)
    await client.wait_ready(3)
    assert not client.kicked_repeatedly
    await client.stop()
    assert not client.kicked_repeatedly


async def test_stop_is_idempotent_and_run_returns(device: FakeDevice, make_client: Factory) -> None:
    client = make_client(start=False)
    task = asyncio.create_task(client.run())
    await client.wait_ready(2)
    await client.stop()
    await client.stop()
    await asyncio.wait_for(task, 2)
    assert not client.connected


async def test_stop_during_backoff_sleep(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    device.reject_next(100)
    client = make_client(start=False, reconnect_delay=60, max_reconnect_delay=60)
    task = asyncio.create_task(client.run())
    await until(lambda: device.ws_requests == 1)
    await asyncio.sleep(0.02)
    await client.stop()
    await asyncio.wait_for(task, 1)


async def test_stop_before_run(make_client: Factory) -> None:
    client = make_client(start=False)
    await client.stop()
    await asyncio.wait_for(client.run(), 1)


async def test_wait_ready_timeout_reports_last_error(
    device: FakeDevice, make_client: Factory
) -> None:
    device.reject_next(100)
    client = make_client()
    with pytest.raises(WarpConnectionError, match="503"):
        await client.wait_ready(0.2)


async def test_wait_ready_timeout_without_end_marker(
    device: FakeDevice, make_client: Factory
) -> None:
    async def partial(dev: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await ws.send_str(msg("a", 1))
        async for _msg in ws:
            pass

    device.scripts.append(partial)
    client = make_client()
    with pytest.raises(WarpConnectionError):
        await client.wait_ready(0.2)
    assert not client.connected


async def test_wait_ready_connection_refused(session: aiohttp.ClientSession) -> None:
    client = WarpClient(session, "http://127.0.0.1:1", reconnect_delay=0.01, jitter=0)
    task = asyncio.create_task(client.run())
    with pytest.raises(WarpConnectionError, match=r"127\.0\.0\.1"):
        await client.wait_ready(0.3)
    await client.stop()
    await task


async def test_receive_timeout_triggers_reconnect(
    device: FakeDevice, make_client: Factory, until: Until
) -> None:
    device.scripts.append(gated_script(asyncio.Event(), None))  # silent after dump
    client = make_client(receive_timeout=0.1)
    await client.wait_ready(2)
    await until(lambda: device.ws_requests == 2 and client.connected)


async def test_malformed_and_binary_messages_ignored(
    device: FakeDevice, make_client: Factory
) -> None:
    async def script(dev: FakeDevice, ws: web.WebSocketResponse, _: web.Request) -> None:
        await ws.send_bytes(b"\x00\x01")
        await ws.send_str("not json\n" + '{"nope": 1}\n' + msg("a", 1) + "\n")
        async for _msg in ws:
            pass

    device.scripts.append(script)
    client = make_client()
    await client.wait_ready(2)
    assert dict(client.states) == {"a": 1}

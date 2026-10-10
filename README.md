# warp-charger

Async Python client for the local API of [Tinkerforge WARP](https://www.warp-charger.com/) wallboxes and the WARP Energy Manager.

It keeps one WebSocket open for push updates (no polling) and sends commands over HTTP. It powers the [Tinkerforge WARP Home Assistant integration](https://github.com/schliflo/ha-warp), but has no Home Assistant dependency.

- Full state mirror: every API topic (`evse/state`, `meters/0/values`, …) in one dict, kept current by the device's push stream
- Topic and connection callbacks
- Automatic reconnect with backoff; survives device reboots
- HTTP Digest authentication (via aiohttp's `DigestAuthMiddleware`), HTTP and HTTPS
- Helpers: enum tables, meter value IDs, UID encoding, NFC scan detection

Requires Python 3.13+ and aiohttp 3.12+.

## Install

```sh
pip install warp-charger
```

## Quick start

```python
import asyncio

import aiohttp

from warp_charger import WarpClient
from warp_charger.const import CHARGER_STATE


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        client = WarpClient(session, "http://warp3-AbCd.local")
        task = asyncio.create_task(client.run())  # reconnect loop
        await client.wait_ready(timeout=30)  # first full state dump received

        state = client.states["evse/state"]
        print("Charger:", CHARGER_STATE[state["charger_state"]])

        client.subscribe("evse/state", lambda payload: print("now", payload["charger_state"]))

        await client.put("evse/external_current", {"current": 16000})  # 16 A
        await asyncio.sleep(60)

        await client.stop()
        await task


asyncio.run(main())
```

### Authentication

If the web interface has a login, put the credentials into the session. The client does not handle them itself:

```python
session = aiohttp.ClientSession(middlewares=(aiohttp.DigestAuthMiddleware("user", "password"),))
client = WarpClient(session, "https://warp3-AbCd.local")
```

The device does not validate digest nonces, so a captured login can be replayed on plain HTTP. Prefer HTTPS when a password is set. For the self-signed certificate, pass a connector with `ssl=False`.

## API

### `WarpClient(session, base_url, **tunables)`

| Member | Description |
|---|---|
| `await run()` | Connect and keep reconnecting until `stop()`. Raises `WarpAuthError` on HTTP 401. |
| `await stop()` | Close the WebSocket and end `run()`. Idempotent. |
| `await wait_ready(timeout)` | Wait for the first complete state dump. Raises `WarpConnectionError` on timeout, `WarpAuthError` on 401. |
| `states` | Read-only mapping `topic -> payload` of the last complete state. |
| `connected` | `True` while a connection with a complete dump is up. |
| `subscribe(topic, cb)` | `cb(payload)` on every update of `topic`. Returns an unsubscribe function. |
| `subscribe_all(cb)` | `cb(topic, payload)` on every update. |
| `subscribe_connection(cb)` | `cb(True)` after each complete dump, `cb(False)` on disconnect. |
| `await get(path)` | `GET /path`, returns parsed JSON. |
| `await put(path, payload=None)` | `PUT /path` with a JSON body (`None` sends `null`). Calls are serialised. |
| `kicked_repeatedly` | `True` if the device dropped the connection ≥ 3 times in 10 min (see limits). |

Optional keyword tunables are `reconnect_delay` (5 s), `max_reconnect_delay` (60 s), `jitter`, `stable_after`, `going_away_delay`, `receive_timeout` (15 s), `request_timeout` (10 s), `kick_threshold`, `kick_window` and `clock`.

Callbacks run in the event loop. Exceptions in callbacks are logged, never propagated.

After a reconnect, the new dump replaces `states` in one step. Then every subscribed topic is called once, then the connection callbacks.

### Exceptions

`WarpError` is the base class of:
- `WarpAuthError`: HTTP 401.
- `WarpConnectionError`: network errors, timeouts, unexpected status.
- `WarpCommandError`: HTTP 400; `str(err)` is the device's error message.

### Helpers

- `warp_charger.const`
  - `CHARGER_STATE`, `IEC61851_STATE`, `ERROR_STATE`, `DC_FAULT_STATE`: int → key.
  - `CHARGE_MODE`, plus `CHARGE_MODE_DEFAULT` (4 is a state only, never selectable).
  - `DURATION_LIMITS` for charge-limit durations.
  - `METER_VALUES`: meter value id → `MeterValue(key, unit, digits)`, for decoding `meters/N/values` with `meters/N/value_ids`.
  - `INT32_MAX`.
- `warp_charger.uid`: `encode(int) -> str` and `decode(str) -> int`. These convert between the numeric UID from mDNS TXT records and the device UID string shown in `info/name` (base58, or zbase32 for large UIDs).
- `warp_charger.NfcScanDetector`: `feed(nfc/seen_tags payload)` returns the newly scanned `(tag_type, tag_id)` pairs. It uses the firmware web UI's 2 s threshold.

## Device API in a nutshell

The topics and payloads are the device's MQTT/HTTP API, documented in the [WARP API reference](https://docs.warp-charger.com/en/docs/interfaces/mqtt_http/introduction).

- **Read:** everything arrives over `/ws`. On connect the device sends a full dump of all topics, then only changes, at most once per second per topic.
- **Write:** `put("<topic>_update", {...})` for configuration and state topics, e.g. `evse/auto_start_charging_update`. `put("<command>")` for commands, e.g. `evse/start_charging` or `reboot`.
  - The device answers 200 even for some silent no-ops; for example, start/stop without a vehicle is ignored.
- **Limits:**
  - At most 5 WebSocket clients (web UI tabs count). When full, the device drops the client that answered pings least recently.
  - The ESP32 serves one request at a time, so avoid parallel or high-rate requests.

**Never GET a command path.** The firmware executes payload-less commands on GET, so `GET /reboot` reboots the device. `get()` is meant for state topics only.

## Recording a fixture

`scripts/record_dump.py` writes one state dump as JSONL (one `{"topic", "payload"}` per line). It anonymises MACs, IPs, SSIDs, tag ids, user names and coordinates. It is read-only: it only opens the WebSocket.

```sh
uv run python scripts/record_dump.py warp3-AbCd.local --replace-uid -o warp3.jsonl
uv run python scripts/record_dump.py 192.168.1.20 -u admin -p secret --https --insecure
```

## Development

```sh
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

The tests run against a fake device on an aiohttp test server and never touch real hardware.

Release: bump `version` in `pyproject.toml`, then tag `vX.Y.Z`. CI publishes to PyPI via trusted publishing.

## License

MIT

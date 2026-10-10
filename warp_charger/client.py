"""Push-based client for the local WebSocket + HTTP API of a WARP device."""

import asyncio
import json
import logging
import random
import time
from collections import deque
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

import aiohttp
from yarl import URL

from .exceptions import WarpAuthError, WarpCommandError, WarpConnectionError

_LOGGER = logging.getLogger(__name__)

CLOSE_GOING_AWAY = 1001
_MAX_WS_MESSAGE = 16 * 1024 * 1024

type Callback = Callable[..., None]


def ws_url(base_url: str) -> str:
    """Derive the ``/ws`` URL (http->ws, https->wss) from a base URL."""
    url = URL(base_url.rstrip("/"))
    scheme = "wss" if url.scheme == "https" else "ws"
    return str(url.with_scheme(scheme).with_path(url.path.rstrip("/") + "/ws"))


class WarpClient:
    """Keeps a topic -> payload store in sync over ``/ws`` and sends commands over HTTP.

    Credentials are not handled here: pass a session with ``DigestAuthMiddleware``.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        *,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        jitter: float = 0.2,
        stable_after: float = 60.0,
        going_away_delay: float = 5.0,
        receive_timeout: float = 15.0,
        request_timeout: float = 10.0,
        kick_threshold: int = 3,
        kick_window: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._ws_url = ws_url(base_url)
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._jitter = jitter
        self._stable_after = stable_after
        self._going_away_delay = going_away_delay
        self._receive_timeout = receive_timeout
        self._request_timeout = request_timeout
        self._kick_threshold = kick_threshold
        self._kick_window = kick_window
        self._clock = clock

        self._states: dict[str, Any] = {}
        self._connected = False
        self._kicks: deque[float] = deque()
        self._ready_at: float | None = None
        self._last_error = "not connected"
        self._auth_error: WarpAuthError | None = None
        self._offline_logged = False

        self._stop = asyncio.Event()
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ready_or_failed = asyncio.Event()
        self._put_lock = asyncio.Lock()

        self._topic_subs: dict[str, list[Callback]] = {}
        self._all_subs: list[Callback] = []
        self._conn_subs: list[Callback] = []

    # ---- public state ----

    @property
    def base_url(self) -> str:
        """HTTP base URL without trailing slash."""
        return self._base_url

    @property
    def ws_url(self) -> str:
        """WebSocket URL derived from the base URL."""
        return self._ws_url

    @property
    def states(self) -> Mapping[str, Any]:
        """Read-only view of the current complete state (topic -> payload)."""
        return MappingProxyType(self._states)

    @property
    def connected(self) -> bool:
        """True after a full dump was received, False after disconnect."""
        return self._connected

    @property
    def kicked_repeatedly(self) -> bool:
        """True when the device kicked us at least ``kick_threshold`` times recently."""
        self._prune_kicks()
        return len(self._kicks) >= self._kick_threshold

    # ---- subscriptions ----

    def subscribe(self, topic: str, cb: Callable[[Any], None]) -> Callable[[], None]:
        """Call ``cb(payload)`` on every message for ``topic``."""
        return self._add(self._topic_subs.setdefault(topic, []), cb)

    def subscribe_all(self, cb: Callable[[str, Any], None]) -> Callable[[], None]:
        """Call ``cb(topic, payload)`` on every message."""
        return self._add(self._all_subs, cb)

    def subscribe_connection(self, cb: Callable[[bool], None]) -> Callable[[], None]:
        """Call ``cb(True)`` when a dump completes, ``cb(False)`` on disconnect."""
        return self._add(self._conn_subs, cb)

    @staticmethod
    def _add(subs: list[Callback], cb: Callback) -> Callable[[], None]:
        subs.append(cb)

        def unsubscribe() -> None:
            if cb in subs:
                subs.remove(cb)

        return unsubscribe

    @staticmethod
    def _call(cb: Callback, *args: Any) -> None:
        try:
            cb(*args)
        except Exception:
            _LOGGER.exception("Error in callback %r", cb)

    def _dispatch(self, topic: str, payload: Any) -> None:
        for cb in list(self._topic_subs.get(topic, ())):
            self._call(cb, payload)
        for cb in list(self._all_subs):
            self._call(cb, topic, payload)

    def _notify_connection(self, connected: bool) -> None:
        for cb in list(self._conn_subs):
            self._call(cb, connected)

    # ---- lifecycle ----

    async def wait_ready(self, timeout: float) -> None:  # noqa: ASYNC109
        """Wait for the first complete dump."""
        if self._auth_error:
            raise self._auth_error
        if not self._connected:
            try:
                async with asyncio.timeout(timeout):
                    await self._ready_or_failed.wait()
            except TimeoutError:
                raise WarpConnectionError(self._last_error) from None
        if self._auth_error:
            raise self._auth_error

    async def stop(self) -> None:
        """Close the WebSocket and make :meth:`run` return. Idempotent."""
        self._stop.set()
        ws = self._ws
        if ws is not None and not ws.closed:
            await ws.close()

    async def run(self) -> None:
        """Reconnect loop. Returns after :meth:`stop`; raises WarpAuthError on 401."""
        delay = self._reconnect_delay
        try:
            while not self._stop.is_set():
                self._ready_at = None
                close_code: int | None = None
                try:
                    close_code = await self._connect_once()
                except WarpAuthError as err:
                    self._auth_error = err
                    self._last_error = str(err)
                    self._go_offline()
                    self._ready_or_failed.set()
                    raise
                except (aiohttp.ClientError, TimeoutError, OSError) as err:
                    self._last_error = str(err) or type(err).__name__
                    _LOGGER.debug("Connection to %s failed: %r", self._ws_url, err)
                if self._stop.is_set():
                    break

                ready_at = self._ready_at
                self._go_offline()
                if ready_at is not None:
                    if close_code != CLOSE_GOING_AWAY:
                        self._kicks.append(self._clock())
                    if self._clock() - ready_at >= self._stable_after:
                        delay = self._reconnect_delay

                if close_code == CLOSE_GOING_AWAY:
                    wait = self._going_away_delay
                else:
                    wait = delay * (1 + random.uniform(-self._jitter, self._jitter))
                    delay = min(delay * 2, self._max_reconnect_delay)
                await self._sleep(wait)
        finally:
            self._go_offline(log=False)
            self._ws = None

    async def _sleep(self, delay: float) -> None:
        """Sleep, but wake up immediately on stop()."""
        try:
            async with asyncio.timeout(delay):
                await self._stop.wait()
        except TimeoutError:
            pass

    def _prune_kicks(self) -> None:
        cutoff = self._clock() - self._kick_window
        while self._kicks and self._kicks[0] <= cutoff:
            self._kicks.popleft()

    def _go_offline(self, *, log: bool = True) -> None:
        """Mark disconnected (notify listeners) and warn once until we are back."""
        if not self._auth_error:
            self._ready_or_failed.clear()
        if self._connected:
            self._connected = False
            self._notify_connection(False)
        if log and not self._offline_logged and not self._stop.is_set():
            self._offline_logged = True
            _LOGGER.warning(
                "Lost connection to WARP device at %s: %s", self._base_url, self._last_error
            )

    def _go_online(self) -> None:
        if self._offline_logged:
            self._offline_logged = False
            _LOGGER.info("Connection to WARP device at %s restored", self._base_url)

    # ---- websocket ----

    async def _connect_once(self) -> int | None:
        """One connection attempt; returns the close code of the finished connection."""
        try:
            # Connect via the http(s) URL: DigestAuthMiddleware pins itself to the origin of the
            # first request, and ws:// would be a different origin than later http:// commands.
            ws = await self._session.ws_connect(
                f"{self._base_url}/ws",
                timeout=aiohttp.ClientWSTimeout(ws_receive=self._receive_timeout),
                max_msg_size=_MAX_WS_MESSAGE,
            )
        except aiohttp.WSServerHandshakeError as err:
            if err.status == 401:
                raise WarpAuthError("Authentication failed (HTTP 401)") from err
            raise
        self._ws = ws
        try:
            if self._stop.is_set():  # stop() raced with the handshake
                return None
            return await self._receive_loop(ws)
        finally:
            self._ws = None
            if not ws.closed:
                await ws.close()

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> int | None:
        pending: dict[str, Any] | None = {}
        while True:
            msg = await ws.receive()
            if msg.type is aiohttp.WSMsgType.TEXT:
                pending = self._handle_frame(msg.data, pending)
            elif msg.type is aiohttp.WSMsgType.ERROR:
                raise ws.exception() or aiohttp.ClientError("websocket error")
            elif msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSED,
            ):
                code = ws.close_code
                self._last_error = f"connection closed (code {code})"
                return code
            # BINARY and others are ignored.

    def _handle_frame(self, data: str, pending: dict[str, Any] | None) -> dict[str, Any] | None:
        """Process one frame; returns the dump accumulator (None once the dump is done)."""
        for line in data.split("\n"):
            if not line:
                continue
            try:
                msg = json.loads(line)
                topic, payload = msg["topic"], msg["payload"]
            except (ValueError, KeyError, TypeError):
                _LOGGER.debug("Ignoring malformed message: %.200s", line)
                continue
            if pending is not None:
                pending[topic] = payload
            else:
                self._states[topic] = payload
                self._dispatch(topic, payload)
        if pending is not None and "\n\n" in data:
            self._finish_dump(pending)
            return None
        return pending

    def _finish_dump(self, dump: dict[str, Any]) -> None:
        self._states = dump
        for topic, payload in dump.items():
            self._dispatch(topic, payload)
        self._ready_at = self._clock()
        self._connected = True
        self._go_online()
        self._ready_or_failed.set()
        self._notify_connection(True)

    # ---- http ----

    def _url(self, path: str) -> str:
        return f"{self._base_url}/{path.lstrip('/')}"

    async def get(self, path: str) -> Any:
        """GET ``/path`` and return the parsed JSON."""
        try:
            async with self._session.get(
                self._url(path), timeout=aiohttp.ClientTimeout(total=self._request_timeout)
            ) as resp:
                if resp.status == 401:
                    raise WarpAuthError("Authentication failed (HTTP 401)")
                if resp.status != 200:
                    raise WarpConnectionError(f"GET {path}: HTTP {resp.status}")
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as err:
            raise WarpConnectionError(f"GET {path}: {err or type(err).__name__}") from err

    async def put(self, path: str, payload: Any = None) -> None:
        """PUT a JSON payload (``None`` -> ``null``) to ``/path``. Calls are serialized."""
        async with self._put_lock:
            try:
                async with self._session.put(
                    self._url(path),
                    data=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=self._request_timeout),
                ) as resp:
                    if resp.status == 200:
                        return
                    text = await resp.text()
                    if resp.status == 400:
                        raise WarpCommandError(text)
                    if resp.status == 401:
                        raise WarpAuthError("Authentication failed (HTTP 401)")
                    raise WarpConnectionError(f"PUT {path}: HTTP {resp.status}")
            except (aiohttp.ClientError, TimeoutError, OSError) as err:
                raise WarpConnectionError(f"PUT {path}: {err or type(err).__name__}") from err

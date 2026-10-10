"""Record an anonymised state dump of a WARP device as JSONL (one topic per line).

    uv run python scripts/record_dump.py HOST [-u USER -p PASS] [--https] [--insecure]
        [--replace-uid] [-o FILE]

Read-only: only opens the WebSocket and waits for the initial dump.
"""

import argparse
import asyncio
import json
import re
import sys
from typing import Any

import aiohttp

from warp_charger import WarpClient, WarpError

REPLACEMENT_UID = "2jEx"

REDACT_KEYS = frozenset(
    {
        "mac", "sta_mac", "ap_bssid", "sta_bssid", "bssid", "ip", "sta_ip", "gateway", "dns",
        "dns2", "ssid", "passphrase", "password", "lat", "long", "tag_id", "username", "uuid",
        "email", "broker_host", "broker_username", "broker_password", "private_key",
        "public_key", "preshared_key", "remote_host", "remote_port", "internal_ip",
        "internal_gateway", "url", "api_url", "server", "server1", "server2", "host",
        "digest_hash", "note", "secret", "enable_secret", "relay_host", "client_id",
        "global_topic_prefix", "place",
    }
)  # fmt: skip
MAC = re.compile(r"^[0-9A-Fa-f]{2}(:[0-9A-Fa-f]{2}){5}$")
IP = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
MASKED_MAC = "00:00:00:00:00:00"
MASKED_IP = "192.0.2.1"
REDACTED = "**REDACTED**"


def anonymise(value: Any, key: str | None = None, replace: tuple[str, str] | None = None) -> Any:
    """Redact secrets/identifiers; ``replace=(old, new)`` is substituted in all strings."""
    if isinstance(value, dict):
        return {k: anonymise(v, k, replace) for k, v in value.items()}
    if isinstance(value, list):
        return [anonymise(v, key, replace) for v in value]
    if key in ("lat", "long") and isinstance(value, int | float) and not isinstance(value, bool):
        return 0
    if isinstance(value, str):
        if key in REDACT_KEYS and value:
            if MAC.match(value):
                return MASKED_MAC
            if IP.match(value):
                return value if value == "0.0.0.0" else MASKED_IP
            return REDACTED
        if MAC.match(value):
            return MASKED_MAC
        if replace:
            return value.replace(*replace)
    return value


def anonymise_dump(states: dict[str, Any], *, replace_uid: bool = False) -> list[dict[str, Any]]:
    """Anonymise a topic -> payload dump, keeping order."""
    replace = None
    if replace_uid:
        uid = (states.get("info/name") or {}).get("uid")
        if isinstance(uid, str) and uid:
            replace = (uid, REPLACEMENT_UID)
    return [
        {"topic": topic, "payload": anonymise(payload, replace=replace)}
        for topic, payload in states.items()
    ]


async def record(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Connect, wait for the full dump, return the anonymised lines."""
    middlewares = ()
    if args.user is not None:
        middlewares = (aiohttp.DigestAuthMiddleware(args.user, args.password or ""),)
    connector = aiohttp.TCPConnector(ssl=False) if args.insecure else None
    base_url = f"{'https' if args.https else 'http'}://{args.host}"
    async with aiohttp.ClientSession(connector=connector, middlewares=middlewares) as session:
        client = WarpClient(session, base_url)
        task = asyncio.create_task(client.run())
        try:
            await client.wait_ready(args.timeout)
            states = dict(client.states)
        finally:
            await client.stop()
            await asyncio.gather(task, return_exceptions=True)
    return anonymise_dump(states, replace_uid=args.replace_uid)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("host", help="device host[:port]")
    parser.add_argument("-u", "--user")
    parser.add_argument("-p", "--password")
    parser.add_argument("--https", action="store_true")
    parser.add_argument("--insecure", action="store_true", help="do not verify the TLS certificate")
    parser.add_argument(
        "--replace-uid", action="store_true", help=f"replace device uid with {REPLACEMENT_UID}"
    )
    parser.add_argument("-o", "--output", help="output file (default: stdout)")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    try:
        lines = asyncio.run(record(args))
    except WarpError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    text = "".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"wrote {len(lines)} topics to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

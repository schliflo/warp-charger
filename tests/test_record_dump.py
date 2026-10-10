import argparse
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest
from aiohttp.test_utils import TestServer

from conftest import FakeDevice

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "record_dump.py"


@pytest.fixture(scope="module")
def rd() -> ModuleType:
    spec = importlib.util.spec_from_file_location("record_dump", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_anonymise_rules(rd: ModuleType) -> None:
    out = rd.anonymise(
        {
            "ssid": "MyWifi",
            "mac": "AA:BB:CC:DD:EE:FF",
            "ip": "10.0.0.5",
            "gateway": "0.0.0.0",
            "lat": 48.1,
            "long": 11,
            "latitude": 1.5,
            "tag_id": "04:AA:BB",
            "tags": [{"tag_id": "x"}],
            "free": "AA:BB:CC:DD:EE:FF",
            "empty_pw": {"password": ""},
            "keep": "hello",
            "n": 5,
            "flag": True,
        }
    )
    assert out == {
        "ssid": "**REDACTED**",
        "mac": "00:00:00:00:00:00",
        "ip": "192.0.2.1",
        "gateway": "0.0.0.0",
        "lat": 0,
        "long": 0,
        "latitude": 1.5,
        "tag_id": "**REDACTED**",
        "tags": [{"tag_id": "**REDACTED**"}],
        "free": "00:00:00:00:00:00",
        "empty_pw": {"password": ""},
        "keep": "hello",
        "n": 5,
        "flag": True,
    }


def test_replace_uid(rd: ModuleType) -> None:
    states = {
        "info/keep_alive": {"uptime": 1},
        "info/name": {"uid": "CGy", "name": "warp3-CGy", "display_name": "x"},
        "other": {"host": "warp3-CGy.local", "list": ["aCGy"]},
    }
    lines = rd.anonymise_dump(states, replace_uid=True)
    assert [line["topic"] for line in lines] == list(states)
    assert lines[1]["payload"] == {"uid": "2jEx", "name": "warp3-2jEx", "display_name": "x"}
    assert lines[2]["payload"] == {"host": "**REDACTED**", "list": ["a2jEx"]}
    assert "CGy" not in json.dumps(lines)
    # without the flag the uid is untouched
    assert rd.anonymise_dump(states)[1]["payload"]["uid"] == "CGy"


async def test_record_end_to_end(rd: ModuleType, device: FakeDevice, server: TestServer) -> None:
    device.auth = ("u", "p")
    args = argparse.Namespace(
        host=f"127.0.0.1:{server.port}",
        user="u",
        password="p",
        https=False,
        insecure=False,
        replace_uid=True,
        timeout=3.0,
    )
    lines = await rd.record(args)
    assert [line["topic"] for line in lines] == [
        "info/keep_alive",
        "info/name",
        "evse/state",
        "meters/0/value",
        "nfc/seen_tags",
    ]
    assert lines[1]["payload"] == {"uid": "2jEx"}

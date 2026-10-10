from warp_charger import NfcScanDetector


def tag(tag_id: str, age: int, tag_type: int = 2) -> dict:
    return {"tag_type": tag_type, "tag_id": tag_id, "last_seen": age}


EMPTY = tag("", 0, 0)


def test_first_feed_is_silent() -> None:
    d = NfcScanDetector()
    assert d.feed([tag("AA:BB", 100)]) == []
    assert d.feed([tag("AA:BB", 1100)]) == []


def test_new_tag_is_reported_once() -> None:
    d = NfcScanDetector()
    d.feed([EMPTY])
    assert d.feed([tag("AA:BB", 300), EMPTY]) == [(2, "AA:BB")]
    assert d.feed([tag("AA:BB", 1300)]) == []


def test_rescan_after_leaving_for_two_seconds() -> None:
    d = NfcScanDetector()
    d.feed([tag("AA:BB", 500)])
    assert d.feed([tag("AA:BB", 1500)]) == []
    assert d.feed([tag("AA:BB", 2500)]) == []
    assert d.feed([tag("AA:BB", 3500)]) == []
    assert d.feed([tag("AA:BB", 200)]) == [(2, "AA:BB")]


def test_holding_tag_does_not_retrigger() -> None:
    d = NfcScanDetector()
    d.feed([tag("AA:BB", 1900)])
    assert d.feed([tag("AA:BB", 100)]) == []
    assert d.feed([tag("AA:BB", 100)]) == []


def test_old_tag_appearing_is_not_a_scan() -> None:
    d = NfcScanDetector()
    d.feed([EMPTY])
    assert d.feed([tag("AA:BB", 30000)]) == []
    assert d.feed([tag("AA:BB", 31000), tag("CC:DD", 2000)]) == []


def test_stale_tag_after_reset_not_reported() -> None:
    d = NfcScanDetector()
    d.feed([EMPTY])
    d.reset()
    assert d.feed([tag("AA:BB", 100)]) == []
    assert d.feed([tag("AA:BB", 1100)]) == []


def test_same_tag_in_multiple_slots_uses_min_age() -> None:
    d = NfcScanDetector()
    d.feed([tag("AA:BB", 5000), tag("AA:BB", 9000)])
    assert d.feed([tag("AA:BB", 6000), tag("AA:BB", 150)]) == [(2, "AA:BB")]
    # duplicate slots must not produce duplicate events
    d.feed([tag("AA:BB", 6000), tag("AA:BB", 9000)])
    assert d.feed([tag("AA:BB", 100), tag("AA:BB", 100)]) == [(2, "AA:BB")]


def test_type_is_part_of_key_and_output_sorted_by_age() -> None:
    d = NfcScanDetector()
    d.feed([EMPTY])
    out = d.feed([tag("AA", 900, 1), tag("AA", 100, 2), tag("BB", 500, 2)])
    assert out == [(2, "AA"), (2, "BB"), (1, "AA")]

import pytest

from warp_charger import uid


@pytest.mark.parametrize(
    ("n", "s"),
    [(123456, "CGy"), (257899, "2jEx"), (257900, "855c"), (0, "1"), (57, "Z")],
)
def test_vectors(n: int, s: str) -> None:
    assert uid.encode(n) == s
    assert uid.decode(s) == n


def test_zbase32_form_of_last_base58_uid() -> None:
    # firmware comment: "2jEx (base58); 855m (zbase32)"
    assert uid.decode("855m") == 257899


@pytest.mark.parametrize("n", [1, 58, 4711, 257899, 257900, 10**6, 2**32 - 1])
def test_roundtrip(n: int) -> None:
    assert uid.decode(uid.encode(n)) == n


@pytest.mark.parametrize("bad", ["", "0O", "l!"])
def test_invalid(bad: str) -> None:
    with pytest.raises(ValueError, match=r"uid|character"):
        uid.decode(bad)


def test_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        uid.encode(-1)

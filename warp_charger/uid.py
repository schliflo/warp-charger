"""Device UID <-> integer, as shown in ``info/name.uid``.

Firmware uses base58 up to ``BASE58_MAX`` and zbase32 above. Most significant digit first.
"""

BASE58 = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
ZBASE32 = "ybndrfg8ejkmcpqxot1uwisza345h769"
BASE58_MAX = 257899


def _encode(n: int, alphabet: str) -> str:
    base = len(alphabet)
    digits = []
    while True:
        n, rem = divmod(n, base)
        digits.append(alphabet[rem])
        if n == 0:
            break
    return "".join(reversed(digits))


def _decode(s: str, alphabet: str) -> int:
    if not s:
        raise ValueError("empty uid")
    base = len(alphabet)
    n = 0
    for ch in s:
        idx = alphabet.find(ch)
        if idx < 0:
            raise ValueError(f"invalid character {ch!r} in uid {s!r}")
        n = n * base + idx
    return n


def encode(n: int) -> str:
    """Encode an integer UID the way the device displays it."""
    if n < 0:
        raise ValueError("uid must be non-negative")
    return _encode(n, BASE58 if n <= BASE58_MAX else ZBASE32)


def decode(s: str) -> int:
    """Decode a device UID string (base58, or zbase32 if base58 is out of range)."""
    try:
        n = _decode(s, BASE58)
    except ValueError:
        n = BASE58_MAX + 1
    if n <= BASE58_MAX:
        return n
    return _decode(s, ZBASE32)

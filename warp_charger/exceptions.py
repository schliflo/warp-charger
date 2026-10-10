"""Exceptions raised by warp_charger."""


class WarpError(Exception):
    """Base class for all warp_charger errors."""


class WarpAuthError(WarpError):
    """The device rejected the credentials (HTTP 401)."""


class WarpConnectionError(WarpError):
    """The device could not be reached or answered unexpectedly."""


class WarpCommandError(WarpError):
    """The device rejected a command (HTTP 400); the message is the device's text."""

"""Errors that the adapter raises."""


class AsbctlError(Exception):
    """A failed asbctl run, or a failure to start one: its code, message and exit code."""

    def __init__(self, code: str, message: str, exit_code: int) -> None:
        super().__init__(f"{code}: {message} (exit {exit_code})")
        self.code = code
        self.message = message
        self.exit_code = exit_code


class BinaryNotFoundError(AsbctlError):
    """The asbctl binary is not where ASBCTL_BIN says, and not on PATH."""

    def __init__(self, message: str) -> None:
        super().__init__("binary_not_found", message, 127)

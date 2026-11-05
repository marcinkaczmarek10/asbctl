"""Errors that the adapter raises."""


class AsbctlError(Exception):
    """A failed asbctl run: the code and message it reported, and its exit code."""

    def __init__(self, code: str, message: str, exit_code: int) -> None:
        super().__init__(f"{code}: {message} (exit {exit_code})")
        self.code = code
        self.message = message
        self.exit_code = exit_code


class BinaryNotFoundError(Exception):
    """The asbctl binary is not where ASBCTL_BIN says, and not on PATH."""

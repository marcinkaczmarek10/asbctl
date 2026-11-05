"""Run the asbctl binary and stream the JSON it prints."""

import contextlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
from collections.abc import Generator, Iterable
from typing import Any

BINARY_ENV = "ASBCTL_BIN"

type FlagValue = str | int | float | bool | None


class AsbctlError(Exception):
    """A failed asbctl run: the code and message asbctl reported, and its exit code."""

    def __init__(self, code: str, message: str, exit_code: int) -> None:
        super().__init__(f"{code}: {message} (exit {exit_code})")
        self.code = code
        self.message = message
        self.exit_code = exit_code


def find_binary() -> str:
    """Return ASBCTL_BIN when it is set, and the asbctl found on PATH otherwise."""
    configured = os.environ.get(BINARY_ENV)
    if configured:
        return configured

    found = shutil.which("asbctl")
    if found is None:
        raise FileNotFoundError(f"asbctl not found: set {BINARY_ENV} or put asbctl on PATH")
    return found


def build_arguments(
    command: str, subcommands: tuple[str, ...], flags: dict[str, FlagValue]
) -> list[str]:
    """Build the argument list: the command, the flags, then --output json.

    A value of True gives a bare flag, and False or None leave the flag out.
    """
    if "output" in flags:
        raise ValueError("output is always json, do not pass it")

    arguments = [command, *subcommands]
    for name, value in flags.items():
        flag = "--" + name.replace("_", "-")
        if value is None or value is False:
            continue
        if value is True:
            arguments.append(flag)
        elif isinstance(value, str | int | float):
            arguments.extend([flag, str(value)])
        else:
            raise TypeError(f"flag {name} has unsupported type {type(value).__name__}")
    return [*arguments, "--output", "json"]


def error_from_stderr(stderr: str, exit_code: int) -> AsbctlError:
    """Read the JSON error object that asbctl writes in json mode, or keep the raw text."""
    for line in reversed(stderr.splitlines()):
        try:
            detail = json.loads(line)["error"]
            return AsbctlError(str(detail["code"]), str(detail["message"]), exit_code)
        except (ValueError, KeyError, TypeError):
            continue
    return AsbctlError("unknown", stderr.strip(), exit_code)


def run(
    command: str,
    *subcommands: str,
    timeout: float | None = None,
    stdin_lines: Iterable[str] | None = None,
    **flags: FlagValue,
) -> Generator[dict[str, Any]]:
    """Run an asbctl command and yield each JSON object it prints, as it prints it.

    Keyword arguments become flags: payload_type_id="X" is --payload-type-id X. Each string in
    stdin_lines is written to the command's stdin as one line. Raises AsbctlError when asbctl exits
    non-zero, and subprocess.TimeoutExpired when timeout seconds pass first.
    """
    arguments = [find_binary(), *build_arguments(command, subcommands, flags)]

    # stderr goes to a file so that a chatty log cannot fill a pipe and stall the stdout reads.
    with tempfile.TemporaryFile("w+") as stderr_file:
        # The executable is the caller's choice (ASBCTL_BIN or PATH) and the arguments are a list.
        process = subprocess.Popen(  # noqa: S603
            arguments,
            stdin=subprocess.DEVNULL if stdin_lines is None else subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            text=True,
        )
        timed_out = threading.Event()

        def kill_on_timeout() -> None:
            timed_out.set()
            process.kill()

        timer = None if timeout is None else threading.Timer(timeout, kill_on_timeout)
        if timer:
            timer.start()
        if stdin_lines is not None:
            threading.Thread(target=_feed, args=(process, stdin_lines), daemon=True).start()
        try:
            assert process.stdout is not None  # noqa: S101
            for line in process.stdout:
                if line.strip():
                    yield _object_from_line(line)
            exit_code = process.wait()
        finally:
            if timer:
                timer.cancel()
            if process.poll() is None:
                process.kill()
            process.wait()
            if process.stdout:
                process.stdout.close()

        if timed_out.is_set():
            raise subprocess.TimeoutExpired(arguments, timeout or 0.0)
        if exit_code != 0:
            stderr_file.seek(0)
            raise error_from_stderr(stderr_file.read(), exit_code)


def _feed(process: subprocess.Popen[str], lines: Iterable[str]) -> None:
    """Write the lines to the process, then close its stdin so that it sees the end of the input.

    A write error is ignored on purpose: it means the process exited early, and its exit code
    and stderr are what run() reports.
    """
    stdin = process.stdin
    assert stdin is not None  # noqa: S101
    try:
        for line in lines:
            stdin.write(line + "\n")
    except (OSError, ValueError):
        pass
    finally:
        with contextlib.suppress(OSError, ValueError):
            stdin.close()


def _object_from_line(line: str) -> dict[str, Any]:
    value = json.loads(line)
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object per line, got {type(value).__name__}")
    return value

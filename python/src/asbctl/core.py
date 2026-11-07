"""Run the asbctl binary and stream the JSON it prints."""

import asyncio
import contextlib
import json
import os
import shutil
import sys
from collections.abc import AsyncGenerator, AsyncIterable, Iterable, Mapping
from pathlib import Path
from typing import Any, cast

from asbctl.bundled import find_bundled_binary_path
from asbctl.exceptions import AsbctlError, BinaryNotFoundError

BINARY_ENV_NAME = "ASBCTL_BIN"

# A premium namespace allows a 100 MiB message, which is about 134 MiB as base64, and a value body
# is written a second time as body_text, so one output line can reach 256 MiB.
OUTPUT_LINE_LIMIT_BYTES = 256 * 1024 * 1024

type FlagValue = str | int | float | bool | None


def get_binary_path(environment: Mapping[str, str], bundled_path: Path | None) -> str:
    """Return the binary that ASBCTL_BIN names, then the bundled one, then the asbctl on PATH.

    Args:
        environment: The environment variables to read ASBCTL_BIN and PATH from.
        bundled_path: The binary that a platform wheel installed, or None.

    Raises:
        BinaryNotFoundError: ASBCTL_BIN is unset and asbctl is not on PATH.
    """
    configured_path = environment.get(BINARY_ENV_NAME)
    if configured_path:
        return configured_path

    if bundled_path is not None:
        return str(bundled_path)

    found_path = shutil.which("asbctl", path=environment.get("PATH"))
    if found_path is None:
        raise BinaryNotFoundError(f"set {BINARY_ENV_NAME} or put asbctl on PATH")
    return found_path


def build_arguments(
    command: str, subcommands: tuple[str, ...], flags: Mapping[str, FlagValue]
) -> list[str]:
    """Build the argument list: the command, the flags, then --output json.

    A value of True gives a bare flag, and False or None leave the flag out.

    Raises:
        ValueError: The flags include output, which is always json.
        TypeError: A flag value is not a string, a number, a boolean or None.
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


def error_from_stderr(stderr_text: str, exit_code: int) -> AsbctlError:
    """Read the JSON error object that asbctl writes in json mode, or keep the raw text."""
    for line in reversed(stderr_text.splitlines()):
        try:
            detail = json.loads(line)["error"]
            return AsbctlError(str(detail["code"]), str(detail["message"]), exit_code)
        except (ValueError, KeyError, TypeError):
            continue
    return AsbctlError("unknown", stderr_text.strip(), exit_code)


def object_from_line(line: str) -> dict[str, Any]:
    """Parse one output line, which must be a JSON object."""
    value = json.loads(line)
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object per line, got {type(value).__name__}")
    return value


async def run(
    command: str,
    *subcommands: str,
    timeout_seconds: float | None = None,
    stdin_lines: AsyncIterable[str] | Iterable[str] | None = None,
    **flags: FlagValue,
) -> AsyncGenerator[dict[str, Any]]:
    """Run an asbctl command and yield each JSON object it prints, as it prints it.

    Keyword arguments become flags: payload_type_id="X" is --payload-type-id X. Each string in
    stdin_lines is written to the command's stdin as one line. Close the iterator with
    contextlib.aclosing when you stop early, so that the process is stopped at once.

    Args:
        command: The asbctl command, such as peek.
        *subcommands: Subcommands of the command, such as summary.
        timeout_seconds: How long the whole run may take, or None for no limit.
        stdin_lines: Lines for the stdin of the command, or None for no stdin.
        **flags: The flags of the command.

    Raises:
        AsbctlError: asbctl exited with a non-zero code.
        BinaryNotFoundError: The asbctl binary cannot be found.
        TimeoutError: The run took longer than timeout_seconds.
        Exception: An exception that the stdin_lines iterable raises propagates as itself.
    """
    arguments = build_arguments(command, subcommands, flags)
    binary_path = get_binary_path(
        os.environ, find_bundled_binary_path(Path(__file__).parent, sys.platform)
    )

    async with contextlib.aclosing(
        _stream_lines(binary_path, arguments, stdin_lines, timeout_seconds, OUTPUT_LINE_LIMIT_BYTES)
    ) as lines:
        async for line in lines:
            if line.strip():
                yield object_from_line(line)


async def run_once(
    command: str,
    *subcommands: str,
    timeout_seconds: float | None = None,
    stdin_lines: AsyncIterable[str] | Iterable[str] | None = None,
    **flags: FlagValue,
) -> dict[str, Any]:
    """Run a command that prints one JSON object and return that object.

    Raises:
        ValueError: The command printed no object, or more than one.
        Exception: An exception that the stdin_lines iterable raises propagates as itself.
    """
    async with contextlib.aclosing(
        run(
            command, *subcommands, timeout_seconds=timeout_seconds, stdin_lines=stdin_lines, **flags
        )
    ) as stream:
        objects = [output async for output in stream]
    if len(objects) != 1:
        raise ValueError(f"expected one JSON object, got {len(objects)}")
    return objects[0]


async def _stream_lines(
    binary_path: str,
    arguments: list[str],
    stdin_lines: AsyncIterable[str] | Iterable[str] | None,
    timeout_seconds: float | None,
    line_limit_bytes: int,
) -> AsyncGenerator[str]:
    is_line_too_long = False
    exit_code = 0
    async with asyncio.timeout(timeout_seconds):
        process = await _start_process(
            binary_path,
            arguments,
            has_stdin=stdin_lines is not None,
            line_limit_bytes=line_limit_bytes,
        )
        try:
            async with asyncio.TaskGroup() as tasks:
                stderr_task = tasks.create_task(
                    _read_text(cast("asyncio.StreamReader", process.stderr))
                )
                if stdin_lines is not None:
                    tasks.create_task(
                        _write_lines(cast("asyncio.StreamWriter", process.stdin), stdin_lines)
                    )
                stdout = cast("asyncio.StreamReader", process.stdout)
                while True:
                    try:
                        line = await stdout.readline()
                    except ValueError:
                        is_line_too_long = True
                        await _stop_process(process)
                        break
                    if not line:
                        break
                    try:
                        yield line.decode()
                    except GeneratorExit:
                        # A TaskGroup wraps GeneratorExit in an ExceptionGroup, so stop the process
                        # here and end the generator normally.
                        await _stop_process(process)
                        return
                if not is_line_too_long:
                    exit_code = await process.wait()
        except BaseExceptionGroup as group:
            # A TaskGroup wraps the error of a failed stdin writer, so callers get the original.
            if len(group.exceptions) != 1:
                raise
            error = group.exceptions[0]
            raise error from error.__cause__
        finally:
            await _stop_process(process)

    if is_line_too_long:
        raise ValueError(f"an output line is longer than {line_limit_bytes} bytes")
    if exit_code != 0:
        raise error_from_stderr(stderr_task.result(), exit_code)


async def _start_process(
    binary_path: str, arguments: list[str], *, has_stdin: bool, line_limit_bytes: int
) -> asyncio.subprocess.Process:
    try:
        return await asyncio.create_subprocess_exec(
            binary_path,
            *arguments,
            stdin=asyncio.subprocess.PIPE if has_stdin else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=line_limit_bytes,
        )
    except (FileNotFoundError, PermissionError, IsADirectoryError) as error:
        raise BinaryNotFoundError(f"no asbctl binary at {binary_path}: {error}") from error


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
    await process.wait()


async def _read_text(stream: asyncio.StreamReader) -> str:
    return (await stream.read()).decode(errors="replace")


async def _write_lines(
    stream: asyncio.StreamWriter, lines: AsyncIterable[str] | Iterable[str]
) -> None:
    # A closed pipe means the process exited early, and its exit code and stderr say why.
    with contextlib.suppress(BrokenPipeError, ConnectionResetError):
        try:
            async for line in _iterate_lines(lines):
                stream.write(f"{line}\n".encode())
                await stream.drain()
        finally:
            stream.close()
            await stream.wait_closed()


async def _iterate_lines(lines: AsyncIterable[str] | Iterable[str]) -> AsyncGenerator[str]:
    if isinstance(lines, AsyncIterable):
        async for line in lines:
            yield line
    else:
        for line in lines:
            yield line

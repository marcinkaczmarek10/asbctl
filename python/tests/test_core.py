import asyncio
import contextlib
import json
import stat
import time
from pathlib import Path
from typing import Any, cast

import pytest

from asbctl.core import (
    _read_text,
    _stop_process,
    _stream_lines,
    _write_lines,
    build_arguments,
    error_from_stderr,
    get_binary_path,
    object_from_line,
    run,
    run_once,
)
from asbctl.exceptions import AsbctlError, BinaryNotFoundError

from .conftest import FakeFactory


def test_build_arguments_puts_subcommands_before_flags_and_json_output_last():
    # Given
    flags = {"queue": "billing-q"}

    # When
    arguments = build_arguments("dlq", ("summary",), flags)

    # Then
    assert arguments == ["dlq", "summary", "--queue", "billing-q", "--output", "json"]


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ({"payload_type_id": "InvoiceSent"}, ["--payload-type-id", "InvoiceSent"]),
        ({"is_dead_letter": True}, ["--is-dead-letter"]),
        ({"dlq": False}, []),
        ({"queue": None}, []),
        ({"max": 0}, ["--max", "0"]),
        ({"rate": 2.5}, ["--rate", "2.5"]),
    ],
    ids=["string", "true", "false", "none", "zero", "float"],
)
def test_build_arguments_maps_each_flag_value_type(flags: dict[str, Any], expected: list[str]):
    # Given
    command = "peek"

    # When
    arguments = build_arguments(command, (), flags)

    # Then
    assert arguments == [command, *expected, "--output", "json"]


def test_build_arguments_rejects_an_output_flag():
    # Given
    flags = {"output": "table"}

    # When
    with pytest.raises(ValueError, match=r"^output is always json, do not pass it$"):
        build_arguments("peek", (), flags)

    # Then


def test_build_arguments_rejects_a_flag_value_of_an_unsupported_type():
    # Given
    flags: dict[str, Any] = {"queues": ["a", "b"]}

    # When
    with pytest.raises(TypeError, match=r"^flag queues has unsupported type list$"):
        build_arguments("peek", (), flags)

    # Then


def test_get_binary_path_prefers_the_configured_binary():
    # Given
    environment = {"ASBCTL_BIN": "/opt/custom/asbctl", "PATH": "/usr/bin"}

    # When
    binary_path = get_binary_path(environment, None)

    # Then
    assert binary_path == "/opt/custom/asbctl"


def test_get_binary_path_prefers_the_configured_binary_over_the_bundled_one():
    # Given
    environment = {"ASBCTL_BIN": "/opt/custom/asbctl"}
    bundled_path = Path("/site-packages/asbctl/bin/asbctl")

    # When
    binary_path = get_binary_path(environment, bundled_path)

    # Then
    assert binary_path == "/opt/custom/asbctl"


def test_get_binary_path_prefers_the_bundled_binary_over_path(tmp_path: Path):
    # Given
    on_path = tmp_path / "asbctl"
    on_path.write_text("#!/bin/sh\n")
    on_path.chmod(on_path.stat().st_mode | stat.S_IXUSR)
    bundled_path = Path("/site-packages/asbctl/bin/asbctl")

    # When
    binary_path = get_binary_path({"PATH": str(tmp_path)}, bundled_path)

    # Then
    assert binary_path == str(bundled_path)


def test_get_binary_path_finds_asbctl_on_path(tmp_path: Path):
    # Given
    executable = tmp_path / "asbctl"
    executable.write_text("#!/bin/sh\n")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)

    # When
    binary_path = get_binary_path({"PATH": str(tmp_path)}, None)

    # Then
    assert binary_path == str(executable)


def test_get_binary_path_raises_when_asbctl_is_nowhere(tmp_path: Path):
    # Given
    environment = {"PATH": str(tmp_path)}

    # When
    with pytest.raises(BinaryNotFoundError, match="ASBCTL_BIN"):
        get_binary_path(environment, None)

    # Then


def test_get_binary_path_ignores_an_empty_configured_binary(tmp_path: Path):
    # Given
    environment = {"ASBCTL_BIN": "", "PATH": str(tmp_path)}

    # When
    with pytest.raises(BinaryNotFoundError):
        get_binary_path(environment, None)

    # Then


def test_error_from_stderr_reads_the_last_json_error_line():
    # Given
    stderr_text = (
        'time=2026 level=WARN msg="slow"\n'
        '{"schema":1,"error":{"code":"peek_failed","message":"service unavailable"}}\n'
    )

    # When
    error = error_from_stderr(stderr_text, 4)

    # Then
    assert (error.code, error.message, error.exit_code) == ("peek_failed", "service unavailable", 4)


def test_error_from_stderr_keeps_the_raw_text_when_it_is_not_json():
    # Given
    stderr_text = "asbctl: flag provided but not defined: -nope\nrun 'asbctl peek -h' for usage\n"

    # When
    error = error_from_stderr(stderr_text, 2)

    # Then
    assert error.code == "unknown"
    assert error.message == stderr_text.strip()
    assert error.exit_code == 2


@pytest.mark.parametrize(
    "stderr_text", ["", '{"error": "text"}', '{"error": {"code": "x"}}', "[1]"]
)
def test_error_from_stderr_falls_back_to_raw_text_for_an_object_without_code_and_message(
    stderr_text: str,
):
    # Given
    exit_code = 9

    # When
    error = error_from_stderr(stderr_text, exit_code)

    # Then
    assert (error.code, error.message) == ("unknown", stderr_text.strip())


def test_object_from_line_parses_a_json_object():
    # Given
    line = '{"sequence_number": 42}\n'

    # When
    parsed = object_from_line(line)

    # Then
    assert parsed == {"sequence_number": 42}


@pytest.mark.parametrize("line", ["[1, 2]", '"text"', "7"])
def test_object_from_line_rejects_json_that_is_not_an_object(line: str):
    # Given
    expected_message = r"^expected a JSON object per line, got "

    # When
    with pytest.raises(ValueError, match=expected_message):
        object_from_line(line)

    # Then


def test_object_from_line_rejects_invalid_json():
    # Given
    line = "{not json"

    # When
    with pytest.raises(ValueError, match="Expecting"):
        object_from_line(line)

    # Then


async def test_run_yields_one_object_per_nonblank_line(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(stdout='{"n": 11}\n\n{"n": 12}\n{"n": 13}\n')

    # When
    objects = [output async for output in run("peek")]

    # Then
    assert objects == [{"n": 11}, {"n": 12}, {"n": 13}]


async def test_run_passes_the_mapped_arguments_to_the_binary(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl(stdout='{"n": 1}\n')

    # When
    objects = [
        output
        async for output in run(
            "peek", namespace="contoso-test", payload_type_id="Z", dlq=True, queue=None
        )
    ]

    # Then
    assert objects == [{"n": 1}]
    assert fake.read_arguments() == [
        "peek", "--namespace", "contoso-test", "--payload-type-id", "Z", "--dlq",
        "--output", "json",
    ]  # fmt: skip


async def test_run_yields_the_first_object_before_the_binary_exits(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(stdout='{"n": 21}\n{"n": 22}\n', lines_before_sleep=1, sleep_seconds=60)
    started_at = time.monotonic()

    # When
    async with contextlib.aclosing(run("peek", timeout_seconds=30)) as outputs:
        first = await anext(outputs)

    # Then
    assert first == {"n": 21}
    assert time.monotonic() - started_at < 20


async def test_run_stops_the_process_when_the_iterator_is_closed(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl(stdout='{"n": 1}\n', lines_before_sleep=1, sleep_seconds=60)

    # When
    async with contextlib.aclosing(run("peek")) as outputs:
        await anext(outputs)

    # Then
    assert not fake.is_running()


async def test_run_raises_a_timeout_error_and_stops_the_process_when_the_run_is_too_slow(
    fake_asbctl: FakeFactory,
):
    # Given
    fake = fake_asbctl(stdout='{"n": 31}\n', lines_before_sleep=1, sleep_seconds=60)
    outputs = run("peek", timeout_seconds=0.7)

    # When
    first = await anext(outputs)
    with pytest.raises(TimeoutError):
        await anext(outputs)

    # Then
    assert first == {"n": 31}
    assert not fake.is_running()


async def test_run_raises_the_json_error_from_stderr_on_a_nonzero_exit(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(
        stderr='{"schema":1,"error":{"code":"peek_failed","message":"service unavailable"}}\n',
        exit_code=4,
    )

    # When
    with pytest.raises(AsbctlError) as raised:
        [output async for output in run("peek")]

    # Then
    assert (raised.value.code, raised.value.message, raised.value.exit_code) == (
        "peek_failed", "service unavailable", 4,
    )  # fmt: skip


async def test_run_yields_what_was_printed_before_a_nonzero_exit(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(
        stdout='{"n": 41}\n',
        stderr='{"schema":1,"error":{"code":"canceled","message":"stopped"}}',
        exit_code=3,
    )
    outputs = run("peek")

    # When
    first = await anext(outputs)
    with pytest.raises(AsbctlError) as raised:
        await anext(outputs)

    # Then
    assert first == {"n": 41}
    assert raised.value.exit_code == 3


async def test_run_keeps_the_raw_stderr_text_when_it_is_not_json(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(stderr="asbctl: unknown command\n", exit_code=2)

    # When
    with pytest.raises(AsbctlError) as raised:
        [output async for output in run("peek")]

    # Then
    assert (raised.value.code, raised.value.message) == ("unknown", "asbctl: unknown command")


async def test_run_raises_binary_not_found_when_the_configured_binary_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    # Given
    monkeypatch.setenv("ASBCTL_BIN", str(tmp_path / "missing"))

    # When
    with pytest.raises(BinaryNotFoundError, match="missing"):
        [output async for output in run("peek")]

    # Then


async def test_run_raises_binary_not_found_when_asbctl_is_not_on_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    # Given
    monkeypatch.delenv("ASBCTL_BIN", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))

    # When
    with pytest.raises(BinaryNotFoundError, match="ASBCTL_BIN"):
        [output async for output in run("peek")]

    # Then


async def test_run_finds_the_binary_on_path_when_it_is_not_configured(
    fake_asbctl: FakeFactory, monkeypatch: pytest.MonkeyPatch
):
    # Given
    fake = fake_asbctl(stdout='{"n": 51}\n')
    monkeypatch.delenv("ASBCTL_BIN")
    monkeypatch.setenv("PATH", str(fake.path.parent))

    # When
    objects = [output async for output in run("peek")]

    # Then
    assert objects == [{"n": 51}]


async def test_run_writes_a_sync_iterable_of_lines_to_stdin(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl(is_stdin_read=True)

    # When
    [output async for output in run("send", stdin_lines=iter(["first", "second"]))]

    # Then
    assert fake.read_stdin() == "first\nsecond\n"


async def test_run_writes_an_async_iterable_of_lines_to_stdin(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl(is_stdin_read=True)

    async def lines():
        yield "alpha"
        yield "beta"

    # When
    [output async for output in run("send", stdin_lines=lines())]

    # Then
    assert fake.read_stdin() == "alpha\nbeta\n"


async def test_run_reports_the_exit_code_when_the_binary_stops_reading_stdin(
    fake_asbctl: FakeFactory,
):
    # Given
    fake_asbctl(stderr="boom", exit_code=1)
    many_lines = ("x" * 1000 for _ in range(5000))

    # When
    with pytest.raises(AsbctlError) as raised:
        [output async for output in run("send", stdin_lines=many_lines)]

    # Then
    assert raised.value.exit_code == 1


async def test_run_raises_the_error_of_a_failing_stdin_iterable_unwrapped(
    fake_asbctl: FakeFactory,
):
    # Given
    fake_asbctl(is_stdin_read=True)

    def lines():
        yield "first"
        raise KeyError("no such line")

    # When
    with pytest.raises(KeyError) as raised:
        [output async for output in run("send", stdin_lines=lines())]

    # Then
    assert raised.value.args == ("no such line",)


async def test_run_once_returns_the_single_object(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(stdout='{"total": 61}\n')

    # When
    output = await run_once("dlq", "summary")

    # Then
    assert output == {"total": 61}


@pytest.mark.parametrize("stdout", ["", '{"n": 1}\n{"n": 2}\n'], ids=["none", "two"])
async def test_run_once_rejects_a_run_that_prints_no_object_or_several(
    fake_asbctl: FakeFactory, stdout: str
):
    # Given
    fake_asbctl(stdout=stdout)

    # When
    with pytest.raises(ValueError, match="expected one JSON object"):
        await run_once("dlq", "summary")

    # Then


def test_object_from_line_names_the_type_it_got_instead_of_an_object():
    # Given
    line = "[1, 2]"

    # When
    with pytest.raises(ValueError, match=r"got list$"):
        object_from_line(line)

    # Then


def test_error_from_stderr_skips_a_trailing_line_that_is_not_json():
    # Given
    stderr_text = (
        '{"schema":1,"error":{"code":"canceled","message":"stopped after 4 of 9"}}\n'
        "goroutine 1 [running]\n"
    )

    # When
    error = error_from_stderr(stderr_text, 1)

    # Then
    assert (error.code, error.message) == ("canceled", "stopped after 4 of 9")


async def test_run_gives_the_binary_an_empty_stdin_when_there_are_no_lines(
    fake_asbctl: FakeFactory,
):
    # Given
    fake = fake_asbctl(is_stdin_read=True)

    # When
    [output async for output in run("peek", timeout_seconds=20)]

    # Then
    assert fake.read_stdin() == ""


async def test_run_once_applies_the_timeout(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(stdout='{"n": 1}\n', sleep_seconds=60)

    # When
    with pytest.raises(TimeoutError):
        await run_once("dlq", "summary", timeout_seconds=0.7)

    # Then


async def test_run_once_forwards_the_flags_and_the_stdin_lines(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl(stdout='{"n": 1}\n', is_stdin_read=True)

    # When
    await run_once("send", stdin_lines=["line-1"], queue="q-1")

    # Then
    assert fake.read_arguments() == ["send", "--queue", "q-1", "--output", "json"]
    assert fake.read_stdin() == "line-1\n"


class FakeProcess:
    returncode = None

    def __init__(self) -> None:
        self.has_waited = False

    def kill(self) -> None:
        raise ProcessLookupError

    async def wait(self) -> int:
        self.has_waited = True
        return 0


async def test_stop_process_waits_for_a_process_that_exited_before_it_could_be_killed():
    # Given
    process = FakeProcess()

    # When
    await _stop_process(cast("asyncio.subprocess.Process", process))

    # Then
    assert process.has_waited


class FakeStreamWriter:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.is_closed = False

    def write(self, data: bytes) -> None:
        self.data = data

    async def drain(self) -> None:
        raise self.error

    def close(self) -> None:
        self.is_closed = True

    async def wait_closed(self) -> None:
        return None


@pytest.mark.parametrize(
    "error", [BrokenPipeError(), ConnectionResetError()], ids=["broken-pipe", "connection-reset"]
)
async def test_write_lines_stops_quietly_and_closes_the_stream_when_the_process_has_gone(
    error: Exception,
):
    # Given
    writer = FakeStreamWriter(error)

    # When
    await _write_lines(cast("asyncio.StreamWriter", writer), ["first"])

    # Then
    assert writer.is_closed
    assert writer.data == b"first\n"


async def test_read_text_replaces_bytes_that_are_not_utf8():
    # Given
    reader = asyncio.StreamReader()
    reader.feed_data(b"ok \xff")
    reader.feed_eof()

    # When
    text = await _read_text(reader)

    # Then
    assert text == "ok \ufffd"


async def test_run_reads_an_output_line_longer_than_the_default_stream_limit(
    fake_asbctl: FakeFactory,
):
    # Given
    body = "A" * (1536 * 1024)
    fake_asbctl(stdout=json.dumps({"schema": 1, "body": body}) + "\n")

    # When
    objects = [output async for output in run("peek")]

    # Then
    assert objects == [{"schema": 1, "body": body}]


async def test_run_keeps_a_long_stderr_text_when_the_binary_fails(fake_asbctl: FakeFactory):
    # Given
    long_text = "x" * (1536 * 1024)
    fake_asbctl(stderr=long_text, exit_code=1)

    # When
    with pytest.raises(AsbctlError) as raised:
        [output async for output in run("peek")]

    # Then
    assert raised.value.message == long_text


async def test_stream_lines_raises_a_plain_value_error_naming_the_limit_for_an_oversize_line(
    fake_asbctl: FakeFactory,
):
    # Given
    fake = fake_asbctl(stdout="x" * 5000 + "\n", sleep_seconds=0)

    # When
    with pytest.raises(ValueError, match=r"^an output line is longer than 1000 bytes$") as raised:
        [line async for line in _stream_lines(str(fake.path), ["peek"], None, 20, 1000)]

    # Then
    assert type(raised.value) is ValueError
    assert not fake.is_running()

import contextlib
import stat
import time
from pathlib import Path
from typing import Any

import pytest

from asbctl.core import (
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
    with pytest.raises(ValueError, match="always json"):
        build_arguments("peek", (), flags)

    # Then


def test_build_arguments_rejects_a_flag_value_of_an_unsupported_type():
    # Given
    flags: dict[str, Any] = {"queues": ["a", "b"]}

    # When
    with pytest.raises(TypeError, match="queues"):
        build_arguments("peek", (), flags)

    # Then


def test_get_binary_path_prefers_the_configured_binary():
    # Given
    environment = {"ASBCTL_BIN": "/opt/custom/asbctl", "PATH": "/usr/bin"}

    # When
    binary_path = get_binary_path(environment)

    # Then
    assert binary_path == "/opt/custom/asbctl"


def test_get_binary_path_finds_asbctl_on_path(tmp_path: Path):
    # Given
    executable = tmp_path / "asbctl"
    executable.write_text("#!/bin/sh\n")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)

    # When
    binary_path = get_binary_path({"PATH": str(tmp_path)})

    # Then
    assert binary_path == str(executable)


def test_get_binary_path_raises_when_asbctl_is_nowhere(tmp_path: Path):
    # Given
    environment = {"PATH": str(tmp_path)}

    # When
    with pytest.raises(BinaryNotFoundError, match="ASBCTL_BIN"):
        get_binary_path(environment)

    # Then


def test_get_binary_path_ignores_an_empty_configured_binary(tmp_path: Path):
    # Given
    environment = {"ASBCTL_BIN": "", "PATH": str(tmp_path)}

    # When
    with pytest.raises(BinaryNotFoundError):
        get_binary_path(environment)

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
    expected_message = "JSON object"

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

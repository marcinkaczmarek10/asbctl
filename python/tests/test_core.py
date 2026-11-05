import subprocess
import time
from typing import Any

import pytest

from asbctl import AsbctlError, run
from asbctl.core import build_arguments, error_from_stderr

from .conftest import FakeFactory


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ({}, ["peek", "--output", "json"]),
        (
            {"payload_type_id": "OrderPlaced"},
            ["peek", "--payload-type-id", "OrderPlaced", "--output", "json"],
        ),
        ({"dlq": True}, ["peek", "--dlq", "--output", "json"]),
        ({"dlq": False}, ["peek", "--output", "json"]),
        ({"queue": None}, ["peek", "--output", "json"]),
        (
            {"max": 5, "from_sequence": 0},
            ["peek", "--max", "5", "--from-sequence", "0", "--output", "json"],
        ),
        ({"ratio": 1.5}, ["peek", "--ratio", "1.5", "--output", "json"]),
    ],
    ids=[
        "no flags",
        "string value",
        "true is bare",
        "false is omitted",
        "none is omitted",
        "zero is kept",
        "float",
    ],
)
def test_flags_map_to_arguments(flags, expected):
    assert build_arguments("peek", (), flags) == expected


def test_subcommands_come_before_flags():
    assert build_arguments("dlq", ("summary",), {"queue": "q"}) == [
        "dlq", "summary", "--queue", "q", "--output", "json",
    ]  # fmt: skip


def test_an_output_flag_is_rejected_because_it_is_always_json():
    with pytest.raises(ValueError, match="always json"):
        build_arguments("peek", (), {"output": "table"})


def test_an_unsupported_flag_value_is_rejected():
    flags: dict[str, Any] = {"queues": ["a", "b"]}
    with pytest.raises(TypeError, match="queues"):
        build_arguments("peek", (), flags)


def test_run_passes_the_mapped_arguments_to_the_binary(fake_asbctl: FakeFactory):
    fake = fake_asbctl(stdout='{"a": 1}\n')

    list(run("peek", namespace="contoso", payload_type_id="X", dlq=True, queue=None))

    assert fake.arguments() == [
        "peek",
        "--namespace",
        "contoso",
        "--payload-type-id",
        "X",
        "--dlq",
        "--output",
        "json",
    ]


def test_run_yields_one_object_per_line(fake_asbctl: FakeFactory):
    fake_asbctl(stdout='{"n": 1}\n\n{"n": 2}\n{"n": 3}\n')

    assert list(run("peek")) == [{"n": 1}, {"n": 2}, {"n": 3}]


def test_run_streams_the_first_line_before_the_binary_exits(fake_asbctl: FakeFactory):
    fake_asbctl(stdout='{"n": 1}\n{"n": 2}\n', lines_before_sleep=1, sleep_seconds=60)
    records = run("peek", timeout=30)

    started = time.monotonic()
    first = next(records)
    records.close()

    assert first == {"n": 1}
    assert time.monotonic() - started < 20


def test_run_rejects_a_line_that_is_not_an_object(fake_asbctl: FakeFactory):
    fake_asbctl(stdout="[1, 2]\n")

    with pytest.raises(ValueError, match="JSON object"):
        list(run("peek"))


def test_a_failed_run_raises_the_json_error_from_stderr(fake_asbctl: FakeFactory):
    fake_asbctl(
        stderr=(
            'time=2026 level=WARN msg="slow"\n'
            '{"schema":1,"error":{"code":"peek_failed","message":"service unavailable"}}\n'
        ),
        exit_code=1,
    )

    with pytest.raises(AsbctlError) as raised:
        list(run("peek"))

    assert (raised.value.code, raised.value.message, raised.value.exit_code) == (
        "peek_failed", "service unavailable", 1,
    )  # fmt: skip


def test_a_failed_run_keeps_the_objects_it_already_yielded(fake_asbctl: FakeFactory):
    fake_asbctl(
        stdout='{"n": 1}\n',
        stderr='{"schema":1,"error":{"code":"canceled","message":"x"}}',
        exit_code=3,
    )
    records = run("peek")

    assert next(records) == {"n": 1}
    with pytest.raises(AsbctlError) as raised:
        next(records)
    assert raised.value.exit_code == 3


def test_stderr_that_is_not_json_becomes_the_message(fake_asbctl: FakeFactory):
    fake_asbctl(
        stderr="asbctl: flag provided but not defined: -nope\nrun 'asbctl peek -h' for usage\n",
        exit_code=2,
    )

    with pytest.raises(AsbctlError) as raised:
        list(run("peek"))

    assert raised.value.code == "unknown"
    assert raised.value.exit_code == 2
    assert "flag provided but not defined" in raised.value.message


@pytest.mark.parametrize("stderr", ["", '{"error": "text"}', '{"error": {"code": "x"}}', "[1]"])
def test_stderr_without_a_code_and_message_falls_back_to_raw_text(stderr):
    error = error_from_stderr(stderr, 1)

    assert (error.code, error.message) == ("unknown", stderr.strip())


def test_a_run_that_takes_longer_than_the_timeout_is_killed(fake_asbctl: FakeFactory):
    fake_asbctl(stdout='{"n": 1}\n', lines_before_sleep=1, sleep_seconds=60)
    records = run("peek", timeout=1)

    assert next(records) == {"n": 1}
    with pytest.raises(subprocess.TimeoutExpired):
        next(records)


def test_a_missing_binary_on_path_is_a_clear_error(monkeypatch, tmp_path):
    monkeypatch.delenv("ASBCTL_BIN", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="ASBCTL_BIN"):
        list(run("peek"))


def test_an_asbctl_bin_that_does_not_exist_is_an_error(monkeypatch, tmp_path):
    monkeypatch.setenv("ASBCTL_BIN", str(tmp_path / "missing"))

    with pytest.raises(FileNotFoundError):
        list(run("peek"))


def test_the_binary_on_path_is_used_when_asbctl_bin_is_unset(fake_asbctl: FakeFactory, monkeypatch):
    fake = fake_asbctl(stdout='{"n": 1}\n')
    monkeypatch.delenv("ASBCTL_BIN")
    monkeypatch.setenv("PATH", str(fake.path.parent))

    assert list(run("peek")) == [{"n": 1}]

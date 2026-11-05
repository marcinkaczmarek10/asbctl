import json
import os
import stat
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

import pytest

CONTRACT_DIRECTORY = Path(__file__).resolve().parents[2] / "testdata" / "contract"

FAKE_SCRIPT = textwrap.dedent(
    """\
    #!{python}
    import json
    import os
    import sys
    import time

    config = json.loads({config!r})
    with open(config["pid_path"], "w") as pid_file:
        pid_file.write(str(os.getpid()))
    with open(config["arguments_path"], "w") as arguments_file:
        json.dump(sys.argv[1:], arguments_file)
    if config["is_stdin_read"]:
        with open(config["stdin_path"], "w") as stdin_file:
            stdin_file.write(sys.stdin.read())
    lines = config["stdout"].splitlines(keepends=True)
    for line in lines[: config["lines_before_sleep"]]:
        sys.stdout.write(line)
        sys.stdout.flush()
    time.sleep(config["sleep_seconds"])
    for line in lines[config["lines_before_sleep"] :]:
        sys.stdout.write(line)
    sys.stderr.write(config["stderr"])
    sys.exit(config["exit_code"])
    """
)


class FakeAsbctl(NamedTuple):
    """A fake asbctl executable that records how it was called."""

    path: Path
    arguments_path: Path
    stdin_path: Path
    pid_path: Path

    def read_arguments(self) -> list[str]:
        return json.loads(self.arguments_path.read_text())

    def read_stdin(self) -> str:
        return self.stdin_path.read_text()

    def is_running(self) -> bool:
        try:
            os.kill(int(self.pid_path.read_text()), 0)
        except ProcessLookupError:
            return False
        return True


type FakeFactory = Callable[..., FakeAsbctl]


@pytest.fixture
def fake_asbctl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeFactory:
    def build(
        stdout: str = "",
        stderr: str = "",
        exit_code: int = 0,
        lines_before_sleep: int = 0,
        sleep_seconds: float = 0,
        is_stdin_read: bool = False,
    ) -> FakeAsbctl:
        fake = FakeAsbctl(
            path=tmp_path / "asbctl",
            arguments_path=tmp_path / "arguments.json",
            stdin_path=tmp_path / "stdin.ndjson",
            pid_path=tmp_path / "pid",
        )
        config = {
            "arguments_path": str(fake.arguments_path),
            "stdin_path": str(fake.stdin_path),
            "pid_path": str(fake.pid_path),
            "is_stdin_read": is_stdin_read,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "lines_before_sleep": lines_before_sleep,
            "sleep_seconds": sleep_seconds,
        }
        fake.path.write_text(FAKE_SCRIPT.format(python=sys.executable, config=json.dumps(config)))
        fake.path.chmod(fake.path.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("ASBCTL_BIN", str(fake.path))
        return fake

    return build


@pytest.fixture
def read_contract() -> Callable[[str], str]:
    def read(name: str) -> str:
        return (CONTRACT_DIRECTORY / name).read_text()

    return read

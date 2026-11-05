import json
import stat
import sys
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "testdata" / "contract"

FAKE_SCRIPT = textwrap.dedent(
    """\
    #!{python}
    import json
    import sys
    import time

    config = json.loads({config!r})
    with open(config["arguments_path"], "w") as arguments_file:
        json.dump(sys.argv[1:], arguments_file)
    if config["read_stdin"]:
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


@dataclass(frozen=True)
class FakeAsbctl:
    """A fake asbctl executable. arguments() returns what it was called with."""

    path: Path
    arguments_path: Path
    stdin_path: Path

    def arguments(self) -> list[str]:
        return json.loads(self.arguments_path.read_text())

    def stdin(self) -> str:
        """What the fake read on stdin, recorded only when it was built with read_stdin."""
        return self.stdin_path.read_text()


type FakeFactory = Callable[..., FakeAsbctl]


@pytest.fixture
def fake_asbctl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeFactory:
    """Write a fake asbctl into tmp_path and point ASBCTL_BIN at it."""

    def build(
        stdout: str = "",
        stderr: str = "",
        exit_code: int = 0,
        lines_before_sleep: int = 0,
        sleep_seconds: float = 0,
        read_stdin: bool = False,
    ) -> FakeAsbctl:
        arguments_path = tmp_path / "arguments.json"
        stdin_path = tmp_path / "stdin.ndjson"
        config = {
            "arguments_path": str(arguments_path),
            "stdin_path": str(stdin_path),
            "read_stdin": read_stdin,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "lines_before_sleep": lines_before_sleep,
            "sleep_seconds": sleep_seconds,
        }
        path = tmp_path / "asbctl"
        path.write_text(FAKE_SCRIPT.format(python=sys.executable, config=json.dumps(config)))
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("ASBCTL_BIN", str(path))
        return FakeAsbctl(path=path, arguments_path=arguments_path, stdin_path=stdin_path)

    return build


@pytest.fixture
def contract() -> Callable[[str], str]:
    """Read a file of example output from testdata/contract."""

    def read(name: str) -> str:
        return (CONTRACT_DIR / name).read_text()

    return read

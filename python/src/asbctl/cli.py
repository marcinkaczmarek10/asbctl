"""The asbctl command that a platform wheel installs. It runs the bundled binary."""

import os
import subprocess
import sys
from pathlib import Path

from asbctl.bundled import find_bundled_binary_path


def main() -> None:
    """Run the bundled binary with the arguments of this command and exit with its code."""
    binary_path = find_bundled_binary_path(Path(__file__).parent, sys.platform)
    if binary_path is None:
        sys.exit("asbctl: this installation has no binary, install a platform wheel")

    arguments = [str(binary_path), *sys.argv[1:]]
    # The arguments are the ones the user typed, passed to their own bundled binary.
    if sys.platform == "win32":
        sys.exit(subprocess.run(arguments, check=False).returncode)  # noqa: S603
    os.execv(binary_path, arguments)  # noqa: S606

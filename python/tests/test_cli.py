import subprocess
import sys


def test_main_exits_with_a_message_when_the_installation_has_no_binary():
    # Given
    command = [sys.executable, "-c", "from asbctl.cli import main; main()"]

    # When
    completed = subprocess.run(command, capture_output=True, text=True, check=False)

    # Then
    assert completed.returncode == 1
    assert "this installation has no binary" in completed.stderr

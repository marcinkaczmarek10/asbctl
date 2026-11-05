from pathlib import Path

import pytest

from asbctl.bundled import find_bundled_binary_path, get_bundled_binary_name


@pytest.mark.parametrize(
    ("platform", "expected"),
    [("win32", "asbctl.exe"), ("linux", "asbctl"), ("darwin", "asbctl")],
)
def test_get_bundled_binary_name_adds_exe_only_on_windows(platform: str, expected: str):
    # Given
    platform_name = platform

    # When
    name = get_bundled_binary_name(platform_name)

    # Then
    assert name == expected


def test_find_bundled_binary_path_returns_the_binary_in_the_bin_directory(tmp_path: Path):
    # Given
    binary_path = tmp_path / "bin" / "asbctl"
    binary_path.parent.mkdir()
    binary_path.write_text("x")

    # When
    found_path = find_bundled_binary_path(tmp_path, "linux")

    # Then
    assert found_path == binary_path


def test_find_bundled_binary_path_looks_for_the_exe_on_windows(tmp_path: Path):
    # Given
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "asbctl").write_text("x")
    (tmp_path / "bin" / "asbctl.exe").write_text("x")

    # When
    found_path = find_bundled_binary_path(tmp_path, "win32")

    # Then
    assert found_path == tmp_path / "bin" / "asbctl.exe"


def test_find_bundled_binary_path_returns_none_when_the_wheel_has_no_binary(tmp_path: Path):
    # Given
    empty_directory = tmp_path

    # When
    found_path = find_bundled_binary_path(empty_directory, "linux")

    # Then
    assert found_path is None


def test_find_bundled_binary_path_ignores_a_directory_with_the_binary_name(tmp_path: Path):
    # Given
    (tmp_path / "bin" / "asbctl").mkdir(parents=True)

    # When
    found_path = find_bundled_binary_path(tmp_path, "linux")

    # Then
    assert found_path is None

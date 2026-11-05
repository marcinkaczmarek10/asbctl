"""Find the asbctl binary that a platform wheel installs inside the package."""

from pathlib import Path

BUNDLED_DIRECTORY_NAME = "bin"


def get_bundled_binary_name(platform: str) -> str:
    """Return the file name of the bundled binary on a platform, as sys.platform names it."""
    return "asbctl.exe" if platform == "win32" else "asbctl"


def find_bundled_binary_path(package_directory: Path, platform: str) -> Path | None:
    """Return the bundled binary in the package directory, or None when the wheel has none."""
    binary_path = package_directory / BUNDLED_DIRECTORY_NAME / get_bundled_binary_name(platform)
    return binary_path if binary_path.is_file() else None

"""Build the asbctl wheels that carry the Go binary. It uses only the standard library.

A platform wheel holds the package and the binary at asbctl/bin/asbctl, and installs an asbctl
command that runs it. The wheel tagged "any" holds the package alone.

Usage:
    build_wheel.py --out DIR --artifacts dist/artifacts.json [--version 1.2.3]
    build_wheel.py --out DIR --binary PATH --tag macosx_11_0_arm64 [--version 1.2.3]
    build_wheel.py --version v1.2.3-rc.1 --print-version

The paths in artifacts.json are relative to the directory goreleaser ran in, so run the script from
there.
"""

import argparse
import base64
import hashlib
import json
import re
import tomllib
import zipfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NamedTuple

PACKAGE_NAME = "asbctl"
PURE_TAG = "any"

# One Go binary is static, so a linux binary serves both libc families.
PLATFORM_TAGS: dict[tuple[str, str], tuple[str, ...]] = {
    ("darwin", "arm64"): ("macosx_11_0_arm64",),
    ("darwin", "amd64"): ("macosx_10_12_x86_64",),
    ("linux", "amd64"): ("manylinux_2_17_x86_64.manylinux2014_x86_64", "musllinux_1_2_x86_64"),
    ("linux", "arm64"): ("manylinux_2_17_aarch64.manylinux2014_aarch64", "musllinux_1_2_aarch64"),
    ("windows", "amd64"): ("win_amd64",),
}

_VERSION_PATTERN = re.compile(r"^v?(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.(\d+))?$")
_PRERELEASE_MARKERS = {"alpha": "a", "beta": "b", "rc": "rc"}
_FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)


class BinaryArtifact(NamedTuple):
    """A binary that goreleaser built."""

    goos: str
    goarch: str
    path: Path


def normalize_version(text: str) -> str:
    """Turn a release tag such as v1.2.3 or v1.2.3-rc.1 into a PEP 440 version.

    Raises:
        ValueError: The text is not X.Y.Z with an optional alpha, beta or rc suffix.
    """
    match = _VERSION_PATTERN.match(text)
    if match is None:
        raise ValueError(f"{text!r} is not a version like 1.2.3 or v1.2.3-rc.1")
    release, marker, number = match.groups()
    return release if marker is None else f"{release}{_PRERELEASE_MARKERS[marker]}{number}"


def build_wheel_filename(version: str, tag: str) -> str:
    """Return the file name of the wheel for a platform tag, or PURE_TAG."""
    return f"{PACKAGE_NAME}-{version}-py3-none-{tag}.whl"


def build_metadata_text(project: Mapping[str, Any], readme_text: str, version: str) -> str:
    """Build the METADATA file from the [project] table of pyproject.toml.

    The license-files of the project are file names, not globs.
    """
    lines = [
        "Metadata-Version: 2.4",
        f"Name: {project['name']}",
        f"Version: {version}",
        f"Summary: {project['description']}",
        "Description-Content-Type: text/markdown",
        f"Requires-Python: {project['requires-python']}",
        f"License-Expression: {project['license']}",
    ]
    lines.extend(f"License-File: {name}" for name in project["license-files"])
    lines.extend(f"Author: {author['name']}" for author in project.get("authors", []))
    return "\n".join(lines) + "\n\n" + readme_text


def build_wheel_text(tag: str) -> str:
    """Build the WHEEL file. A wheel with a binary is not pure Python."""
    is_pure = tag == PURE_TAG
    return (
        "Wheel-Version: 1.0\n"
        "Generator: asbctl build_wheel\n"
        f"Root-Is-Purelib: {'true' if is_pure else 'false'}\n"
        f"Tag: py3-none-{tag}\n"
    )


def build_entry_points_text() -> str:
    """Build entry_points.txt, which gives a platform wheel its asbctl command."""
    return f"[console_scripts]\n{PACKAGE_NAME} = {PACKAGE_NAME}.cli:main\n"


def build_record_text(files: Mapping[str, bytes], record_path: str) -> str:
    """Build RECORD: the sha256 and size of every file, and RECORD itself without a hash."""
    lines = [
        f"{path},sha256={_digest_of(data)},{len(data)}" for path, data in sorted(files.items())
    ]
    lines.append(f"{record_path},,")
    return "\n".join(lines) + "\n"


def read_binary_artifacts(artifacts_path: Path) -> list[BinaryArtifact]:
    """Read the binaries from the artifacts.json that goreleaser writes next to its output."""
    artifacts = json.loads(artifacts_path.read_text())
    return [
        BinaryArtifact(artifact["goos"], artifact["goarch"], Path(artifact["path"]))
        for artifact in artifacts
        if artifact["type"] == "Binary"
    ]


def build_wheel(
    *,
    project_directory: Path,
    version: str,
    tag: str,
    binary_path: Path | None,
    output_directory: Path,
) -> Path:
    """Write one wheel and return its path.

    Args:
        project_directory: The directory with pyproject.toml, README.md, license-files and src.
        version: A PEP 440 version.
        tag: A platform tag, or PURE_TAG.
        binary_path: The binary to bundle, or None for a wheel without one.
        output_directory: Where to write the wheel.
    """
    project = tomllib.loads((project_directory / "pyproject.toml").read_text())["project"]
    dist_info = f"{PACKAGE_NAME}-{version}.dist-info"
    files = _read_package_files(project_directory / "src" / PACKAGE_NAME)
    modes = dict.fromkeys(files, 0o644)

    if binary_path is not None:
        binary_entry = f"{PACKAGE_NAME}/bin/{binary_path.name}"
        files[binary_entry] = binary_path.read_bytes()
        modes[binary_entry] = 0o755
        files[f"{dist_info}/entry_points.txt"] = build_entry_points_text().encode()

    files[f"{dist_info}/METADATA"] = build_metadata_text(
        project, (project_directory / "README.md").read_text(), version
    ).encode()
    files[f"{dist_info}/WHEEL"] = build_wheel_text(tag).encode()
    for license_name in project["license-files"]:
        files[f"{dist_info}/licenses/{license_name}"] = (
            project_directory / license_name
        ).read_bytes()
    record_path = f"{dist_info}/RECORD"
    files[record_path] = build_record_text(files, record_path).encode()

    output_directory.mkdir(parents=True, exist_ok=True)
    wheel_path = output_directory / build_wheel_filename(version, tag)
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path, data in files.items():
            entry = zipfile.ZipInfo(path, _FIXED_DATE_TIME)
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = (0o100000 | modes.get(path, 0o644)) << 16
            archive.writestr(entry, data)
    return wheel_path


def build_all_wheels(
    *, project_directory: Path, version: str, binaries: list[BinaryArtifact], output_directory: Path
) -> list[Path]:
    """Write a wheel for every tag of every binary that has one, and the wheel without a binary."""
    wheel_paths = [
        build_wheel(
            project_directory=project_directory,
            version=version,
            tag=PURE_TAG,
            binary_path=None,
            output_directory=output_directory,
        )
    ]
    for binary in binaries:
        for tag in PLATFORM_TAGS.get((binary.goos, binary.goarch), ()):
            wheel_paths.append(
                build_wheel(
                    project_directory=project_directory,
                    version=version,
                    tag=tag,
                    binary_path=binary.path,
                    output_directory=output_directory,
                )
            )
    return wheel_paths


def main(argv: list[str] | None = None) -> int:
    """Parse the command line, build the wheels and print their paths."""
    parser = argparse.ArgumentParser(description="Build asbctl wheels that carry the Go binary.")
    parser.add_argument("--out", type=Path, help="directory for the wheels")
    parser.add_argument(
        "--version", help="release tag or version, default the one in pyproject.toml"
    )
    parser.add_argument("--artifacts", type=Path, help="goreleaser artifacts.json")
    parser.add_argument("--binary", type=Path, help="one binary to bundle")
    parser.add_argument("--tag", help="platform tag for --binary, or any")
    parser.add_argument(
        "--print-version", action="store_true", help="print the PEP 440 version and build nothing"
    )
    arguments = parser.parse_args(argv)

    project_directory = Path(__file__).resolve().parent.parent
    version = _resolve_version(arguments.version, project_directory)

    if arguments.print_version:
        print(version)
        return 0
    if arguments.out is None:
        parser.error("pass --out, or --print-version")

    if arguments.artifacts is not None:
        wheel_paths = build_all_wheels(
            project_directory=project_directory,
            version=version,
            binaries=read_binary_artifacts(arguments.artifacts),
            output_directory=arguments.out,
        )
    elif arguments.binary is not None and arguments.tag is not None:
        wheel_paths = [
            build_wheel(
                project_directory=project_directory,
                version=version,
                tag=arguments.tag,
                binary_path=arguments.binary,
                output_directory=arguments.out,
            )
        ]
    else:
        parser.error("pass --artifacts, or --binary with --tag")

    for wheel_path in wheel_paths:
        print(wheel_path)
    return 0


def _resolve_version(requested: str | None, project_directory: Path) -> str:
    if requested is not None:
        return normalize_version(requested)
    project = tomllib.loads((project_directory / "pyproject.toml").read_text())["project"]
    return str(project["version"])


def _digest_of(data: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")


def _read_package_files(package_directory: Path) -> dict[str, bytes]:
    return {
        f"{PACKAGE_NAME}/{path.relative_to(package_directory).as_posix()}": path.read_bytes()
        for path in sorted(package_directory.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    }


if __name__ == "__main__":
    raise SystemExit(main())

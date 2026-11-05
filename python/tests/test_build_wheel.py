import base64
import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from build_wheel import (
    PURE_TAG,
    BinaryArtifact,
    build_all_wheels,
    build_entry_points_text,
    build_metadata_text,
    build_record_text,
    build_wheel,
    build_wheel_filename,
    build_wheel_text,
    main,
    normalize_version,
    read_binary_artifacts,
)

PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent
FAKE_BINARY = b'#!/bin/sh\necho "{\\"fake\\": \\"$*\\"}"\n'


def digest_of(data: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


@pytest.fixture
def fake_binary(tmp_path: Path) -> Path:
    binary_path = tmp_path / "built" / "asbctl"
    binary_path.parent.mkdir()
    binary_path.write_bytes(FAKE_BINARY)
    return binary_path


@pytest.fixture
def small_project(tmp_path: Path) -> Path:
    project_directory = tmp_path / "project"
    package_directory = project_directory / "src" / "asbctl"
    (package_directory / "__pycache__").mkdir(parents=True)
    (package_directory / "sub").mkdir()
    (package_directory / "__init__.py").write_text("")
    (package_directory / "sub" / "module.py").write_text("x = 1\n")
    (package_directory / "__pycache__" / "module.cpython-313.pyc").write_bytes(b"pyc")
    (project_directory / "pyproject.toml").write_text(
        '[project]\nname = "asbctl"\nversion = "0.0.1"\ndescription = "d"\n'
        'requires-python = ">=3.13"\nlicense = "MIT"\n'
        'license-files = ["LICENSE", "THIRD_PARTY_LICENSES"]\n'
    )
    (project_directory / "README.md").write_text("# small\n")
    (project_directory / "LICENSE").write_text("MIT text\n")
    (project_directory / "THIRD_PARTY_LICENSES").write_text("notices\n")
    return project_directory


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("v1.2.3", "1.2.3"),
        ("4.5.6", "4.5.6"),
        ("v1.2.3-rc.1", "1.2.3rc1"),
        ("v0.9.0-beta.12", "0.9.0b12"),
        ("v2.0.0-alpha.3", "2.0.0a3"),
    ],
)
def test_normalize_version_turns_a_tag_into_a_pep_440_version(text: str, expected: str):
    # Given
    tag = text

    # When
    version = normalize_version(tag)

    # Then
    assert version == expected


@pytest.mark.parametrize("text", ["1.2", "v1.2.3-snapshot", "v1.2.3.4", "latest", "", "v1.2.3-rc"])
def test_normalize_version_rejects_text_that_is_not_a_release_version(text: str):
    # Given
    tag = text

    # When
    with pytest.raises(ValueError, match="is not a version"):
        normalize_version(tag)

    # Then


def test_build_wheel_filename_names_the_package_version_and_tag():
    # Given
    version, tag = "3.4.5", "macosx_11_0_arm64"

    # When
    filename = build_wheel_filename(version, tag)

    # Then
    assert filename == "asbctl-3.4.5-py3-none-macosx_11_0_arm64.whl"
    assert build_wheel_filename(version, PURE_TAG) == "asbctl-3.4.5-py3-none-any.whl"


def test_build_wheel_text_marks_a_platform_wheel_as_not_pure():
    # Given
    tag = "win_amd64"

    # When
    text = build_wheel_text(tag)

    # Then
    assert text.splitlines() == [
        "Wheel-Version: 1.0",
        "Generator: asbctl build_wheel",
        "Root-Is-Purelib: false",
        "Tag: py3-none-win_amd64",
    ]


def test_build_wheel_text_marks_the_wheel_without_a_binary_as_pure():
    # Given
    tag = PURE_TAG

    # When
    text = build_wheel_text(tag)

    # Then
    assert "Root-Is-Purelib: true" in text
    assert "Tag: py3-none-any" in text


def test_build_entry_points_text_declares_the_asbctl_command():
    # Given
    expected_line = "asbctl = asbctl.cli:main"

    # When
    text = build_entry_points_text()

    # Then
    assert text == f"[console_scripts]\n{expected_line}\n"


def test_build_metadata_text_reads_the_project_table_and_appends_the_readme():
    # Given
    project = {
        "name": "asbctl",
        "description": "a summary",
        "requires-python": ">=3.13",
        "license": "MIT",
        "license-files": ["LICENSE", "THIRD_PARTY_LICENSES"],
        "authors": [{"name": "A. Person"}, {"name": "B. Person"}],
    }

    # When
    text = build_metadata_text(project, "# Readme\n", "7.8.9")

    # Then
    assert text == (
        "Metadata-Version: 2.4\n"
        "Name: asbctl\n"
        "Version: 7.8.9\n"
        "Summary: a summary\n"
        "Description-Content-Type: text/markdown\n"
        "Requires-Python: >=3.13\n"
        "License-Expression: MIT\n"
        "License-File: LICENSE\n"
        "License-File: THIRD_PARTY_LICENSES\n"
        "Author: A. Person\n"
        "Author: B. Person\n"
        "\n"
        "# Readme\n"
    )


def test_build_record_text_lists_every_file_with_its_hash_and_size():
    # Given
    files = {"b/second.txt": b"two", "a/first.txt": b"one!"}

    # When
    text = build_record_text(files, "x.dist-info/RECORD")

    # Then
    assert text.splitlines() == [
        f"a/first.txt,sha256={digest_of(b'one!')},4",
        f"b/second.txt,sha256={digest_of(b'two')},3",
        "x.dist-info/RECORD,,",
    ]


def test_build_metadata_text_has_no_author_line_when_the_project_names_none():
    # Given
    project = {
        "name": "asbctl",
        "description": "d",
        "requires-python": ">=3.13",
        "license": "MIT",
        "license-files": ["LICENSE"],
    }

    # When
    text = build_metadata_text(project, "", "1.0.0")

    # Then
    assert "Author:" not in text


def test_build_record_text_encodes_a_hash_as_unpadded_urlsafe_base64():
    # Given
    files = {"empty.txt": b""}

    # When
    text = build_record_text(files, "x.dist-info/RECORD")

    # Then
    assert text.splitlines()[0] == "empty.txt,sha256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU,0"


def test_read_binary_artifacts_keeps_only_the_binaries(tmp_path: Path):
    # Given
    artifacts_path = tmp_path / "artifacts.json"
    artifacts_path.write_text(
        json.dumps(
            [
                {"type": "Archive", "goos": "linux", "goarch": "amd64", "path": "dist/a.tar.gz"},
                {"type": "Binary", "goos": "linux", "goarch": "arm64", "path": "dist/x/asbctl"},
                {"type": "Checksum", "path": "dist/checksums.txt"},
                {
                    "type": "Binary",
                    "goos": "windows",
                    "goarch": "amd64",
                    "path": "dist/y/asbctl.exe",
                },
            ]
        )
    )

    # When
    binaries = read_binary_artifacts(artifacts_path)

    # Then
    assert binaries == [
        BinaryArtifact("linux", "arm64", Path("dist/x/asbctl")),
        BinaryArtifact("windows", "amd64", Path("dist/y/asbctl.exe")),
    ]


def test_build_wheel_writes_the_package_the_binary_and_a_record_that_matches(
    tmp_path: Path, fake_binary: Path
):
    # Given
    tag = "macosx_11_0_arm64"

    # When
    wheel_path = build_wheel(
        project_directory=PROJECT_DIRECTORY,
        version="6.5.4",
        tag=tag,
        binary_path=fake_binary,
        output_directory=tmp_path / "out",
    )

    # Then
    assert wheel_path.name == "asbctl-6.5.4-py3-none-macosx_11_0_arm64.whl"
    with zipfile.ZipFile(wheel_path) as archive:
        names = archive.namelist()
        assert "asbctl/__init__.py" in names
        assert "asbctl/py.typed" in names
        assert "asbctl/bin/asbctl" in names
        assert "asbctl-6.5.4.dist-info/entry_points.txt" in names
        assert "asbctl-6.5.4.dist-info/licenses/LICENSE" in names
        assert "asbctl-6.5.4.dist-info/licenses/THIRD_PARTY_LICENSES" in names
        assert names[-1] == "asbctl-6.5.4.dist-info/RECORD"
        assert archive.read("asbctl/bin/asbctl") == FAKE_BINARY
        assert (archive.getinfo("asbctl/bin/asbctl").external_attr >> 16) & 0o777 == 0o755
        assert (archive.getinfo("asbctl/__init__.py").external_attr >> 16) & 0o777 == 0o644
        assert (
            "Tag: py3-none-macosx_11_0_arm64"
            in archive.read("asbctl-6.5.4.dist-info/WHEEL").decode()
        )
        assert "Version: 6.5.4" in archive.read("asbctl-6.5.4.dist-info/METADATA").decode()
        record_lines = archive.read("asbctl-6.5.4.dist-info/RECORD").decode().splitlines()
        recorded = {line.split(",")[0]: line.split(",")[1:] for line in record_lines}
        for name in names:
            if name.endswith("RECORD"):
                assert recorded[name] == ["", ""]
            else:
                data = archive.read(name)
                assert recorded[name] == [f"sha256={digest_of(data)}", str(len(data))]


def test_build_wheel_without_a_binary_has_no_bin_directory_and_no_command(tmp_path: Path):
    # Given
    output_directory = tmp_path / "out"

    # When
    wheel_path = build_wheel(
        project_directory=PROJECT_DIRECTORY,
        version="6.5.4",
        tag=PURE_TAG,
        binary_path=None,
        output_directory=output_directory,
    )

    # Then
    with zipfile.ZipFile(wheel_path) as archive:
        names = archive.namelist()
    assert wheel_path.name == "asbctl-6.5.4-py3-none-any.whl"
    assert not [name for name in names if "/bin/" in name]
    assert "asbctl-6.5.4.dist-info/entry_points.txt" not in names


def test_build_wheel_reads_the_project_files_and_skips_bytecode_caches(
    tmp_path: Path, small_project: Path
):
    # Given
    output_directory = tmp_path / "nested" / "out"

    # When
    wheel_path = build_wheel(
        project_directory=small_project,
        version="0.0.1",
        tag=PURE_TAG,
        binary_path=None,
        output_directory=output_directory,
    )

    # Then
    with zipfile.ZipFile(wheel_path) as archive:
        names = archive.namelist()
        assert names == [
            "asbctl/__init__.py",
            "asbctl/sub/module.py",
            "asbctl-0.0.1.dist-info/METADATA",
            "asbctl-0.0.1.dist-info/WHEEL",
            "asbctl-0.0.1.dist-info/licenses/LICENSE",
            "asbctl-0.0.1.dist-info/licenses/THIRD_PARTY_LICENSES",
            "asbctl-0.0.1.dist-info/RECORD",
        ]
        assert archive.read("asbctl-0.0.1.dist-info/METADATA").endswith(b"\n\n# small\n")
        assert archive.read("asbctl-0.0.1.dist-info/licenses/LICENSE") == b"MIT text\n"
        assert archive.read("asbctl-0.0.1.dist-info/licenses/THIRD_PARTY_LICENSES") == b"notices\n"
        assert {info.compress_type for info in archive.infolist()} == {zipfile.ZIP_DEFLATED}
        assert {info.date_time for info in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}
        assert {(info.external_attr >> 16) & 0o777 for info in archive.infolist()} == {0o644}


def test_build_wheel_gives_the_same_bytes_for_the_same_input(tmp_path: Path, fake_binary: Path):
    # Given
    arguments = {
        "project_directory": PROJECT_DIRECTORY,
        "version": "1.0.1",
        "tag": "win_amd64",
        "binary_path": fake_binary,
    }

    # When
    first = build_wheel(**arguments, output_directory=tmp_path / "one")
    second = build_wheel(**arguments, output_directory=tmp_path / "two")

    # Then
    assert first.read_bytes() == second.read_bytes()


def test_build_all_wheels_writes_every_platform_tag_and_the_wheel_without_a_binary(
    tmp_path: Path, fake_binary: Path
):
    # Given
    binaries = [
        BinaryArtifact(goos, goarch, fake_binary)
        for goos in ("darwin", "linux", "windows")
        for goarch in ("amd64", "arm64")
    ]

    # When
    wheel_paths = build_all_wheels(
        project_directory=PROJECT_DIRECTORY,
        version="2.3.4",
        binaries=binaries,
        output_directory=tmp_path / "out",
    )

    # Then
    for wheel_path in wheel_paths[1:]:
        with zipfile.ZipFile(wheel_path) as archive:
            assert archive.read("asbctl/bin/asbctl") == FAKE_BINARY
    assert sorted(path.name for path in wheel_paths) == sorted(
        [
            "asbctl-2.3.4-py3-none-any.whl",
            "asbctl-2.3.4-py3-none-macosx_11_0_arm64.whl",
            "asbctl-2.3.4-py3-none-macosx_10_12_x86_64.whl",
            "asbctl-2.3.4-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
            "asbctl-2.3.4-py3-none-musllinux_1_2_x86_64.whl",
            "asbctl-2.3.4-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
            "asbctl-2.3.4-py3-none-musllinux_1_2_aarch64.whl",
            "asbctl-2.3.4-py3-none-win_amd64.whl",
        ]
    )


def test_main_builds_the_wheels_of_an_artifacts_file_for_a_release_tag(
    tmp_path: Path, fake_binary: Path, capsys: pytest.CaptureFixture[str]
):
    # Given
    artifacts_path = tmp_path / "artifacts.json"
    artifacts_path.write_text(
        json.dumps(
            [{"type": "Binary", "goos": "darwin", "goarch": "arm64", "path": str(fake_binary)}]
        )
    )

    # When
    exit_code = main(
        [
            "--out",
            str(tmp_path / "out"),
            "--version",
            "v8.7.6-rc.2",
            "--artifacts",
            str(artifacts_path),
        ]
    )

    # Then
    printed = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert [Path(line).name for line in printed] == [
        "asbctl-8.7.6rc2-py3-none-any.whl",
        "asbctl-8.7.6rc2-py3-none-macosx_11_0_arm64.whl",
    ]


def test_main_builds_one_wheel_for_a_binary_and_a_tag(
    tmp_path: Path, fake_binary: Path, capsys: pytest.CaptureFixture[str]
):
    # Given
    arguments = ["--out", str(tmp_path / "out"), "--binary", str(fake_binary), "--tag", "win_amd64"]

    # When
    exit_code = main(arguments)

    # Then
    printed = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert [Path(line).name for line in printed] == ["asbctl-0.0.0.dev0-py3-none-win_amd64.whl"]
    with zipfile.ZipFile(printed[0]) as archive:
        assert archive.read("asbctl/bin/asbctl") == FAKE_BINARY


def test_main_prints_the_pep_440_version_of_a_tag(capsys: pytest.CaptureFixture[str]):
    # Given
    arguments = ["--version", "v3.2.1-beta.4", "--print-version"]

    # When
    exit_code = main(arguments)

    # Then
    assert exit_code == 0
    assert capsys.readouterr().out == "3.2.1b4\n"


@pytest.mark.parametrize("arguments", [["--binary", "x"], ["--tag", "win_amd64"]])
def test_main_exits_with_a_usage_error_when_only_one_of_binary_and_tag_is_given(
    tmp_path: Path, arguments: list[str]
):
    # Given
    command_line = ["--out", str(tmp_path / "out"), *arguments]

    # When
    with pytest.raises(SystemExit) as raised:
        main(command_line)

    # Then
    assert raised.value.code == 2


def test_main_exits_with_a_usage_error_without_an_output_directory():
    # Given
    arguments = ["--binary", "x", "--tag", "win_amd64"]

    # When
    with pytest.raises(SystemExit) as raised:
        main(arguments)

    # Then
    assert raised.value.code == 2


def test_main_exits_with_a_usage_error_without_a_binary_or_artifacts(tmp_path: Path):
    # Given
    arguments = ["--out", str(tmp_path / "out")]

    # When
    with pytest.raises(SystemExit) as raised:
        main(arguments)

    # Then
    assert raised.value.code == 2


@pytest.mark.skipif(sys.platform == "win32", reason="the fake binary is a shell script")
def test_a_wheel_installs_the_binary_where_the_adapter_finds_it_and_the_command_runs_it(
    tmp_path: Path, fake_binary: Path
):
    # Given
    wheel_path = build_wheel(
        project_directory=PROJECT_DIRECTORY,
        version="5.4.3",
        tag=PURE_TAG,
        binary_path=fake_binary,
        output_directory=tmp_path / "wheels",
    )
    environment_directory = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(environment_directory)], check=True)
    python_path = environment_directory / "bin" / "python"
    clean_environment = {key: value for key, value in os.environ.items() if key != "ASBCTL_BIN"}
    adapter_script = (
        "import asyncio\n"
        "import sys\n"
        "from pathlib import Path\n"
        "import asbctl\n"
        "from asbctl.bundled import find_bundled_binary_path\n"
        "from asbctl.core import get_binary_path\n"
        "bundled = find_bundled_binary_path(Path(asbctl.__file__).parent, sys.platform)\n"
        "print(get_binary_path({}, bundled))\n"
        "print(asyncio.run(asbctl.run_once('peek', queue='q-test')))"
    )

    # When
    subprocess.run(
        [str(python_path), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel_path)],
        check=True,
        capture_output=True,
    )
    resolved, adapter_output = subprocess.run(
        [str(python_path), "-c", adapter_script],
        check=True,
        capture_output=True,
        text=True,
        env=clean_environment,
    ).stdout.splitlines()
    command_output = subprocess.run(
        [str(environment_directory / "bin" / "asbctl"), "peek", "--queue", "q-test"],
        check=True,
        capture_output=True,
        text=True,
        env=clean_environment,
    ).stdout

    # Then
    assert Path(resolved).is_relative_to(environment_directory.resolve())
    assert resolved.endswith("asbctl/bin/asbctl")
    assert adapter_output == "{'fake': 'peek --queue q-test --output json'}"
    assert command_output == '{"fake": "peek --queue q-test"}\n'

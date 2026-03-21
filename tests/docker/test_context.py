"""Tests for datasmith.docker.context — DockerContext."""

from __future__ import annotations

import io
import tarfile

import pytest

from datasmith.docker.context import DockerContext


class TestToTarBytes:
    def test_to_tar_includes_all_scripts(self) -> None:
        ctx = DockerContext(
            dockerfile="FROM python:3.11",
            build_base_sh="#!/bin/bash\necho base",
            build_env_sh="#!/bin/bash\necho env",
            build_pkg_sh="#!/bin/bash\necho pkg",
            build_run_sh="#!/bin/bash\necho run",
            build_final_sh="#!/bin/bash\necho final",
            profile_sh="#!/bin/bash\necho profile",
            run_tests_sh="#!/bin/bash\necho tests",
            entrypoint_sh="#!/bin/bash\necho entry",
        )
        tar_data = ctx.to_tar_bytes()
        assert len(tar_data) > 0

        buf = io.BytesIO(tar_data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            names = sorted(tar.getnames())

        expected = sorted([
            "Dockerfile",
            "docker_build_base.sh",
            "docker_build_env.sh",
            "docker_build_pkg.sh",
            "docker_build_run.sh",
            "docker_build_final.sh",
            "profile.sh",
            "run-tests.sh",
            "entrypoint.sh",
        ])
        assert names == expected

    def test_to_tar_skips_empty_fields(self) -> None:
        ctx = DockerContext(
            dockerfile="FROM python:3.11",
            build_pkg_sh="#!/bin/bash\necho pkg",
        )
        tar_data = ctx.to_tar_bytes()

        buf = io.BytesIO(tar_data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            names = sorted(tar.getnames())

        assert names == ["Dockerfile", "docker_build_pkg.sh"]

    def test_to_tar_content_matches(self) -> None:
        content = "FROM ubuntu:22.04\nRUN apt-get update"
        ctx = DockerContext(dockerfile=content)
        tar_data = ctx.to_tar_bytes()

        buf = io.BytesIO(tar_data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            member = tar.getmember("Dockerfile")
            extracted = tar.extractfile(member)
            assert extracted is not None
            assert extracted.read().decode("utf-8") == content

    def test_to_tar_deterministic_metadata(self) -> None:
        ctx = DockerContext(dockerfile="FROM python:3.11")
        tar_data = ctx.to_tar_bytes()

        buf = io.BytesIO(tar_data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            member = tar.getmember("Dockerfile")
            assert member.mtime == 0
            assert member.uid == 0
            assert member.gid == 0
            assert member.uname == ""
            assert member.gname == ""


class TestFromDirectory:
    def test_from_directory_loads_files(self, tmp_path: pytest.TempPathFactory) -> None:
        files = {
            "Dockerfile": "FROM python:3.11",
            "docker_build_base.sh": "#!/bin/bash\necho base",
            "docker_build_env.sh": "#!/bin/bash\necho env",
            "docker_build_pkg.sh": "#!/bin/bash\necho pkg",
            "docker_build_run.sh": "#!/bin/bash\necho run",
            "docker_build_final.sh": "#!/bin/bash\necho final",
            "profile.sh": "#!/bin/bash\necho profile",
            "run_tests.sh": "#!/bin/bash\necho tests",
            "entrypoint.sh": "#!/bin/bash\necho entry",
        }
        for name, content in files.items():
            (tmp_path / name).write_text(content)  # type: ignore[union-attr]

        ctx = DockerContext.from_directory(str(tmp_path))

        assert ctx.dockerfile == "FROM python:3.11"
        assert ctx.build_base_sh == "#!/bin/bash\necho base"
        assert ctx.build_env_sh == "#!/bin/bash\necho env"
        assert ctx.build_pkg_sh == "#!/bin/bash\necho pkg"
        assert ctx.build_run_sh == "#!/bin/bash\necho run"
        assert ctx.build_final_sh == "#!/bin/bash\necho final"
        assert ctx.profile_sh == "#!/bin/bash\necho profile"
        assert ctx.run_tests_sh == "#!/bin/bash\necho tests"
        assert ctx.entrypoint_sh == "#!/bin/bash\necho entry"

    def test_from_directory_missing_files_default_empty(self, tmp_path: pytest.TempPathFactory) -> None:
        (tmp_path / "Dockerfile").write_text("FROM python:3.11")  # type: ignore[union-attr]

        ctx = DockerContext.from_directory(str(tmp_path))

        assert ctx.dockerfile == "FROM python:3.11"
        assert ctx.build_base_sh == ""
        assert ctx.build_env_sh == ""
        assert ctx.build_pkg_sh == ""
        assert ctx.build_run_sh == ""
        assert ctx.build_final_sh == ""
        assert ctx.profile_sh == ""
        assert ctx.run_tests_sh == ""
        assert ctx.entrypoint_sh == ""

    def test_from_directory_empty_dir(self, tmp_path: pytest.TempPathFactory) -> None:
        ctx = DockerContext.from_directory(str(tmp_path))
        assert ctx.dockerfile == ""
        assert ctx.build_pkg_sh == ""


class TestToDirectory:
    def test_roundtrip(self, tmp_path: pytest.TempPathFactory) -> None:
        ctx = DockerContext(
            dockerfile="FROM python:3.11",
            build_base_sh="#!/bin/bash\necho base",
            build_pkg_sh="#!/bin/bash\necho pkg",
            build_final_sh="#!/bin/bash\necho final",
            profile_sh="#!/bin/bash\necho profile",
        )
        out = str(tmp_path / "output")  # type: ignore[operator]
        ctx.to_directory(out)
        ctx2 = DockerContext.from_directory(out)
        assert ctx2.dockerfile == ctx.dockerfile
        assert ctx2.build_base_sh == ctx.build_base_sh
        assert ctx2.build_pkg_sh == ctx.build_pkg_sh
        assert ctx2.build_final_sh == ctx.build_final_sh
        assert ctx2.profile_sh == ctx.profile_sh
        assert ctx2.build_env_sh == ""
        assert ctx2.build_run_sh == ""

    def test_skips_empty_fields(self, tmp_path: pytest.TempPathFactory) -> None:
        ctx = DockerContext(dockerfile="FROM python:3.11")
        out = str(tmp_path / "output")  # type: ignore[operator]
        ctx.to_directory(out)
        import os

        written = sorted(os.listdir(out))
        assert written == ["Dockerfile"]


class TestFromLegacyDict:
    def test_maps_all_fields(self) -> None:
        legacy = {
            "dockerfile_data": "FROM python:3.11",
            "base_building_data": "echo base",
            "env_building_data": "echo env",
            "building_data": "echo pkg",
            "run_building_data": "echo run",
            "final_building_data": "echo final",
            "profile_data": "echo profile",
            "run_tests_data": "echo tests",
            "entrypoint_data": "echo entry",
            "created_unix": 1234567890,
        }
        ctx = DockerContext.from_legacy_dict(legacy)
        assert ctx.dockerfile == "FROM python:3.11"
        assert ctx.build_base_sh == "echo base"
        assert ctx.build_env_sh == "echo env"
        assert ctx.build_pkg_sh == "echo pkg"
        assert ctx.build_run_sh == "echo run"
        assert ctx.build_final_sh == "echo final"
        assert ctx.profile_sh == "echo profile"
        assert ctx.run_tests_sh == "echo tests"
        assert ctx.entrypoint_sh == "echo entry"

    def test_missing_fields_default_empty(self) -> None:
        legacy = {"dockerfile_data": "FROM python:3.11"}
        ctx = DockerContext.from_legacy_dict(legacy)
        assert ctx.dockerfile == "FROM python:3.11"
        assert ctx.build_pkg_sh == ""
        assert ctx.build_final_sh == ""


class TestPydanticSerialization:
    def test_context_pydantic_serialization_roundtrip(self) -> None:
        ctx = DockerContext(
            dockerfile="FROM python:3.11",
            build_base_sh="#!/bin/bash\necho base",
            build_env_sh="#!/bin/bash\necho env",
            build_pkg_sh="#!/bin/bash\necho pkg",
            build_run_sh="#!/bin/bash\necho run",
            build_final_sh="#!/bin/bash\necho final",
            profile_sh="#!/bin/bash\necho profile",
            run_tests_sh="#!/bin/bash\necho tests",
            entrypoint_sh="#!/bin/bash\necho entry",
        )

        data = ctx.model_dump()
        assert isinstance(data, dict)
        assert data["dockerfile"] == "FROM python:3.11"

        json_str = ctx.model_dump_json()
        ctx2 = DockerContext.model_validate_json(json_str)
        assert ctx2.dockerfile == ctx.dockerfile
        assert ctx2.build_base_sh == ctx.build_base_sh
        assert ctx2.build_env_sh == ctx.build_env_sh
        assert ctx2.build_pkg_sh == ctx.build_pkg_sh
        assert ctx2.build_run_sh == ctx.build_run_sh
        assert ctx2.build_final_sh == ctx.build_final_sh
        assert ctx2.profile_sh == ctx.profile_sh
        assert ctx2.run_tests_sh == ctx.run_tests_sh
        assert ctx2.entrypoint_sh == ctx.entrypoint_sh

    def test_context_is_mutable(self) -> None:
        ctx = DockerContext(dockerfile="FROM python:3.11")
        ctx.dockerfile = "FROM python:3.12"
        assert ctx.dockerfile == "FROM python:3.12"

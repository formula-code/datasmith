"""Docker build context model."""

from __future__ import annotations

import io
import os
import tarfile
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict


class DockerContext(BaseModel):
    model_config = ConfigDict(frozen=False)

    dockerfile: str = ""
    build_base_sh: str = ""
    build_env_sh: str = ""
    build_pkg_sh: str = ""
    build_run_sh: str = ""
    build_final_sh: str = ""
    profile_sh: str = ""
    run_tests_sh: str = ""
    entrypoint_sh: str = ""

    _FILE_MAP: ClassVar[dict[str, str]] = {
        "Dockerfile": "dockerfile",
        "docker_build_base.sh": "build_base_sh",
        "docker_build_env.sh": "build_env_sh",
        "docker_build_pkg.sh": "build_pkg_sh",
        "docker_build_run.sh": "build_run_sh",
        "docker_build_final.sh": "build_final_sh",
        "profile.sh": "profile_sh",
        "run_tests.sh": "run_tests_sh",
        "entrypoint.sh": "entrypoint_sh",
    }

    _LEGACY_MAP: ClassVar[dict[str, str]] = {
        "dockerfile_data": "dockerfile",
        "base_building_data": "build_base_sh",
        "env_building_data": "build_env_sh",
        "building_data": "build_pkg_sh",
        "run_building_data": "build_run_sh",
        "final_building_data": "build_final_sh",
        "profile_data": "profile_sh",
        "run_tests_data": "run_tests_sh",
        "entrypoint_data": "entrypoint_sh",
    }

    def to_tar_bytes(self) -> bytes:
        """Serialize context to in-memory tar for Docker build."""
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for filename, field in sorted(self._FILE_MAP.items()):
                content = getattr(self, field)
                if not content:
                    continue
                data = content.encode("utf-8")
                info = tarfile.TarInfo(name=filename)
                info.size = len(data)
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                tar.addfile(info, io.BytesIO(data))
        return buf.getvalue()

    def to_directory(self, path: str) -> None:
        """Write all context files to a directory on disk."""
        os.makedirs(path, exist_ok=True)
        for filename, field in self._FILE_MAP.items():
            content = getattr(self, field)
            if content:
                with open(os.path.join(path, filename), "w") as f:
                    f.write(content)

    @classmethod
    def from_directory(cls, path: str) -> DockerContext:
        """Load a DockerContext from a task directory."""

        def _read(name: str) -> str:
            fp = os.path.join(path, name)
            if os.path.exists(fp):
                with open(fp) as f:
                    return f.read()
            return ""

        kwargs = {field: _read(filename) for filename, field in cls._FILE_MAP.items()}
        return cls(**kwargs)

    @classmethod
    def from_legacy_dict(cls, data: dict[str, Any]) -> DockerContext:
        """Create a DockerContext from the old context registry format."""
        kwargs = {}
        for legacy_key, field in cls._LEGACY_MAP.items():
            value = data.get(legacy_key, "")
            if value:
                kwargs[field] = value
        return cls(**kwargs)

"""Docker build context model."""

from __future__ import annotations

import io
import os
import tarfile

from pydantic import BaseModel, ConfigDict


class DockerContext(BaseModel):
    model_config = ConfigDict(frozen=False)

    dockerfile: str = ""
    build_base_sh: str = ""
    build_env_sh: str = ""
    build_pkg_sh: str = ""
    build_run_sh: str = ""
    profile_sh: str = ""
    run_tests_sh: str = ""
    entrypoint_sh: str = ""

    def to_tar_bytes(self) -> bytes:
        """Serialize context to in-memory tar for Docker build."""
        buf = io.BytesIO()
        files = {
            "Dockerfile": self.dockerfile,
            "build_base.sh": self.build_base_sh,
            "build_env.sh": self.build_env_sh,
            "build_pkg.sh": self.build_pkg_sh,
            "build_run.sh": self.build_run_sh,
            "profile.sh": self.profile_sh,
            "run_tests.sh": self.run_tests_sh,
            "entrypoint.sh": self.entrypoint_sh,
        }
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for name, content in sorted(files.items()):
                if not content:
                    continue
                data = content.encode("utf-8")
                info = tarfile.TarInfo(name=name)
                info.size = len(data)
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                tar.addfile(info, io.BytesIO(data))
        return buf.getvalue()

    @classmethod
    def from_directory(cls, path: str) -> DockerContext:
        """Load a DockerContext from a task directory."""

        def _read(name: str) -> str:
            fp = os.path.join(path, name)
            if os.path.exists(fp):
                with open(fp) as f:
                    return f.read()
            return ""

        return cls(
            dockerfile=_read("Dockerfile"),
            build_base_sh=_read("build_base.sh"),
            build_env_sh=_read("build_env.sh"),
            build_pkg_sh=_read("build_pkg.sh"),
            build_run_sh=_read("build_run.sh"),
            profile_sh=_read("profile.sh"),
            run_tests_sh=_read("run_tests.sh"),
            entrypoint_sh=_read("entrypoint.sh"),
        )

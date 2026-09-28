"""download_snapshots extracts on Pythons without tarfile's `filter` argument."""

import importlib.util
import io
import tarfile
from pathlib import Path

_UPLOAD = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "upload.py"


def test_extracts_without_tar_filter_support(tmp_path: Path, monkeypatch) -> None:
    spec = importlib.util.spec_from_file_location("_fc_upload", _UPLOAD)
    upload = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(upload)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        data = b'{"a": 1}'
        info = tarfile.TarInfo(".snapshots/baseline.json")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    monkeypatch.setenv("SUPABASE_ANON_KEY", "k")
    monkeypatch.setattr(upload, "_request", lambda *a, **k: buf.getvalue())
    monkeypatch.delattr(tarfile, "data_filter", raising=False)
    real = tarfile.TarFile.extractall
    monkeypatch.setattr(tarfile.TarFile, "extractall", lambda self, path=".", members=None, *, numeric_owner=False: real(self, path, members))
    assert upload.download_snapshots("http://x", "o", "r", 1, tmp_path / ".snapshots")
    assert (tmp_path / ".snapshots" / "baseline.json").read_text() == '{"a": 1}'

"""Only declared dependencies. No inference, no globbing, no conda files."""

from datasmith.resolution.declare import Declared, declare
from datasmith.resolution.models import CandidateMeta

# The exact string fix_marker_spacing produced for apache/arrow's `pyuwsgi`
# requirement -- the same literal tests/resolution/test_requirements.py uses.
# The original, `pyuwsgi;sys.platform!='win32'`, parses: `sys.platform` is a
# legacy PEP 508 marker alias the real parser accepts. Only the corrupted form
# does not, so it is the one that exercises the drop path.
CORRUPTED_MARKER = "pyuwsgi; sys.platf or m!='win32'"


def _meta(**kw) -> CandidateMeta:
    m = CandidateMeta()
    for k, v in kw.items():
        setattr(m, k, v)
    return m


def test_runtime_and_build_are_separate():
    d = declare(_meta(core_deps={"numpy>=1.25"}, build_requires={"cython>=3.1"}), None)
    assert d.runtime == ["numpy>=1.25"]
    assert d.build == ["cython>=3.1"]


def test_extras_are_kept_but_not_merged_into_runtime():
    d = declare(_meta(core_deps={"numpy"}, extras={"docs": {"sphinx"}}), None)
    assert d.runtime == ["numpy"]
    assert d.extras == {"docs": ["sphinx"]}
    assert "sphinx" not in d.runtime


def test_asv_matrix_req_is_a_declaration():
    d = declare(_meta(core_deps={"numpy"}), {"cython": {"0.29.21"}, "pandas": set()})
    assert d.runtime == ["cython==0.29.21", "numpy", "pandas"]


def test_matrix_keys_are_not_discarded():
    # The predecessor fed bare versions to the resolver, which read '0.29.21' as
    # a package name. The key carries the name and must survive.
    d = declare(_meta(), {"cython": {"0.29.21"}})
    assert d.runtime == ["cython==0.29.21"]


def test_matrix_none_version_is_dropped_not_stringified():
    # str(None) would put the literal 'None' into the requirement set.
    d = declare(_meta(), {"cython": {"None"}})
    assert "cython==None" not in d.runtime


def test_unparseable_declarations_are_recorded():
    d = declare(_meta(core_deps={"numpy", CORRUPTED_MARKER}), None)
    assert d.runtime == ["numpy"]
    assert [x.raw for x in d.dropped] == [CORRUPTED_MARKER]


def test_output_is_deterministic():
    a = declare(_meta(core_deps={"b", "a", "c"}), None)
    b = declare(_meta(core_deps={"c", "a", "b"}), None)
    assert a == b


def test_declared_is_the_type_returned():
    assert isinstance(declare(_meta(), None), Declared)


def test_import_analyzer_is_deleted():
    import importlib

    try:
        importlib.import_module("datasmith.resolution.import_analyzer")
    except ModuleNotFoundError:
        return
    raise AssertionError("import_analyzer still exists")

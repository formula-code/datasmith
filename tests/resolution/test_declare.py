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


def test_a_matrix_entry_that_is_only_null_installs_nothing():
    # ASV's `null` means "do not install in this combination". Emitting the bare
    # key here would invert that into "install any version", and asserting only
    # that 'cython==None' is absent cannot tell the two apart: that string never
    # parses, so it lands in `dropped` under either implementation.
    d = declare(_meta(), {"cython": {"None"}})
    assert d.runtime == []
    assert d.dropped == []


def test_a_matrix_entry_with_one_real_version_keeps_it():
    d = declare(_meta(), {"cython": {"None", "0.29.21"}})
    assert d.runtime == ["cython==0.29.21"]


def test_a_matrix_sweep_does_not_become_two_conflicting_pins():
    # lmfit-py benchmarks scipy at 0.18 and 0.19. Emitting one `==` pin per
    # version makes the compile unsatisfiable, and the failure is not confined to
    # scipy: `pin` returns nothing at all, so every unrelated requirement in the
    # seed goes with it. A seed is one environment, so it states the weakest true
    # thing about a sweep.
    d = declare(_meta(core_deps={"numpy"}), {"scipy": {"0.18", "0.19"}})
    assert d.runtime == ["numpy", "scipy"]


def test_a_sweep_keeps_the_other_requirements_intact():
    d = declare(_meta(core_deps={"numpy"}), {"scipy": {"0.18", "0.19"}, "cython": {"0.29.21"}})
    assert d.runtime == ["cython==0.29.21", "numpy", "scipy"]


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

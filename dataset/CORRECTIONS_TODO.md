# CORRECTIONS TODO

Scanned `86` files matching `formulacode_verified/*/*/failure.json`.

## Group Summary

| Group | Count | Stage distribution |
| --- | ---: | --- |
| Missing `pkg_resources` During Build | 35 | build:35 |
| Idempotency Bug In `micromamba remove` | 29 | tests:29 |
| Broken SciPy Install (`_promote` ImportError) | 9 | build:3, profile:1, tests:5 |
| ASV Discovery Crash (`dist.metadata` Is `None`) | 4 | profile:4 |
| Cython 0.29 Incompatible With Source Syntax | 2 | build:2 |
| NumPy Alias Removal (`np.object`/`np.bool`) | 2 | tests:2 |
| GPU Adapter Unavailable For WGPU Tests | 2 | tests:2 |
| Kedro Env Stage Cannot Resolve ASV Python Versions | 1 | build:1 |
| libstdc++ ABI Mismatch | 1 | tests:1 |
| Missing Built Pandas Extension (`pandas_parser`) | 1 | tests:1 |

## 1. Missing `pkg_resources` During Build (35)

- Signature: `ModuleNotFoundError: No module named 'pkg_resources'` during editable/build metadata generation.
- Correction direction: In `docker_build_pkg.sh`, pre-pin a compatible setuptools stack before package install (for example a setuptools release that still provides `pkg_resources`), and preinstall build deps that import `pkg_resources` during setup.
- Tasks:
- `optuna_optuna/fe695316219030a6b49ea2078e3f906960b0a578`
- `pandas-dev_pandas/1840423213b8d796da7b9bb863b04d37e4c28562`
- `pandas-dev_pandas/18bc585ae6eb6918911243b9486bb2c1a9dec570`
- `pandas-dev_pandas/18c43651b0497b5e93fd02e4f0bf6d254a25c316`
- `pandas-dev_pandas/1c71d7db926f4247798a7182f85aba1124fa894f`
- `pandas-dev_pandas/1ce4455424898eb08719295205faacb2f0d9a4b1`
- `pandas-dev_pandas/22491dc041315d40e525b722a92202f656639957`
- `pandas-dev_pandas/2e224c7e57870196e47fc8656713834f3d8afa41`
- `pandas-dev_pandas/3d8993146ccb6054ecad6b7e97ee39e97c6d84e5`
- `pandas-dev_pandas/3ece807d6d85663cb0f9811ccabb8426f506bb5d`
- `pandas-dev_pandas/428c2a6c6159889b7ce213468351989fd702ed5f`
- `pandas-dev_pandas/502919e4b18e5d5aca584d4ebb84d663fcefffc9`
- `pandas-dev_pandas/5fad2e4e90be46c14e947ac1a7eba3275c4b656d`
- `pandas-dev_pandas/6d89d8c900ea27fe6c55f204d6b96961e73aa67f`
- `pandas-dev_pandas/70eef55e438823697515e834ee4ac350bdb0aaa7`
- `pandas-dev_pandas/76c39d547c42644f8ce42801d696334cd3e199a2`
- `pandas-dev_pandas/80527f4865d68a147d82fc7953f809e446ac9510`
- `pandas-dev_pandas/8117a55618ade79272fc91edfdfdaed4d460b08e`
- `pandas-dev_pandas/9918c84043a340cc0a62625273c842c05f3d71b1`
- `pandas-dev_pandas/9d8e7048affb31aa33bfe6f09b626b3045ee0d33`
- `pandas-dev_pandas/9eee1073c2d0602569ca445b7509dedf906f4af5`
- `pandas-dev_pandas/a1b6fcb31b71ca6cd01d2239ebbfbb848da54423`
- `pandas-dev_pandas/a28cadbeb6f21da6c768b84473b3415e6efb3115`
- `pandas-dev_pandas/a6d3e13b3a3c0d2b902c6229a7261bc299f36a03`
- `pandas-dev_pandas/ad98c2b771326502900e007a830ff69cb2e6c6e1`
- `pandas-dev_pandas/afdf0a3e409d02cdfff6c813f512717f1503a4d8`
- `pandas-dev_pandas/b00148b6c3a73a9d83cdf60a2e18befeaadf68bd`
- `pandas-dev_pandas/bf5ee72d5b81962db91fa12f637b4369d3e30f77`
- `pandas-dev_pandas/c4a84ab20b5699302e6bfc6854a894a30383ee98`
- `pandas-dev_pandas/c537b3635128ef6347d57bb83771363b3d7e3cbf`
- `pandas-dev_pandas/e1dd15b41e02ec78ca61379dad74a99f5ec16aa0`
- `pandas-dev_pandas/f351f74f9a01fd8e06f8d7318d4596756c9c82f5`
- `pandas-dev_pandas/fa78ea801392f4f0d37ea7ddbbfe44e9c8c102bd`
- `pandas-dev_pandas/fdd71632615d1db02fb606667e90b632470fb4dc`
- `pyapp-kit_psygnal/e9ee49698b53aa441e069e0c14b56bc42fbad08d`

## 2. Idempotency Bug In `micromamba remove` (29)

- Signature: `Failure: packages to remove not found in the environment: scipy, pyarrow` in tests stage.
- Correction direction: In `docker_build_run.sh`, make the remove step conditional/idempotent (`micromamba remove ... || true` or check package existence before removing).
- Tasks:
- `pandas-dev_pandas/02e2baed7769bb62620cfa198f8e4fc302ab145b`
- `pandas-dev_pandas/0cedcbf6ec769fbc6075c3be1bed9087d85e2dec`
- `pandas-dev_pandas/0e11d6dfde943dd3c355aba178eb732c6e6d6223`
- `pandas-dev_pandas/0e12bfcd4b4d4e6f5ebe6cc6f99c0a5836df4478`
- `pandas-dev_pandas/1625d5a9112a0143c1dc59eadab825536e7e1240`
- `pandas-dev_pandas/1f622e2b5303650fa5e497e4552d0554e51049cb`
- `pandas-dev_pandas/2b82b8635d17c3a020e6e40ba72b9cc6a76f3149`
- `pandas-dev_pandas/38e29ab7d049015b428a880ed0a160b10418fae6`
- `pandas-dev_pandas/3c15cfdf0e961c4e8f74a205bac6d34e0930f988`
- `pandas-dev_pandas/3c96b8ff6d399fbec8d4d533e8e8618c592bb64b`
- `pandas-dev_pandas/457690995ccbfc5b8eee80a0818d62070d078bcf`
- `pandas-dev_pandas/47cd690501c55c266a845fbbeb5515804a66d05d`
- `pandas-dev_pandas/562523602b4ac10d1b30d813d7c2bfe02adc0469`
- `pandas-dev_pandas/5789f15402a97bbaf590c8de2696ef94c22a6bf9`
- `pandas-dev_pandas/5955ca6645e45d23c978076ab8e556cb91ef124c`
- `pandas-dev_pandas/5fc2ed2703a1370207f4ebad834e665b6c2ad42f`
- `pandas-dev_pandas/6725e37684a24afeaea2757fb3512c58d37cef86`
- `pandas-dev_pandas/7012d6a60724bf55d1fc0d03d6c50484aa19fb85`
- `pandas-dev_pandas/72814750548b5ead5b08cd9b90d56a23c9a520ea`
- `pandas-dev_pandas/746e5eee860b6e143c33c9b985e095dac2e42677`
- `pandas-dev_pandas/7c876ed63294a260f3a7a3e3071127a7d2e2659e`
- `pandas-dev_pandas/87d3fe4702bf885ab8f9c01d22804352189469f2`
- `pandas-dev_pandas/8f0c4d2ca6856cc345917d62c3f989ada00617c0`
- `pandas-dev_pandas/984d75543fce5c68f0dcc4f0b3500256e4a9c0c9`
- `pandas-dev_pandas/a730486036790f3cd26145542257a837e241144c`
- `pandas-dev_pandas/aa4922a96b4c6c34b5fc6943437d902f6cc662b1`
- `pandas-dev_pandas/b89f1d0d05f4c9f360985abc6bda421d73bae85f`
- `pandas-dev_pandas/c7fa61113b4cf09581e8f31f6053f2e64b83a9fc`
- `pandas-dev_pandas/dc830eab5516f6911c894abdb1e3782cb18c503e`

## 3. Broken SciPy Install (`_promote` ImportError) (9)

- Signature: `ImportError: cannot import name '_promote' from scipy.spatial.transform._rotation`.
- Correction direction: In `docker_build_pkg.sh` or `docker_build_run.sh`, force a consistent SciPy+NumPy reinstall/pin (single source, avoid mixed pip/conda artifacts) before running profile/tests.
- Tasks:
- `UXARRAY_uxarray/6d5bd3edb8743621556845c89edb742688ebaaf2`
- `UXARRAY_uxarray/ac525d6e41b8e244f4ee3da8157ead7259292623`
- `optuna_optuna/3126551d3ccb1406d74ac132f1068ff626a3d88a`
- `optuna_optuna/353d569fa906d820448caee155a437ffd067c39a`
- `optuna_optuna/b5f8b037153c0f93c1a8fd5f197303cdb0322d50`
- `optuna_optuna/d763594b1c081695e1fd14283210802521b59649`
- `pybamm-team_PyBaMM/6f20992513b219e5753d62d690c38a45029fd537`
- `pybop-team_PyBOP/98d40034e42dab9e537623eca6fe3b9399148480`
- `scikit-image_scikit-image/af8005c3c41148415cb11a4368ac517d3cb6e35c`

## 4. ASV Discovery Crash (`dist.metadata` Is `None`) (4)

- Signature: `TypeError: 'NoneType' object is not subscriptable` in `asv_runner/benchmarks/__init__.py`, followed by missing `benchmarks.json`.
- Correction direction: In `docker_build_run.sh`, pin ASV/asv_runner to a known working pair for these commits before profile execution.
- Tasks:
- `pandas-dev_pandas/0021d241c6aa1b8db91151361b48d7864201fd01`
- `pandas-dev_pandas/00f10db680c6cf836fad80dda33849081e540230`
- `pandas-dev_pandas/43c7d4023009ceee4ca11f2148a8debd372e437a`
- `pandas-dev_pandas/ea65f90ec60bc596872bfe92e7b7cd135608bc85`

## 5. Cython 0.29 Incompatible With Source Syntax (2)

- Signature: Build shows `Cython compiler ... 0.29.37` with errors like `Expected ':', found '('` and missing `cpython/*.pxd`.
- Correction direction: In `docker_build_pkg.sh`, upgrade/pin `cython>=3` (and related build tools) before editable install for these commits.
- Tasks:
- `pandas-dev_pandas/92a52e231534de236c4e878008a4365b4b1da291`
- `pandas-dev_pandas/c8646541e9a2e27cc14e550c722364ded1dcba5f`

## 6. NumPy Alias Removal (`np.object`/`np.bool`) (2)

- Signature: `AttributeError: module 'numpy' has no attribute 'object'` (and `np.bool`) in tests.
- Correction direction: In `docker_build_run.sh` or `docker_build_pkg.sh`, pin NumPy to a pre-alias-removal version for these old commits, or patch usages to builtins/`np.bool_`.
- Tasks:
- `JDASoftwareGroup_kartothek/549288f9893527f07a2fdb016f7736eb30f62ecc`
- `JDASoftwareGroup_kartothek/5cba248cbfe9f7375ea3656850bbc98c7fb1746c`

## 7. GPU Adapter Unavailable For WGPU Tests (2)

- Signature: `RuntimeError: Request adapter failed (1): Validation Error` with `No suitable adapter found`.
- Correction direction: In `docker_build_run.sh`, skip/mark GPU-only tests or configure a software/CPU backend for WGPU in CI containers.
- Tasks:
- `xdslproject_xdsl/b87c549f060b4843d8cdd445f648ea4217e51ec4`
- `xdslproject_xdsl/d7f48f10b163efd1dc7d71ce250f69444b407f3a`

## 8. Kedro Env Stage Cannot Resolve ASV Python Versions (1)

- Signature: `No Satisfying PY_VERSIONS found in asv.conf.json` plus missing `/etc/profile.d/asv_build_vars.sh`.
- Correction direction: This fails in `docker_build_env.sh` (before pkg/run). Flag for manual handling or task-level policy exception, because normal pkg/run-only fixes cannot execute.
- Tasks:
- `kedro-org_kedro/70734ce00ee46b58c85b4cf04afbe89d32c06758`

## 9. libstdc++ ABI Mismatch (1)

- Signature: `ImportError: ... libstdc++.so.6: version 'CXXABI_1.3.15' not found`.
- Correction direction: In `docker_build_run.sh`, ensure conda libstdc++ is selected at runtime (`libstdcxx-ng` present, env lib path precedence).
- Tasks:
- `scikit-learn_scikit-learn/0173b916739dc17fe522ab64c691682a30d1d17b`

## 10. Missing Built Pandas Extension (`pandas_parser`) (1)

- Signature: `ModuleNotFoundError: No module named 'pandas._libs.pandas_parser'` during test import.
- Correction direction: In `docker_build_pkg.sh`, force a complete C-extension build for this commit (re-run editable install/build_ext with matching toolchain).
- Tasks:
- `pandas-dev_pandas/9ff14a3ec4259b04b25aa9ce5b23185574c2c771`

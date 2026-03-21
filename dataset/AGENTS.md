# FormulaCode Dataset Verification Guide

This guide explains how to verify and fix tasks in the FormulaCode dataset using the verification workflow.

## Quick Start

```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith && python dataset/verify.py --task dataset/formulacode_verified/{repo_name}/{sha}
```

**IMPORTANT**: In this workflow, you should only make changes to `docker_build_pkg.sh` and/or `docker_build_run.sh` (sometimes referred to as `build_docker_pkg.sh` / `build_docker_run.sh`). Do **not** modify the Dockerfile, `task.txt`, `env_payload`, or any other scripts.

## Main Workflow

The verification process is iterative and follows this cycle:

```
1. Run verify.py (from parent directory)
   ↓
2. Check for failure.json (if verification failed)
   ↓
3. Read error details from failure.json
   ↓
4. Make changes only to docker_build_pkg.sh and/or docker_build_run.sh
   ↓
5. Rerun verify.py
   ↓
6. Repeat until verification_success.json is written
```

### Success Indicator

When verification completes successfully, a `verification_success.json` file is created in the task directory containing:

* Local Docker image reference
* DockerHub image reference

### Failure Indicator

When verification fails, a `failure.json` file is created containing:

* **stage**: Which stage failed (build, profile, tests, dockerhub_push)
* **return_code**: Exit code of the failed command
* **error_message**: Human-readable error description
* **stdout**: Full stdout from the failed operation
* **stderr**: Full stderr from the failed operation

## Task Directory Structure

Each task directory (`dataset/formulacode_verified/<owner>_<repo>/<sha>/`) contains these files:

### Core Configuration

#### task.txt

Contains the Task object definition as a Python expression:

```python
Task(
    owner='optuna',
    repo='optuna',
    sha='fe695316219030a6b49ea2078e3f906960b0a578',
    commit_date=1656038914.0,
    env_payload='{"dependencies": [...]}',  # JSON string with pinned deps
    python_version='3.10',
    tag='pkg',
    benchmarks=''
)
```

### Docker Build Configuration

#### Dockerfile

Multi-stage Docker build file with the following stages:

1. **base**: Sets up base system with:

   * Rust toolchain
   * System packages (cmake, ninja, MPI, etc.)
   * Micromamba package manager
   * Python 3.10
   * UV package manager
   * Runs `docker_build_base.sh`

2. **repo**: Clones the Git repository

3. **env**:

   * Checks out the specific commit SHA
   * Runs `docker_build_env.sh` to create Python environments
   * Installs dependencies from env_payload

4. **pkg**:

   * Runs `docker_build_pkg.sh` to install the package
   * Copies `profile.sh` and `run-tests.sh`

5. **run**:

   * Runs `docker_build_run.sh`
   * Prepares environment for testing/profiling

6. **final**:

   * Runs `docker_build_final.sh`
   * Adds benchmark information
   * Final production image

### Build Scripts

#### docker_build_base.sh

**Purpose**: Set up the base environment before repository checkout

**What it does**:

* Creates `/etc/profile.d/asv_utils.sh` with helper functions
* Configures micromamba initialization
* Defines utility functions like `detect_import_name()`
* Sets up shell environment for all subsequent stages
* Optionally configures specific Python version if PY_VERSION is set

**When to modify**: Do not modify as part of verification fixes; instead, add required dependencies or setup steps in `docker_build_pkg.sh` and/or `docker_build_run.sh`

#### docker_build_env.sh

**Purpose**: Create Python environments and install dependencies

**What it does**:

* Parses the env_payload JSON containing pinned dependencies
* Creates one or more micromamba environments (e.g., `asv_3.10`)
* Installs dependencies from the env_payload into each environment
* Persists environment configuration to `/etc/profile.d/asv_build_vars.sh`
* Exports variables like `ASV_PY_VERSIONS`, `ENV_NAME`, `CONF_NAME`

**When to modify**: Do not modify as part of verification fixes; instead, install any additional dependencies in `docker_build_pkg.sh` and/or `docker_build_run.sh`

#### docker_build_pkg.sh

**Purpose**: Build and install the repository package in editable mode

**What it does**:

* Sources environment helpers from previous stages
* For each Python version in `ASV_PY_VERSIONS`:

  * Activates the corresponding environment (`asv_3.10`)
  * Detects the package import name
  * Installs the package in editable mode: `pip install --no-build-isolation -v -e .`
  * Runs health checks to verify installation
* This is the **primary script you'll edit** when fixing verification issues

**When to modify**:

* Add extra conda/pip build dependencies
* Set compiler flags (CFLAGS, CXXFLAGS, LDFLAGS)
* Run pre-build steps (submodule init, Cython generation)
* Apply repository-specific patches
* Modify source files to fix build errors

**Common fixes**:

```bash
# Add build dependency
micromamba install -y -n "$ENV_NAME" -c conda-forge some-build-tool

# Set compiler flags
export CFLAGS="${CFLAGS:--Wno-error=incompatible-pointer-types}"

# Install extra pip dependencies before main install
micromamba run -n "$ENV_NAME" pip install build-dependency

# Patch source file
sed -i 's/old_code/new_code/' path/to/file.py
```

#### docker_build_run.sh

**Purpose**: Prepare the environment for running tests and benchmarks

**What it does**:

* Removes build scripts to save space
* Locks the repository to the current commit using `lock_repo_to_current_commit()`
* Detaches from upstream branches
* Sets up ASV machine configuration
* Handles repository-specific setup (special cases for dask, joblib, astropy, etc.)
* This is the **secondary script you'll edit** when fixing verification issues

**When to modify**: When you need special repository-specific configuration or runtime/test/benchmark dependencies for profile/tests runs

#### docker_build_final.sh

**Purpose**: Finalize the Docker image for distribution

**What it does**:

* Activates the primary environment
* Installs `snapshot-tester` (for test validation)
* Installs/upgrades `coverage` tool
* Creates `/workspace/repo/asv-benchmarks.txt` with benchmark list
* Exports `BENCHMARK_DIR` location
* Adds environment activation to `~/.bashrc`

**When to modify**: Do not modify as part of verification fixes; if profile/tests need additional runtime configuration, do it in `docker_build_run.sh` (or `docker_build_pkg.sh` when it impacts installation)

### Validation Scripts

#### profile.sh

**Purpose**: Generate baseline ASV (Airspeed Velocity) benchmark results

**What it does**:

* Takes a log path as argument
* Sets up ASV environment and configuration
* Installs coverage and asv_runner in the task environment
* Discovers and runs ASV benchmarks for the current commit
* Generates performance measurements
* Creates a tarball with results
* Cleans up intermediate files

**When verification fails at profile stage**:

* Check if ASV configuration file (`asv.*.json`) exists
* Verify benchmark files are accessible
* Check for benchmark syntax errors
* Ensure dependencies needed by benchmarks are installed (via `docker_build_run.sh` and/or `docker_build_pkg.sh`)

**When to modify**: Do not modify; make fixes in `docker_build_run.sh` and/or `docker_build_pkg.sh`

#### run-tests.sh

**Purpose**: Execute pytest tests to validate the package

**What it does**:

* Activates the appropriate Python environment
* Takes base commit SHA as argument (or uses HEAD_SHA)
* Handles repository-specific test setup (for repos like dask, joblib, astropy)
* Uses `reset_repo_state()` to prepare the repository
* Runs pytest with appropriate configuration
* May run snapshot-based tests for validation

**When verification fails at tests stage**:

* Check pytest output in failure.json stderr/stdout
* Common issues:

  * Missing test dependencies
  * Import errors
  * Test configuration problems
  * Repository state issues

**When to modify**: Do not modify; install missing test/runtime dependencies or apply required setup in `docker_build_run.sh` and/or `docker_build_pkg.sh`

#### entrypoint.sh

**Purpose**: Container entrypoint for running ASV benchmarks

**What it does**:

* Requires `COMMIT_SHA`, `ASV_ARGS`, `ASV_CONF_PATH` environment variables
* Activates micromamba base environment
* Checks out the specified commit
* Changes to ASV config directory
* For each Python version:

  * Creates dedicated environment
  * Installs ASV
  * Runs benchmarks with specified arguments
  * Stores results in `/output/{COMMIT_SHA}/{version}/`

**When to modify**: Do not modify as part of verification fixes; use `docker_build_run.sh` and/or `docker_build_pkg.sh` instead

## Verification Stages

The `verify.py` script runs through these stages in order:

### 1. Build Stage

* Builds the Docker image using all stages (base → repo → env → pkg → run)
* Build must complete without errors (return code 0)
* Timeout: 3600 seconds (1 hour)

**Common failures**:

* Missing system dependencies → install via `docker_build_pkg.sh` or `docker_build_run.sh` (e.g., apt/conda/pip steps there), not by editing the Dockerfile/base scripts
* Build dependencies missing → modify `docker_build_pkg.sh`
* Compilation errors → modify `docker_build_pkg.sh` to add compiler flags or patch code
* Build timeout → complex build may need optimization or longer timeout

### 2. Profile Stage

* Runs `profile.sh` inside the container
* Validates that ASV benchmarks can be discovered and executed
* Generates baseline performance measurements
* Timeout: 3600 seconds (1 hour)

**Common failures**:

* ASV config not found → check repository structure
* Benchmark import errors → install missing runtime dependencies via `docker_build_run.sh` and/or `docker_build_pkg.sh`
* Benchmark execution failures → check benchmark code compatibility

### 3. Tests Stage

* Runs `run-tests.sh` inside the container
* Executes pytest test suite
* Validates that tests pass
* Timeout: 3600 seconds (1 hour)

**Common failures**:

* Test dependencies missing → install in `docker_build_run.sh` and/or `docker_build_pkg.sh`
* Import errors → package installation issue in pkg stage
* Test failures → may need code patches or repository-specific setup in `docker_build_run.sh` and/or `docker_build_pkg.sh`

### 4. DockerHub Push Stage

* Builds the final image with benchmark data
* Pushes to DockerHub registry
* Requires DockerHub credentials in config.json or environment variables

**Common failures**:

* Authentication errors → check DOCKERHUB_USERNAME, DOCKERHUB_TOKEN
* Network issues → retry
* Registry quota exceeded → contact registry administrator

## Common Debugging Workflow

### 1. Identify the Failure Stage

```bash
python dataset/verify.py --task dataset/formulacode_verified/<repo>/<sha>
```

Check `failure.json` in the task directory:

```bash
cat dataset/formulacode_verified/<repo>/<sha>/failure.json | jq '.stage'
```

### 2. Read the Error Details

```bash
# Get the error message
cat failure.json | jq -r '.error_message'

# Get stderr (most useful for debugging)
cat failure.json | jq -r '.stderr' | less

# Get stdout
cat failure.json | jq -r '.stdout' | less
```

### 3. Make Targeted Changes

Based on the stage (only edit `docker_build_pkg.sh` and/or `docker_build_run.sh`):

**Build failures** → Edit `docker_build_pkg.sh`:

```bash
# Example: Add missing build dependency
vim dataset/formulacode_verified/<repo>/<sha>/docker_build_pkg.sh

# Add before the MODEL EDIT AREA:
micromamba install -y -n "$ENV_NAME" -c conda-forge cython numpy
```

**Profile failures** → Edit `docker_build_run.sh` (or `docker_build_pkg.sh` if it impacts installation):

```bash
# Example: Add missing runtime dependency needed by benchmarks
vim dataset/formulacode_verified/<repo>/<sha>/docker_build_run.sh
```

**Test failures** → Edit `docker_build_run.sh` or `docker_build_pkg.sh`:

```bash
# Add test dependency
vim dataset/formulacode_verified/<repo>/<sha>/docker_build_pkg.sh

# Add after package installation:
micromamba run -n "$ENV_NAME" pip install pytest-xdist pytest-timeout
```

### 4. Rerun Verification

```bash
python dataset/verify.py --task dataset/formulacode_verified/<repo>/<sha>
```

### 5. Iterate Until Success

Repeat steps 1-4 until `verification_success.json` appears.

## Tips and Best Practices

### Reading Logs

* `failure.json` stdout/stderr may be truncated
* Full logs are in `dataset/verify.log`
* Use `jq` to parse JSON output

### Common Patterns

**Missing Compiler Flags**:

```bash
# In docker_build_pkg.sh, before pip install:
export CFLAGS="${CFLAGS:--Wno-error=incompatible-pointer-types}"
```

**Submodule Issues**:

```bash
# In docker_build_pkg.sh:
git -C "$REPO_ROOT" submodule update --init --recursive
```

**Cython/Extension Building**:

```bash
# In docker_build_pkg.sh:
micromamba install -y -n "$ENV_NAME" -c conda-forge cython
micromamba run -n "$ENV_NAME" python setup.py build_ext --inplace
```

**Editing Source Files**:

```bash
# In docker_build_pkg.sh, before pip install:
sed -i 's/problematic_line/fixed_line/' src/module/file.py
```

### Environment Variables Available

From `docker_build_base.sh` and `docker_build_env.sh`:

* `REPO_ROOT`: Repository root path (usually `/workspace/repo`)
* `ENV_NAME`: Current environment name (e.g., `asv_3.10`)
* `ASV_PY_VERSIONS`: Space-separated Python versions
* `CONF_NAME`: Path to ASV config file
* `ALL_EXTRAS`: Extras to install (if any)
* `MAMBA_ROOT_PREFIX`: Micromamba root (`/opt/conda`)

### Docker Build Optimization

The Dockerfile uses layer caching:

* Changes to later stages don't rebuild earlier ones
* Modify the earliest stage necessary
* `docker_build_pkg.sh` is the most commonly edited file (followed by `docker_build_run.sh`)

### Testing Changes Locally

You can manually build and test the Docker image:

```bash
cd dataset/formulacode_verified/<repo>/<sha>

# Build just the 'run' stage
docker build -t test-image \
  --build-arg REPO_URL=https://github.com/<owner>/<repo> \
  --build-arg COMMIT_SHA=<sha> \
  --target run \
  -f Dockerfile \
  .

# Run profile.sh manually
docker run --rm test-image /profile.sh /tmp/profile

# Run run-tests.sh manually
docker run --rm test-image /run-tests.sh <base_commit>
```

## Configuration

### dataset/config.json

Required configuration:

```json
{
  "context_registry_path": "path/to/registry.json",
  "dockerhub_repo": "all",
  "dockerhub_username": "your-username",
  "dockerhub_token": "your-token"
}
```

Or use environment variables:

* `DOCKERHUB_USERNAME`
* `DOCKERHUB_TOKEN`

## Advanced Usage

### Batch Verification

Verify all tasks in a repository:

```bash
python dataset/verify.py --task dataset/formulacode_verified/optuna_optuna
```

Verify all tasks:

```bash
python dataset/verify.py --task dataset/formulacode_verified
```

This runs verification in parallel (max 32 workers) and creates:

* Individual `failure.json` or `verification_success.json` per task
* `all_verification_successes.jsonl` with all successful builds

### Understanding the Context Registry

After successful verification, the task context is registered in the context registry JSON file. This allows the verified Docker build context to be reused for:

* Building images with different tags (pkg, run, final)
* Reproducing builds
* Sharing verified configurations

## Troubleshooting

### Build Hangs/Timeouts

* Default timeout: 3600 seconds
* Check for network issues during dependency installation
* Some packages have very long build times (increase timeout if needed)

### Permission Errors

* All containers run as root
* Host files should be readable
* Output directories are created with `umask 022`

### Micromamba Issues

* Environment not activating → check `source /etc/profile.d/asv_utils.sh`
* Package conflicts → review env_payload dependencies
* Cache issues → build with `docker build --no-cache`

### Import Errors

* Package not found → verify editable install succeeded
* Module not found → check all dependencies in env_payload
* C extension import fails → check compiler flags, system libraries

## Summary

The verification workflow is designed to be iterative and debuggable:

1. **Run** verify.py
2. **Check** failure.json for errors
3. **Edit** `docker_build_pkg.sh` and/or `docker_build_run.sh`
4. **Rerun** verify.py
5. **Succeed** when verification_success.json appears

Most fixes involve editing `docker_build_pkg.sh` to add dependencies, set compiler flags, or patch source code, and editing `docker_build_run.sh` for repository-specific runtime/test/benchmark setup. The Docker multi-stage build ensures efficient iteration by caching earlier stages.

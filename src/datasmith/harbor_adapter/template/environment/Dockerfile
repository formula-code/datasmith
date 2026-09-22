FROM {{ base_image }}

# Install required system packages
RUN apt-get update && apt-get install -y git tmux asciinema curl
RUN curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
RUN mkdir -p /logs

# ── Bake the qwen-code agent (Node 22 via nvm) at build time ──────────────────
# Previously installed per-trial by src/harbor/agents/installed/qwen_code.py's
# install() over external DNS/npm on EVERY container — the documented setup-time
# sink / empty-trajectory culprit. The layout mirrors that install() (~/.nvm) so
# `. ~/.nvm/nvm.sh; qwen --version` works at runtime with no network. Pin matches
# the launcher's QWEN_VERSION (run_local_train_qwen.sh, default 0.22.3).
ARG QWEN_CODE_VERSION=0.22.3
ENV NVM_DIR=/root/.nvm
# nvm.sh is a bash script and is NOT 'set -u' clean (it dereferences $_), so the
# default /bin/sh (dash) + 'set -u' aborts it with
# "/root/.nvm/nvm.sh: _: parameter not set" and the whole layer dies. Run these
# layers under bash and relax -u only across the source. bash is also what makes
# the `micromamba shell hook --shell=bash` below honest.
SHELL ["/bin/bash", "-c"]
RUN set -eux; \
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.2/install.sh | bash; \
    set +u; . "$NVM_DIR/nvm.sh"; set -u; \
    nvm install 22; \
    npm install -g "@qwen-code/qwen-code@${QWEN_CODE_VERSION}"; \
    set +u; . "$NVM_DIR/nvm.sh"; set -u; qwen --version

# ── Bake the verifier deps into the ASV micromamba env at build time ──────────
# Previously installed per-trial in template/tests/setup.sh (with '|| true', so
# failures were silent — a "no-learning" failure mode). Same specs, now baked.
#
# Pinned for reproducibility (was: unpinned '--upgrade' + tip-of-branch git):
#   PyPI pins are the latest stable at 2026-09-22.
#   coverage is marker-split: 7.16.1 requires Python >= 3.10 and some ASV envs
#   are older (joblib's is 3.8.20), where a single pin aborts the build.
#   asv_runner==0.3.1 and jinja2==3.1.6 are verified fine on 3.8.
#   The two git deps are pinned to the resolved HEAD commit SHAs (git ls-remote
#   on 2026-09-22); bump both the SHA and the date comment together when you
#   deliberately want newer verifier code.
# The explicit PATH export is load-bearing: uv's installer only edits rc files,
# which a non-interactive RUN never sources.
ARG LSV_REF=fc16ba47239f68b93c335be22b1d2c37d259667c
ARG SNAPSHOT_TESTER_REF=f08bb0cd51a03c891e65060e25e69f8700055ba2
RUN set -eux; \
    set +u; \
    . /etc/profile.d/asv_utils.sh 2>/dev/null || true; \
    . /etc/profile.d/asv_build_vars.sh 2>/dev/null || true; \
    set -u; \
    : "${ENV_NAME:?ENV_NAME did not resolve from /etc/profile.d/asv_build_vars.sh}"; \
    export PATH="/root/.local/bin:${HOME}/.local/bin:${PATH}"; \
    eval "$(micromamba shell hook --shell=bash)"; \
    micromamba run -n "$ENV_NAME" uv pip install \
        "asv_runner==0.3.1" "jinja2==3.1.6" \
        "coverage==7.16.1; python_version >= '3.10'" \
        "coverage==7.6.1;  python_version <  '3.10'"; \
    micromamba run -n "$ENV_NAME" uv pip install --no-cache \
        "git+https://github.com/formula-code/lsv.git@${LSV_REF}" \
        "git+https://github.com/formula-code/snapshot-tester.git@${SNAPSHOT_TESTER_REF}"

# Copy environment setup scripts
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# ── Build-time copies for the baseline bake below ─────────────────────────────
# At RUNTIME the LSV helpers live at /tests: src/harbor/trial/trial.py's
# _run_setup_script uploads the whole tests/ dir to /tests BEFORE running
# setup.sh, so tests/setup.sh and tests/test.sh read /tests/*.py as usual.
# The bake below runs during `docker build`, where nothing is mounted yet, so
# lsv_init.py and the config it reads must already exist in the image. They are
# copied to /opt/lsv for that one purpose; nothing at runtime imports from
# there, so there is deliberately no PYTHONPATH=/opt/lsv (it would shadow the
# uploaded /tests copies with a frozen duplicate). /tests/config.json is
# overwritten at runtime by the identical uploaded copy.
# The docker build context is the task's environment/ directory, so the adapter
# writes lsv_init.py + config.json next to this Dockerfile as well.
# (The mkdir sits here rather than next to `mkdir -p /logs` above so the
# expensive qwen/deps layers keep their cache when only the bake tail changes.)
RUN mkdir -p /opt/lsv /tests
COPY lsv_init.py /opt/lsv/lsv_init.py
COPY config.json /tests/config.json

# Thread caps. The base image (docker/templates/Dockerfile.base) already pins
# OMP/MKL/OPENBLAS_NUM_THREADS=1, but numba reads its own NUMBA_NUM_THREADS and
# otherwise defaults to the HOST core count (128 on the training boxes) inside
# a {{ numba_threads }}-cpu cgroup quota -- heavy oversubscription that distorts
# timings on numba-heavy families (flox, uxarray, numpy). Pinned to the trial
# cpu quota, and set BEFORE the baseline bake below so the baked baseline and
# every trial measure under the same thread count.
ENV NUMBA_NUM_THREADS={{ numba_threads }}

# ── Bake the base-commit LSV baseline ONCE (was re-measured EVERY trial, ─────
# 100-490s each). Correct BY CONSTRUCTION: HEAD == base_commit here and no
# patch is applied, so the baseline is measured pre-patch (shapely defense);
# built in-lineage from this exact base image, so deps fshas match the trial
# (h11 defense). lsv_init.py stages this cache per-trial and runs force=False;
# any missing/partial/sha-mismatched cache falls back to force=True (the
# unbaked behavior) -> zero regression. DATASMITH_LSV_ROUNDS must equal the
# trial rounds; the adapter renders the task's own --rounds as the default.
# Non-fatal by design: a failed bake leaves no cache and every trial simply
# re-measures, exactly as before.
ARG DATASMITH_LSV_ROUNDS={{ lsv_rounds }}
RUN set +e; \
    set +u; \
    . /etc/profile.d/asv_utils.sh 2>/dev/null || true; \
    . /etc/profile.d/asv_build_vars.sh 2>/dev/null || true; \
    export PATH="/root/.local/bin:${HOME}/.local/bin:${PATH}"; \
    eval "$(micromamba shell hook --shell=bash)" 2>/dev/null || true; \
    LSV_OUTPUT_DIR=/opt/lsv/cache FORMULACODE_BAKE_BASELINE=1 \
      micromamba run -n "$ENV_NAME" python /opt/lsv/lsv_init.py --rounds "${DATASMITH_LSV_ROUNDS}"; \
    true

WORKDIR /workspace/repo
ENTRYPOINT ["/entrypoint.sh"]

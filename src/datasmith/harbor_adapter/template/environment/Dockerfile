FROM {{ base_image }}

RUN apt-get update && apt-get install -y git tmux asciinema curl
RUN curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
RUN mkdir -p /logs

# Bake qwen-code (same ~/.nvm layout as harbor's per-trial install) so trials need no network.
# Pin matches the launcher's QWEN_VERSION.
ARG QWEN_CODE_VERSION=0.22.3
ENV NVM_DIR=/root/.nvm
# nvm.sh is not `set -u` clean ($_), so relax -u around each source.
SHELL ["/bin/bash", "-c"]
RUN set -eux; \
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.2/install.sh | bash; \
    set +u; . "$NVM_DIR/nvm.sh"; set -u; \
    nvm install 22; \
    npm install -g "@qwen-code/qwen-code@${QWEN_CODE_VERSION}"; \
    set +u; . "$NVM_DIR/nvm.sh"; set -u; qwen --version

# Bake pinned verifier deps into the ASV env. coverage is split by python version: 7.16.1 needs >= 3.10.
# PATH export: uv's installer only edits rc files, which RUN never sources.
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

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Build-time copies for the bake only; runtime reads the uploaded /tests. No PYTHONPATH=/opt/lsv:
# it would shadow the /tests helpers with a frozen copy.
RUN mkdir -p /opt/lsv /tests
COPY lsv_init.py /opt/lsv/lsv_init.py
COPY config.json /tests/config.json

# numba otherwise uses the host core count; pin it to the trial cpu quota before the bake.
ENV NUMBA_NUM_THREADS={{ numba_threads }}

# Bake the base-commit LSV baseline once (HEAD == base_commit, unpatched), pinned to the trial's cpu count.
# Non-fatal: trials re-measure unless sha and env fingerprint (cpu, rounds, lsv) match.
ARG DATASMITH_LSV_ROUNDS={{ lsv_rounds }}
RUN set +e; \
    set +u; \
    . /etc/profile.d/asv_utils.sh 2>/dev/null || true; \
    . /etc/profile.d/asv_build_vars.sh 2>/dev/null || true; \
    export PATH="/root/.local/bin:${HOME}/.local/bin:${PATH}"; \
    eval "$(micromamba shell hook --shell=bash)" 2>/dev/null || true; \
    LSV_OUTPUT_DIR=/opt/lsv/cache FORMULACODE_BAKE_BASELINE=1 FORMULACODE_BAKE_CPUS={{ numba_threads }} \
      micromamba run -n "$ENV_NAME" python /opt/lsv/lsv_init.py --rounds "${DATASMITH_LSV_ROUNDS}"; \
    true

WORKDIR /workspace/repo
ENTRYPOINT ["/entrypoint.sh"]

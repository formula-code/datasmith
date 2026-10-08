FROM {{ base_image }}

RUN apt-get update && apt-get install -y git tmux asciinema curl
RUN curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
RUN mkdir -p /logs

# Install qwen-code at image build (same ~/.nvm layout as harbor's per-trial install) so trials need no network.
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

# Install pinned verifier deps into the ASV env at image build. coverage is split by python version: 7.16.1 needs >= 3.10.
# PATH export: uv's installer only edits rc files, which RUN never sources.
ARG LSV_REF=9d1b667b93f26e0a9eb23f63c7cc8e5a30a5d56a
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
{%- if restore_paths %}

# The base image's stored build script edited these files where the oracle patch changes them; start from the base commit.
COPY restore_base.sh rebuild.sh /opt/restore/
RUN set -e; set +u; . /etc/profile.d/asv_build_vars.sh; set -u; \
    eval "$(micromamba shell hook --shell=bash)"; micromamba activate "$ENV_NAME"; \
    bash /opt/restore/restore_base.sh {{ restore_paths }} && rm -rf /opt/restore
{%- endif %}

# Build-time copies for the baseline measurement only; runtime reads the uploaded /tests. This config.json has no patch
# or gt_hash. No PYTHONPATH=/opt/lsv: it would shadow the /tests helpers with a frozen copy.
RUN mkdir -p /opt/lsv /tests
COPY lsv_init.py project_imports.py /opt/lsv/
COPY config.json /tests/config.json

# numba otherwise uses the host core count; pin it to the trial cpu quota before the baseline measurement.
ENV NUMBA_NUM_THREADS={{ numba_threads }}

# Measure the base-commit LSV baseline once at image build (HEAD == base_commit, unpatched), pinned to the trial's cpu count.
# Non-fatal: trials re-measure unless sha and env fingerprint (cpu, rounds, lsv) match.
ARG DATASMITH_LSV_ROUNDS={{ lsv_rounds }}
RUN set +e; \
    set +u; \
    . /etc/profile.d/asv_utils.sh 2>/dev/null || true; \
    . /etc/profile.d/asv_build_vars.sh 2>/dev/null || true; \
    export PATH="/root/.local/bin:${HOME}/.local/bin:${PATH}"; \
    eval "$(micromamba shell hook --shell=bash)" 2>/dev/null || true; \
    FC_PROJECT_IMPORTS_REPORT=/opt/lsv/project_imports.json micromamba run -n "$ENV_NAME" python /opt/lsv/project_imports.py --remove; \
    LSV_OUTPUT_DIR=/opt/lsv/cache FORMULACODE_IMAGE_BASELINE=1 FORMULACODE_IMAGE_CPUS={{ numba_threads }} \
      micromamba run -n "$ENV_NAME" python /opt/lsv/lsv_init.py --rounds "${DATASMITH_LSV_ROUNDS}"; \
    true

# Processes that skip micromamba activation (an agent's `bash -c`) get the task env through /opt/task-env. Set after the
# baseline so the measurement environment does not change.
RUN set -e; . /etc/profile.d/asv_build_vars.sh; \
    ln -sfn "${MAMBA_ROOT_PREFIX:-/opt/conda}/envs/${ENV_NAME}" /opt/task-env; /opt/task-env/bin/python --version
ENV PATH=/opt/task-env/bin:$PATH

# Last, so no later step brings history back. Base image layers still hold it, but the container cannot see them.
COPY scrub_git.sh /opt/scrub_git.sh
RUN bash /opt/scrub_git.sh /workspace/repo && rm /opt/scrub_git.sh

WORKDIR /workspace/repo
ENTRYPOINT ["/entrypoint.sh"]

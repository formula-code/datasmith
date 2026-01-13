# syntax=docker/dockerfile:1.7

ARG BASE_IMAGE=buildpack-deps:jammy
ARG PY_VERSION=""                     # passed at build time, used inside stages

FROM ${BASE_IMAGE} AS base
# Re-declare any ARGs you need in this stage
ARG PY_VERSION=""

# Install Rust using rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Configure the PATH for Rust tools
ENV PATH="/root/.cargo/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq cmake ninja-build libopenmpi-dev libgeos-dev pkg-config graphviz libgraphviz-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest \
      | tar -xvj -C /usr/local/bin --strip-components=1 bin/micromamba

ENV MAMBA_ROOT_PREFIX=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    MAMBA_DOCKERFILE_ACTIVATE=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    OMP_NUM_THREADS=1

RUN micromamba install -y -p $MAMBA_ROOT_PREFIX -c conda-forge \
        python=3.10 \
        git asv pyperf mamba conda libmambapy && \
    micromamba clean --all --yes

RUN mkdir -p /workspace /output
WORKDIR /workspace

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"


COPY docker_build_base.sh /workspace/docker_build_base.sh
RUN chmod +x /workspace/docker_build_base.sh && \
    if [ -n "${PY_VERSION}" ]; then \
        /workspace/docker_build_base.sh --py-version "${PY_VERSION}"; \
    else \
        /workspace/docker_build_base.sh; \
    fi

RUN micromamba clean --all --yes

# ---------- ENV STAGE ----------
FROM base AS env
ARG PY_VERSION=""

ARG REPO_URL
ARG COMMIT_SHA
ARG ENV_PAYLOAD="{}"

# Entrypoint is inherited by pkg
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

RUN git clone "$REPO_URL" /workspace/repo
WORKDIR /workspace/repo
RUN git checkout "$COMMIT_SHA"
LABEL vcs.url="$REPO_URL" vcs.ref="$COMMIT_SHA"

# ---- ENV STAGE: create envs, persist vars, install smoke tool ----
COPY docker_build_env.sh /workspace/repo/docker_build_env.sh
RUN chmod +x /workspace/repo/docker_build_env.sh && \
    echo "${ENV_PAYLOAD}" > /tmp/env_payload.json && \
    /workspace/repo/docker_build_env.sh --env-payload /tmp/env_payload.json


# Ensure test dependency 'ddt' is installed in every conda env we just created
RUN for env in /opt/conda/envs/*; do \
      if [ -d "$env" ] && [ -x "$env/bin/python" ]; then \
        "$env/bin/python" -m pip install ddt; \
      fi; \
    done

# ---------- PKG STAGE ----------
FROM env AS pkg
ARG PY_VERSION=""

# ---- PKG STAGE: build+test the package for each ASV Python ----
COPY docker_build_pkg.sh /workspace/repo/docker_build_pkg.sh
RUN chmod +x /workspace/repo/docker_build_pkg.sh && \
    /workspace/repo/docker_build_pkg.sh
# If you want to restrict to one version at build time, replace with:
# RUN PY_VERSION=3.10 /workspace/repo/docker_build_pkg.sh

# Copy these files after to avoid rerunning pkg stage
COPY profile.sh /profile.sh
COPY run_tests.sh /run_tests.sh
RUN chmod +x /profile.sh /run_tests.sh

# run micromamba clean in all
RUN micromamba clean --all --yes

from pkg AS run

COPY docker_build_run.sh /docker_build_run.sh
RUN chmod +x /docker_build_run.sh \
    && /docker_build_run.sh

# ---------- FINAL STAGE ----------
FROM run AS final
ARG BENCHMARKS=""

COPY docker_build_final.sh /docker_build_final.sh
RUN chmod +x /docker_build_final.sh \
    && printf "%s\n" "${BENCHMARKS}" > /tmp/asv_benchmarks.txt; \
    ARG_PATH=/tmp/asv_benchmarks.txt; \
    /docker_build_final.sh "${ARG_PATH}";

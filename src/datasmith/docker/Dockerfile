# syntax=docker/dockerfile:1.7

ARG BASE_IMAGE=buildpack-deps:jammy
FROM ${BASE_IMAGE} AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq cmake ninja-build && \
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


COPY docker_build_base.sh /workspace/docker_build_base.sh
RUN chmod +x /workspace/docker_build_base.sh && \
    /workspace/docker_build_base.sh

RUN micromamba clean --all --yes
FROM base AS env

ARG REPO_URL
ARG COMMIT_SHA

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
    /workspace/repo/docker_build_env.sh

FROM env AS pkg
COPY profile.sh /profile.sh
RUN chmod +x /profile.sh

# ---- PKG STAGE: build+test the package for each ASV Python ----
COPY docker_build_pkg.sh /workspace/repo/docker_build_pkg.sh
RUN chmod +x /workspace/repo/docker_build_pkg.sh && \
    /workspace/repo/docker_build_pkg.sh
# If you want to restrict to one version at build time, replace with:
# RUN PY_VERSION=3.10 /workspace/repo/docker_build_pkg.sh

# run micromamba clean in all
RUN micromamba clean --all --yes

from pkg AS run

COPY docker_build_run.sh /docker_build_run.sh
RUN chmod +x /docker_build_run.sh \
    && /docker_build_run.sh

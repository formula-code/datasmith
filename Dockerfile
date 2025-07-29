FROM buildpack-deps:jammy

ARG REPO_URL
ARG COMMIT_SHA
ARG BUILD_SCRIPT # A build script with custom installation commands provided by the user
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl git build-essential jq && \
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
        git asv pyperf mamba conda libmambapy jq && \
    micromamba clean --all --yes

RUN mkdir -p /workspace /output
WORKDIR /workspace

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN git clone "${REPO_URL}" repo && \
    cd repo && \
    git checkout "${COMMIT_SHA}" && \
    \
    CONF_FILE=$(find . -type f -name "asv.*.json" | head -n 1) && \
    if [[ -z "${CONF_FILE}" ]]; then \
        echo "❌  No asv.*.json found." && exit 1; \
    fi && \
    echo "✅  Using ASV config: ${CONF_FILE}" && \
    \
PY_VERS=$(echo "import json, pathlib; \
cfg = pathlib.Path('${CONF_FILE}').read_text(); \
data = json.loads(cfg); \
vers = data.get('pythons') or data.get('python') or []; \
print(' '.join(dict.fromkeys(vers)))" | python -) && \
    if [[ -z "${PY_VERS}" ]]; then \
        echo "❌  No Python versions declared in ${CONF_FILE}" && exit 1; \
    fi && \
    echo "🐍  Creating Conda envs for: ${PY_VERS}" && \
    \
    for v in ${PY_VERS}; do \
        micromamba create -y -n "asv_${v}" -c conda-forge \
            python=${v} git mamba conda "libmambapy<=1.9.9"; \
    done

WORKDIR /workspace/repo

RUN echo "${BUILD_SCRIPT}" > /workspace/repo/docker_build.sh && \
    chmod +x /workspace/repo/docker_build.sh && \
    /workspace/repo/docker_build.sh

ENTRYPOINT ["/entrypoint.sh"]

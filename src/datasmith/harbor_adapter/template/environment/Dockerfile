FROM {{ base_image }}

# Install required system packages
RUN apt-get update && apt-get install -y git tmux asciinema
RUN curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
RUN mkdir -p /logs /opt/lsv /tests

# Copy environment setup scripts
COPY entrypoint.sh /entrypoint.sh

# Bake LSV helper scripts into the image so setup.sh can find them before
# Harbor's verifier mounts /tests/ (verifier runs AFTER setup.sh).
COPY lsv_init.py /opt/lsv/lsv_init.py

# lsv_init.py reads /tests/config.json during setup.sh — bake it in too.
# Harbor's verifier later overwrites this with identical content.
COPY config.json /tests/config.json

RUN chmod +x /entrypoint.sh

# Thread caps. The base image (docker/templates/Dockerfile.base) already pins
# OMP/MKL/OPENBLAS_NUM_THREADS=1, but numba reads its own NUMBA_NUM_THREADS and
# otherwise defaults to the HOST core count (128 on the training boxes) inside
# a {{ numba_threads }}-cpu cgroup quota -- heavy oversubscription that distorts
# timings on numba-heavy families (flox, uxarray, numpy). Pinned to the trial
# cpu quota, and set BEFORE the baseline bake below so the baked baseline and
# every trial measure under the same thread count.
ENV NUMBA_NUM_THREADS={{ numba_threads }}

# NOTE: LSV is already installed in the base image's asv env (Layer A), and
# build-time networking on this box cannot reach GitHub (DNS-restricted), so we
# do NOT pip-install deps here. The runtime setup.sh keeps a guarded install as
# a fallback (it runs with host networking, where GitHub is reachable).

# ── Bake the base-commit LSV baseline ONCE (was re-measured EVERY trial, ─────
# 100-490s each). Correct BY CONSTRUCTION: HEAD == base_commit here and no
# patch is applied, so the baseline is measured pre-patch (shapely defense);
# built in-lineage from this exact base image, so deps fshas match the trial
# (h11 defense). lsv_init.py stages this cache per-trial and runs force=False;
# any missing/partial/sha-mismatched cache falls back to force=True (today's
# behavior) -> zero regression. DATASMITH_LSV_ROUNDS must equal the trial rounds.
ARG DATASMITH_LSV_ROUNDS={{ lsv_rounds }}
RUN bash -c 'set +e; \
    source /etc/profile.d/asv_utils.sh 2>/dev/null || true; \
    source /etc/profile.d/asv_build_vars.sh 2>/dev/null || true; \
    eval "$(micromamba shell hook --shell=bash)" 2>/dev/null || true; \
    LSV_OUTPUT_DIR=/opt/lsv/cache FORMULACODE_BAKE_BASELINE=1 \
      micromamba run -n "$ENV_NAME" python /opt/lsv/lsv_init.py --rounds "'"${DATASMITH_LSV_ROUNDS}"'"; \
    true'

WORKDIR /workspace/repo
ENTRYPOINT ["/entrypoint.sh"]

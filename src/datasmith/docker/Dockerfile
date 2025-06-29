FROM buildpack-deps:jammy

ARG REPO_URL

RUN curl -Ls "https://micromamba.snakepit.net/api/micromamba/linux-64/latest" \
      | tar -xvj -C /usr/local/bin --strip-components=1 bin/micromamba

ENV MAMBA_ROOT_PREFIX=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    MAMBA_DOCKERFILE_ACTIVATE=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    OMP_NUM_THREADS=1

RUN micromamba install -y -p $MAMBA_ROOT_PREFIX -c conda-forge \
       python=3.10 \
       git \
       asv \
       pyperf \
       libmambapy \
       mamba \
       conda \
    && micromamba clean --all --yes

RUN mkdir /workspace /output
WORKDIR /workspace

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN git clone ${REPO_URL} /workspace/repo
WORKDIR /workspace/repo
ENTRYPOINT ["/entrypoint.sh"]

from datasmith.docker.context import DockerContext

DEFAULT_DOCKER_CONTEXT = DockerContext()

ASTROPY_DOCKER_CONTEXT = DockerContext(
    dockerfile_data="""
FROM buildpack-deps:jammy

RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]
RUN ["apt-get", "install", "-y", "curl", "git", "build-essential"]
ARG REPO_URL

RUN curl -Ls "https://micro.mamba.pm/api/micromamba/linux-64/latest" \
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

RUN git clone https://github.com/astropy/astropy /workspace/repo
WORKDIR /workspace/repo
RUN git clone -b main https://github.com/astropy/astropy-benchmarks.git --single-branch
ENTRYPOINT ["/entrypoint.sh"]
""".strip(),
    entrypoint_data=DEFAULT_DOCKER_CONTEXT.entrypoint_data,
)

CONTEXT_REGISTRY: dict[str, DockerContext] = {
    "default": DEFAULT_DOCKER_CONTEXT,
    "asv-astropy-astropy": ASTROPY_DOCKER_CONTEXT,
}

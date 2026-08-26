FROM {{ base_image }}

# Install required system packages
RUN apt-get update && apt-get install -y git tmux asciinema
RUN curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
RUN mkdir -p /logs /opt/lsv /tests

# Copy environment setup scripts
COPY entrypoint.sh /entrypoint.sh

# Bake LSV helper scripts into the image so setup.sh can find them before
# Harbor's verifier mounts /tests/ (verifier runs AFTER setup.sh).
COPY lsv_init.py lsv_measure.py lsv_cache_writeback.py parser.py upload.py pytest_runner.py jinja_patch_plugin_pandas.py /opt/lsv/

# lsv_init.py reads /tests/config.json during setup.sh — bake it in too.
# Harbor's verifier later overwrites this with identical content.
COPY config.json /tests/config.json

RUN chmod +x /entrypoint.sh
ENV PYTHONPATH=/opt/lsv:${PYTHONPATH:-}

WORKDIR /workspace/repo
ENTRYPOINT ["/entrypoint.sh"]

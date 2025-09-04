#!/usr/bin/env bash
set -euo pipefail

# -------- Persisted build variables --------
write_build_vars() {
    local py_versions="$1"
    local import_name="$2"

    mkdir -p /etc/asv_env
    echo "$py_versions" > /etc/asv_env/py_versions
    echo "$import_name" > /etc/asv_env/import_name

    # Exported for every future shell (pkg script, interactive, etc.)
    cat >/etc/profile.d/asv_build_vars.sh <<EOF
# Auto-generated during docker_build_env.sh
export ASV_PY_VERSIONS="${py_versions}"
export IMPORT_NAME="${import_name}"
EOF
}

# Append install-related variables (extras/specs) so the follow-up script can use them.
append_install_vars() {
    local extras_all="$1"
    local setuppy_cmd="$2"

    mkdir -p /etc/asv_env
    printf "%s\n" "$extras_all" > /etc/asv_env/extras_all
    printf "%s\n" "$setuppy_cmd" > /etc/asv_env/setuppy_cmd

    # Export for future shells
    cat >>/etc/profile.d/asv_build_vars.sh <<EOF
export ALL_EXTRAS="${extras_all}"
export SAVED_SETUPPY_CMD="${setuppy_cmd}"
EOF
}

# shellcheck disable=SC1091
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

# Ensure base micromamba is active for introspecting ASV config
micromamba activate base

IMPORT_NAME="$(detect_import_name || true)"
if [[ -z "$IMPORT_NAME" ]]; then
    echo "WARN: Could not determine import name; the pkg stage will fall back to local detection."
fi

# Move into the directory that contains asv.*.json
cd_asv_json_dir || { echo "No 'asv.*.json' file found." >&2; exit 1; }

CONF_NAME="$(asv_conf_name || true)"
if [[ -z "${CONF_NAME:-}" ]]; then
    echo "No 'asv.*.json' file found." >&2
    exit 1
fi

# Read python versions from the ASV config
PY_VERSIONS=$(python - <<PY
import asv
cfg = asv.config.Config.load("$CONF_NAME")
# remove any python less than 3.7 (ASV dropped support for 3.6+ in v0.5)
cfg.pythons = [v for v in cfg.pythons if tuple(map(int, v.split('.'))) >= (3,7)]
print(" ".join(cfg.pythons))
PY
)

# If none found, throw a noticeable error and exit
if [[ -z "$PY_VERSIONS" ]]; then
    echo "No Satisfying PY_VERSIONS found in $CONF_NAME" >&2
    # echo asv config for debugging
    cat "$CONF_NAME" >&2
    exit 1
fi

# for each python version, ensure asv_{version} env exists. Otherwise error out.
for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"

    if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
        echo "Error: Expected micromamba environment '$ENV_NAME' not found."
        exit 1
    fi
done

# Persist base variables
write_build_vars "$PY_VERSIONS" "${IMPORT_NAME:-}"
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
ALL_EXTRAS="$(detect_extras --repo-root "$REPO_ROOT" 2>/dev/null | tr -s ' ' | tr ' ' ',')"


SETUPPY_CMD="develop"
append_install_vars "${ALL_EXTRAS}" "${SETUPPY_CMD}"

echo "Environment setup complete."

import datetime
import json
import re
from copy import deepcopy
from pathlib import Path

from datasmith.docker.context import ContextRegistry, DockerContext

curr_date: str = datetime.datetime.now().isoformat()


def patch_dockerfile_data(text: str) -> str:
    # The single-line command we want to expand
    old_cmd = (
        'PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" '
        'python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS'
    )

    # Already patched? (look for the failure message marker, which is robust to spacing)
    if "Failed to install with --no-build-isolation$EXTRAS" in text:
        # Still replace the IMP= line even if already patched
        return re.sub(
            r'^([ \t]*)IMP="\$\{IMPORT_NAME:-\}"', r'\1IMP="$(detect_import_name || true)"', text, flags=re.MULTILINE
        )

    # Replacement block (we will indent it to match the leading whitespace we capture)
    base_block = (
        'if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then\n'
        '    warn "Failed to install with --no-build-isolation$EXTRAS, trying without extras"\n'
        '    PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"\n'
        "fi\n"
    )

    def indent_block(indent: str, block: str) -> str:
        return "".join(indent + line if line.strip() else line for line in block.splitlines(keepends=True))

    # 1) Replace an existing "if ! <old_cmd> ; then" header (avoid nested if ! if !)
    pattern_if_then = re.compile(rf"^([ \t]*)if\s+!\s*{re.escape(old_cmd)}\s*;\s*then[ \t]*\n?", flags=re.MULTILINE)

    def repl_if_then(m: re.Match) -> str:
        return indent_block(m.group(1), base_block)

    text_new, n_if = pattern_if_then.subn(repl_if_then, text)
    if n_if > 0:
        # Ensure IMP= is also replaced
        return re.sub(
            r'^([ \t]*)IMP="\$\{IMPORT_NAME:-\}"',
            r'\1IMP="$(detect_import_name || true)"',
            text_new,
            flags=re.MULTILINE,
        )

    # 2) Replace a standalone command line (with or without trailing newline)
    pattern_cmd = re.compile(rf"^([ \t]*){re.escape(old_cmd)}[ \t]*\n?", flags=re.MULTILINE)

    def repl_cmd(m: re.Match) -> str:
        return indent_block(m.group(1), base_block)

    text_new, _ = pattern_cmd.subn(repl_cmd, text)

    # 3) Replace IMP= line everywhere
    text_new = re.sub(
        r'^([ \t]*)IMP="\$\{IMPORT_NAME:-\}"', r'\1IMP="$(detect_import_name || true)"', text_new, flags=re.MULTILINE
    )

    return text_new


def update_cr(cr: ContextRegistry) -> ContextRegistry:
    new_reg = {}
    for k, v in cr.registry.items():
        new_v = deepcopy(v)
        new_v.entrypoint_data = DockerContext().entrypoint_data
        new_v.dockerfile_data = DockerContext().dockerfile_data
        new_v.building_data = patch_dockerfile_data(v.building_data)
        new_v.env_building_data = DockerContext().env_building_data
        new_v.base_building_data = DockerContext().base_building_data
        new_v.run_building_data = DockerContext().run_building_data
        new_v.profile_data = DockerContext().profile_data
        new_v.run_tests_data = DockerContext().run_tests_data
        new_reg[k] = new_v

    cr.registry = new_reg
    return cr


def merge_with_mtime(merged_json: dict, reg_json: dict, modified_time: float) -> None:
    # when updating, if there is a conflict, prefer the one with the latest modified time
    for context in reg_json["contexts"]:
        new_context = reg_json["contexts"][context]
        if "building_data" not in new_context or not new_context["building_data"]:
            continue
        if context in merged_json.get("contexts", {}):
            existing_time = merged_json["contexts"][context].get("modified_time", 0)
            # Only prefer if new context is not empty and has newer modified_time
            if modified_time > existing_time:
                merged_json["contexts"][context] = new_context
                merged_json["contexts"][context]["modified_time"] = modified_time
        else:
            if "contexts" not in merged_json:
                merged_json["contexts"] = {}
            merged_json["contexts"][context] = new_context
            merged_json["contexts"][context]["modified_time"] = modified_time
    # also merge other top-level keys if they don't exist
    for key in reg_json:
        if key != "contexts" and key not in merged_json:
            merged_json[key] = reg_json[key]


def merge_registries(registries: list[Path]) -> dict:
    merged_json: dict = {}
    for reg in registries:
        reg_json = json.loads(reg.read_text())
        modified_time = reg.stat().st_mtime
        merge_with_mtime(merged_json, reg_json, modified_time)
    return merged_json

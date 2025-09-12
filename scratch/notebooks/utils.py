import datetime
import json
from copy import deepcopy
from pathlib import Path

from datasmith.docker.context import ContextRegistry, DockerContext

curr_date: str = datetime.datetime.now().isoformat()


def update_cr(cr: ContextRegistry) -> ContextRegistry:
    new_reg = {}
    for k, v in cr.registry.items():
        new_v = deepcopy(v)
        new_v.entrypoint_data = DockerContext().entrypoint_data
        new_v.dockerfile_data = DockerContext().dockerfile_data
        new_v.env_building_data = DockerContext().env_building_data
        new_v.base_building_data = DockerContext().base_building_data
        new_v.run_building_data = DockerContext().run_building_data
        new_reg[k] = new_v

    cr.registry = new_reg
    return cr


def merge_with_mtime(merged_json: dict, reg_json: dict, modified_time: float) -> None:
    # when updating, if there is a conflict, prefer the one with the latest modified time
    for key in reg_json["contexts"]:
        if key in merged_json.get("contexts", {}):
            existing_time = merged_json["contexts"][key].get("modified_time", 0)
            if modified_time > existing_time:
                merged_json["contexts"][key] = reg_json["contexts"][key]
                merged_json["contexts"][key]["modified_time"] = modified_time
        else:
            if "contexts" not in merged_json:
                merged_json["contexts"] = {}
            merged_json["contexts"][key] = reg_json["contexts"][key]
            merged_json["contexts"][key]["modified_time"] = modified_time
    # also merge other top-level keys if they don't exist
    for key in reg_json:
        if key != "contexts" and key not in merged_json:
            merged_json[key] = reg_json[key]


def merge_registries(registries: list[Path]) -> dict:
    merged_json = {}
    for reg in registries:
        reg_json = json.loads(reg.read_text())
        modified_time = reg.stat().st_mtime
        print(f"{reg} : {len(reg_json['contexts'])} entries")
        merge_with_mtime(merged_json, reg_json, modified_time)
    return merged_json

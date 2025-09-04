from copy import deepcopy
from pathlib import Path

from datasmith.docker.context import ContextRegistry, DockerContext


def update_cr_entrypoint(cr: ContextRegistry):
    new_reg = {}
    for k, v in cr.registry.items():
        new_v = deepcopy(v)
        new_v.entrypoint_data = DockerContext().entrypoint_data
        new_v.dockerfile_data = DockerContext().dockerfile_data
        new_v.env_building_data = DockerContext().env_building_data
        new_v.base_building_data = DockerContext().base_building_data
        new_reg[k] = new_v

    cr.registry = new_reg
    return cr


if __name__ == "__main__":
    # cr = ContextRegistry.load_from_file(Path("scratch/merged_context_registry_2025-09-04T08:32:08.486247.json"))
    # new_cr = update_cr_entrypoint(cr)
    # new_cr.save_to_file(Path("scratch/merged_context_registry_2025-09-04T08:32:08.486247.json"))

    # Detect all context_registry.json files in scratch/ and update them. The files might be named like merged_context_registry*.json or context_registry*.json
    for p in Path("scratch").glob("**/*context_registry*.json"):
        print(f"Updating {p}...")
        cr = ContextRegistry.load_from_file(p)
        new_cr = update_cr_entrypoint(cr)
        new_cr.save_to_file(p)
        print(f"Updated {p}.")

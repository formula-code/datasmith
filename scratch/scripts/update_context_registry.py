from copy import deepcopy
from pathlib import Path

from datasmith.docker.context import ContextRegistry, DockerContext


def update_cr_entrypoint(cr: ContextRegistry):
    new_reg = {}
    for k, v in cr.registry.items():
        new_v = deepcopy(v)
        new_v.entrypoint_data = DockerContext().entrypoint_data
        new_reg[k] = new_v

    cr.registry = new_reg
    return cr


if __name__ == "__main__":
    cr = ContextRegistry.load_from_file(Path("scratch/context_registry.json"))
    new_cr = update_cr_entrypoint(cr)
    new_cr.save_to_file(Path("scratch/context_registry_updated.json"))

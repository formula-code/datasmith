# Method to add new building_data.


1. Run `python scripts/validate_containers.py` with correct args. If it fails, it will print an exception message with building commands for the containers that need to be built.

2. Run each one individually, e.g.:
    ```bash
    $ docker build -t asv-scikit-learn-scikit-learn-bbdb2eff9b877c0ae00ed9854099b92119504f62 src/datasmith/docker/ --build-arg REPO_URL=https://www.github.com/scikit-learn/scikit-learn --build-arg COMMIT_SHA=bbdb2eff9b877c0ae00ed9854099b92119504f62
    ```
3. Change the `docker_build.sh` accordingly.

4. Add the new docker_build.sh to the context registry. e.g.:
    ```python
    CONTEXT_REGISTRY.register(
        "asv-scikit-learn-scikit-learn",
        DockerContext(
            building_data="""#!/usr/bin/env bash
    (The rest of the modified docker_build.sh script is omitted for brevity)
    """.strip(),
            dockerfile_data=CONTEXT_REGISTRY["default"].dockerfile_data,
            entrypoint_data=CONTEXT_REGISTRY["default"].entrypoint_data,
        ),
    )
    ```

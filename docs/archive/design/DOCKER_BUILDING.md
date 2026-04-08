Right now, we have a multi-stage dockerfile that builds the image in multiple stages. We should move towards a system where we have a base image for each toolchain, and then we can build the image for each repository, and each commit-sha on top of that.

Presently, each dockerfile is:
```
## Stage 1: Base
from {} as base

from base as repo
## Stage 2: Repo
from repo as env

## Stage 3: Env
from env as pkg

## Stage 4: Pkg
from pkg as run

## Stage 5: Run
from run as final
```

We want to break thisapart into multiple dockerfiles, each of which can be built independently.

Proposed:
```
$ docker build src/datasmith/docker/base/Dockerfile
This downloads a base image with uv pre-installed and installs some basic dependencies and tool chains.
stored as formulacode/base:latest

$ docker build src/datasmith/docker/repo/Dockerfile --build-arg REPO_URL=<repo_url>
This clones the target repository.
stored as formulacode/<repo_name>:latest

$ docker build src/datasmith/docker/

$ docker build src/datasmith/docker/env/Dockerfile --build-arg COMMIT_SHA=<commit_sha>
This builds the base environment with uv.
stored as formulacode/env:<commit_sha>

$ docker build src/datasmith/docker/pkg/Dockerfile --build-arg COMMIT_SHA=<commit_sha>
This actually installs the package and runs some basic smoke tests to ensure that the package is installable.
stored as formulacode/pkg:<commit_sha>

$ docker build src/datasmith/docker/run/Dockerfile --build-arg COMMIT_SHA=<commit_sha>
This installs some packages and scrubs future commits.
stored as formulacode/run:<commit_sha>

$ docker build src/datasmith/docker/final/Dockerfile --build-arg COMMIT_SHA=<commit_sha>
This installs some more packages and exports variables for asv to use.
stored as formulacode/final:<commit_sha>
```

()
This triggers a full build from scratch each time that has to
(base) download the base image and install uv and micromamba.
(repo) clone the target repository
(env) checkout the commit sha and build the base environment with uv.
(pkg) actually install the package and run some basic smoke tests to ensure that the package is installable.
(run) install some packages and scrub future commits.
(final) install some more packages and export variables for asv to use.
```

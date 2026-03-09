---
tags:
  - documentation
  - formulacode
---
## Abstract

A large part of formulacode's design requires interacting with docker images and containers. We might have to use the docker-sdk heavily in this repo. though any more scalable alternative is welcome! This document details all the code that helps create update and edit the dockerfiles we use here.

## High level overview

```mermaid
graph LR
    A --->|scrape| B
	A2 <-->|sync| B
    B --->|build&push| C
    
    A[Docker Script]
    A2[Database]
    B["`DockerScriptManager
    (This Feature)`"]
    C[DockerHub]
```


## Implementation Overview

For every PR `pandas-dev/pandas/issues/1234` , there are three docker images needed:
1. `formulacode/x86-py3_11:latest` -- The base image with dependencies preinstalled for python 3.XX. This is specified in the PR metadata.
2. `formulacode/x86-py3_11-pandas-dev-pandas:latest` -- The repository level image with the base package pulled to `/workspace/repo` along with the full history. This will make it much easier for us to checkout a specific commit later.
3. `formulacode/x86-py3_11-pandas-dev-pandas-1234:{broken|latest}` -- This is the `base_commit` of pull request `1234` on `pandas-dev`. 
	1. When the tag is `broken` it means that we've pulled the specific commit and added base utilities. However, the script is failing the verification check right now. These packages should only be kept locally and should not be committed.
	2. When the tag is `latest` it means that we've been able to synthesize the build script and pass the verification check.

**Verification**: This is exposed as a script called `verify.py` which, when loaded and run on the docker container, should exit without failure. The script should do the following checks:
* Check that `import {package_name}` works properly. This might be done inside/outside the folder. This can be another python script.
* Check that we can collect all asv benchmarks. The collected benchmarks should be saved as `asv_benchmarks.txt`. This can be some python script
* Check that we can run asv with `--quick`. Do on the command line.
* Check that we can collect all pytest tests without any errors `some python script`.
* Check that we can run pytest `call pytest on the cmd line`.
* If any of these fail, we should exit and return the stderr and stdout.
* This might be implemented by a different object oriented module. That is fine, as long as it has a command line interface that a command line agent (codex) can call inside a sandbox to verify that the docker image builds properly.


## Verification

* Unit tests: The unit tests may be split apart even further but we wish to answer these statements through the unit tests:
	* We must be able to build multiple images in parallel without docker client being a bottleneck.
	* If we query multiple images with the same repository, we should block all of them until the repository image is created.
	* We 
# Verification

Dataset verification ensures that each task's Docker image builds correctly and passes all validation checks.

## Task structure

Each task lives in `dataset/formulacode_verified/<owner_repo>/<sha>/` with:

- A multi-stage **Dockerfile**
- Shell build scripts (`docker_build_pkg.sh`, `docker_build_run.sh`)
- Validation scripts

## Verification loop

The iterative debugging workflow:

```bash
# Run verification for a specific task
python dataset/verify.py --task dataset/formulacode_verified/<owner_repo>/<sha>

# Check failure.json for errors
# Edit docker_build_pkg.sh and/or docker_build_run.sh
# Rerun until verification_success.json appears
```

!!! important
    Only modify `docker_build_pkg.sh` and `docker_build_run.sh` during verification fixes. Do not edit the Dockerfile or other scripts.

## Preflight check

Before running verification, confirm your environment is properly configured:

```bash
python -m datasmith.preflight
```

This checks:

| Check | What it validates |
|-------|-------------------|
| Environment | `SUPABASE_URL`, `SUPABASE_KEY`, `GH_TOKENS`, `HF_TOKEN` |
| Supabase | Database connection |
| Docker | Docker daemon is running |
| GitHub | API access and remaining rate limit |

## Programmatic verification

Use verifiers directly in Python:

```python
from datasmith.docker import MultiObjVerifier, SmokeVerifier, ProfileVerifier

verifier = MultiObjVerifier(verifiers=[
    SmokeVerifier("pandas"),
    ProfileVerifier(timeout=300),
])

result = verifier.verify("formulacode/pandas-dev-pandas:16222")
print(result.ok)        # True/False
print(result.stderr)    # Error output if failed
print(result.duration_s)  # Time taken
```

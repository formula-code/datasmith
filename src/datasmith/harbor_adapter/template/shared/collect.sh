# Harbor runs this in the agent container after the agent stops. tree.diff is the only file the verifier gets.
cd /workspace/repo || exit 0
mkdir -p /fc_submission
export GIT_INDEX_FILE="$(mktemp -d)/index"
git read-tree {{ base_commit }}
{% include "shared/stage_tree.sh" %}
git diff --cached --binary {{ base_commit }} > /fc_submission/tree.diff
# Processes the agent left running would compete for CPU with the timing; kill -1 spares PID 1 and this shell.
kill -9 -1 2>/dev/null
true

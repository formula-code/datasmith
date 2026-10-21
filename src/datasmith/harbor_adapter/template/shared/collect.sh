# Harbor runs this in the agent container after the agent stops. tree.diff is the only file the verifier gets.
cd /workspace/repo || exit 0
mkdir -p /fc_submission
# Against the starting tree that setup.sh recorded, so the image's own files stay out of the diff; else the base commit.
_base="$(cat /opt/fc_start_tree 2>/dev/null || echo {{ base_commit }})"
echo "${_base}" > /fc_submission/base.txt
export GIT_INDEX_FILE="$(mktemp -d)/index"
git read-tree "${_base}"
{% include "shared/stage_tree.sh" %}
GIT_NO_REPLACE_OBJECTS=1 git diff --cached --binary --no-ext-diff --no-textconv "${_base}" > /fc_submission/tree.diff
true

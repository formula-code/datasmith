from datasmith.verify import selection


def pr(owner="o", repo="r", n=1, **kw):
    return {
        "owner": owner,
        "repo": repo,
        "issue_number": n,
        "merge_commit_sha": "m",
        "base_sha": "b",
        "patch": "diff --git a/x.py b/x.py\n",
        "rendered_problem": "slow",
        "difficulty": "easy",
        "container_name": f"formulacode/{owner}-{repo}:{n}",
        **kw,
    }


def cand(task, repo, tier, diff, evidence=False, large=False, n_measured=10):
    return {
        "task": task,
        "repo_key": repo,
        "tier": tier,
        "stratum": f"{diff}/4-20",
        "large": large,
        "evidence": evidence,
        "n_measured": n_measured,
        "record": {},
    }


def test_record_fields():
    r = selection.record(pr(difficulty="weird"))
    assert (r["gt_hash"], r["base_commit"], r["instructions"], r["difficulty"], r["repo_name"]) == (
        "m",
        "b",
        "slow",
        "unknown",
        "o/r",
    )


def test_filter_rules():
    pool = {
        "o/r#1": {"record": selection.record(pr())},
        "o/r#2": {"record": selection.record(pr(n=2, patch="diff --git a/benchmarks/b.py b/benchmarks/b.py\n"))},
        "o/r#3": {"record": selection.record(pr(n=3))},
        "x/y#1": {"record": selection.record(pr("x", "y"))},
    }
    hub = {
        ("o-r", 1): {"bytes": 1, "pushed": "2026-08-20"},
        ("o-r", 2): {"bytes": 1, "pushed": "2026-08-20"},
        ("o-r", 3): {"bytes": 7e9, "pushed": "2026-08-20"},
        ("x-y", 1): {"bytes": 1, "pushed": "2026-08-20"},
    }
    out = selection.filter_pool(pool, hub, set(), {"x-y"}, ("2026-08-18", "2026-09-01"), "2026-04-12T19")
    assert [c["task"] for c in out] == ["o/r#1"]
    assert (out[0]["tier"], out[0]["stratum"], out[0]["large"]) == (1, "easy/na", False)


def test_sample_respects_cap_and_puts_large_last():
    pool = [cand(f"a/a#{i}", "a-a", 2, "easy") for i in range(5)] + [
        cand("p/p#1", "pandas-dev-pandas", 2, "hard", large=True)
    ]
    rows = selection.sample([], pool, 4, "s", lambda repo: 2)
    assert sum(r["repo_key"] == "a-a" for r in rows) == 2
    assert rows[-1]["group"] == "large"


class FakeQuery:
    def __init__(self, rows):
        self.rows, self.filters = rows, {}

    def select(self, _):
        return self

    def eq(self, k, v):
        self.filters[k] = v
        return self

    def execute(self):
        data = [r for r in self.rows if all(r.get(k) == v for k, v in self.filters.items())]
        return type("R", (), {"data": data})()


class FakeClient:
    def __init__(self, rows):
        self.rows = rows

    def table(self, name):
        return FakeQuery(self.rows)


def test_records_for_reports_incomplete_rows():
    client = FakeClient([pr(n=1), pr(n=2, base_sha=None)])
    recs, missing = selection.records_for(client, ["o/r#1", "o/r#2", "o/r#3"])
    assert [r["issue_number"] for r in recs] == [1]
    assert missing == ["o/r#2", "o/r#3"]


def test_sample_order_groups():
    def c(task, tier=2, large=False):
        repo = task.split("#")[0]
        return {"task": task, "repo_key": selection.image_name(*repo.split("/")), "tier": tier, "stratum": "easy/na", "large": large}

    seeds = [c("a/x#1", tier=0)]
    pool = [c("pandas-dev/pandas#1", large=True), c("b/y#1"), c("b/y#2"), c("b/y#3"), c("c/z#1", tier=1)]
    rows = selection.sample(seeds, pool, 5, "s", lambda repo: 2)
    assert [r["group"] for r in rows] == ["seed", "tier1", "tier2", "fill", "large"]


def test_a_rejected_row_is_replaced():
    pool = [cand(f"b/y#{i}", "b-y", 2, "easy") for i in range(3)]
    first = selection.sample([], pool, 1, "s", lambda repo: 5)[0]["task"]
    rows = selection.sample([], pool, 2, "s", lambda repo: 5, accept=lambda c: c["task"] != first)
    assert len(rows) == 2 and first not in [r["task"] for r in rows]

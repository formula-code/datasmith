#!/usr/bin/env python3
"""Fast pipeline status. Direct Postgres, not PostgREST.

`fetch_all` pages an entire table through PostgREST and filters in Python, so a
status check on `error_logs` took over a minute and sometimes timed out. The
same question answered with a WHERE clause takes well under a second.

    python scripts/status.py            # last 60 minutes
    python scripts/status.py --minutes 240
"""

from __future__ import annotations

import argparse

import psycopg2

DSN = "host=127.0.0.1 port=54322 dbname=postgres user=postgres password=postgres"

BUILDS = """
SELECT owner, repo, issue_number, success, round(duration_s) AS secs
FROM error_logs
WHERE agent_name = 'default_template' AND created_at > now() - make_interval(mins => %s)
ORDER BY created_at
"""

HARBOR = """
SELECT owner, repo, issue_number, status, max_speedup, geomean_speedup,
       n_benchmarks, round(wallclock_sec) AS secs
FROM harbor_runs
WHERE ran_at > now() - make_interval(mins => %s)
ORDER BY ran_at
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=60)
    args = ap.parse_args()

    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(BUILDS, (args.minutes,))
        builds = cur.fetchall()
        cur.execute(HARBOR, (args.minutes,))
        harbor = cur.fetchall()

    ok = sum(1 for b in builds if b[3])
    print(f"BUILDS  {ok} ok / {len(builds)} in the last {args.minutes} min")
    for owner, repo, num, success, secs in builds:
        print(f"  {'OK  ' if success else 'FAIL'} {owner}/{repo}#{num} {secs}s")

    print(f"\nHARBOR  {len(harbor)} run(s)")
    for owner, repo, num, status, mx, geo, n, secs in harbor:
        print(f"  {status} {owner}/{repo}#{num} max={mx} geo={geo} n={n} {secs}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

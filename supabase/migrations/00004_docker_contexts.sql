-- Stores synthesized Docker build contexts with one column per file.
-- Each row is a verified DockerContext for a specific (owner, repo, sha).
-- Human-readable: edit individual scripts directly in Supabase Studio.

CREATE TABLE IF NOT EXISTS docker_contexts (
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    sha TEXT NOT NULL,
    issue_number INT,
    dockerfile TEXT DEFAULT '',
    build_base_sh TEXT DEFAULT '',
    build_env_sh TEXT DEFAULT '',
    build_pkg_sh TEXT DEFAULT '',
    build_run_sh TEXT DEFAULT '',
    build_final_sh TEXT DEFAULT '',
    profile_sh TEXT DEFAULT '',
    run_tests_sh TEXT DEFAULT '',
    entrypoint_sh TEXT DEFAULT '',
    python_version TEXT DEFAULT '',
    env_payload TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (owner, repo, sha)
);

-- FormulaCode / DataSmith schema
-- Supabase migration: initial table creation

CREATE TABLE IF NOT EXISTS repositories (
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    url TEXT,
    language TEXT,
    stars INT,
    topics JSONB,
    description TEXT,
    last_scraped TIMESTAMPTZ,
    PRIMARY KEY (owner, repo)
);

CREATE TABLE IF NOT EXISTS pull_requests (
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    issue_number INT NOT NULL,
    title TEXT,
    body TEXT,
    state TEXT,
    created_at TIMESTAMPTZ,
    merged_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    merge_commit_sha TEXT,
    base_sha TEXT,
    head_sha TEXT,
    labels JSONB,
    file_changes JSONB,
    is_performance_commit BOOLEAN,
    classification TEXT,
    difficulty TEXT,
    container_name TEXT,
    published_at TIMESTAMPTZ,
    rendered_problem TEXT,
    patch TEXT,
    PRIMARY KEY (owner, repo, issue_number)
);

CREATE TABLE IF NOT EXISTS hook_cache (
    entity_key TEXT NOT NULL,
    hook_name TEXT NOT NULL,
    args_hash TEXT NOT NULL,
    result_json JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (entity_key, hook_name, args_hash)
);

CREATE TABLE IF NOT EXISTS build_attempts (
    id SERIAL PRIMARY KEY,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    issue_number INT NOT NULL,
    attempt_idx INT NOT NULL,
    model TEXT,
    script TEXT,
    ok BOOLEAN,
    rc INT,
    duration_s FLOAT,
    stderr_tail TEXT,
    stdout_tail TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS runner_progress (
    runner_id TEXT PRIMARY KEY,
    runner_name TEXT NOT NULL,
    total INT DEFAULT 0,
    completed INT DEFAULT 0,
    failed INT DEFAULT 0,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS runner_failures (
    id SERIAL PRIMARY KEY,
    runner_id TEXT NOT NULL REFERENCES runner_progress(runner_id),
    item_id TEXT NOT NULL,
    error_message TEXT,
    traceback TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

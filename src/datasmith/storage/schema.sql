-- DataSmith Database Schema
-- Version: 1.0
-- Created: 2025-10-02

-- Enable foreign keys and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- Repositories table
CREATE TABLE IF NOT EXISTS repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    url TEXT NOT NULL,
    stars INTEGER DEFAULT 0,
    forks INTEGER DEFAULT 0,
    language TEXT,
    description TEXT,
    homepage TEXT,
    is_valid BOOLEAN DEFAULT FALSE,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata TEXT, -- JSON for additional repo info
    
    UNIQUE(owner, repo)
);

-- Commits table
CREATE TABLE IF NOT EXISTS commits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
    sha TEXT NOT NULL,
    commit_date TIMESTAMP NOT NULL,
    author_name TEXT,
    author_email TEXT,
    message TEXT,
    pr_number INTEGER,
    is_merge BOOLEAN DEFAULT FALSE,
    is_performance_relevant BOOLEAN DEFAULT FALSE,
    metadata TEXT, -- JSON for labels, changed files, etc.
    
    UNIQUE(repository_id, sha)
);

-- Build contexts table
CREATE TABLE IF NOT EXISTS build_contexts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
    sha TEXT NOT NULL,
    tag TEXT,
    commit_date TIMESTAMP,
    dockerfile_data TEXT,
    entrypoint_data TEXT,
    env_building_data TEXT,
    base_building_data TEXT,
    building_data TEXT,
    run_building_data TEXT,
    validated BOOLEAN DEFAULT FALSE,
    validation_result TEXT, -- JSON with validation details
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(repository_id, sha, tag)
);

-- Benchmark collections table
CREATE TABLE IF NOT EXISTS benchmark_collections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
    base_url TEXT NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    param_keys TEXT, -- JSON array of parameter keys
    index_data TEXT, -- JSON with index metadata
    collection_metadata TEXT -- JSON for additional collection info
);

-- Benchmark runs table (individual benchmark results)
CREATE TABLE IF NOT EXISTS benchmark_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER REFERENCES benchmark_collections(id) ON DELETE CASCADE,
    commit_sha TEXT NOT NULL,
    benchmark_name TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT,
    
    -- Timing and metadata
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    run_metadata TEXT, -- JSON for additional run info
    
    params TEXT -- JSON with parameter values
);

-- Detected performance changes/regressions table
CREATE TABLE IF NOT EXISTS breakpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER REFERENCES benchmark_collections(id) ON DELETE CASCADE,
    commit_sha TEXT NOT NULL,
    benchmark_name TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    
    -- Change analysis
    change_type TEXT CHECK(change_type IN ('improvement', 'regression', 'neutral')) DEFAULT 'neutral',
    confidence_score REAL DEFAULT 0.0, -- 0.0 to 1.0
    detection_method TEXT DEFAULT 'unknown', -- 'asv', 'statistical', 'manual', etc.
    
    -- Performance metrics
    before_value REAL NOT NULL,
    after_value REAL NOT NULL,
    relative_change REAL, -- Percentage change
    absolute_change REAL, -- Absolute difference
    
    -- Context data
    coverage_data TEXT, -- JSON with code coverage info
    github_data TEXT, -- JSON with GitHub PR/commit data
    
    -- Detection metadata
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    breakpoint_metadata TEXT -- JSON for additional analysis data
);

-- Pipeline runs table (execution tracking)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL,
    script_name TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    status TEXT DEFAULT 'running' CHECK(status IN ('running', 'completed', 'failed', 'cancelled')),
    config TEXT, -- JSON with script arguments/config
    output_summary TEXT, -- JSON with counts, errors, etc.
    error_message TEXT NULL,
    environment_info TEXT -- JSON with system/env info
);

-- Pipeline run items table (detailed tracking)
CREATE TABLE IF NOT EXISTS pipeline_run_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL, -- 'repository', 'commit', 'context', etc.
    item_id TEXT NOT NULL, -- Repository ID, commit SHA, etc.
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    error_message TEXT NULL,
    output_data TEXT, -- JSON with item-specific results
    retry_count INTEGER DEFAULT 0
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_repositories_owner_repo ON repositories(owner, repo);
CREATE INDEX IF NOT EXISTS idx_repositories_stars ON repositories(stars DESC);
CREATE INDEX IF NOT EXISTS idx_repositories_valid ON repositories(is_valid);
CREATE INDEX IF NOT EXISTS idx_repositories_language ON repositories(language);

CREATE INDEX IF NOT EXISTS idx_commits_repo_date ON commits(repository_id, commit_date DESC);
CREATE INDEX IF NOT EXISTS idx_commits_sha ON commits(sha);
CREATE INDEX IF NOT EXISTS idx_commits_perf_relevant ON commits(is_performance_relevant);
CREATE INDEX IF NOT EXISTS idx_commits_pr_number ON commits(pr_number);

CREATE INDEX IF NOT EXISTS idx_build_contexts_repo_sha ON build_contexts(repository_id, sha);
CREATE INDEX IF NOT EXISTS idx_build_contexts_validated ON build_contexts(validated);
CREATE INDEX IF NOT EXISTS idx_build_contexts_tag ON build_contexts(tag);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_commit_benchmark ON benchmark_runs(commit_sha, benchmark_name);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_collection_commit ON benchmark_runs(collection_id, commit_sha);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_value ON benchmark_runs(value);

CREATE INDEX IF NOT EXISTS idx_breakpoints_change_type ON breakpoints(change_type);
CREATE INDEX IF NOT EXISTS idx_breakpoints_confidence ON breakpoints(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_breakpoints_detected_at ON breakpoints(detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status, started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_items_status ON pipeline_run_items(run_id, status);

-- Useful views for common queries
CREATE VIEW IF NOT EXISTS repository_stats AS
SELECT 
    r.id,
    r.owner,
    r.repo,
    r.stars,
    r.language,
    COUNT(DISTINCT c.id) as commit_count,
    COUNT(DISTINCT CASE WHEN c.is_performance_relevant THEN c.id END) as perf_commits,
    COUNT(DISTINCT bc.id) as context_count,
    COUNT(DISTINCT bcol.id) as collection_count,
    MAX(c.commit_date) as latest_commit_date
FROM repositories r
LEFT JOIN commits c ON r.id = c.repository_id
LEFT JOIN build_contexts bc ON r.id = bc.repository_id
LEFT JOIN benchmark_collections bcol ON r.id = bcol.repository_id
WHERE r.is_valid = TRUE
GROUP BY r.id, r.owner, r.repo, r.stars, r.language;

CREATE VIEW IF NOT EXISTS performance_summary AS
SELECT 
    r.owner,
    r.repo,
    COUNT(DISTINCT bp.id) as total_breakpoints,
    COUNT(DISTINCT CASE WHEN bp.change_type = 'improvement' THEN bp.id END) as improvements,
    COUNT(DISTINCT CASE WHEN bp.change_type = 'regression' THEN bp.id END) as regressions,
    AVG(bp.confidence_score) as avg_confidence,
    MAX(bp.detected_at) as latest_detection
FROM repositories r
JOIN benchmark_collections bc ON r.id = bc.repository_id
JOIN breakpoints bp ON bc.id = bp.collection_id
WHERE r.is_valid = TRUE
GROUP BY r.id, r.owner, r.repo;

CREATE VIEW IF NOT EXISTS pipeline_run_summary AS
SELECT 
    pr.id,
    pr.run_name,
    pr.script_name,
    pr.started_at,
    pr.finished_at,
    pr.status,
    COUNT(pri.id) as total_items,
    COUNT(CASE WHEN pri.status = 'completed' THEN 1 END) as completed_items,
    COUNT(CASE WHEN pri.status = 'failed' THEN 1 END) as failed_items,
    COUNT(CASE WHEN pri.status = 'pending' THEN 1 END) as pending_items
FROM pipeline_runs pr
LEFT JOIN pipeline_run_items pri ON pr.id = pri.run_id
GROUP BY pr.id, pr.run_name, pr.script_name, pr.started_at, pr.finished_at, pr.status;
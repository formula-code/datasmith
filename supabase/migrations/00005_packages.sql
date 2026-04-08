-- Stores resolved Python dependencies per commit.
-- Populated by the resolve_packages pipeline stage via ds.resolution.analyze_commit().
-- Keyed by (owner, repo, sha) since resolution results are identical for the same commit
-- regardless of which PR references it.

CREATE TABLE IF NOT EXISTS packages (
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    sha TEXT NOT NULL,

    -- Resolution outputs
    package_name TEXT,
    package_version TEXT,
    python_version TEXT NOT NULL,
    env_payload TEXT NOT NULL,       -- JSON array of pinned requirement strings
    build_commands JSONB,
    install_commands JSONB,
    primary_root TEXT,
    resolution_strategy TEXT,
    can_install BOOLEAN NOT NULL,
    requires_python TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (owner, repo, sha)
);

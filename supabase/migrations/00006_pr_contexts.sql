-- Stores deconstructed PR context used to build rendered_problem.
-- Populated by the render_problems pipeline stage (stage 5).
-- Keeps all raw components so the rendered_problem can be re-generated
-- with a different master prompt without re-running scraping or LLM extraction.

CREATE TABLE IF NOT EXISTS pr_contexts (
    owner                 TEXT NOT NULL,
    repo                  TEXT NOT NULL,
    issue_number          INT  NOT NULL,

    -- Identifiers
    merge_commit_sha      TEXT,

    -- Raw scraped inputs
    repo_description      TEXT,
    issues_json           JSONB,  -- list[IssueExpanded] (number, title, url, description, comments, cross_references)

    -- ProblemExtraction fields (from ProblemExtractor DSPy agent)
    initial_observations  TEXT,   -- objective symptoms of the problem (problem-only, no solution)
    triage_attempts       TEXT,   -- investigative steps and reasoning
    solution_overview     TEXT,   -- description of the change made
    solution_observations TEXT,   -- observations after applying the change

    -- Final rendered output
    rendered_problem      TEXT,   -- full Jinja2-rendered markdown written to pull_requests.rendered_problem

    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (owner, repo, issue_number)
);

-- problem_description stores only the extracted problem statement (initial_observations),
-- keeping it separate from rendered_problem which includes repo context, linked issues, etc.
ALTER TABLE pull_requests
    ADD COLUMN IF NOT EXISTS problem_description TEXT;

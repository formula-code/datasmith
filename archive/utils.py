"""Legacy compatibility layer forwarding to datasmith.core."""

from __future__ import annotations

import re

from datasmith.core.api.codecov_client import get_codecov_metadata as _get_codecov_metadata
from datasmith.core.api.github_client import (
    get_github_metadata as _get_github_metadata,
)
from datasmith.core.api.github_client import (
    get_github_metadata_graphql as _get_github_metadata_graphql,
)
from datasmith.core.api.http_utils import (
    build_headers as _build_headers,
)
from datasmith.core.api.http_utils import (
    get_session,
    prepare_url,
)
from datasmith.core.api.http_utils import (
    request_with_backoff as _request_with_backoff,
)
from datasmith.core.cache import CACHE_LOCATION, cache_completion, get_db_connection
from datasmith.core.file_utils import (
    dl_and_open,
)
from datasmith.core.file_utils import (
    extract_repo_full_name as _extract_repo_full_name,
)
from datasmith.core.file_utils import (
    parse_commit_url as _parse_commit_url,
)

find_json_block = re.compile(r"```json(.*?)```", re.DOTALL)

__all__ = [
    "CACHE_LOCATION",
    "_build_headers",
    "_extract_repo_full_name",
    "_get_codecov_metadata",
    "_get_github_metadata",
    "_get_github_metadata_graphql",
    "_parse_commit_url",
    "_request_with_backoff",
    "cache_completion",
    "dl_and_open",
    "find_json_block",
    "get_db_connection",
    "get_session",
    "prepare_url",
]

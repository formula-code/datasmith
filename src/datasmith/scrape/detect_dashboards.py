import csv
import random
import time
from collections.abc import Generator

import requests

from datasmith.logging_config import get_logger
from datasmith.scrape.utils import SEARCH_URL
from datasmith.utils import _build_headers, _request_with_backoff

logger = get_logger("scrape.detect_dashboards")


def search_pages(
    max_pages: int,
    per_page: int,
    query: str,
    base_delay: float = 1.1,
    max_backoff: int = 60,
    max_retries: int = 6,
    jitter: float = 0.3,
) -> Generator[str, None, None]:
    seen: set[str] = set()
    headers = _build_headers(name="github")

    with requests.Session() as sess:
        sess.headers.update(headers)
        for page in range(1, max_pages + 1):
            url = f"{SEARCH_URL}?q={query}&per_page={per_page}&page={page}"
            data = _request_with_backoff(
                url=url,
                site_name="github",
                session=sess,
                rps=2,
                base_delay=base_delay,
                max_retries=max_retries,
                max_backoff=max_backoff,
            ).json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                repo = item["repository"]["full_name"]
                if repo not in seen:
                    seen.add(repo)
                    yield repo

            time.sleep(base_delay + random.random() * jitter)  # noqa: S311

            if not data.get("incomplete_results") and len(items) < per_page:
                break


def scrape_github(query: str, outfile: str, search_args: dict) -> None:
    logger.info("🔍  Query: %s", query)
    logger.info("Writing unique repos to %s", outfile)

    with open(outfile, "w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        count = 0
        for count, repo in enumerate(
            search_pages(
                search_args["max_pages"],
                search_args["per_page"],
                search_args["query"],
                search_args["base_delay"],
                search_args["max_backoff"],
                search_args["max_retries"],
            ),
            1,
        ):
            writer.writerow([repo])
            if count % 50 == 0:
                logger.info("%s repositories so far …", count)

    logger.info("✅  Finished: %s repos saved.", count)

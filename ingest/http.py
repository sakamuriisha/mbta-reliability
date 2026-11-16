"""A requests session with polite retries for both MBTA endpoints."""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import USER_AGENT


def make_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=2,  # 2s, 4s, 8s, 16s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

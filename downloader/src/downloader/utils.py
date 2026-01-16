"""Util functions."""
import logging

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError

logging.basicConfig()
logger = logging.getLogger()

def get_session() -> requests.Session:
    """Get session with cookie."""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=12,
        pool_maxsize=15,
        max_retries=3,
    )
    session.mount("https://", adapter)

    return session

def _raise_for_status(response: requests.Response) -> None:
    """Raise if Server/Client error."""
    http_error_msg = ""

    if 400 <= response.status_code < 500:
        http_error_msg = (
            f"{response.status_code} Client Error:"
            f" {response.reason} for url: {response.url}"
        )

    elif 500 <= response.status_code < 600:
        http_error_msg = (
            f"{response.status_code} Server Error:"
            f" {response.reason} for url: {response.url}"
        )

    if http_error_msg:
        raise HTTPError(http_error_msg, response=response)
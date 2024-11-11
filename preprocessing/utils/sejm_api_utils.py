import logging

import requests
from requests.adapters import HTTPAdapter


def make_api_call(url):
    try:
        session_requests = requests.Session()
        session_requests.mount(url, HTTPAdapter(max_retries=10))

        response = session_requests.get(url, timeout=10)
        response.raise_for_status()

        return response.json()
    except requests.exceptions.Timeout as timeout_err:
        raise Exception(f"Request timed out while calling the Sejm API for url: {url}")
    except requests.exceptions.HTTPError as http_err:
        raise Exception("Error occurred while calling the Sejm API")
    except Exception as err:
        raise Exception("Error occurred while calling the Sejm API")
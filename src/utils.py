import time
import requests

def fetch_with_retry(url, max_tries=5, backoff=1.5, timeout=20):
    """Exponential backoff for polite API usage (handles 429/5xx)."""
    last_err = None
    for i in range(max_tries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} {r.text}")
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            sleep = backoff ** i
            time.sleep(sleep)
    raise RuntimeError(f"Failed after {max_tries} tries: {last_err}")

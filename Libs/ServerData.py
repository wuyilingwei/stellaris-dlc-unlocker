import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import sys
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
headers = {'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}

GITHUB_DLC_URL    = 'https://raw.githubusercontent.com/seuyh/stellaris-dlc-unlocker/main/dlc_data.json'
GITHUB_DATA_URL   = 'https://raw.githubusercontent.com/seuyh/stellaris-dlc-unlocker/main/data.json'

JSDELIVR_DLC_URL  = 'https://cdn.jsdelivr.net/gh/seuyh/stellaris-dlc-unlocker@main/dlc_data.json'
JSDELIVR_DATA_URL = 'https://cdn.jsdelivr.net/gh/seuyh/stellaris-dlc-unlocker@main/data.json'

SITE_DLC_URL  = "https://femboysex.pro/unlocker/dlc_data.json"
SITE_DATA_URL = "https://femboysex.pro/unlocker/data.json"

GITHUB_SOFT_TIMEOUT = 5   # seconds: prefer GitHub if it responds within this window
HARD_TIMEOUT        = 20  # seconds: absolute per-request deadline for both sources


def _fetch_json(url, timeout):
    # Returns None on any error so futures never raise.
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def _race(github_url, jsdelivr_url):
    """
    Soft-priority race between GitHub (authoritative) and jsDelivr (cached).
    Both requests share the same HARD_TIMEOUT; GitHub gets a soft priority
    window of GITHUB_SOFT_TIMEOUT seconds.

    Step 0 — non-blocking check on jsDelivr right away, so its future is
              already in a known state when GitHub's window expires.

    Phase 1 — wait up to GITHUB_SOFT_TIMEOUT for GitHub:
      - Responded and valid   → use GitHub (jsDelivr discarded).
      - Responded but invalid → fall through.

    Phase 2 — GitHub missed the window; re-check jsDelivr non-blocking:
      - Already finished and valid → use jsDelivr immediately.

    Phase 3 — neither ready → wait for whichever arrives first.
    """
    executor = ThreadPoolExecutor(max_workers=2)
    f_gh  = executor.submit(_fetch_json, github_url,  HARD_TIMEOUT)
    f_jsd = executor.submit(_fetch_json, jsdelivr_url, HARD_TIMEOUT)

    # Step 0: non-blocking snapshot of jsDelivr so it can be used instantly
    # if GitHub's soft window expires and jsDelivr has already finished.
    wait({f_jsd}, timeout=0)

    # Phase 1: give GitHub its soft priority window
    done, _ = wait({f_gh}, timeout=GITHUB_SOFT_TIMEOUT)
    if done:
        result = f_gh.result()
        if result is not None:
            executor.shutdown(wait=False)
            return result

    # Phase 2: GitHub missed the window — check jsDelivr non-blocking
    done, _ = wait({f_jsd}, timeout=0)
    if done:
        result = f_jsd.result()
        if result is not None:
            executor.shutdown(wait=False)
            return result

    # Phase 3: neither ready — wait for the fastest remaining response
    pending = {f for f in (f_gh, f_jsd) if not f.done()}
    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for f in done:
            result = f.result()
            if result is not None:
                executor.shutdown(wait=False)
                return result

    executor.shutdown(wait=False)
    return None


def get_dlc_data():
    result = _race(GITHUB_DLC_URL, JSDELIVR_DLC_URL)
    if result:
        print("DLC data fetched from GitHub/jsDelivr.")
        return result
    try:
        print("Fetching DLC data from fallback server...")
        response = requests.get(SITE_DLC_URL, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        raise Exception(f"Server returned {response.status_code}")
    except Exception as e:
        print(f"CRITICAL: Could not fetch DLC data: {e}")
        sys.exit(2)


def get_server_data():
    result = _race(GITHUB_DATA_URL, JSDELIVR_DATA_URL)
    if result:
        print("Server config fetched from GitHub/jsDelivr.")
        return result
    try:
        print("Fetching server config from fallback server...")
        response = requests.get(SITE_DATA_URL, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        raise Exception(f"Server returned {response.status_code}")
    except Exception as e:
        print(f"CRITICAL: Could not fetch server config: {e}")
        sys.exit(2)

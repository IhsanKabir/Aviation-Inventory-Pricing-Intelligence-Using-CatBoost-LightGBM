# core/requester.py
"""
Upgraded Requester.

Features:
- Global HAR-like header injection (Option B).
- Dynamic per-request fields: conversation-id, execution (UUIDs).
- ADRUM / application-id / x-sabre-storefront and other HAR fields inserted automatically.
- Verbose logging and curl-mode (prints curl for requests when enabled).
- Retry via urllib3 Retry + requests.adapters.HTTPAdapter.
- Cookie helpers: load_static_cookies, save_cookies, generate_new_cookies.
- GraphQL helper: send_graphql(query or payload).
"""

from __future__ import annotations
import json
import logging
import os
import random
import uuid
from typing import Any, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (10, 30)  # connect, read
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF = 0.8

class RequesterError(Exception):
    pass

class Requester:
    def __init__(
        self,
        base_headers: Optional[Dict[str, str]] = None,
        timeout: Tuple[int, int] = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        session: Optional[requests.Session] = None,
        verbose: bool = False,
        curl_mode: bool = False,
        har_inject: bool = True,  # Option B: global HAR header injection
        har_template: Optional[Dict[str, str]] = None,
    ):
        """
        har_inject: when True, inject HAR-like headers on every request.
        har_template: base HAR header values to inject (can be extended or overridden).
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist
        self.session = session or requests.Session()
        self.verbose = verbose
        self.curl_mode = curl_mode
        self.har_inject = har_inject

        # default base headers and random user-agent if none provided
        headers = base_headers or {}
        headers.setdefault("User-Agent", self._random_user_agent())
        self.session.headers.update(headers)

        # configure retries
        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # HAR template default (can be overridden by har_template)
        default_har = {
            "ADRUM": "isAjax:true",
            "application-id": "SWS1:SBR-GCPDCShpBk:2ceb6478a8",
            "x-sabre-storefront": "BGDX",
            "Origin": "https://booking.biman-airlines.com",
            "Referer": "https://booking.biman-airlines.com/dx/BGDX/",
            "accept": "*/*",
            "content-type": "application/json",
        }
        self.har_template = dict(default_har)
        if har_template:
            self.har_template.update(har_template)

    def _random_user_agent(self) -> str:
        uas = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/16.4 Safari/605.1.15",
        ]
        return random.choice(uas)

    # ------------------------------------------------------------------------
    # HAR header builder (global)
    # ------------------------------------------------------------------------
    def _build_har_headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Build HAR-like headers. Runs for EVERY request when har_inject True.
        Adds dynamic conversation-id and execution UUID per-request.
        """
        h = dict(self.har_template)
        # dynamic fields per request
        h["conversation-id"] = str(uuid.uuid4())
        h["execution"] = str(uuid.uuid4())
        # ensure User-Agent preserved (requests.Session has its own header)
        if extra:
            # merge user provided extra (explicit overrides)
            h.update(extra)
        return h

    # ------------------------------------------------------------------------
    # Curl printing helper
    # ------------------------------------------------------------------------
    def _print_curl(self, method: str, url: str, headers: Dict[str, str], data: Optional[Any] = None):
        hdrs = [f"-H {json.dumps(f'{k}: {v}')}" for k, v in headers.items()]
        data_part = ""
        if data is not None:
            # ensure JSON string
            try:
                body = json.dumps(data, ensure_ascii=False)
            except Exception:
                body = str(data)
            data_part = f"--data-raw {json.dumps(body)}"
        curl_cmd = f"curl -X {method.upper()} {json.dumps(url)} \\\n  " + " \\\n  ".join(hdrs)
        if data_part:
            curl_cmd += " \\\n  " + data_part
        curl_cmd += " --compressed"
        logger.debug("cURL (approx):\n%s", curl_cmd)
        if self.curl_mode:
            print("\n# ---- cURL ----")
            print(curl_cmd)
            print("# ---- end cURL ----\n")

    # ------------------------------------------------------------------------
    # request wrapper
    # ------------------------------------------------------------------------
    def request(self, method: str, url: str, headers: Optional[Dict[str, str]] = None, **kwargs) -> requests.Response:
        """
        Performs requests.Session.request with HAR header injection and uniform error handling.
        - headers: explicit headers passed by caller (merged with HAR template; caller overrides).
        """
        kwargs.setdefault("timeout", self.timeout)
        # Build request headers: start with session headers, then HAR-inject, then user headers
        req_headers = {}
        # session-level headers are automatically used by requests; we explicitly build
        if self.har_inject:
            req_headers.update(self._build_har_headers(headers or {}))
        else:
            if headers:
                req_headers.update(headers)

        # If Content-Type is not present, let requests infer; but many endpoints expect application/json
        # Caller can still override Content-Type via headers param.

        # log verbose debug
        if self.verbose:
            logger.debug("REQUEST [%s] %s", method.upper(), url)
            logger.debug("Headers: %s", json.dumps(req_headers, indent=2))
            if "json" in kwargs:
                logger.debug("JSON body: %s", json.dumps(kwargs["json"], indent=2, ensure_ascii=False))
            elif "data" in kwargs:
                logger.debug("DATA body: %s", kwargs["data"])

        # print curl if asked
        if self.curl_mode:
            data_for_curl = kwargs.get("json") or kwargs.get("data")
            self._print_curl(method, url, req_headers, data_for_curl)

        try:
            resp = self.session.request(method, url, headers=req_headers, **kwargs)
            # raise for http errors to trigger retries etc.
            resp.raise_for_status()
            if self.verbose:
                logger.debug("Response [%s] %s", resp.status_code, resp.text[:1000])
            return resp
        except requests.HTTPError as e:
            # include response text in debug
            resp_text = None
            try:
                resp_text = e.response.text
            except Exception:
                resp_text = "<no body>"
            logger.debug("HTTP error response: %s", resp_text)
            raise RequesterError(f"{e.response.status_code} Client Error: {e.response.reason} for url: {url} - body: {resp_text}") from e
        except requests.RequestException as e:
            logger.exception("Request failed: %s", e)
            raise RequesterError(f"Request failed: {e}") from e

    # convenience methods
    def get(self, url: str, **kwargs) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        return self.request("POST", url, **kwargs)

    # ------------------------------------------------------------------------
    # GraphQL helper
    # ------------------------------------------------------------------------
    def send_graphql(self, url: str, query: str, variables: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Send a GraphQL POST with JSON body and return parsed JSON.
        Uses self.post so HAR headers will be injected.
        """
        payload = {
            "operationName": "bookingAirSearch" if "bookingAirSearch" in query else None,
            "query": query,
            "variables": variables or {},
            "extensions": {},
        }
        # remove operationName if None
        if payload["operationName"] is None:
            payload.pop("operationName")

        try:
            resp = self.post(url, json=payload, headers=headers or {})
            # return parsed JSON
            return resp.json()
        except ValueError:
            # JSON decode error
            try:
                logger.debug("Non-JSON response: %s", resp.text[:1000])
            except Exception:
                pass
            raise RequesterError("Invalid JSON response")
        except RequesterError:
            # bubble up RequesterError with full message already set
            raise
        except Exception as e:
            raise RequesterError(str(e))

    # ------------------------------------------------------------------------
    # Cookie helpers
    # ------------------------------------------------------------------------
    def load_static_cookies(self, path: str):
        """
        Load cookie dict JSON from path into session.cookies (requests cookiejar).
        Returns True on success, False on failure.
        """
        if not path:
            return False
        # try multiple path forms for convenience (absolute or relative)
        tried_paths = [path, os.path.join(os.getcwd(), path)]
        for p in tried_paths:
            if not os.path.exists(p):
                continue
            try:
                with open(p, "r", encoding="utf-8") as fh:
                    d = json.load(fh)
                    cj = requests.utils.cookiejar_from_dict(d)
                    self.session.cookies = cj
                    logger.info("Loaded cookies from %s", p)
                    return True
            except Exception:
                logger.exception("Failed to load cookies from %s", p)
                return False
        return False

    def save_cookies(self, path: str):
        if not path:
            return False
        try:
            cj = requests.utils.dict_from_cookiejar(self.session.cookies)
            # ensure parent dir exists
            parent = os.path.dirname(path)
            if parent and not os.path.exists(parent):
                os.makedirs(parent, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(cj, fh, indent=2)
            logger.info("Saved cookies to %s", path)
            return True
        except Exception:
            logger.exception("Failed to save cookies to %s", path)
            return False

    def generate_new_cookies(self, start_url: str, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Perform a GET to obtain fresh cookies (for sites using session cookies).
        """
        try:
            resp = self.get(start_url, headers=headers or {})
            logger.info("Preflight GET to %s returned %s cookies", start_url, len(self.session.cookies))
            return True
        except RequesterError:
            logger.exception("Failed to generate cookies via preflight GET")
            return False

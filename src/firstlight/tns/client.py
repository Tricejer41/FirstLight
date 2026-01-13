from __future__ import annotations

import json
import os
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

log = logging.getLogger(__name__)

# NOTE:
# The official TNS manuals are sometimes blocked to automated fetchers.
# To avoid hard-coding a single endpoint, we probe a small set of plausible paths.
# This makes the client robust against minor deployment differences between sandbox/prod.

DEFAULT_ENDPOINT_CANDIDATES = [
    # most common naming
    "bulk-report",
    "bulk_report",
    "bulkreport",
    "bulk-report/upload",
    "bulk_report/upload",
    # some deployments nest under /bulk
    "bulk/at-report",
    "bulk/at_report",
    "bulk/at",
]

DEFAULT_STATUS_CANDIDATES = [
    "bulk-report/status",
    "bulk_report/status",
    "bulkreport/status",
    "bulk-report/retrieve",
    "bulk_report/retrieve",
    "bulkreport/retrieve",
    "bulk-report/get",
    "bulk_report/get",
    "bulkreport/get",
]

def _safe(s: str, keep: int = 4) -> str:
    s = s or ""
    if len(s) <= keep:
        return "*" * len(s)
    return "*" * (len(s) - keep) + s[-keep:]

@dataclass(frozen=True)
class TNSConfig:
    bot_id: str
    bot_name: str
    api_key: str
    api_url: str  # e.g. https://sandbox.wis-tns.org/api
    base_url: str = ""  # optional; informational
    reporter_name: str = ""
    reporter_email: str = ""
    reporter_institution: str = ""

    @staticmethod
    def from_env() -> "TNSConfig":
        return TNSConfig(
            bot_id=os.getenv("TNS_BOT_ID", "").strip(),
            bot_name=os.getenv("TNS_BOT_NAME", "").strip(),
            api_key=os.getenv("TNS_API_KEY", "").strip(),
            api_url=os.getenv("TNS_API_URL", "").strip().rstrip("/"),
            base_url=os.getenv("TNS_BASE_URL", "").strip().rstrip("/"),
            reporter_name=os.getenv("TNS_REPORTER_NAME", "").strip(),
            reporter_email=os.getenv("TNS_REPORTER_EMAIL", "").strip(),
            reporter_institution=os.getenv("TNS_REPORTER_INSTITUTION", "").strip(),
        )

    def enabled(self) -> bool:
        return bool(self.bot_id and self.bot_name and self.api_key and self.api_url)

    def tns_marker(self) -> str:
        # TNS expects a "tns_marker{...}" string in the User-Agent header.
        # This is also used for bulk downloads; it's safe and standard.
        marker = {"tns_id": int(self.bot_id), "type": "bot", "name": self.bot_name}
        return "tns_marker" + json.dumps(marker, separators=(",", ":"))

class TNSClient:
    def __init__(self, cfg: Optional[TNSConfig] = None, timeout_s: float = 30.0):
        self.cfg = cfg or TNSConfig.from_env()
        self.timeout_s = timeout_s
        self._session = requests.Session()

    def enabled(self) -> bool:
        return self.cfg.enabled()

    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.cfg.tns_marker(),
        }

    def probe_endpoints(self) -> Dict[str, Any]:
        """Try a set of candidate endpoints and return the best match.

        We do a POST with an intentionally invalid/minimal body and look for:
        - HTTP 200/400 with JSON that includes known error fields
        - or a clear 'unknown endpoint' vs 'invalid payload' difference.

        Returns:
            dict with keys: submit_url, status_url, notes
        """
        if not self.enabled():
            raise RuntimeError("TNS client not enabled (missing env vars).")

        notes: List[str] = []
        submit_url = self._probe_submit(notes)
        status_url = self._probe_status(notes)

        return {"submit_url": submit_url, "status_url": status_url, "notes": notes}

    def _probe_submit(self, notes: List[str]) -> Optional[str]:
        for ep in DEFAULT_ENDPOINT_CANDIDATES:
            url = f"{self.cfg.api_url}/{ep}"
            ok, msg = self._try_post(url, data={"api_key": self.cfg.api_key, "data": "{}"})
            notes.append(f"submit probe {ep}: {msg}")
            if ok:
                return url
        return None

    def _probe_status(self, notes: List[str]) -> Optional[str]:
        for ep in DEFAULT_STATUS_CANDIDATES:
            url = f"{self.cfg.api_url}/{ep}"
            ok, msg = self._try_post(url, data={"api_key": self.cfg.api_key, "data": "{}"})
            notes.append(f"status probe {ep}: {msg}")
            if ok:
                return url
        return None

    def _try_post(self, url: str, data: Dict[str, str]) -> Tuple[bool, str]:
        try:
            files = {k: (None, v) for k, v in data.items()}
            r = self._session.post(url, headers=self._headers(), files=files, timeout=self.timeout_s)
        except Exception as e:
            return False, f"EXC {type(e).__name__}: {e}"
        ct = (r.headers.get("content-type") or "").lower()
        short = f"HTTP {r.status_code}"
        # success for probing is: endpoint exists (not 404/405) and responds meaningfully
        if r.status_code in (404,):
            return False, short + " (404)"
        if r.status_code in (405,):
            return False, short + " (405)"
        if "json" in ct:
            try:
                j = r.json()
                # many TNS responses include 'id_message' or 'id_code' or 'message'
                keys = list(j.keys())[:6] if isinstance(j, dict) else type(j).__name__
                return True, short + f" JSON keys={keys}"
            except Exception:
                return True, short + " JSON (unparsed)"
        # HTML usually means auth wall or generic error; treat as non-confirming
        if "text/html" in ct:
            return False, short + " HTML"
        return True, short + f" CT={ct or 'unknown'}"

    # ----------------------------
    # Bulk AT report submission
    # ----------------------------

    def submit_at_report(self, at_report_json: Dict[str, Any], submit_url: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
        """Submit a discovery (AT) report to TNS.

        We send standard form fields:
            api_key=<api_key>
            data=<json-string>

        Args:
            at_report_json: dict already shaped as expected by TNS bulk-report schema
            submit_url: if known (from probe), use it; otherwise tries candidates

        Returns:
            (ok, response_json_or_error)
        """
        if not self.enabled():
            return False, {"error": "TNS disabled (missing env vars)."}
        payload_str = json.dumps(at_report_json, separators=(",", ":"), ensure_ascii=False)

        urls = [submit_url] if submit_url else [f"{self.cfg.api_url}/{ep}" for ep in DEFAULT_ENDPOINT_CANDIDATES]
        last_err: Dict[str, Any] = {}
        for url in urls:
            if not url:
                continue
            try:
                r = self._session.post(
                    url,
                    headers=self._headers(),
                    files={"api_key": (None, self.cfg.api_key), "data": (None, payload_str)},
                    timeout=self.timeout_s,
                )
            except Exception as e:
                last_err = {"error": f"EXC {type(e).__name__}: {e}", "url": url}
                continue

            try:
                j = r.json()
            except Exception:
                j = {"raw": r.text[:500], "status_code": r.status_code, "url": url}

            if r.status_code >= 200 and r.status_code < 300:
                return True, j
            # If endpoint exists but payload invalid, don't keep rotating endpoints blindly.
            if r.status_code in (400, 422):
                return False, {"url": url, "status_code": r.status_code, "response": j}
            last_err = {"url": url, "status_code": r.status_code, "response": j}

        return False, last_err

def build_minimal_at_report(
    *,
    objname: str,
    ra_deg: float,
    dec_deg: float,
    discovery_utc_iso: str,
    mag: float,
    filt: str,
    instrument: str = "ZTF",
    observer: str = "",
    reporter_name: str = "",
    reporter_email: str = "",
    reporter_institution: str = "",
) -> Dict[str, Any]:
    """Build a conservative minimal AT report structure.

    IMPORTANT:
    - TNS schemas vary slightly across versions.
    - This is a best-effort minimal payload; if TNS rejects due to missing fields,
      the response will tell you what is missing. You then add it here.

    We keep keys explicit & human-readable to reduce mistakes.
    """
    # Keep degrees as strings to avoid float serialization differences
    at = {
        "objname": objname,
        "ra": f"{ra_deg:.7f}",
        "dec": f"{dec_deg:.7f}",
        "discovery_datetime": discovery_utc_iso,
        "reporting_group": reporter_institution or "None",
        "reporter": reporter_name or observer or "Unknown",
        "reporter_email": reporter_email or "",
        "instrument": instrument,
        "mag": float(mag),
        "filter": filt,
    }
    return {"at_report": {"0": at}}

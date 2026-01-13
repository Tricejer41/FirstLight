from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests

@dataclass(frozen=True)
class ProbeResult:
    submit_url: str
    status_url: str
    notes: List[str]
    ok_auth: Optional[bool]

class TNSClient:
    """Minimal TNS Bulk-Report client.

    - Requires strict `User-Agent` containing `tns_marker{...}` for your BOT.
    - Uses multipart/form-data for `api_key` and `data`.
    - Debug prints never include secrets (only lengths).
    """

    def __init__(self, api_url: str | None = None):
        self.api_url = (api_url or os.getenv("TNS_API_URL", "").strip()).rstrip("/")
        self.bot_id = os.getenv("TNS_BOT_ID", "").strip()
        self.bot_name = os.getenv("TNS_BOT_NAME", "").strip()
        self.api_key = os.getenv("TNS_API_KEY", "").strip()
        self.user_agent = os.getenv("TNS_USER_AGENT", "").strip()

        if not self.user_agent and self.bot_id and self.bot_name:
            self.user_agent = f'tns_marker{{"tns_id":{self.bot_id},"type":"bot","name":"{self.bot_name}"}}'

    def enabled(self) -> bool:
        return bool(self.api_url and self.api_key and self.user_agent)

    def _headers(self) -> Dict[str, str]:
        return {"User-Agent": self.user_agent}

    def _post_multipart(self, url: str, fields: Dict[str, str], timeout_s: int = 10) -> Tuple[int, Dict[str, Any] | str]:
        files = {k: (None, v) for k, v in fields.items()}
        r = requests.post(url, headers=self._headers(), files=files, timeout=timeout_s)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text

    def probe(self) -> ProbeResult:
        submit_url = f"{self.api_url}/bulk-report"
        status_url = f"{self.api_url}/bulk-report/status"
        notes: List[str] = []

        if not self.enabled():
            notes.append("TNS client disabled: missing TNS_API_URL and/or TNS_API_KEY and/or TNS_USER_AGENT.")
            notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)} api_url={len(self.api_url)}")
            return ProbeResult(submit_url, status_url, notes, ok_auth=None)

        payload = {"api_key": self.api_key, "data": "{}"}

        code, body = self._post_multipart(submit_url, payload, timeout_s=10)
        keys = list(body.keys()) if isinstance(body, dict) else None
        notes.append(f"submit probe bulk-report: HTTP {code} JSON keys={keys}" if keys is not None else f"submit probe bulk-report: HTTP {code}")

        code2, body2 = self._post_multipart(status_url, payload, timeout_s=10)
        keys2 = list(body2.keys()) if isinstance(body2, dict) else None
        notes.append(f"status probe bulk-report/status: HTTP {code2} JSON keys={keys2}" if keys2 is not None else f"status probe bulk-report/status: HTTP {code2}")

        ok_auth = None
        if code != 401 and code2 != 401:
            ok_auth = True
        elif code == 401 and code2 == 401:
            ok_auth = False

        notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)}")
        return ProbeResult(submit_url, status_url, notes, ok_auth=ok_auth)

    def submit_at_report(self, data_json: str) -> Tuple[bool, str]:
        if not self.enabled():
            return False, "TNS client disabled (missing env vars)."

        submit_url = f"{self.api_url}/bulk-report"
        code, body = self._post_multipart(submit_url, {"api_key": self.api_key, "data": data_json}, timeout_s=20)
        if code in (200, 201, 202):
            return True, f"HTTP {code}"
        if isinstance(body, dict):
            return False, f"HTTP {code} id_code={body.get('id_code')} id_message={body.get('id_message')}"
        return False, f"HTTP {code}"

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests

@dataclass(frozen=True)
class ProbeResult:
    submit_url: str
    reply_url: str
    notes: List[str]
    ok_auth: Optional[bool]

class TNSClient:
    """Minimal TNS Bulk-Report client.

    IMPORTANT (TNS 2.0): endpoints use `/api/set/...` and `/api/get/...`.
      - Submit:  POST {API_BASE}/set/bulk-report
      - Reply:   POST {API_BASE}/get/bulk-report-reply

    Where API_BASE is typically:
      - Sandbox: https://sandbox.wis-tns.org/api
      - Prod:    https://www.wis-tns.org/api
    """

    def __init__(self, api_base: str | None = None):
        self.api_base = (api_base or os.getenv("TNS_API_URL", "").strip()).rstrip("/")
        self.bot_id = os.getenv("TNS_BOT_ID", "").strip()
        self.bot_name = os.getenv("TNS_BOT_NAME", "").strip()
        self.api_key = os.getenv("TNS_API_KEY", "").strip()
        self.user_agent = os.getenv("TNS_USER_AGENT", "").strip()

        if not self.user_agent and self.bot_id and self.bot_name:
            self.user_agent = f'tns_marker{{"tns_id":{self.bot_id},"type":"bot","name":"{self.bot_name}"}}'

    def enabled(self) -> bool:
        return bool(self.api_base and self.api_key and self.user_agent)

    def _headers(self) -> Dict[str, str]:
        return {"User-Agent": self.user_agent}

    def _post_multipart(self, url: str, fields: Dict[str, str], timeout_s: int = 10) -> Tuple[int, Dict[str, Any] | str]:
        files = {k: (None, v) for k, v in fields.items()}
        r = requests.post(url, headers=self._headers(), files=files, timeout=timeout_s)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text

    @property
    def submit_url(self) -> str:
        return f"{self.api_base}/set/bulk-report"

    @property
    def reply_url(self) -> str:
        return f"{self.api_base}/get/bulk-report-reply"

    def probe(self) -> ProbeResult:
        notes: List[str] = []
        if not self.enabled():
            notes.append("TNS client disabled: missing TNS_API_URL and/or TNS_API_KEY and/or TNS_USER_AGENT.")
            notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)} api_base={len(self.api_base)}")
            return ProbeResult(self.submit_url, self.reply_url, notes, ok_auth=None)

        # Probe 1: hit submit endpoint with minimal (invalid) data to distinguish auth vs schema.
        payload = {"api_key": self.api_key, "data": "{}"}
        code, body = self._post_multipart(self.submit_url, payload, timeout_s=10)
        keys = list(body.keys()) if isinstance(body, dict) else None
        notes.append(f"submit probe set/bulk-report: HTTP {code} JSON keys={keys}" if keys is not None else f"submit probe set/bulk-report: HTTP {code}")

        # Probe 2: hit reply endpoint with empty data; should not be 404 if endpoint exists.
        code2, body2 = self._post_multipart(self.reply_url, payload, timeout_s=10)
        keys2 = list(body2.keys()) if isinstance(body2, dict) else None
        notes.append(f"reply probe get/bulk-report-reply: HTTP {code2} JSON keys={keys2}" if keys2 is not None else f"reply probe get/bulk-report-reply: HTTP {code2}")

        ok_auth = None
        if code != 401 and code2 != 401:
            ok_auth = True
        elif code == 401 and code2 == 401:
            ok_auth = False

        notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)}")
        return ProbeResult(self.submit_url, self.reply_url, notes, ok_auth=ok_auth)

    def submit_bulk_report(self, data_json: str) -> Tuple[bool, str, Optional[str]]:
        """Submit bulk report; returns (ok, detail, report_id)."""
        if not self.enabled():
            return False, "TNS client disabled (missing env vars).", None

        code, body = self._post_multipart(self.submit_url, {"api_key": self.api_key, "data": data_json}, timeout_s=20)
        if isinstance(body, dict):
            # report_id usually is in body['data']['report_id'] depending on API version; keep robust:
            report_id = None
            for k in ("report_id", "reportid", "id"):
                if k in body:
                    report_id = str(body.get(k))
            data = body.get("data") if isinstance(body.get("data"), dict) else None
            if data and report_id is None and "report_id" in data:
                report_id = str(data.get("report_id"))

            if code in (200, 201, 202):
                return True, f"HTTP {code}", report_id
            return False, f"HTTP {code} id_code={body.get('id_code')} id_message={body.get('id_message')}", report_id

        if code in (200, 201, 202):
            return True, f"HTTP {code}", None
        return False, f"HTTP {code}", None

    def fetch_reply(self, report_id: str) -> Tuple[bool, str]:
        """Fetch bulk-report reply given a report_id."""
        if not self.enabled():
            return False, "TNS client disabled (missing env vars)."

        # According to TNS manuals, reply expects JSON specifying report_id.
        data = f'{{"report_id":"{report_id}"}}'
        code, body = self._post_multipart(self.reply_url, {"api_key": self.api_key, "data": data}, timeout_s=20)
        if code in (200, 201, 202):
            return True, f"HTTP {code}"
        if isinstance(body, dict):
            return False, f"HTTP {code} id_code={body.get('id_code')} id_message={body.get('id_message')}"
        return False, f"HTTP {code}"

def build_minimal_at_report() -> str:
    return "{}"

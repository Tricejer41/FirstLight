from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests


@dataclass(frozen=True)
class ProbeResult:
    submit_url: str
    notes: List[str]
    ok_auth: Optional[bool]


class TNSClient:
    """
    Minimal TNS Bulk-Report client (TNS 2.0 endpoints).

    Endpoints:
      - Submit: POST {API_BASE}/set/bulk-report
      - Reply:  POST {API_BASE}/get/bulk-report-reply

    API_BASE examples:
      - Sandbox: https://sandbox.wis-tns.org/api
      - Prod:    https://www.wis-tns.org/api

    Auth requirements:
      - Strict User-Agent containing tns_marker{...} for your bot
      - api_key provided as a multipart/form-data field
      - data provided as a multipart/form-data field (JSON string)
    """

    def __init__(self, api_base: str | None = None):
        self.api_base = (api_base or os.getenv("TNS_API_URL", "").strip()).rstrip("/")
        self.bot_id = os.getenv("TNS_BOT_ID", "").strip()
        self.bot_name = os.getenv("TNS_BOT_NAME", "").strip()
        self.api_key = os.getenv("TNS_API_KEY", "").strip()
        self.user_agent = os.getenv("TNS_USER_AGENT", "").strip()

        # Fallback UA if user didn't set it explicitly
        if not self.user_agent and self.bot_id and self.bot_name:
            self.user_agent = f'tns_marker{{"tns_id":{self.bot_id},"type":"bot","name":"{self.bot_name}"}}'

    def enabled(self) -> bool:
        return bool(self.api_base and self.api_key and self.user_agent)

    def _headers(self) -> Dict[str, str]:
        return {"User-Agent": self.user_agent}

    def _post_multipart(self, url: str, fields: Dict[str, str], timeout_s: int = 10) -> Tuple[int, Dict[str, Any] | str]:
        # multipart/form-data even without files
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
        """
        Probe ONLY the submit endpoint.
        Reply endpoint cannot be probed without a valid report_id (otherwise you may get 401/400).
        """
        notes: List[str] = []
        if not self.enabled():
            notes.append("TNS client disabled: missing TNS_API_URL and/or TNS_API_KEY and/or TNS_USER_AGENT.")
            notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)} api_base={len(self.api_base)}")
            return ProbeResult(self.submit_url, notes, ok_auth=None)

        # Intentionally invalid payload; we only care that auth passes (expect 400 not 401, and not 404)
        payload = {"api_key": self.api_key, "data": "{}"}
        code, body = self._post_multipart(self.submit_url, payload, timeout_s=10)
        keys = list(body.keys()) if isinstance(body, dict) else None
        notes.append(f"submit probe set/bulk-report: HTTP {code} JSON keys={keys}" if keys is not None else f"submit probe set/bulk-report: HTTP {code}")

        ok_auth = None
        if code != 401:
            ok_auth = True
        elif code == 401:
            ok_auth = False

        notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)}")
        return ProbeResult(self.submit_url, notes, ok_auth=ok_auth)

    def submit_raw(self, data_obj: dict) -> Tuple[bool, str, Optional[str]]:
        """
        Submit arbitrary JSON object as the 'data' field.
        Returns (ok, detail, report_id_if_any).
        """
        if not self.enabled():
            return False, "TNS client disabled (missing env vars).", None

        data_json = json.dumps(data_obj, ensure_ascii=False)
        code, body = self._post_multipart(self.submit_url, {"api_key": self.api_key, "data": data_json}, timeout_s=25)

        report_id = None
        if isinstance(body, dict):
            # Different deployments may store report_id in different places; be tolerant.
            if "report_id" in body:
                report_id = str(body.get("report_id"))
            data = body.get("data") if isinstance(body.get("data"), dict) else None
            if data and "report_id" in data:
                report_id = str(data.get("report_id"))

            if code in (200, 201, 202):
                return True, f"HTTP {code}", report_id

            return False, f"HTTP {code} id_code={body.get('id_code')} id_message={body.get('id_message')}", report_id

        if code in (200, 201, 202):
            return True, f"HTTP {code}", None
        return False, f"HTTP {code}", None

    def fetch_reply(self, report_id: str) -> Tuple[bool, str]:
        """
        Fetch bulk-report reply for a given report_id.
        """
        if not self.enabled():
            return False, "TNS client disabled (missing env vars)."

        data_obj = {"report_id": str(report_id)}
        data_json = json.dumps(data_obj, ensure_ascii=False)
        code, body = self._post_multipart(self.reply_url, {"api_key": self.api_key, "data": data_json}, timeout_s=25)

        if code in (200, 201, 202):
            return True, f"HTTP {code}"

        if isinstance(body, dict):
            return False, f"HTTP {code} id_code={body.get('id_code')} id_message={body.get('id_message')}"
        return False, f"HTTP {code}"

    def build_submit_min_payload(self) -> dict:
        """
        Minimal-ish payload for bulk-report.
        This is intentionally conservative: it may still 400 depending on required schema.
        The goal is to get a clear id_message from TNS so we can lock the exact schema quickly.
        """
        # NOTE: We DON'T know your deployment's exact minimal schema yet.
        # This structure is a common pattern: send a list of reports under "reports".
        return {
            "reports": [
                {
                    "report_type": "AT",
                    "at_report": {
                        # Dummy candidate; will be rejected if schema requires more fields.
                        "ra": 0.0,
                        "dec": 0.0,
                        "ra_unit": "deg",
                        "dec_unit": "deg",
                        "discovery_datetime": "2026-01-01 00:00:00",
                        "reporting_group_id": os.getenv("TNS_REPORTER_GROUP", "unknown"),
                        "reporting_group_name": os.getenv("TNS_REPORTER_GROUP", "unknown"),
                        "reporter": os.getenv("TNS_REPORTER_NAME", "unknown"),
                        "reporter_institution": os.getenv("TNS_REPORTER_INSTITUTION", "Independent"),
                    },
                }
            ]
        }

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List, Union

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

    Auth requirements:
      - Strict User-Agent containing tns_marker{...} for your bot
      - api_key provided as some form field (varies by endpoint / deployment)
      - data provided as JSON string in some form field OR as JSON body

    Reality:
      - /set/bulk-report is usually permissive (multipart works).
      - /get/bulk-report-reply can be strict; some deployments only accept JSON body,
        others accept urlencoded form. We implement fallbacks to eliminate guesswork.
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

    @property
    def submit_url(self) -> str:
        return f"{self.api_base}/set/bulk-report"

    @property
    def reply_url(self) -> str:
        return f"{self.api_base}/get/bulk-report-reply"

    # ---------- HTTP helpers ----------

    def _post_multipart(self, url: str, fields: Dict[str, str], timeout_s: int = 10) -> Tuple[int, Union[Dict[str, Any], str]]:
        files = {k: (None, v) for k, v in fields.items()}
        r = requests.post(url, headers=self._headers(), files=files, timeout=timeout_s)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text

    def _post_urlencoded(self, url: str, fields: Dict[str, str], timeout_s: int = 10) -> Tuple[int, Union[Dict[str, Any], str]]:
        r = requests.post(url, headers=self._headers(), data=fields, timeout=timeout_s)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text

    def _post_json(self, url: str, obj: Dict[str, Any], timeout_s: int = 10) -> Tuple[int, Union[Dict[str, Any], str]]:
        r = requests.post(url, headers=self._headers(), json=obj, timeout=timeout_s)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text

    # ---------- Public methods ----------

    def probe(self) -> ProbeResult:
        notes: List[str] = []
        if not self.enabled():
            notes.append("TNS client disabled: missing TNS_API_URL and/or TNS_API_KEY and/or TNS_USER_AGENT.")
            notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)} api_base={len(self.api_base)}")
            return ProbeResult(self.submit_url, notes, ok_auth=None)

        payload = {"api_key": self.api_key, "data": "{}"}  # intentionally invalid
        code, body = self._post_multipart(self.submit_url, payload, timeout_s=10)
        keys = list(body.keys()) if isinstance(body, dict) else None
        notes.append(f"submit probe set/bulk-report: HTTP {code} JSON keys={keys}" if keys is not None else f"submit probe set/bulk-report: HTTP {code}")

        ok_auth = (code != 401)
        notes.append(f"env lengths: api_key={len(self.api_key)} ua={len(self.user_agent)}")
        return ProbeResult(self.submit_url, notes, ok_auth=ok_auth)

    def submit_raw(self, data_obj: dict) -> Tuple[bool, str, Optional[str]]:
        if not self.enabled():
            return False, "TNS client disabled (missing env vars).", None

        data_json = json.dumps(data_obj, ensure_ascii=False)
        code, body = self._post_multipart(self.submit_url, {"api_key": self.api_key, "data": data_json}, timeout_s=25)

        report_id = None
        if isinstance(body, dict):
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

    def fetch_reply_detailed(self, report_id: str) -> Tuple[bool, int, Union[Dict[str, Any], str], str]:
        """
        Try multiple encodings to retrieve the reply.
        Returns: (ok, http_code, body, method_used)

        This is intentionally diagnostic: we want the first method that returns
        a non-401 and gives us a meaningful message.
        """
        if not self.enabled():
            return False, 0, "TNS client disabled (missing env vars).", "disabled"

        # Many servers are picky about report_id type (string vs int)
        rid_str = str(report_id).strip()
        rid_int = int(rid_str) if rid_str.isdigit() else None

        # Candidate payloads
        data_obj_str = {"report_id": rid_str}
        data_obj_int = {"report_id": rid_int} if rid_int is not None else None

        data_json_str = json.dumps(data_obj_str, ensure_ascii=False)
        data_json_int = json.dumps(data_obj_int, ensure_ascii=False) if data_obj_int else None

        attempts: List[Tuple[str, callable]] = []

        # 1) urlencoded with report_id as string (classic)
        attempts.append(("urlencoded:data_str", lambda: self._post_urlencoded(
            self.reply_url,
            {"api_key": self.api_key, "data": data_json_str},
            timeout_s=25
        )))

        # 2) urlencoded with report_id as int
        if data_json_int:
            attempts.append(("urlencoded:data_int", lambda: self._post_urlencoded(
                self.reply_url,
                {"api_key": self.api_key, "data": data_json_int},
                timeout_s=25
            )))

        # 3) JSON body with (api_key, data) fields
        attempts.append(("json:{api_key,data_str}", lambda: self._post_json(
            self.reply_url,
            {"api_key": self.api_key, "data": data_json_str},
            timeout_s=25
        )))

        if data_json_int:
            attempts.append(("json:{api_key,data_int}", lambda: self._post_json(
                self.reply_url,
                {"api_key": self.api_key, "data": data_json_int},
                timeout_s=25
            )))

        # 4) JSON body with direct report_id (some APIs do this)
        attempts.append(("json:{api_key,report_id_str}", lambda: self._post_json(
            self.reply_url,
            {"api_key": self.api_key, "report_id": rid_str},
            timeout_s=25
        )))
        if rid_int is not None:
            attempts.append(("json:{api_key,report_id_int}", lambda: self._post_json(
                self.reply_url,
                {"api_key": self.api_key, "report_id": rid_int},
                timeout_s=25
            )))

        # 5) multipart fallback (in case server expects it)
        attempts.append(("multipart:data_str", lambda: self._post_multipart(
            self.reply_url,
            {"api_key": self.api_key, "data": data_json_str},
            timeout_s=25
        )))

        if data_json_int:
            attempts.append(("multipart:data_int", lambda: self._post_multipart(
                self.reply_url,
                {"api_key": self.api_key, "data": data_json_int},
                timeout_s=25
            )))

        last_code = 0
        last_body: Union[Dict[str, Any], str] = ""
        last_method = "none"

        for method, fn in attempts:
            code, body = fn()
            last_code, last_body, last_method = code, body, method

            # Success
            if code in (200, 201, 202):
                return True, code, body, method

            # If not 401, we want to surface it (means request reached validation)
            if code != 401:
                return False, code, body, method

        # All attempts were 401
        return False, last_code, last_body, last_method

    def fetch_reply(self, report_id: str) -> Tuple[bool, str]:
        ok, code, body, method = self.fetch_reply_detailed(report_id)

        if ok:
            return True, f"HTTP {code} via {method}"

        if isinstance(body, dict):
            return False, f"HTTP {code} via {method} id_code={body.get('id_code')} id_message={body.get('id_message')}"
        return False, f"HTTP {code} via {method}"

    def build_submit_min_payload(self) -> dict:
        # This worked for you to obtain report_id on sandbox.
        return {
            "reports": [
                {
                    "report_type": "AT",
                    "at_report": {
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

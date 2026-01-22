import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List, Union

import requests


def _utc_now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def _stats_api_key(api_key: str) -> str:
    return f"api_key_len={len(api_key)} has_dot={'.' in api_key}"


def _stats_ua(ua: str) -> str:
    return f"ua_len={len(ua)} ua_has_tns_marker={'tns_marker' in ua}"


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def _extract_id_fields(j: Any) -> Tuple[Optional[Any], Optional[str]]:
    """
    Defensive parse for typical TNS JSON:
      {"id_code": 200, "id_message": "OK", ...}
    Sometimes nested under "data".
    """
    if not isinstance(j, dict):
        return None, None
    if "id_code" in j or "id_message" in j:
        return j.get("id_code"), j.get("id_message")
    if isinstance(j.get("data"), dict):
        return _extract_id_fields(j["data"])
    return None, None


def _find_first_key_recursive(obj: Any, keys: Tuple[str, ...]) -> Optional[Any]:
    """
    Recursively search dict/list for first occurrence of any key in `keys`.
    """
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
        for _, v in obj.items():
            found = _find_first_key_recursive(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_first_key_recursive(item, keys)
            if found is not None:
                return found
    return None


@dataclass
class TNSResponse:
    ok: bool
    http: int
    elapsed_ms: int
    id_code: Optional[Any] = None
    id_message: Optional[str] = None
    report_id: Optional[Any] = None
    raw_json: Optional[Dict[str, Any]] = None
    raw_text_snip: str = ""
    method: str = ""


class TNSClient:
    """
    Practical TNS Bulk API client (TNS 2.0).
    Focus: stable submit/reply plumbing + diagnostics without secrets.
    """

    def __init__(self, api_base_url: str, api_key: str, user_agent: str, timeout_s: int = 30):
        self.api_base_url = api_base_url.rstrip("/")
        self.api_key = api_key
        self.user_agent = user_agent
        self.timeout_s = timeout_s

        self.submit_url = f"{self.api_base_url}/set/bulk-report"
        self.reply_url = f"{self.api_base_url}/get/bulk-report-reply"
        self.test_url = f"{self.api_base_url}/test"

        # alt path for some deployments/manuals
        self.reply_url_alt = f"{self.api_base_url}/bulk-report-reply"

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.user_agent, "Accept": "application/json"})

    @classmethod
    def from_env(cls) -> "TNSClient":
        api_base_url = os.getenv("TNS_API_URL", "").strip()
        api_key = os.getenv("TNS_API_KEY", "").strip()
        user_agent = os.getenv("TNS_USER_AGENT", "").strip()

        missing = []
        if not api_base_url:
            missing.append("TNS_API_URL")
        if not api_key:
            missing.append("TNS_API_KEY")
        if not user_agent:
            missing.append("TNS_USER_AGENT")
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

        return cls(api_base_url=api_base_url, api_key=api_key, user_agent=user_agent)

    # ---------- Payload builders ----------

    def build_submit_min_payload(self) -> Dict[str, Any]:
        """
        Minimal *plausible* bulk AT report skeleton.
        Goal: force async job creation so reply becomes queryable.

        WARNING:
        - reporting_group_id / discovery_data_source_id must match your bot perms.
          If wrong, reply should still exist and show validation error.
        """
        now = _utc_now_str()
        internal_name = f"ZTFTEST_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}"

        at_entry = {
            "ra": "10:00:00.00",
            "dec": "+02:00:00.0",
            "discoverydate": now,
            "discoverymag": "19.5",
            "discmagfilter": "r",
            "reporter": "Firstlight Bot Test",
            "internal_name": internal_name,
            "remarks": "submit-min sanity check (sandbox) — expect validation if IDs mismatch",
            "reporting_group_id": "1",
            "discovery_data_source_id": "1",
        }

        return {"at_report": {"0": at_entry}}

    # ---------- HTTP helpers ----------

    def _post_form(self, url: str, form: Dict[str, str], method_tag: str) -> TNSResponse:
        t0 = time.time()
        r = self._session.post(url, data=form, timeout=self.timeout_s)
        elapsed_ms = int((time.time() - t0) * 1000)
        return self._parse_response(r, elapsed_ms, method_tag)

    def _post_json(self, url: str, obj: Dict[str, Any], method_tag: str) -> TNSResponse:
        t0 = time.time()
        r = self._session.post(url, json=obj, timeout=self.timeout_s)
        elapsed_ms = int((time.time() - t0) * 1000)
        return self._parse_response(r, elapsed_ms, method_tag)

    def _post_multipart(self, url: str, data: Dict[str, str], files: Dict[str, Any], method_tag: str) -> TNSResponse:
        t0 = time.time()
        r = self._session.post(url, data=data, files=files, timeout=self.timeout_s)
        elapsed_ms = int((time.time() - t0) * 1000)
        return self._parse_response(r, elapsed_ms, method_tag)

    def _get_params(self, url: str, params: Dict[str, str], method_tag: str) -> TNSResponse:
        t0 = time.time()
        r = self._session.get(url, params=params, timeout=self.timeout_s)
        elapsed_ms = int((time.time() - t0) * 1000)
        return self._parse_response(r, elapsed_ms, method_tag)

    def _parse_response(self, r: requests.Response, elapsed_ms: int, method_tag: str) -> TNSResponse:
        http = r.status_code
        raw_json = None
        raw_text_snip = (r.text[:900].replace("\n", "\\n") if r.text else "")

        try:
            j_any = r.json()
            raw_json = j_any if isinstance(j_any, dict) else None
        except Exception:
            raw_json = None

        id_code, id_message = _extract_id_fields(raw_json)

        # Robust report_id extraction (may be nested)
        report_id = _find_first_key_recursive(raw_json, ("report_id", "reportId", "id_report", "idReport"))

        # For submit: OK when http==200 and id_code==200 (string/int)
        ok = (http == 200) and (str(id_code) == "200")

        return TNSResponse(
            ok=ok,
            http=http,
            elapsed_ms=elapsed_ms,
            id_code=id_code,
            id_message=id_message,
            report_id=report_id,
            raw_json=raw_json,
            raw_text_snip=raw_text_snip,
            method=method_tag,
        )

    # ---------- Public API ----------

    def envcheck_dict(self, show_ua: bool = False) -> Dict[str, Any]:
        d = {
            "api_base_url": self.api_base_url,
            "api_key_stats": _stats_api_key(self.api_key),
            "reply_url": self.reply_url,
            "submit_url": self.submit_url,
            "test_url": self.test_url,
            "ua_stats": _stats_ua(self.user_agent),
        }
        if show_ua:
            d["user_agent"] = self.user_agent
        return d

    def submit_raw(self, payload: Dict[str, Any]) -> Tuple[bool, str, Optional[Any], Optional[Dict[str, Any]]]:
        """
        Submit bulk report. Known-good path in your environment:
          form(api_key, data=jsonstr)
        Returns:
          ok, detail, report_id, raw_json
        """
        data_jsonstr = json.dumps(payload)

        attempts: List[TNSResponse] = []

        # 1) form-urlencoded (your stable success path)
        attempts.append(
            self._post_form(
                self.submit_url,
                {"api_key": self.api_key, "data": data_jsonstr},
                "submit:form(api_key,data=jsonstr)",
            )
        )

        # 2) multipart data as part (keep only for diagnostics)
        attempts.append(
            self._post_multipart(
                self.submit_url,
                {"api_key": self.api_key},
                {"data": (None, data_jsonstr)},
                "submit:multipart(api_key in data, data as part)",
            )
        )

        # 3) JSON (often 401 in sandbox; keep only for diagnostics)
        attempts.append(
            self._post_json(
                self.submit_url,
                {"api_key": self.api_key, "data": payload},
                "submit:json(api_key,data_obj)",
            )
        )

        best = next((a for a in attempts if a.http == 200 and str(a.id_code) == "200"), attempts[0])

        summary = " | ".join([f"{a.method}:{a.http}:{a.id_code}:{a.id_message}:rid={a.report_id}" for a in attempts])
        detail = (
            f"via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
            f"id_code={best.id_code} id_message={best.id_message} report_id={best.report_id} "
            f"({summary}) "
            f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
        )
        return best.ok, detail, best.report_id, best.raw_json

    def submit_min(self) -> Tuple[bool, str, Optional[Any], Optional[Dict[str, Any]]]:
        payload = self.build_submit_min_payload()
        return self.submit_raw(payload)

    def reply(self, report_id: Any, wait_s: int = 600) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Poll for reply. Treat 404 as pending.
        """
        rid = str(report_id).strip()
        if not rid:
            return False, "report_id is empty; cannot query reply", {"id_code": "client_error", "id_message": "empty report_id"}

        def one_round(url: str) -> List[TNSResponse]:
            data_obj = {"report_id": rid}
            data_jsonstr = json.dumps(data_obj)
            out: List[TNSResponse] = []
            out.append(self._post_form(url, {"api_key": self.api_key, "report_id": rid}, "reply:form(api_key,report_id)"))
            out.append(self._post_form(url, {"api_key": self.api_key, "data": data_jsonstr}, "reply:form(api_key,data=jsonstr)"))
            out.append(self._post_json(url, {"api_key": self.api_key, "data": data_obj}, "reply:json(api_key,data_obj)"))
            out.append(self._get_params(url, {"api_key": self.api_key, "report_id": rid}, "reply:GET(params)"))
            return out

        start = time.time()
        tries = 0
        sleep_s = 2.0

        last_json: Dict[str, Any] = {"id_code": None, "id_message": None}

        while True:
            tries += 1
            attempts = one_round(self.reply_url)

            # If everything is 401, try alt endpoint once
            if all(a.http == 401 for a in attempts):
                attempts += one_round(self.reply_url_alt)

            best = next((a for a in attempts if a.http == 200 and a.raw_json is not None), None)
            if best is None:
                # prefer non-401 informative
                non401 = [a for a in attempts if a.http != 401]
                best = non401[0] if non401 else attempts[0]

            last_json = best.raw_json or {"id_code": best.id_code, "id_message": best.id_message}

            # terminal success
            if best.http == 200 and isinstance(best.raw_json, dict):
                ok = str(best.raw_json.get("id_code")) == "200"
                detail = (
                    f"via={best.method} http=200 elapsed_ms={best.elapsed_ms} "
                    f"id_code={best.id_code} id_message={best.id_message} tries={tries} "
                    f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
                )
                return ok, detail, best.raw_json

            # pending
            if best.http == 404 and (time.time() - start) < wait_s:
                time.sleep(sleep_s)
                sleep_s = min(20.0, sleep_s * 1.35)
                continue

            status = "pending_timeout" if best.http == 404 else "terminal"
            detail = (
                f"via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
                f"id_code={best.id_code} id_message={best.id_message} tries={tries} status={status} "
                f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
            )
            return False, detail, last_json

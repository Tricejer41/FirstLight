import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests


def _utc_now_str_ms() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + ".000"


def _utc_str_ms_offset(seconds: int) -> str:
    t = time.time() + float(seconds)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t)) + ".000"


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
    if not isinstance(j, dict):
        return None, None
    if "id_code" in j or "id_message" in j:
        return j.get("id_code"), j.get("id_message")
    if isinstance(j.get("data"), dict):
        return _extract_id_fields(j["data"])
    return None, None


def _find_first_key_recursive(obj: Any, keys: Tuple[str, ...]) -> Optional[Any]:
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
        for v in obj.values():
            found = _find_first_key_recursive(v, keys)
            if found is not None:
                return found
    if isinstance(obj, list):
        for item in obj:
            found = _find_first_key_recursive(item, keys)
            if found is not None:
                return found
    return None


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


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
    def __init__(self, api_base_url: str, api_key: str, user_agent: str, timeout_s: int = 30):
        self.api_base_url = api_base_url.rstrip("/")
        self.api_key = api_key
        self.user_agent = user_agent
        self.timeout_s = timeout_s

        self.submit_url = f"{self.api_base_url}/set/bulk-report"
        self.reply_url = f"{self.api_base_url}/get/bulk-report-reply"
        self.test_url = f"{self.api_base_url}/test"
        self.reply_url_alt = f"{self.api_base_url}/bulk-report-reply"

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.user_agent, "Accept": "application/json"})

        # IMPORTANT: do NOT guess IDs. Empty => field omitted, reply will say "Required field" if needed.
        self.reporting_groupid = _env("TNS_REPORTING_GROUPID", "")
        self.discovery_data_sourceid = _env("TNS_DISCOVERY_DATA_SOURCEID", "1")  # kept as before; no error yet
        self.instrumentid = _env("TNS_INSTRUMENTID", "1")  # kept as before; no error yet

        self.photometry_flux_units = _env("TNS_PHOT_FLUX_UNITS", "1")   # previously accepted
        self.nondet_flux_units = _env("TNS_NONDET_FLUX_UNITS", "1")     # previously accepted

        self.photometry_filterid = _env("TNS_PHOT_FILTERID", "")
        self.nondet_filter_value = _env("TNS_NONDET_FILTER_VALUE", "")

    @classmethod
    def from_env(cls) -> "TNSClient":
        api_base_url = _env("TNS_API_URL")
        api_key = _env("TNS_API_KEY")
        user_agent = _env("TNS_USER_AGENT")

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

    def build_submit_min_payload(self) -> Dict[str, Any]:
        discovery_dt = _utc_now_str_ms()
        nondet_dt = _utc_str_ms_offset(-86400)
        internal_name = f"ZTFTEST_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}"

        ra_deg = "150.000000"
        dec_deg = "2.000000"

        at_entry: Dict[str, Any] = {
            "internal_name": internal_name,
            "reporter": "Firstlight Bot Test",
            "ra": {"value": ra_deg},
            "dec": {"value": dec_deg},
            "discovery_datetime": discovery_dt,
            "at_type": "1",
            "remarks": "submit-min (schema convergence).",
        }

        if self.reporting_groupid:
            at_entry["reporting_groupid"] = str(self.reporting_groupid)
        if self.discovery_data_sourceid:
            at_entry["discovery_data_sourceid"] = str(self.discovery_data_sourceid)

        phot0: Dict[str, Any] = {
            "obsdate": discovery_dt,
            "flux": "19.5",
            "fluxerr": "0.10",
            "flux_units": str(self.photometry_flux_units),
            "remarks": "discovery point (sandbox submit-min)",
        }
        if self.instrumentid:
            phot0["instrumentid"] = str(self.instrumentid)
        if self.photometry_filterid:
            phot0["filterid"] = str(self.photometry_filterid)

        at_entry["photometry"] = {"0": phot0}

        nd: Dict[str, Any] = {
            "obsdate": nondet_dt,
            "archiveid": "1",
            "limiting_flux": "22.0",
            "flux_units": str(self.nondet_flux_units),
            "remarks": "placeholder non-detection (schema convergence)",
        }
        if self.instrumentid:
            nd["instrument_value"] = str(self.instrumentid)
        if self.nondet_filter_value:
            nd["filter_value"] = str(self.nondet_filter_value)

        at_entry["non_detection"] = nd

        return {"at_report": {"0": at_entry}}

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
        report_id = _find_first_key_recursive(raw_json, ("report_id", "reportId", "id_report", "idReport"))
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

    def envcheck_dict(self, show_ua: bool = False) -> Dict[str, Any]:
        warnings: List[str] = []
        if not self.reporting_groupid:
            warnings.append("TNS_REPORTING_GROUPID is empty (will omit reporting_groupid; set a real group ID).")
        if not self.photometry_filterid:
            warnings.append("TNS_PHOT_FILTERID is empty (will omit photometry.filterid; set a real filter ID).")
        if not self.nondet_filter_value:
            warnings.append("TNS_NONDET_FILTER_VALUE is empty (will omit non_detection.filter_value; set a real filter ID).")

        d = {
            "api_base_url": self.api_base_url,
            "api_key_stats": _stats_api_key(self.api_key),
            "reply_url": self.reply_url,
            "submit_url": self.submit_url,
            "test_url": self.test_url,
            "ua_stats": _stats_ua(self.user_agent),
            "tns_ids_stats": (
                f"reporting_groupid={self.reporting_groupid!r} "
                f"discovery_data_sourceid={self.discovery_data_sourceid!r} "
                f"instrumentid={self.instrumentid!r} "
                f"phot_flux_units={self.photometry_flux_units!r} nondet_flux_units={self.nondet_flux_units!r} "
                f"phot_filterid={self.photometry_filterid!r} nondet_filter_value={self.nondet_filter_value!r}"
            ),
            "warnings": warnings,
        }
        if show_ua:
            d["user_agent"] = self.user_agent
        return d

    def submit_raw(self, payload: Dict[str, Any]) -> Tuple[bool, str, Optional[Any], Optional[Dict[str, Any]]]:
        data_jsonstr = json.dumps(payload)

        attempts: List[TNSResponse] = []
        attempts.append(self._post_form(self.submit_url, {"api_key": self.api_key, "data": data_jsonstr}, "submit:form(api_key,data=jsonstr)"))
        attempts.append(self._post_multipart(self.submit_url, {"api_key": self.api_key}, {"data": (None, data_jsonstr)}, "submit:multipart(api_key in data, data as part)"))
        attempts.append(self._post_json(self.submit_url, {"api_key": self.api_key, "data": payload}, "submit:json(api_key,data_obj)"))

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
        return self.submit_raw(self.build_submit_min_payload())

    def reply(self, report_id: Any, wait_s: int = 600) -> Tuple[bool, str, Dict[str, Any]]:
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

        tries = 1
        attempts = one_round(self.reply_url)
        if all(a.http == 401 for a in attempts):
            attempts += one_round(self.reply_url_alt)

        best = next((a for a in attempts if a.http == 200 and a.raw_json is not None), None)
        if best is None:
            non401 = [a for a in attempts if a.http != 401]
            best = non401[0] if non401 else attempts[0]

        if best.http == 200 and isinstance(best.raw_json, dict):
            ok = str(best.raw_json.get("id_code")) == "200"
            detail = (
                f"via={best.method} http=200 elapsed_ms={best.elapsed_ms} "
                f"id_code={best.id_code} id_message={best.id_message} tries={tries} "
                f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
            )
            return ok, detail, best.raw_json

        detail = (
            f"via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
            f"id_code={best.id_code} id_message={best.id_message} tries={tries} status=terminal "
            f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
        )
        return False, detail, (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

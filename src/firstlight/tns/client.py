# src/firstlight/tns/client.py
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests

from ..utils.time import jd_to_datetime_utc


def _utc_now_str_ms() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + ".000"


def _utc_str_ms_offset(seconds: int) -> str:
    t = time.time() + float(seconds)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t)) + ".000"


def _fmt_tns_dt(dt) -> str:
    # TNS Bulk API accepts "YYYY-MM-DD HH:MM:SS.sss" (UTC). Keep ms fixed.
    import datetime as _dt
    return dt.astimezone(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + ".000"


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


def _env_int(name: str, default: int) -> int:
    s = _env(name, "")
    try:
        return int(s)
    except Exception:
        return int(default)


def _choose_filter_ids(
    fid: int,
    phot_filterid_env: str,
    nondet_filter_value_env: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    If env var is 'auto', we MUST NOT send 'auto' to TNS.
    We instead map fid -> proper numeric filter ids.

    ZTF fid: 1=g, 2=r, 3=i
    Common TNS AUX mapping used in your branch: 110/111/112
    """
    ztf_map = {1: "110", 2: "111", 3: "112"}

    phot = (phot_filterid_env or "").strip()
    nd = (nondet_filter_value_env or "").strip()

    if phot and phot.lower() != "auto":
        phot_id = phot
    else:
        phot_id = ztf_map.get(int(fid), None)

    if nd and nd.lower() != "auto":
        nd_id = nd
    else:
        nd_id = ztf_map.get(int(fid), None)

    return phot_id, nd_id


def _extract_reply_objname(reply_json: Dict[str, Any]) -> Optional[str]:
    """
    Best-effort extraction of 'objname' from the reply feedback structure.
    Safe to keep even if structure changes: returns None on any mismatch.
    """
    try:
        fb = reply_json.get("data", {}).get("feedback", {})
        at = fb.get("at_report", [])
        if isinstance(at, list) and at:
            first = at[0]
            if isinstance(first, dict):
                objname = _find_first_key_recursive(first, ("objname",))
                return str(objname) if objname is not None else None
    except Exception:
        pass
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

        # Can be "auto" or a numeric id like "111"
        self.photometry_filterid = _env("TNS_PHOT_FILTERID", "")
        self.nondet_filter_value = _env("TNS_NONDET_FILTER_VALUE", "")

        # Reporter name shown in discovery certificate text ("<reporter> report/s ...")
        # IMPORTANT: do NOT hardcode a default here. In production we want explicit identity.
        # Keep it empty until payload-building time; build_*() will fail loudly if missing.
        self.reporter = _env("TNS_REPORTER_NAME", "") or _env("TNS_REPORTER", "")

        # Internal name presentation in TNS search/results UI.
        # Prefix can be empty; mode controls uniqueness/traceability.
        self.internal_name_prefix = _env("TNS_INTERNAL_NAME_PREFIX", "")
        self.internal_name_mode = (_env("TNS_INTERNAL_NAME_MODE", "objectid_candid") or "objectid_candid").strip().lower()

        # Option B: submission mode selector (auto|form|multipart|json)
        self.submit_mode = (_env("TNS_SUBMIT_MODE", "auto") or "auto").strip().lower()

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

        timeout_s = _env_int("TNS_TIMEOUT_S", 30)
        return cls(api_base_url=api_base_url, api_key=api_key, user_agent=user_agent, timeout_s=timeout_s)

    # -------------------------
    # Preflight auth (NO spam, NO false fatal)
    # -------------------------

    def test_auth(self) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Robust auth preflight.

        Strategy:
          - Probe reply endpoint with a dummy report_id ("0") using POST form.
          - If primary looks unsupported OR fatal, also probe alt once.
          - Mark AUTH_FATAL only if ALL HTTP attempts are 401/403.
          - Any other HTTP response means "auth accepted (or at least not fatal)".

        Returns:
          ok=True  => safe to run dispatch (auth is not fatal)
          ok=False => fatal auth (401/403 everywhere) OR client-side error (no HTTP)
        """
        attempts: List[TNSResponse] = []
        dummy_rid = "0"

        def probe(url: str, tag: str) -> TNSResponse:
            return self._post_form(url, {"api_key": self.api_key, "report_id": dummy_rid}, tag)

        # primary
        try:
            attempts.append(probe(self.reply_url, "preflight:reply:form(primary)"))
        except Exception as exc:
            attempts.append(self._exc_response("preflight:reply:form(primary,exception)", exc))

        # alt (only if primary is unsupported OR fatal)
        if attempts:
            h0 = attempts[0].http
            if h0 in (401, 403, 404, 405):
                try:
                    attempts.append(probe(self.reply_url_alt, "preflight:reply:form(alt)"))
                except Exception as exc:
                    attempts.append(self._exc_response("preflight:reply:form(alt,exception)", exc))

        any_http = any(a.http != 0 for a in attempts)
        http_attempts = [a for a in attempts if a.http != 0]
        all_fatal = bool(http_attempts) and all(a.http in (401, 403) for a in http_attempts)

        any_ok200 = any(a.http == 200 and str(a.id_code) == "200" for a in http_attempts)

        summary = " | ".join([f"{a.method}:{a.http}:{a.id_code}:{a.id_message}" for a in attempts])
        raw = next((a.raw_json for a in attempts if isinstance(a.raw_json, dict)), None)

        if all_fatal:
            detail = (
                f"preflight=fatal_auth fatal=auth ({summary}) "
                f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
            )
            return False, detail, (raw or {})

        if any_http:
            best = next((a for a in http_attempts if a.http == 200), http_attempts[0])
            status = "ok" if (any_ok200 or best.http in (200, 400, 404, 405, 422)) else "ok"
            detail = (
                f"preflight={status} via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
                f"id_code={best.id_code} id_message={best.id_message} "
                f"({summary}) [{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
            )
            return True, detail, (best.raw_json or {})

        detail = (
            f"preflight=error (no http response) ({summary}) "
            f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
        )
        return False, detail, (raw or {})

    # -------------------------
    # Payload builders
    # -------------------------

    def _require_reporter(self) -> str:
        """Reporter is mandatory for any AT report payload."""
        rep = (self.reporter or "").strip()
        if not rep:
            raise RuntimeError("Missing required env var: TNS_REPORTER_NAME (needed for TNS payload field 'reporter').")
        return rep


    def _build_internal_name(self, objid: str, cand_id: Optional[str] = None) -> str:
        prefix = (self.internal_name_prefix or "").strip()
        mode = (self.internal_name_mode or "objectid_candid").strip().lower()

        obj = (objid or "").strip()
        cand = (cand_id or "").strip()

        if mode == "objectid":
            base = obj
        else:
            base = f"{obj}_{cand}" if cand else obj

        return f"{prefix}_{base}" if prefix else base

    def build_submit_min_payload(self) -> Dict[str, Any]:
        rep = self._require_reporter()
        discovery_dt = _utc_now_str_ms()
        nondet_dt = _utc_str_ms_offset(-86400)
        test_prefix = (self.internal_name_prefix or "ZTFTEST").strip()
        internal_name = f"{test_prefix}_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}"

        ra_deg = "150.000000"
        dec_deg = "2.000000"

        at_entry: Dict[str, Any] = {
            "internal_name": internal_name,
            "reporter": rep,
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

        phot_filterid, _ = _choose_filter_ids(2, self.photometry_filterid or "auto", self.nondet_filter_value or "auto")
        if phot_filterid:
            phot0["filterid"] = str(phot_filterid)

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

        _, nd_filter = _choose_filter_ids(2, self.photometry_filterid or "auto", self.nondet_filter_value or "auto")
        if nd_filter:
            nd["filter_value"] = str(nd_filter)

        at_entry["non_detection"] = nd
        return {"at_report": {"0": at_entry}}

    def build_at_report_from_fink_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        rep = self._require_reporter()
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        if "objectId" not in payload or "candidate" not in payload:
            raise ValueError("payload missing objectId/candidate")

        objid = str(payload["objectId"])
        c = payload["candidate"] or {}
        d = payload.get("derived", {}) or {}

        ra = float(c["ra"])
        dec = float(c["dec"])
        jd = float(c["jd"])
        fid = int(c.get("fid", 0))

        mag = float(c["magpsf"])
        magerr = float(c.get("sigmapsf", 0.0))
        limmag = float(c.get("diffmaglim", mag + 1.0))

        cand_id = str(c.get("candid", "")).strip()
        internal_name = self._build_internal_name(objid, cand_id)

        discovery_dt = _fmt_tns_dt(jd_to_datetime_utc(jd))

        nd_jd = None
        nd_lim = None

        for k in ("last_nondet_jd", "last_nondetjd", "last_nd_jd", "last_jd_nd", "jd_last_nondet"):
            if k in d:
                try:
                    nd_jd = float(d[k])
                    break
                except Exception:
                    pass

        for k in ("last_nondet_lim", "last_nondet_diffmaglim", "last_nd_lim", "limmag_last_nondet"):
            if k in d:
                try:
                    nd_lim = float(d[k])
                    break
                except Exception:
                    pass

        if nd_jd is None:
            nd_jd = jd - 1.0
        if nd_lim is None:
            nd_lim = limmag
        if nd_jd >= jd:
            nd_jd = jd - 1.0

        nondet_dt = _fmt_tns_dt(jd_to_datetime_utc(nd_jd))

        phot_filterid, nondet_filter_value = _choose_filter_ids(
            fid, self.photometry_filterid or "auto", self.nondet_filter_value or "auto"
        )

        at_entry: Dict[str, Any] = {
            "internal_name": internal_name,
            "reporter": rep,
            "ra": {"value": f"{ra:.6f}"},
            "dec": {"value": f"{dec:.6f}"},
            "discovery_datetime": discovery_dt,
            "at_type": "1",
            "remarks": f"firstlight auto (objId={objid} candid={cand_id} fid={fid})",
        }

        if self.reporting_groupid:
            at_entry["reporting_groupid"] = str(self.reporting_groupid)
        if self.discovery_data_sourceid:
            at_entry["discovery_data_sourceid"] = str(self.discovery_data_sourceid)

        phot0: Dict[str, Any] = {
            "obsdate": discovery_dt,
            "flux": f"{mag:.3f}",
            "fluxerr": f"{magerr:.3f}",
            "flux_units": str(self.photometry_flux_units),
            "remarks": "discovery photometry (ZTF magpsf)",
        }
        if self.instrumentid:
            phot0["instrumentid"] = str(self.instrumentid)
        if phot_filterid:
            phot0["filterid"] = str(phot_filterid)

        at_entry["photometry"] = {"0": phot0}

        nd: Dict[str, Any] = {
            "obsdate": nondet_dt,
            "archiveid": "1",
            "limiting_flux": f"{nd_lim:.3f}",
            "flux_units": str(self.nondet_flux_units),
            "remarks": "last non-detection (fallback if broker lacks prv_candidates)",
        }
        if self.instrumentid:
            nd["instrument_value"] = str(self.instrumentid)
        if nondet_filter_value:
            nd["filter_value"] = str(nondet_filter_value)

        at_entry["non_detection"] = nd

        return {"at_report": {"0": at_entry}}

    # -------------------------
    # HTTP primitives
    # -------------------------

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

    def _exc_response(self, method_tag: str, exc: Exception) -> TNSResponse:
        # http=0 marks "client-side exception" (no HTTP response).
        msg = (str(exc) or exc.__class__.__name__)[:240]
        return TNSResponse(
            ok=False,
            http=0,
            elapsed_ms=0,
            id_code="exception",
            id_message=msg,
            report_id=None,
            raw_json=None,
            raw_text_snip=msg,
            method=method_tag,
        )

    # -------------------------
    # Diagnostics
    # -------------------------

    def envcheck_dict(self, show_ua: bool = False) -> Dict[str, Any]:
        warnings: List[str] = []
        if not self.reporting_groupid:
            warnings.append("TNS_REPORTING_GROUPID is empty (will omit reporting_groupid; set a real group ID).")
        if not self.photometry_filterid:
            warnings.append("TNS_PHOT_FILTERID is empty (will omit photometry.filterid; set a real filter ID).")
        if not self.nondet_filter_value:
            warnings.append("TNS_NONDET_FILTER_VALUE is empty (will omit non_detection.filter_value; set a real filter ID).")
        if not self.reporter:
            warnings.append("TNS_REPORTER_NAME is empty (payload building will fail; set it explicitly).")

        d = {
            "api_base_url": self.api_base_url,
            "api_key_stats": _stats_api_key(self.api_key),
            "reply_url": self.reply_url,
            "reply_url_alt": self.reply_url_alt,
            "submit_url": self.submit_url,
            "test_url": self.test_url,
            "ua_stats": _stats_ua(self.user_agent),
            "timeout_s": self.timeout_s,
            "submit_mode": self.submit_mode,
            "tns_ids_stats": (
                f"reporting_groupid={self.reporting_groupid!r} "
                f"discovery_data_sourceid={self.discovery_data_sourceid!r} "
                f"instrumentid={self.instrumentid!r} "
                f"phot_flux_units={self.photometry_flux_units!r} nondet_flux_units={self.nondet_flux_units!r} "
                f"phot_filterid={self.photometry_filterid!r} nondet_filter_value={self.nondet_filter_value!r} "
                f"reporter={self.reporter!r} internal_name_prefix={self.internal_name_prefix!r} internal_name_mode={self.internal_name_mode!r}"
            ),
            "warnings": warnings,
        }
        if show_ua:
            d["user_agent"] = self.user_agent
        return d

    # -------------------------
    # Submit / reply
    # -------------------------

    def submit_raw(self, payload: Dict[str, Any]) -> Tuple[bool, str, Optional[Any], Optional[Dict[str, Any]]]:
        """
        Option B (fix): try methods in-order and STOP at first ok.
        This prevents "401 Unauthorized" from later fallbacks polluting the detail string
        when an earlier method already succeeded (rid assigned).
        """
        data_jsonstr = json.dumps(payload)

        def do_form() -> TNSResponse:
            return self._post_form(
                self.submit_url,
                {"api_key": self.api_key, "data": data_jsonstr},
                "submit:form(api_key,data=jsonstr)",
            )

        def do_multipart() -> TNSResponse:
            return self._post_multipart(
                self.submit_url,
                {"api_key": self.api_key},
                {"data": (None, data_jsonstr)},
                "submit:multipart(api_key in data, data as part)",
            )

        def do_json() -> TNSResponse:
            return self._post_json(
                self.submit_url,
                {"api_key": self.api_key, "data": payload},
                "submit:json(api_key,data_obj)",
            )

        # Mode selection (auto is safest: form -> multipart -> json)
        mode = (self.submit_mode or "auto").strip().lower()
        if mode not in ("auto", "form", "multipart", "json"):
            mode = "auto"

        chain: List[Tuple[str, Any]] = []
        if mode == "form":
            chain = [("form", do_form)]
        elif mode == "multipart":
            chain = [("multipart", do_multipart)]
        elif mode == "json":
            chain = [("json", do_json)]
        else:
            chain = [("form", do_form), ("multipart", do_multipart), ("json", do_json)]

        attempts: List[TNSResponse] = []
        best: Optional[TNSResponse] = None

        for _name, fn in chain:
            try:
                a = fn()
            except Exception as exc:
                a = self._exc_response(f"submit:{_name}(exception)", exc)

            attempts.append(a)

            # STOP on first success (critical)
            if a.ok:
                best = a
                break

        if best is None:
            best = attempts[0] if attempts else self._exc_response("submit:none", RuntimeError("no submit attempts"))

        summary = " | ".join(
            [f"{a.method}:{a.http}:{a.id_code}:{a.id_message}:rid={a.report_id}" for a in attempts]
        )

        http_attempts = [a for a in attempts if a.http != 0]
        all_auth_fatal = bool(http_attempts) and all(a.http in (401, 403) for a in http_attempts)
        flags: List[str] = []
        if all_auth_fatal:
            flags.append("fatal=auth")

        status = "ok" if best.ok else "failed"

        detail = (
            f"submit_status={status} via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
            f"id_code={best.id_code} id_message={best.id_message} report_id={best.report_id} "
            f"(attempts={len(attempts)} {summary}) "
            f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}"
            + (f" {' '.join(flags)}" if flags else "")
            + "]"
        )
        return best.ok, detail, best.report_id, best.raw_json

    def submit_min(self) -> Tuple[bool, str, Optional[Any], Optional[Dict[str, Any]]]:
        return self.submit_raw(self.build_submit_min_payload())

    def reply_fast(self, report_id: Any) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Fast one-shot reply check for morning sweeps:
        - POST form to primary
        - if primary unsupported (404/405) OR fatal (401/403), probe alt once
        - fatal only if all HTTP attempts are 401/403
        """
        rid = str(report_id).strip()
        if not rid:
            return False, "report_id is empty", {"id_code": "client_error", "id_message": "empty report_id"}

        attempts: List[TNSResponse] = []
        try:
            attempts.append(self._post_form(self.reply_url, {"api_key": self.api_key, "report_id": rid}, "reply_fast:form(primary)"))
        except Exception as exc:
            attempts.append(self._exc_response("reply_fast:form(primary,exception)", exc))

        if attempts:
            h0 = attempts[0].http
            if h0 in (401, 403, 404, 405):
                try:
                    attempts.append(self._post_form(self.reply_url_alt, {"api_key": self.api_key, "report_id": rid}, "reply_fast:form(alt)"))
                except Exception as exc:
                    attempts.append(self._exc_response("reply_fast:form(alt,exception)", exc))

        http_attempts = [a for a in attempts if a.http != 0]
        if not http_attempts:
            a0 = attempts[0]
            return False, f"preflight=error (no http) ({a0.method}:{a0.id_message})", {"id_code": "exception", "id_message": a0.id_message}

        all_fatal = all(a.http in (401, 403) for a in http_attempts)
        if all_fatal:
            best = http_attempts[0]
            summary = " | ".join([f"{a.method}:{a.http}:{a.id_code}:{a.id_message}" for a in attempts])
            detail = (
                f"fatal=auth via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
                f"id_code={best.id_code} id_message={best.id_message} "
                f"({summary}) [{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
            )
            return False, detail, (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

        best = next((a for a in http_attempts if a.http == 200 and isinstance(a.raw_json, dict)), http_attempts[0])

        if best.http == 404:
            return False, "not_ready http=404", (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

        if best.http != 200:
            detail = f"http={best.http} via={best.method} id_code={best.id_code} id_message={best.id_message}"
            return False, detail, (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

        ok = (str(best.id_code) == "200")
        detail = f"ok={ok} via={best.method} http=200 id_code={best.id_code} id_message={best.id_message}"
        return ok, detail, (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

    def reply(self, report_id: Any, wait_s: int = 600, poll_s: int = 10) -> Tuple[bool, str, Dict[str, Any]]:
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

        deadline = time.time() + max(0, int(wait_s))
        tries = 0

        while True:
            tries += 1
            attempts = one_round(self.reply_url)
            http_attempts0 = [a for a in attempts if a.http != 0]
            all_auth0 = bool(http_attempts0) and all(a.http in (401, 403) for a in http_attempts0)
            if all_auth0:
                attempts += one_round(self.reply_url_alt)

            http_attempts = [a for a in attempts if a.http != 0]
            all_auth = bool(http_attempts) and all(a.http in (401, 403) for a in http_attempts)

            best = next((a for a in attempts if a.http == 200 and isinstance(a.raw_json, dict)), None)
            if best is None:
                non401 = [a for a in attempts if a.http != 401]
                best = non401[0] if non401 else attempts[0]

            if all_auth:
                summary = " | ".join([f"{a.method}:{a.http}:{a.id_code}:{a.id_message}" for a in attempts])
                detail = (
                    f"via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
                    f"id_code={best.id_code} id_message={best.id_message} tries={tries} fatal=auth "
                    f"({summary}) [{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
                )
                return False, detail, (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

            if best.http == 200 and isinstance(best.raw_json, dict):
                ok = str(best.raw_json.get("id_code")) == "200"
                detail = (
                    f"via={best.method} http=200 elapsed_ms={best.elapsed_ms} "
                    f"id_code={best.id_code} id_message={best.id_message} tries={tries} "
                    f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
                )
                return ok, detail, best.raw_json

            if best.http == 404 and time.time() < deadline:
                time.sleep(max(1, int(poll_s)))
                continue

            detail = (
                f"via={best.method} http={best.http} elapsed_ms={best.elapsed_ms} "
                f"id_code={best.id_code} id_message={best.id_message} tries={tries} status=terminal "
                f"[{_stats_api_key(self.api_key)} {_stats_ua(self.user_agent)}]"
            )
            return False, detail, (best.raw_json or {"id_code": best.id_code, "id_message": best.id_message})

    def submit_and_reply(
        self, payload: Dict[str, Any], wait_s: int = 600, poll_s: int = 10
    ) -> Tuple[bool, str, Optional[str], Dict[str, Any]]:
        ok_s, detail_s, report_id, submit_json = self.submit_raw(payload)
        if not ok_s or report_id is None:
            return (
                False,
                f"submit_failed: {detail_s}",
                None,
                (submit_json or {"id_code": "submit_failed", "id_message": "submit failed"}),
            )

        ok_r, detail_r, reply_json = self.reply(report_id=report_id, wait_s=wait_s, poll_s=poll_s)
        objname = _extract_reply_objname(reply_json) if isinstance(reply_json, dict) else None
        return ok_r, f"{detail_s} | reply: {detail_r}", objname, reply_json
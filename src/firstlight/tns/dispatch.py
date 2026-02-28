# src/firstlight/tns/dispatch.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from ..storage.db import DB
from .client import TNSClient


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_auth_fatal(detail: str) -> bool:
    """
    IMPORTANT: be strict to avoid false positives.
    Only treat as fatal if the detail explicitly carries our fatal markers.
    """
    s = str(detail).lower()
    if "fatal=auth" in s:
        return True
    if "preflight=fatal_auth" in s:
        return True
    if "auth_fatal" in s:
        return True
    return False


def _is_transient(detail: str) -> bool:
    s = str(detail).lower()
    transient_tokens = [
        "timeout",
        "timed out",
        "connection",
        "temporarily",
        "temporarily unavailable",
        "bad gateway",
        "gateway",
        "service unavailable",
        "http=500",
        "http=502",
        "http=503",
        "http=504",
    ]
    return any(t in s for t in transient_tokens)


def dispatch_sandbox(
    *,
    db_path: str,
    since_hours: float,
    max_submit: int,
    dry_run: bool,
    topic: Optional[str] = None,
    skip_reply: bool = False,
    wait_s: int = 60,
    poll_s: int = 5,
) -> Dict[str, Any]:
    """
    Dispatch robusto:
      - No bloquea toda la noche esperando reply (wait_s corto + timeout en run_night).
      - Corta en seco si detecta auth fatal REAL (evita spamear 401/403).
      - Si hay fallos transitorios consecutivos, corta temprano (y deja que run_night reintente más tarde).
      - Loguea en tns_actions con action: submitted / failed / failed_auth / skipped.
    """
    client = TNSClient.from_env()
    db = DB(db_path)

    summary: Dict[str, Any] = {
        "candidates": 0,
        "submitted": 0,
        "failed_submit": 0,
        "reply_failed": 0,
        "aborted_auth": False,
        "aborted_transient": False,
        "detail": "",
        "items": [],
    }

    try:
        # Robust preflight: probes reply endpoint (dummy report_id) and only treats 401/403 as fatal.
        ok_auth, detail_auth, _raw_auth = client.test_auth()
        if not ok_auth and _is_auth_fatal(detail_auth):
            summary["aborted_auth"] = True
            summary["detail"] = f"TNS auth preflight looks fatal: {detail_auth}"
            return summary

        since_dt = _utcnow() - timedelta(hours=float(since_hours))
        candidates = db.iter_dispatch_candidates(since_dt=since_dt, max_rows=int(max_submit), topic=topic)
        summary["candidates"] = len(candidates)

        if summary["candidates"] == 0:
            summary["detail"] = "no candidates"
            return summary

        consecutive_transient = 0
        max_consecutive_transient = 2

        for cand in candidates:
            objid = cand.object_id
            candid = str(cand.candid) if cand.candid is not None else None

            if dry_run:
                summary["items"].append(
                    {
                        "objectId": objid,
                        "candid": candid,
                        "score": cand.decision_score,
                        "action": "DRY_RUN",
                        "reason": cand.decision_reason,
                    }
                )
                continue

            # Build payload
            try:
                payload = client.build_at_report_from_fink_payload(cand.alert_json)
            except Exception as e:
                summary["failed_submit"] += 1
                msg = f"build_payload_failed: {e}"
                db.tns_log(
                    action="skipped",
                    object_id=objid,
                    candid=candid,
                    report_id=None,
                    detail=msg,
                    reply_json=None,
                    outcome="skip",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "ok": False, "detail": msg})
                continue

            # Submit
            try:
                ok_s, detail_s, report_id_any, submit_json = client.submit_raw(payload)
            except Exception as e:
                summary["failed_submit"] += 1
                msg = f"submit_exception: {e}"
                db.tns_log(
                    action="failed",
                    object_id=objid,
                    candid=candid,
                    report_id=None,
                    detail=msg,
                    reply_json=None,
                    outcome="fail",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "ok": False, "detail": msg})
                consecutive_transient += 1
                if consecutive_transient >= max_consecutive_transient:
                    summary["aborted_transient"] = True
                    summary["detail"] = "aborted after consecutive transient submit exceptions"
                    break
                continue

            if _is_auth_fatal(str(detail_s)):
                summary["failed_submit"] += 1
                summary["aborted_auth"] = True
                msg = f"auth_fatal: {detail_s}"
                db.tns_log(
                    action="failed_auth",
                    object_id=objid,
                    candid=candid,
                    report_id=None,
                    detail=msg,
                    reply_json=submit_json,
                    outcome="fail",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "ok": False, "detail": msg})
                summary["detail"] = "auth fatal during submit"
                break

            # report_id a int si se puede
            report_id: Optional[int] = None
            try:
                if report_id_any is not None and str(report_id_any).isdigit():
                    report_id = int(str(report_id_any))
            except Exception:
                report_id = None

            if not ok_s or report_id_any is None:
                summary["failed_submit"] += 1
                msg = f"submit_failed: {detail_s}"
                db.tns_log(
                    action="failed",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id,
                    detail=msg,
                    reply_json=submit_json,
                    outcome="fail",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "ok": False, "detail": msg})

                if _is_transient(str(detail_s)):
                    consecutive_transient += 1
                    if consecutive_transient >= max_consecutive_transient:
                        summary["aborted_transient"] = True
                        summary["detail"] = "aborted after consecutive transient submit failures"
                        break
                else:
                    consecutive_transient = 0
                continue

            # Submit OK -> (opcional) reply corto
            if skip_reply:
                summary["submitted"] += 1
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id,
                    detail=f"{detail_s} | reply: skipped",
                    reply_json=submit_json,
                    outcome="ok",
                )
                summary["items"].append(
                    {"objectId": objid, "candid": candid, "ok": True, "report_id": report_id_any, "detail": "reply skipped"}
                )
                consecutive_transient = 0
                continue

            try:
                ok_r, detail_r, reply_json = client.reply(report_id=str(report_id_any), wait_s=int(wait_s), poll_s=int(poll_s))
            except Exception as e:
                summary["reply_failed"] += 1
                summary["submitted"] += 1
                msg = f"{detail_s} | reply_exception: {e}"
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id,
                    detail=msg,
                    reply_json=submit_json,
                    outcome="warn",
                )
                summary["items"].append(
                    {"objectId": objid, "candid": candid, "ok": True, "report_id": report_id_any, "detail": msg}
                )
                consecutive_transient = 0
                continue

            if _is_auth_fatal(str(detail_r)):
                summary["reply_failed"] += 1
                summary["submitted"] += 1
                summary["aborted_auth"] = True
                msg = f"{detail_s} | reply_auth_fatal: {detail_r}"
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id,
                    detail=msg,
                    reply_json=reply_json,
                    outcome="warn",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "ok": True, "report_id": report_id_any, "detail": msg})
                summary["detail"] = "auth fatal during reply"
                break

            summary["submitted"] += 1
            if ok_r:
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id,
                    detail=f"{detail_s} | reply: {detail_r}",
                    reply_json=reply_json,
                    outcome="ok",
                )
            else:
                summary["reply_failed"] += 1
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id,
                    detail=f"{detail_s} | reply: {detail_r}",
                    reply_json=reply_json,
                    outcome="warn",
                )

            summary["items"].append(
                {"objectId": objid, "candid": candid, "ok": True, "report_id": report_id_any, "detail": f"{detail_s} | reply={detail_r}"}
            )
            consecutive_transient = 0

        if not summary["detail"]:
            summary["detail"] = "ok"

        return summary

    finally:
        db.close()

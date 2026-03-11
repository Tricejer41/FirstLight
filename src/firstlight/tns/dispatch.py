# src/firstlight/tns/dispatch.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from typing import Any, Dict, Optional

from ..storage.db import DB
from .client import TNSClient


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _send_enabled() -> bool:
    """Kill-switch: real TNS submit is only allowed when explicitly enabled."""
    v = (os.getenv("FIRSTLIGHT_TNS_SEND", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


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
    max_attempts: Optional[int] = None,
    dry_run: bool,
    print_payload: bool = False,
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
        "submitted_existing": 0,
        "submitted_total": 0,
        "failed_submit": 0,
        "reply_failed": 0,
        "aborted_auth": False,
        "aborted_transient": False,
        "aborted_send_disabled": False,
        "detail": "",
        "items": [],
    }

    try:
        # Kill-switch: do not allow real sending unless explicitly enabled.
        if not dry_run and not _send_enabled():
            summary["aborted_send_disabled"] = True
            summary["detail"] = "send disabled (set FIRSTLIGHT_TNS_SEND=1 to allow real submits)"
            # Persist a trace for audit.
            db.tns_log(
                action="send_disabled",
                object_id="_SYSTEM",
                candid="_SYSTEM",
                report_id=None,
                detail=summary["detail"],
                reply_json=None,
                outcome="skip",
            )
            return summary

        # Robust preflight: probes reply endpoint (dummy report_id) and only treats 401/403 as fatal.
        ok_auth, detail_auth, _raw_auth = client.test_auth()
        if not ok_auth and _is_auth_fatal(detail_auth):
            summary["aborted_auth"] = True
            summary["detail"] = f"TNS auth preflight looks fatal: {detail_auth}"
            return summary

        since_dt = _utcnow() - timedelta(hours=float(since_hours))

        # Hard cap: max_submit counts ONLY successful submits (action='submitted').
        existing_ok = int(db.count_tns_actions("submitted", since_dt=since_dt))
        summary["submitted_existing"] = existing_ok
        submitted_total = existing_ok
        summary["submitted_total"] = submitted_total

        if submitted_total >= int(max_submit):
            summary["detail"] = f"cap reached: already submitted {submitted_total}/{int(max_submit)} in last {since_hours}h"
            db.tns_log(
                action="cap_reached",
                object_id="_SYSTEM",
                candid="_SYSTEM",
                report_id=None,
                detail=summary["detail"],
                reply_json=None,
                outcome="skip",
            )
            return summary

        # Attempt budget (guardrail against spammy failures).
        if max_attempts is None:
            env_attempts = (os.getenv("FIRSTLIGHT_DISPATCH_MAX_ATTEMPTS", "") or "").strip()
            if env_attempts:
                try:
                    max_attempts = int(env_attempts)
                except Exception:
                    max_attempts = None
        if max_attempts is None:
            max_attempts = max(int(max_submit) * 5, int(max_submit))
        max_attempts = max(1, int(max_attempts))

        candidates = list(db.iter_dispatch_candidates(since_dt=since_dt, max_rows=int(max_attempts), topic=topic))
        summary["candidates"] = len(candidates)

        if summary["candidates"] == 0:
            summary["detail"] = "no candidates"
            return summary

        consecutive_transient = 0
        max_consecutive_transient = 2

        attempts_done = 0

        for cand in candidates:
            # Stop once we reach the cap of successful submits.
            if submitted_total >= int(max_submit):
                summary["detail"] = f"cap reached during run: submitted_total={submitted_total}/{int(max_submit)}"
                db.tns_log(
                    action="cap_reached",
                    object_id="_SYSTEM",
                    candid="_SYSTEM",
                    report_id=None,
                    detail=summary["detail"],
                    reply_json=None,
                    outcome="skip",
                )
                break

            attempts_done += 1
            if attempts_done > int(max_attempts):
                summary["detail"] = f"attempt budget reached: attempts={attempts_done-1}/{int(max_attempts)}"
                break

            objid = cand.object_id
            candid = str(cand.candid) if cand.candid is not None else ""
            topic_r = cand.topic

            if dry_run and not print_payload:
                summary["items"].append(
                    {
                        "objectId": objid,
                        "candid": candid,
                        "topic": topic_r,
                        "score": cand.decision_score,
                        "action": "DRY_RUN",
                        "reason": cand.decision_reason,
                    }
                )
                continue

            # Build payload (also for dry-run when print_payload=True)
            try:
                payload = client.build_at_report_from_fink_payload(cand.alert_json)
            except Exception as e:
                summary["failed_submit"] += 1
                msg = f"topic={topic_r} build_payload_failed: {e}"
                db.tns_log(
                    action="skipped",
                    object_id=objid,
                    candid=candid,
                    report_id=None,
                    detail=msg,
                    reply_json=None,
                    outcome="skip",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "topic": topic_r, "ok": False, "detail": msg})
                continue

            if dry_run:
                summary["items"].append(
                    {
                        "objectId": objid,
                        "candid": candid,
                        "topic": topic_r,
                        "score": cand.decision_score,
                        "action": "DRY_RUN",
                        "reason": cand.decision_reason,
                        "payload": payload if print_payload else None,
                    }
                )
                continue

            # Submit
            try:
                ok_s, detail_s, report_id_any, submit_json = client.submit_raw(payload)
            except Exception as e:
                summary["failed_submit"] += 1
                msg = f"topic={topic_r} submit_exception: {e}"
                db.tns_log(
                    action="failed",
                    object_id=objid,
                    candid=candid,
                    report_id=None,
                    detail=msg,
                    reply_json=None,
                    outcome="fail",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "topic": topic_r, "ok": False, "detail": msg})
                consecutive_transient += 1
                if consecutive_transient >= max_consecutive_transient:
                    summary["aborted_transient"] = True
                    summary["detail"] = "aborted after consecutive transient submit exceptions"
                    break
                continue

            if _is_auth_fatal(str(detail_s)):
                summary["failed_submit"] += 1
                summary["aborted_auth"] = True
                msg = f"topic={topic_r} auth_fatal: {detail_s}"
                db.tns_log(
                    action="failed_auth",
                    object_id=objid,
                    candid=candid,
                    report_id=None,
                    detail=msg,
                    reply_json=submit_json,
                    outcome="fail",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "topic": topic_r, "ok": False, "detail": msg})
                summary["detail"] = "auth fatal during submit"
                break

            # Treat report_id as TEXT always (DB stores TEXT)
            report_id_txt: Optional[str] = None
            if report_id_any is not None:
                report_id_txt = str(report_id_any).strip() or None

            if not ok_s or report_id_txt is None:
                summary["failed_submit"] += 1
                msg = f"topic={topic_r} submit_failed: {detail_s}"
                db.tns_log(
                    action="failed",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id_txt,
                    detail=msg,
                    reply_json=submit_json,
                    outcome="fail",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "topic": topic_r, "ok": False, "detail": msg})

                if _is_transient(str(detail_s)):
                    consecutive_transient += 1
                    if consecutive_transient >= max_consecutive_transient:
                        summary["aborted_transient"] = True
                        summary["detail"] = "aborted after consecutive transient submit failures"
                        break
                else:
                    consecutive_transient = 0
                continue

            # Submit OK (counts toward the cap regardless of reply outcome)
            summary["submitted"] += 1
            submitted_total += 1
            summary["submitted_total"] = submitted_total

            # (opcional) reply corto
            if skip_reply:
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id_txt,
                    detail=f"topic={topic_r} {detail_s} | reply: skipped",
                    reply_json=submit_json,
                    outcome="ok",
                )
                summary["items"].append(
                    {"objectId": objid, "candid": candid, "topic": topic_r, "ok": True, "report_id": report_id_txt, "detail": "reply skipped"}
                )
                consecutive_transient = 0
                continue

            try:
                ok_r, detail_r, reply_json = client.reply(report_id=str(report_id_txt), wait_s=int(wait_s), poll_s=int(poll_s))
            except Exception as e:
                summary["reply_failed"] += 1
                msg = f"topic={topic_r} {detail_s} | reply_exception: {e}"
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id_txt,
                    detail=msg,
                    reply_json=submit_json,
                    outcome="warn",
                )
                summary["items"].append(
                    {"objectId": objid, "candid": candid, "topic": topic_r, "ok": True, "report_id": report_id_txt, "detail": msg}
                )
                consecutive_transient = 0
                continue

            if _is_auth_fatal(str(detail_r)):
                summary["reply_failed"] += 1
                summary["aborted_auth"] = True
                msg = f"topic={topic_r} {detail_s} | reply_auth_fatal: {detail_r}"
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id_txt,
                    detail=msg,
                    reply_json=reply_json,
                    outcome="warn",
                )
                summary["items"].append({"objectId": objid, "candid": candid, "topic": topic_r, "ok": True, "report_id": report_id_txt, "detail": msg})
                summary["detail"] = "auth fatal during reply"
                break

            if ok_r:
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id_txt,
                    detail=f"topic={topic_r} {detail_s} | reply: {detail_r}",
                    reply_json=reply_json,
                    outcome="ok",
                )
            else:
                summary["reply_failed"] += 1
                db.tns_log(
                    action="submitted",
                    object_id=objid,
                    candid=candid,
                    report_id=report_id_txt,
                    detail=f"topic={topic_r} {detail_s} | reply: {detail_r}",
                    reply_json=reply_json,
                    outcome="warn",
                )

            summary["items"].append(
                {"objectId": objid, "candid": candid, "topic": topic_r, "ok": True, "report_id": report_id_txt, "detail": f"{detail_s} | reply={detail_r}"}
            )
            consecutive_transient = 0

        if not summary["detail"]:
            summary["detail"] = "ok"

        return summary

    finally:
        db.close()
# src/firstlight/tns/dispatch.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ..storage.db import FirstlightDB
from .client import TNSClient


@dataclass
class DispatchItem:
    object_id: str
    candid: Optional[str]
    jd: Optional[float]
    score: Optional[float]
    alert_json: Dict[str, Any]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dispatch_sandbox(
    db_path: str,
    since_hours: float,
    max_submit: int,
    dry_run: bool,
    wait_s: int = 600,
) -> List[Dict[str, Any]]:
    """
    Devuelve una lista de resultados (dict) para logging/tests.
    """
    client = TNSClient.from_env()
    db = FirstlightDB(db_path)

    # Preflight auth hard-stop on fatal auth
    ok_auth, detail_auth, _raw_auth = client.test_auth()
    if not ok_auth:
        return [{
            "ok": False,
            "action": "ABORT",
            "detail": f"TNS auth preflight failed: {detail_auth}",
        }]

    since_dt = _utcnow() - timedelta(hours=float(since_hours))
    candidates = db.get_dispatch_candidates(since_dt=since_dt, limit=int(max_submit))

    out: List[Dict[str, Any]] = []

    for it in candidates:
        objid = it["objectId"]
        candid = (it.get("candidate", {}) or {}).get("candid")
        score = it.get("_score")

        if dry_run:
            out.append({
                "objectId": objid,
                "candid": candid,
                "score": score,
                "action": "DRY_RUN",
            })
            continue

        payload = client.build_at_report_from_fink_payload(it)

        ok, detail, objname, reply_json = client.submit_and_reply(payload, wait_s=wait_s)

        # Abort quickly if auth is broken mid-run
        if "fatal=auth" in str(detail):
            db.add_tns_action(
                object_id=objid,
                candid=str(candid) if candid is not None else None,
                action="submit_sandbox",
                ok=False,
                detail=f"auth_fatal: {detail}",
                objname=None,
                reply_json=reply_json,
            )
            out.append({
                "objectId": objid,
                "candid": candid,
                "score": score,
                "ok": False,
                "detail": f"AUTH_FATAL: {detail}",
                "objname": None,
            })
            break

        db.add_tns_action(
            object_id=objid,
            candid=str(candid) if candid is not None else None,
            action="submit_sandbox",
            ok=bool(ok),
            detail=str(detail),
            objname=objname,
            reply_json=reply_json,
        )

        out.append({
            "objectId": objid,
            "candid": candid,
            "score": score,
            "ok": ok,
            "detail": detail,
            "objname": objname,
        })

    return out

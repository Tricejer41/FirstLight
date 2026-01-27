# src/firstlight/tns/dispatch.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..storage.db import FirstlightDB
from .client import TNSClient  # tu cliente actual

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
    client = TNSClient.from_env()  # usa TNS_API_URL sandbox en tu .env
    db = FirstlightDB(db_path)

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

        # Construir payload TNS desde el alert Fink
        # Si tu TNSClient tiene build_at_report_from_fink_payload, úsalo:
        payload = client.build_at_report_from_fink_payload(it)

        ok, detail, objname, reply_json = client.submit_and_reply(payload, wait_s=wait_s)

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

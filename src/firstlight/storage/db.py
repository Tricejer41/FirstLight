"""
SQLite storage helpers for FirstLight.

Compat layer:
- Supports legacy schema (payload_json / decided_utc / received_utc / tns_actions.outcome, etc.)
- Supports newer schema (raw_json / created_utc / composite PK style)

Goals:
- Idempotent schema init.
- Backwards compatible with existing scripts (DB.add_alert/add_decision/close).
- Dispatch helpers work even if DB was created with old schema.

IMPORTANT stability rule:
- Only successful submissions should block re-dispatch.
- "skipped" used for transient failures must NOT permanently block candidates.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().replace(microsecond=0).isoformat()


def _is_legacy_epoch(s: Optional[str]) -> bool:
    if not s:
        return True
    return s.startswith("1970-01-01T00:00:00")


def _safe_json_loads(s: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(s, str) or not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


@dataclass(frozen=True)
class DispatchCandidate:
    object_id: str
    candid: str
    topic: str
    alert_json: Dict[str, Any]
    decision_reason: str
    decision_metrics: Dict[str, Any]
    decision_created_utc: str

    @property
    def decision_score(self) -> Optional[float]:
        """
        Compatibility helper:
        - Older / other modules may expect a .decision_score attribute.
        - We derive it from decision_metrics if present, otherwise None.
        """
        if not isinstance(self.decision_metrics, dict):
            return None

        for k in ("decision_score", "score", "rank_score", "decisionScore"):
            if k in self.decision_metrics:
                v = self.decision_metrics.get(k)
                try:
                    return None if v is None else float(v)
                except Exception:
                    return None
        return None


class DB:
    """
    Thin SQLite wrapper.

    Notes:
    - object_id maps to Fink payload["objectId"]
    - candid maps to payload["candidate"]["candid"] (stringified)
    - topic is the filtering topic (e.g. "n1" or "hostless_ztf")
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._con = sqlite3.connect(self.db_path)
        self._con.row_factory = sqlite3.Row
        self._con.execute("PRAGMA journal_mode=WAL;")
        self._con.execute("PRAGMA foreign_keys=ON;")

        # Detected column names (set in _init_schema)
        self._alerts_json_col = "raw_json"         # fallback to payload_json
        self._decisions_time_col = "created_utc"   # fallback to decided_utc
        self._tns_has_outcome = False
        self._tns_has_reply_json = False
        self._tns_has_report_id = False
        self._init_schema()

    # -------------------------
    # Introspection helpers
    # -------------------------

    def _table_info(self, table: str) -> List[Tuple[Any, ...]]:
        try:
            return self._con.execute(f"PRAGMA table_info({table})").fetchall()
        except Exception:
            return []

    def _has_table(self, table: str) -> bool:
        row = self._con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None

    def _has_column(self, table: str, col: str) -> bool:
        info = self._table_info(table)
        for r in info:
            if len(r) >= 2 and r[1] == col:
                return True
        return False

    def _try_exec(self, sql: str) -> None:
        try:
            self._con.execute(sql)
        except Exception:
            pass

    # -------------------------
    # Schema / migrations
    # -------------------------

    def _init_schema(self) -> None:
        cur = self._con.cursor()

        if not self._has_table("alerts"):
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                  object_id      TEXT NOT NULL,
                  candid         TEXT NOT NULL,
                  topic          TEXT NOT NULL,
                  raw_json       TEXT NOT NULL,
                  created_utc    TEXT NOT NULL,
                  PRIMARY KEY (object_id, candid, topic)
                );
                """
            )

        if not self._has_table("decisions"):
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS decisions (
                  object_id      TEXT NOT NULL,
                  candid         TEXT NOT NULL,
                  topic          TEXT NOT NULL,
                  passed         INTEGER NOT NULL,
                  reason         TEXT NOT NULL,
                  metrics_json   TEXT NOT NULL,
                  created_utc    TEXT NOT NULL,
                  PRIMARY KEY (object_id, candid, topic)
                );
                """
            )

        if not self._has_table("tns_actions"):
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tns_actions (
                  id             INTEGER PRIMARY KEY AUTOINCREMENT,
                  object_id      TEXT NOT NULL,
                  candid         TEXT NOT NULL,
                  action         TEXT NOT NULL,         -- "submitted" | "failed" | ...
                  report_id      TEXT,
                  detail         TEXT NOT NULL,
                  reply_json     TEXT,
                  created_utc    TEXT NOT NULL
                );
                """
            )

        self._con.commit()

        # alerts: prefer raw_json, fallback to payload_json
        alerts_has_raw = self._has_column("alerts", "raw_json")
        alerts_has_payload = self._has_column("alerts", "payload_json")

        if (not alerts_has_raw) and alerts_has_payload:
            self._try_exec("ALTER TABLE alerts ADD COLUMN raw_json TEXT;")
            self._try_exec("UPDATE alerts SET raw_json = payload_json WHERE raw_json IS NULL;")
            alerts_has_raw = self._has_column("alerts", "raw_json")

        self._alerts_json_col = "raw_json" if alerts_has_raw else ("payload_json" if alerts_has_payload else "raw_json")

        # decisions: prefer created_utc if usable, else decided_utc
        decisions_has_created = self._has_column("decisions", "created_utc")
        decisions_has_decided = self._has_column("decisions", "decided_utc")

        if decisions_has_created and decisions_has_decided:
            row = self._con.execute("SELECT MIN(created_utc) AS mn, MAX(created_utc) AS mx FROM decisions").fetchone()
            mn = (row["mn"] if row else None)
            mx = (row["mx"] if row else None)
            if _is_legacy_epoch(mn) and _is_legacy_epoch(mx):
                self._try_exec(
                    "UPDATE decisions SET created_utc = decided_utc WHERE created_utc LIKE '1970-01-01T00:00:00%';"
                )

        if decisions_has_decided:
            row2 = self._con.execute("SELECT MIN(created_utc) AS mn, MAX(created_utc) AS mx FROM decisions").fetchone()
            mn2 = (row2["mn"] if row2 else None)
            mx2 = (row2["mx"] if row2 else None)
            if decisions_has_created and (not (_is_legacy_epoch(mn2) and _is_legacy_epoch(mx2))):
                self._decisions_time_col = "created_utc"
            else:
                self._decisions_time_col = "decided_utc"
        else:
            self._decisions_time_col = "created_utc" if decisions_has_created else "created_utc"

        self._tns_has_outcome = self._has_column("tns_actions", "outcome")
        self._tns_has_reply_json = self._has_column("tns_actions", "reply_json") or self._has_column("tns_actions", "detail_json")
        self._tns_has_report_id = self._has_column("tns_actions", "report_id")

        self._try_exec("CREATE INDEX IF NOT EXISTS idx_decisions_passed_created ON decisions(passed, created_utc);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_decisions_passed_decided ON decisions(passed, decided_utc);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_alerts_obj_cand_topic ON alerts(object_id, candid, topic);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_tns_actions_obj_cand ON tns_actions(object_id, candid);")

        # Production-friendly indexes (sweep replies / health checks)
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_tns_actions_created ON tns_actions(created_utc);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_tns_actions_action_created ON tns_actions(action, created_utc);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_tns_actions_report_id ON tns_actions(report_id);")

        self._con.commit()

    # -------------------------
    # Writes (ingest / decisions / logs)
    # -------------------------

    def add_alert(self, object_id: str, candid: str, topic: str, raw_json: Dict[str, Any]) -> None:
        now = _utcnow_iso()
        js = json.dumps(raw_json, separators=(",", ":"), sort_keys=False)

        if self._has_column("alerts", "payload_json"):
            cur = self._con.execute(
                "UPDATE alerts SET payload_json=?, created_utc=? WHERE object_id=? AND candid=? AND topic=?",
                (js, now, str(object_id), str(candid), str(topic)),
            )
            if cur.rowcount == 0:
                emitted_jd = 0.0
                received_utc = now
                self._con.execute(
                    "INSERT INTO alerts(object_id, candid, topic, emitted_jd, received_utc, payload_json, created_utc) VALUES(?,?,?,?,?,?,?)",
                    (str(object_id), str(candid), str(topic), float(emitted_jd), str(received_utc), js, now),
                )
        else:
            self._con.execute(
                """
                INSERT INTO alerts(object_id, candid, topic, raw_json, created_utc)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(object_id, candid, topic)
                DO UPDATE SET raw_json=excluded.raw_json, created_utc=excluded.created_utc
                """,
                (str(object_id), str(candid), str(topic), js, now),
            )

        if self._has_column("alerts", "raw_json"):
            self._con.execute(
                "UPDATE alerts SET raw_json=? WHERE object_id=? AND candid=? AND topic=?",
                (js, str(object_id), str(candid), str(topic)),
            )

        self._con.commit()

    def add_decision(
        self,
        object_id: str,
        candid: str,
        topic: str,
        passed: bool,
        reason: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = _utcnow_iso()
        metrics = metrics or {}
        mjs = json.dumps(metrics, separators=(",", ":"), sort_keys=False)

        if self._has_column("decisions", "decided_utc"):
            cur = self._con.execute(
                "UPDATE decisions SET decided_utc=?, passed=?, reason=?, metrics_json=?, created_utc=? WHERE object_id=? AND candid=? AND topic=?",
                (now, 1 if passed else 0, str(reason), mjs, now, str(object_id), str(candid), str(topic)),
            )
            if cur.rowcount == 0:
                self._con.execute(
                    "INSERT INTO decisions(object_id, candid, topic, decided_utc, passed, reason, metrics_json, created_utc) VALUES(?,?,?,?,?,?,?,?)",
                    (str(object_id), str(candid), str(topic), now, 1 if passed else 0, str(reason), mjs, now),
                )
        else:
            self._con.execute(
                """
                INSERT INTO decisions(object_id, candid, topic, passed, reason, metrics_json, created_utc)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(object_id, candid, topic)
                DO UPDATE SET
                  passed=excluded.passed,
                  reason=excluded.reason,
                  metrics_json=excluded.metrics_json,
                  created_utc=excluded.created_utc
                """,
                (str(object_id), str(candid), str(topic), 1 if passed else 0, str(reason), mjs, now),
            )

        self._con.commit()

    def tns_log(
        self,
        action: str,
        object_id: str,
        candid: str,
        report_id: Optional[Any],
        detail: str,
        reply_json: Optional[Dict[str, Any]] = None,
        outcome: Optional[str] = None,
    ) -> None:
        now = _utcnow_iso()
        rj = None if reply_json is None else json.dumps(reply_json, separators=(",", ":"), sort_keys=False)

        if self._has_column("tns_actions", "action_utc"):
            self._con.execute(
                """
                INSERT INTO tns_actions(object_id, candid, action_utc, action, outcome, detail, detail_json, created_utc)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(object_id),
                    str(candid),
                    now,
                    str(action),
                    str(outcome or ""),
                    str(detail),
                    rj,
                    now,
                ),
            )
        else:
            self._con.execute(
                """
                INSERT INTO tns_actions(object_id, candid, action, report_id, detail, reply_json, created_utc)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(object_id),
                    str(candid),
                    str(action),
                    (None if report_id is None else str(report_id)),
                    str(detail),
                    rj,
                    now,
                ),
            )

        self._con.commit()

    # -------------------------
    # Reads (dispatch helpers)
    # -------------------------

    def count_tns_actions(self, action: str, since_dt: datetime) -> int:
        """Count rows in tns_actions for a given action since a UTC datetime."""
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
        since_iso = since_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

        row = self._con.execute(
            "SELECT COUNT(*) AS n FROM tns_actions WHERE action=? AND created_utc>=?",
            (str(action), since_iso),
        ).fetchone()
        try:
            return int(row["n"]) if row is not None else 0
        except Exception:
            return 0

    def was_submitted_or_skipped(self, object_id: str, candid: str) -> bool:
        """
        Stability fix:
        - ONLY treat successful submission as 'handled'.
        - Do NOT treat "skipped"/"failed" as handled; those must be retryable after auth/code fixes.

        If you ever want a permanent skip, use action='skipped_permanent' (optional).
        """
        obj = str(object_id)
        cand = str(candid)

        clauses = ["(action IN ('submitted','skipped_permanent'))"]
        if self._tns_has_outcome:
            clauses.append("(outcome IN ('submitted','skipped_permanent'))")

        where_extra = " OR ".join(clauses)
        row = self._con.execute(
            f"""
            SELECT 1 FROM tns_actions
            WHERE object_id=? AND candid=? AND ({where_extra})
            LIMIT 1
            """,
            (obj, cand),
        ).fetchone()
        return row is not None

    def _get_alert_json(self, object_id: str, candid: str, topic: str) -> Optional[Dict[str, Any]]:
        col = self._alerts_json_col
        if not self._has_column("alerts", col):
            col = "payload_json" if self._has_column("alerts", "payload_json") else col

        row = self._con.execute(
            f"SELECT {col} AS j FROM alerts WHERE object_id=? AND candid=? AND topic=? LIMIT 1",
            (str(object_id), str(candid), str(topic)),
        ).fetchone()
        if not row:
            return None
        return _safe_json_loads(row["j"])

    def iter_dispatch_candidates(
        self,
        since_hours: Optional[float] = None,
        max_rows: int = 1000,
        topic: Optional[str] = None,
        *,
        since_dt: Optional[datetime] = None,
    ) -> List[DispatchCandidate]:
        """
        Returns candidates that:
          - have passed decision (passed=1)
          - are newer than the given lower bound (since_dt or since_hours)
          - have not been successfully submitted (or permanently skipped)

        Backward compatibility:
          - Old callers can keep using (since_hours, max_rows, topic)
          - New callers may use since_dt=... with max_rows=...
        """
        if since_dt is not None and since_hours is not None:
            raise ValueError("Use either since_dt or since_hours, not both")

        if since_dt is None:
            if since_hours is None:
                raise ValueError("iter_dispatch_candidates requires since_dt or since_hours")
            since_dt = _utcnow() - timedelta(hours=float(since_hours))

        # Normalize tz / precision
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)

        since_iso = since_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

        time_col = self._decisions_time_col
        if not self._has_column("decisions", time_col):
            time_col = "created_utc" if self._has_column("decisions", "created_utc") else "decided_utc"

        params: List[Any] = [since_iso]
        topic_sql = ""
        if topic:
            topic_sql = " AND d.topic=? "
            params.append(str(topic))

        rows = self._con.execute(
            f"""
            SELECT d.object_id, d.candid, d.topic, d.reason, d.metrics_json, d.{time_col} AS decision_time
            FROM decisions d
            WHERE d.passed=1
              AND d.{time_col} >= ?
              {topic_sql}
            ORDER BY d.{time_col} DESC
            LIMIT ?
            """,
            (*params, int(max_rows) * 10),
        ).fetchall()

        out: List[DispatchCandidate] = []
        seen: set[Tuple[str, str, str]] = set()

        for r in rows:
            object_id = str(r["object_id"])
            candid = str(r["candid"])
            topic_r = str(r["topic"])
            key = (object_id, candid, topic_r)
            if key in seen:
                continue
            seen.add(key)

            if self.was_submitted_or_skipped(object_id, candid):
                continue

            alert_json = self._get_alert_json(object_id, candid, topic_r)
            if not alert_json:
                continue

            try:
                metrics = json.loads(r["metrics_json"]) if r["metrics_json"] else {}
                if not isinstance(metrics, dict):
                    metrics = {}
            except Exception:
                metrics = {}

            out.append(
                DispatchCandidate(
                    object_id=object_id,
                    candid=candid,
                    topic=topic_r,
                    alert_json=alert_json,
                    decision_reason=str(r["reason"]),
                    decision_metrics=metrics,
                    decision_created_utc=str(r["decision_time"]),
                )
            )
            if len(out) >= int(max_rows):
                break

        return out

    # -------------------------
    # Lifecycle
    # -------------------------

    def close(self) -> None:
        try:
            self._con.commit()
        finally:
            self._con.close()


AlertDB = DB


class FirstlightDB(DB):
    """Backward-compatible alias of DB with a couple of convenience wrappers."""

    def get_dispatch_candidates(self, since_dt: datetime, limit: int) -> List[Dict[str, Any]]:
        """Return raw Fink payload dicts for dispatch."""
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)

        since_hours = max(0.0, (_utcnow() - since_dt).total_seconds() / 3600.0)
        cands = list(self.iter_dispatch_candidates(since_hours=since_hours, max_rows=int(limit)))
        return [c.alert_json for c in cands]

    def add_tns_action(
        self,
        object_id: str,
        candid: Optional[str],
        action: str,
        ok: bool,
        detail: str,
        objname: Optional[str] = None,
        reply_json: Optional[Dict[str, Any]] = None,
        report_id: Optional[int] = None,
    ) -> None:
        """Compatibility wrapper for logging TNS actions."""
        outcome = "ok" if ok else "failed"
        detail2 = detail
        if objname:
            detail2 = f"{detail} | objname={objname}"

        self.tns_log(
            action=action,
            object_id=object_id,
            candid=candid,
            report_id=report_id,
            detail=detail2,
            reply_json=reply_json,
            outcome=outcome,
        )
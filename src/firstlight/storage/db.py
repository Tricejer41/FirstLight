"""
SQLite storage helpers for FirstLight.

Compat layer:
- Supports legacy schema (payload_json / decided_utc / received_utc / tns_actions.outcome, etc.)
- Supports newer schema (raw_json / created_utc / composite PK style)

Goals:
- Idempotent schema init.
- Backwards compatible with existing scripts (DB.add_alert/add_decision/close).
- Dispatch helpers work even if DB was created with old schema.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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
            # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
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

        # Create tables if missing (new schema)
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
                  action         TEXT NOT NULL,         -- "submitted" | "skipped" | ...
                  report_id      TEXT,
                  detail         TEXT NOT NULL,
                  reply_json     TEXT,
                  created_utc    TEXT NOT NULL
                );
                """
            )

        self._con.commit()

        # Detect legacy columns and migrate minimally (non-destructive)

        # alerts: prefer raw_json, fallback to payload_json
        alerts_has_raw = self._has_column("alerts", "raw_json")
        alerts_has_payload = self._has_column("alerts", "payload_json")

        if (not alerts_has_raw) and alerts_has_payload:
            # add raw_json nullable and backfill from payload_json
            self._try_exec("ALTER TABLE alerts ADD COLUMN raw_json TEXT;")
            self._try_exec("UPDATE alerts SET raw_json = payload_json WHERE raw_json IS NULL;")
            alerts_has_raw = self._has_column("alerts", "raw_json")

        self._alerts_json_col = "raw_json" if alerts_has_raw else ("payload_json" if alerts_has_payload else "raw_json")

        # decisions: prefer created_utc if usable, else decided_utc
        decisions_has_created = self._has_column("decisions", "created_utc")
        decisions_has_decided = self._has_column("decisions", "decided_utc")

        # If created_utc exists but is all 1970, and decided_utc exists, backfill created_utc from decided_utc
        if decisions_has_created and decisions_has_decided:
            row = self._con.execute("SELECT MIN(created_utc) AS mn, MAX(created_utc) AS mx FROM decisions").fetchone()
            mn = (row["mn"] if row else None)
            mx = (row["mx"] if row else None)
            if _is_legacy_epoch(mn) and _is_legacy_epoch(mx):
                # Backfill (best effort)
                self._try_exec("UPDATE decisions SET created_utc = decided_utc WHERE created_utc LIKE '1970-01-01T00:00:00%';")

        # choose time column for dispatch
        if decisions_has_decided:
            # Prefer decided_utc if created_utc still looks legacy
            row2 = self._con.execute("SELECT MIN(created_utc) AS mn, MAX(created_utc) AS mx FROM decisions").fetchone()
            mn2 = (row2["mn"] if row2 else None)
            mx2 = (row2["mx"] if row2 else None)
            if decisions_has_created and (not (_is_legacy_epoch(mn2) and _is_legacy_epoch(mx2))):
                self._decisions_time_col = "created_utc"
            else:
                self._decisions_time_col = "decided_utc"
        else:
            self._decisions_time_col = "created_utc" if decisions_has_created else "created_utc"

        # tns_actions schema detection
        self._tns_has_outcome = self._has_column("tns_actions", "outcome")
        self._tns_has_reply_json = self._has_column("tns_actions", "reply_json") or self._has_column("tns_actions", "detail_json")
        self._tns_has_report_id = self._has_column("tns_actions", "report_id")

        # Add missing helpful indexes (best-effort; ignore failures)
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_decisions_passed_created ON decisions(passed, created_utc);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_decisions_passed_decided ON decisions(passed, decided_utc);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_alerts_obj_cand_topic ON alerts(object_id, candid, topic);")
        self._try_exec("CREATE INDEX IF NOT EXISTS idx_tns_actions_obj_cand ON tns_actions(object_id, candid);")

        self._con.commit()

    # -------------------------
    # Writes (ingest / decisions / logs)
    # -------------------------

    def add_alert(self, object_id: str, candid: str, topic: str, raw_json: Dict[str, Any]) -> None:
        """
        Store raw alert JSON. Works for legacy DB too (writes into payload_json if needed).
        """
        now = _utcnow_iso()
        js = json.dumps(raw_json, separators=(",", ":"), sort_keys=False)

        # legacy schema has id PK; we insert a row (and also try to update latest if possible)
        if self._has_column("alerts", "payload_json"):
            # Prefer UPDATE then INSERT if no row
            cur = self._con.execute(
                "UPDATE alerts SET payload_json=?, created_utc=? WHERE object_id=? AND candid=? AND topic=?",
                (js, now, str(object_id), str(candid), str(topic)),
            )
            if cur.rowcount == 0:
                # emitted_jd/received_utc are NOT known here -> best effort
                emitted_jd = 0.0
                received_utc = now
                self._con.execute(
                    "INSERT INTO alerts(object_id, candid, topic, emitted_jd, received_utc, payload_json, created_utc) VALUES(?,?,?,?,?,?,?)",
                    (str(object_id), str(candid), str(topic), float(emitted_jd), str(received_utc), js, now),
                )
        else:
            # new schema
            self._con.execute(
                """
                INSERT INTO alerts(object_id, candid, topic, raw_json, created_utc)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(object_id, candid, topic)
                DO UPDATE SET raw_json=excluded.raw_json, created_utc=excluded.created_utc
                """,
                (str(object_id), str(candid), str(topic), js, now),
            )

        # if we have raw_json col as well, keep it in sync
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
        """
        Store decision. For legacy DB: writes decided_utc + created_utc.
        """
        now = _utcnow_iso()
        metrics = metrics or {}
        mjs = json.dumps(metrics, separators=(",", ":"), sort_keys=False)

        if self._has_column("decisions", "decided_utc"):
            # legacy
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
            # new schema
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
        """
        Record an action taken with TNS for a candidate.
        Compatible with legacy tns_actions schema too.
        """
        now = _utcnow_iso()
        rj = None if reply_json is None else json.dumps(reply_json, separators=(",", ":"), sort_keys=False)

        if self._has_column("tns_actions", "action_utc"):
            # legacy schema: (object_id,candid,action_utc,action,outcome,detail,detail_json,created_utc)
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
            # new schema
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

    def was_submitted_or_skipped(self, object_id: str, candid: str) -> bool:
        """
        True if an action already exists that indicates we handled this candidate.
        Works with both schemas:
        - new: action in ('submitted','skipped')
        - legacy: action or outcome may carry those strings (depends on your older code)
        """
        obj = str(object_id)
        cand = str(candid)

        clauses = ["(action IN ('submitted','skipped'))"]
        if self._tns_has_outcome:
            clauses.append("(outcome IN ('submitted','skipped'))")

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
        # defensive: if selected column is missing, try payload_json
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
        since_hours: float,
        max_rows: int,
        topic: Optional[str] = None,
    ) -> List[DispatchCandidate]:
        """
        Return candidates that:
        - have a decision passed=1 within the time window (uses decided_utc on legacy DB)
        - have an alert JSON stored for the same (object_id,candid,topic)
        - have NOT already been submitted/skipped
        """
        since_dt = datetime.now(timezone.utc) - timedelta(hours=float(since_hours))
        since_iso = since_dt.replace(microsecond=0).isoformat()

        time_col = self._decisions_time_col
        if not self._has_column("decisions", time_col):
            # last fallback
            time_col = "created_utc" if self._has_column("decisions", "created_utc") else "decided_utc"

        params: List[Any] = [since_iso]
        topic_sql = ""
        if topic:
            topic_sql = " AND d.topic=? "
            params.append(str(topic))

        # We may have duplicates in legacy DB; order by time desc and de-dup in Python.
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
            (*params, int(max_rows) * 10),  # fetch extra; we'll de-dup + filter
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


# Backwards-compatible alias
AlertDB = DB

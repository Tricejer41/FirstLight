from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any
import json
import datetime as dt

SCHEMA = r'''
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_id TEXT NOT NULL,
  candid TEXT NOT NULL,
  topic TEXT NOT NULL,
  emitted_jd REAL NOT NULL,
  received_utc TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_objectid ON alerts(object_id);
CREATE INDEX IF NOT EXISTS idx_alerts_emitted ON alerts(emitted_jd);

CREATE TABLE IF NOT EXISTS decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_id TEXT NOT NULL,
  candid TEXT NOT NULL,
  topic TEXT NOT NULL,
  decided_utc TEXT NOT NULL,
  passed INTEGER NOT NULL,
  reason TEXT NOT NULL,
  metrics_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decisions_objectid ON decisions(object_id);
CREATE INDEX IF NOT EXISTS idx_decisions_passed ON decisions(passed);

CREATE TABLE IF NOT EXISTS tns_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_id TEXT NOT NULL,
  candid TEXT NOT NULL,
  action_utc TEXT NOT NULL,
  action TEXT NOT NULL,              -- 'check' | 'submit'
  outcome TEXT NOT NULL,             -- 'skip' | 'ok' | 'error'
  detail TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tns_objectid ON tns_actions(object_id);
'''

class DB:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(str(path))
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def add_alert(self, object_id: str, candid: str, topic: str, emitted_jd: float, received_utc: str, payload: Dict[str, Any]):
        self.conn.execute(
            "INSERT INTO alerts(object_id,candid,topic,emitted_jd,received_utc,payload_json) VALUES (?,?,?,?,?,?)",
            (object_id, candid, topic, float(emitted_jd), received_utc, json.dumps(payload, separators=(",",":")))
        )
        self.conn.commit()

    def add_decision(self, object_id: str, candid: str, topic: str, passed: bool, reason: str, metrics: Dict[str, Any]):
        self.conn.execute(
            "INSERT INTO decisions(object_id,candid,topic,decided_utc,passed,reason,metrics_json) VALUES (?,?,?,?,?,?,?)",
            (object_id, candid, topic, dt.datetime.now(dt.timezone.utc).isoformat(), 1 if passed else 0, reason, json.dumps(metrics, separators=(",",":")))
        )
        self.conn.commit()

    def tns_log(self, object_id: str, candid: str, action: str, outcome: str, detail: str):
        self.conn.execute(
            "INSERT INTO tns_actions(object_id,candid,action_utc,action,outcome,detail) VALUES (?,?,?,?,?,?)",
            (object_id, candid, dt.datetime.now(dt.timezone.utc).isoformat(), action, outcome, detail)
        )
        self.conn.commit()

    def was_submitted_or_skipped(self, object_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM tns_actions WHERE object_id=? AND action='submit' LIMIT 1",
            (object_id,)
        )
        return cur.fetchone() is not None

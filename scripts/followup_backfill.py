# scripts/followup_backfill.py
from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path


TOPIC_RE = re.compile(r"topic=([^\s|]+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill follow-up tables from existing tns_actions submitted rows."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite DB (development DB recommended).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write changes; only print what would be done.",
    )
    return parser.parse_args()


def extract_topic(detail: str | None) -> str:
    if not detail:
        return "unknown"
    match = TOPIC_RE.search(detail)
    if match:
        return match.group(1)
    return "unknown"


def ensure_tables_exist(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {
        "tns_report_state",
        "followup_queue",
    }
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('tns_report_state', 'followup_queue')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(
            f"Missing required tables: {sorted(missing)}. "
            f"Apply sql/001_followup_schema.sql first."
        )


def fetch_submitted_rows(con: sqlite3.Connection) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT object_id, candid, action, report_id, detail, reply_json, created_utc
        FROM tns_actions
        WHERE action = 'submitted'
          AND report_id IS NOT NULL
          AND object_id != '_SYSTEM'
        ORDER BY created_utc ASC
        """
    ).fetchall()
    return rows


def upsert_tns_report_state(con: sqlite3.Connection, row: sqlite3.Row) -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO tns_report_state (
            report_id,
            object_id,
            candid,
            submitted_utc,
            submit_status,
            tns_name,
            public_utc,
            reply_status,
            classification_status,
            tns_url,
            certificate_url,
            last_checked_utc,
            raw_reply_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(report_id) DO UPDATE SET
            object_id = excluded.object_id,
            candid = excluded.candid,
            submitted_utc = excluded.submitted_utc,
            submit_status = excluded.submit_status
        """,
        (
            row["report_id"],
            row["object_id"],
            row["candid"],
            row["created_utc"],
            "submitted",
            None,
            None,
            "pending",
            "unknown",
            None,
            None,
            None,
            row["reply_json"],
        ),
    )


def upsert_followup_queue(con: sqlite3.Connection, row: sqlite3.Row) -> None:
    cur = con.cursor()
    topic = extract_topic(row["detail"])
    cur.execute(
        """
        INSERT INTO followup_queue (
            object_id,
            candid,
            report_id,
            tns_name,
            topic,
            submitted_utc,
            status,
            priority_bucket,
            followup_owner,
            current_score,
            best_score,
            last_score_utc,
            promotion_triggered,
            promotion_utc,
            promotion_reason,
            dropped_reason,
            external_classification,
            external_classification_label,
            next_review_utc,
            last_review_utc,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(object_id, candid) DO UPDATE SET
            report_id = COALESCE(followup_queue.report_id, excluded.report_id),
            tns_name = COALESCE(followup_queue.tns_name, excluded.tns_name),
            topic = excluded.topic,
            submitted_utc = excluded.submitted_utc
        """,
        (
            row["object_id"],
            row["candid"],
            row["report_id"],
            None,
            topic,
            row["created_utc"],
            "watch",
            "normal",
            None,
            None,
            None,
            None,
            0,
            None,
            None,
            None,
            0,
            None,
            row["created_utc"],
            None,
            "backfilled from tns_actions submitted",
        ),
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    con = sqlite3.connect(db_path)
    try:
        ensure_tables_exist(con)
        rows = fetch_submitted_rows(con)

        print(f"submitted_rows_found={len(rows)}")

        if args.dry_run:
            preview = rows[:10]
            for r in preview:
                print(
                    f"DRY-RUN object_id={r['object_id']} candid={r['candid']} "
                    f"report_id={r['report_id']} created_utc={r['created_utc']} "
                    f"topic={extract_topic(r['detail'])}"
                )
            print("dry_run=True -> no changes written")
            return 0

        for row in rows:
            upsert_tns_report_state(con, row)
            upsert_followup_queue(con, row)

        con.commit()

        cur = con.cursor()
        tns_count = cur.execute("SELECT COUNT(*) FROM tns_report_state").fetchone()[0]
        queue_count = cur.execute("SELECT COUNT(*) FROM followup_queue").fetchone()[0]

        print(f"tns_report_state_rows={tns_count}")
        print(f"followup_queue_rows={queue_count}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
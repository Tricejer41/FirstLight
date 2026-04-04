# scripts/followup_daily_refresh.py
from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily refresh of production follow-up mirror DB."
    )
    parser.add_argument("--source-db", required=True, help="Operational production DB (read-only source).")
    parser.add_argument("--target-db", required=True, help="Follow-up mirror DB (read/write target).")
    parser.add_argument("--dry-run", action="store_true", help="Do not write changes; only report what would happen.")
    return parser.parse_args()


def connect_readonly(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def connect_rw(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path)


def ensure_source_tables(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {"alerts", "decisions", "tns_actions"}
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('alerts', 'decisions', 'tns_actions')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(f"Source DB missing tables: {sorted(missing)}")


def ensure_target_tables(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {
        "alerts",
        "decisions",
        "tns_actions",
        "tns_report_state",
        "followup_queue",
        "followup_score_history",
        "followup_actions",
        "followup_observations",
    }
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN (
            'alerts',
            'decisions',
            'tns_actions',
            'tns_report_state',
            'followup_queue',
            'followup_score_history',
            'followup_actions',
            'followup_observations'
          )
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(
            f"Target DB missing tables: {sorted(missing)}. "
            "Bootstrap target DB first."
        )


def sync_alerts(source_con: sqlite3.Connection, target_con: sqlite3.Connection, dry_run: bool) -> int:
    s_cur = source_con.cursor()
    t_cur = target_con.cursor()

    source_rows = s_cur.execute(
        """
        SELECT object_id, candid, topic, raw_json, created_utc
        FROM alerts
        """
    ).fetchall()

    target_keys = set(
        t_cur.execute(
            """
            SELECT object_id, candid, topic
            FROM alerts
            """
        ).fetchall()
    )

    missing = [row for row in source_rows if (row[0], row[1], row[2]) not in target_keys]

    if not dry_run and missing:
        t_cur.executemany(
            """
            INSERT INTO alerts (object_id, candid, topic, raw_json, created_utc)
            VALUES (?, ?, ?, ?, ?)
            """,
            missing,
        )

    return len(missing)


def sync_decisions(source_con: sqlite3.Connection, target_con: sqlite3.Connection, dry_run: bool) -> int:
    s_cur = source_con.cursor()
    t_cur = target_con.cursor()

    source_rows = s_cur.execute(
        """
        SELECT object_id, candid, topic, passed, reason, metrics_json, created_utc
        FROM decisions
        """
    ).fetchall()

    target_keys = set(
        t_cur.execute(
            """
            SELECT object_id, candid, topic
            FROM decisions
            """
        ).fetchall()
    )

    missing = [row for row in source_rows if (row[0], row[1], row[2]) not in target_keys]

    if not dry_run and missing:
        t_cur.executemany(
            """
            INSERT INTO decisions (object_id, candid, topic, passed, reason, metrics_json, created_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            missing,
        )

    return len(missing)


def sync_tns_actions(source_con: sqlite3.Connection, target_con: sqlite3.Connection, dry_run: bool) -> int:
    s_cur = source_con.cursor()
    t_cur = target_con.cursor()

    source_rows = s_cur.execute(
        """
        SELECT object_id, candid, action, report_id, detail, reply_json, created_utc
        FROM tns_actions
        """
    ).fetchall()

    target_keys = set(
        t_cur.execute(
            """
            SELECT object_id, candid, action, report_id, detail, reply_json, created_utc
            FROM tns_actions
            """
        ).fetchall()
    )

    missing = [row for row in source_rows if row not in target_keys]

    if not dry_run and missing:
        t_cur.executemany(
            """
            INSERT INTO tns_actions (object_id, candid, action, report_id, detail, reply_json, created_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            missing,
        )

    return len(missing)


def run_child(script_name: str, target_db: Path, dry_run: bool) -> None:
    cmd = [sys.executable, str(Path("scripts") / script_name), "--db", str(target_db)]
    if dry_run:
        cmd.append("--dry-run")

    print(f"\n[RUN] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def print_target_summary(target_con: sqlite3.Connection) -> None:
    cur = target_con.cursor()

    print("\n=== TARGET FOLLOW-UP SUMMARY ===")
    print("alerts =", cur.execute("SELECT COUNT(*) FROM alerts").fetchone()[0])
    print("decisions =", cur.execute("SELECT COUNT(*) FROM decisions").fetchone()[0])
    print("tns_actions =", cur.execute("SELECT COUNT(*) FROM tns_actions").fetchone()[0])
    print("followup_queue =", cur.execute("SELECT COUNT(*) FROM followup_queue").fetchone()[0])
    print("score_history =", cur.execute("SELECT COUNT(*) FROM followup_score_history").fetchone()[0])
    print(
        "status_counts =",
        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM followup_queue
            GROUP BY status
            ORDER BY status
            """
        ).fetchall(),
    )
    print(
        "actionable_counts =",
        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM followup_queue
            WHERE status IN ('actionable_now', 'actionable_backup')
            GROUP BY status
            ORDER BY status
            """
        ).fetchall(),
    )


def main() -> int:
    args = parse_args()

    source_db = Path(args.source_db)
    target_db = Path(args.target_db)

    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    if not target_db.exists():
        raise FileNotFoundError(
            f"Target DB not found: {target_db}. Bootstrap it first."
        )
    if source_db.resolve() == target_db.resolve():
        raise RuntimeError("Source DB and target DB must be different files.")

    source_con = connect_readonly(source_db)
    target_con = connect_rw(target_db)

    try:
        ensure_source_tables(source_con)
        ensure_target_tables(target_con)

        alerts_added = sync_alerts(source_con, target_con, args.dry_run)
        decisions_added = sync_decisions(source_con, target_con, args.dry_run)
        tns_actions_added = sync_tns_actions(source_con, target_con, args.dry_run)

        print("sync_alerts_added =", alerts_added)
        print("sync_decisions_added =", decisions_added)
        print("sync_tns_actions_added =", tns_actions_added)

        if not args.dry_run:
            target_con.commit()

        run_child("followup_backfill.py", target_db, args.dry_run)
        run_child("followup_sync_tns_state.py", target_db, args.dry_run)
        run_child("followup_score.py", target_db, args.dry_run)
        run_child("followup_promote.py", target_db, args.dry_run)

        if not args.dry_run:
            print_target_summary(target_con)

        print("\nrefresh_completed dry_run=", args.dry_run)
        return 0

    finally:
        source_con.close()
        target_con.close()


if __name__ == "__main__":
    raise SystemExit(main())
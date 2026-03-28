# scripts/followup_promote.py
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


PROMOTION_PROFILE = {
    "watch_high_min": 78.0,
    "promote_photometry_min": 85.0,
    "promote_spectroscopy_min": 90.0,
    "promote_min_scores": 2,
    "promote_spectroscopy_max_mag": 17.2,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply initial follow-up status promotion rules from dynamic scores."
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


def ensure_tables_exist(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {
        "followup_queue",
        "followup_score_history",
        "followup_actions",
    }
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('followup_queue', 'followup_score_history', 'followup_actions')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(
            f"Missing required tables: {sorted(missing)}. "
            "Apply schema and scoring first."
        )


def fetch_queue_rows(con: sqlite3.Connection) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT
            fq.queue_id,
            fq.object_id,
            fq.candid,
            fq.report_id,
            fq.tns_name,
            fq.status,
            fq.priority_bucket,
            fq.current_score,
            fq.best_score,
            fq.external_classification,
            fq.external_classification_label,
            (
                SELECT COUNT(*)
                FROM followup_score_history sh
                WHERE sh.object_id = fq.object_id
                  AND sh.candid = fq.candid
            ) AS score_count,
            (
                SELECT sh.current_mag
                FROM followup_score_history sh
                WHERE sh.object_id = fq.object_id
                  AND sh.candid = fq.candid
                ORDER BY sh.score_utc DESC, sh.score_id DESC
                LIMIT 1
            ) AS current_mag
        FROM followup_queue fq
        WHERE fq.status NOT IN ('classified', 'classified_by_others', 'dropped')
          AND fq.current_score IS NOT NULL
        ORDER BY fq.current_score DESC, fq.best_score DESC
        """
    ).fetchall()
    return rows


def classify_target_status(row: sqlite3.Row) -> tuple[str, str, int, str | None]:
    score = float(row["current_score"])
    score_count = int(row["score_count"] or 0)
    current_mag = row["current_mag"]
    current_mag = float(current_mag) if current_mag is not None else None
    external_classification = int(row["external_classification"] or 0)

    if external_classification == 1:
        return (
            "classified_by_others",
            "normal",
            0,
            "external classification flag is set",
        )

    if (
        score >= PROMOTION_PROFILE["promote_spectroscopy_min"]
        and score_count >= PROMOTION_PROFILE["promote_min_scores"]
        and current_mag is not None
        and current_mag <= PROMOTION_PROFILE["promote_spectroscopy_max_mag"]
    ):
        return (
            "promote_spectroscopy",
            "urgent",
            1,
            (
                f"score={score:.1f} score_count={score_count} "
                f"current_mag={current_mag:.3f} meets spectroscopy trigger"
            ),
        )

    if (
        score >= PROMOTION_PROFILE["promote_photometry_min"]
        and score_count >= PROMOTION_PROFILE["promote_min_scores"]
    ):
        return (
            "promote_photometry",
            "high",
            1,
            f"score={score:.1f} score_count={score_count} meets photometry trigger",
        )

    if score >= PROMOTION_PROFILE["watch_high_min"]:
        return (
            "watch_high",
            "high",
            0,
            f"score={score:.1f} meets watch_high threshold",
        )

    return (
        "watch",
        "normal",
        0,
        f"score={score:.1f} below watch_high threshold",
    )


def insert_action(
    con: sqlite3.Connection,
    row: sqlite3.Row,
    action_utc: str,
    action_type: str,
    old_status: str,
    new_status: str,
    action_reason: str,
) -> None:
    payload = {
        "current_score": row["current_score"],
        "best_score": row["best_score"],
        "score_count": row["score_count"],
        "current_mag": row["current_mag"],
        "promotion_profile": PROMOTION_PROFILE,
        "report_id": row["report_id"],
        "tns_name": row["tns_name"],
    }
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO followup_actions (
            object_id,
            candid,
            action_utc,
            actor,
            action_type,
            old_status,
            new_status,
            action_reason,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["object_id"],
            row["candid"],
            action_utc,
            "system",
            action_type,
            old_status,
            new_status,
            action_reason,
            json.dumps(payload, ensure_ascii=False),
        ),
    )


def update_queue_status(
    con: sqlite3.Connection,
    row: sqlite3.Row,
    new_status: str,
    new_priority: str,
    promotion_triggered: int,
    action_utc: str,
    action_reason: str | None,
) -> None:
    cur = con.cursor()

    promotion_utc = action_utc if promotion_triggered == 1 else None
    promotion_reason = action_reason if promotion_triggered == 1 else None

    cur.execute(
        """
        UPDATE followup_queue
        SET status = ?,
            priority_bucket = ?,
            promotion_triggered = ?,
            promotion_utc = ?,
            promotion_reason = ?,
            last_review_utc = ?
        WHERE queue_id = ?
        """,
        (
            new_status,
            new_priority,
            promotion_triggered,
            promotion_utc,
            promotion_reason,
            action_utc,
            row["queue_id"],
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
        rows = fetch_queue_rows(con)
        print(f"queue_rows_with_scores={len(rows)}")

        action_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        changed = 0
        unchanged = 0
        preview_lines: list[str] = []

        for row in rows:
            old_status = row["status"]
            old_priority = row["priority_bucket"]

            new_status, new_priority, promotion_triggered, action_reason = classify_target_status(row)

            will_change = (new_status != old_status) or (new_priority != old_priority)

            if will_change:
                changed += 1
                preview_lines.append(
                    f"CHANGE object_id={row['object_id']} candid={row['candid']} "
                    f"{old_status}/{old_priority} -> {new_status}/{new_priority} "
                    f"score={float(row['current_score']):.1f} count={int(row['score_count'] or 0)} "
                    f"mag={row['current_mag']}"
                )

                if not args.dry_run:
                    action_type = "promote" if new_status.startswith("promote_") else "status_change"
                    update_queue_status(
                        con,
                        row,
                        new_status,
                        new_priority,
                        promotion_triggered,
                        action_utc,
                        action_reason,
                    )
                    insert_action(
                        con,
                        row,
                        action_utc,
                        action_type,
                        old_status,
                        new_status,
                        action_reason or "",
                    )
            else:
                unchanged += 1

        if args.dry_run:
            for line in preview_lines[:30]:
                print(line)
            print(f"changed={changed} unchanged={unchanged}")
            print("dry_run=True -> no changes written")
            return 0

        con.commit()

        cur = con.cursor()
        status_rows = cur.execute(
            """
            SELECT status, COUNT(*)
            FROM followup_queue
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()

        action_rows = cur.execute(
            """
            SELECT action_type, COUNT(*)
            FROM followup_actions
            GROUP BY action_type
            ORDER BY action_type
            """
        ).fetchall()

        print(f"changed={changed} unchanged={unchanged}")
        print(f"status_counts={status_rows}")
        print(f"action_counts={action_rows}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
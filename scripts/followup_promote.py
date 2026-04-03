# scripts/followup_promote.py
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


PROMOTION_PROFILE = {
    "watch_high_min": 78.0,
    "actionable_now_min_score": 90.0,
    "actionable_now_max_mag": 17.2,
    "actionable_now_min_obs_score": 8.0,
    "actionable_now_min_max_alt": 45.0,
    "actionable_now_min_hours_above": 1.5,
    "actionable_now_max_nmtchps": 2,
    "actionable_now_min_distpsnr1": 10.0,
    "actionable_now_min_srmag1": 21.0,
    "actionable_now_max_days_since_nondet": 5.0,
    "actionable_now_min_evidence": 1,

    "actionable_backup_min_score": 88.0,
    "actionable_backup_max_mag": 17.5,
    "actionable_backup_min_obs_score": 5.0,
    "actionable_backup_min_max_alt": 35.0,
    "actionable_backup_min_hours_above": 0.75,
    "actionable_backup_max_nmtchps": 2,
    "actionable_backup_min_distpsnr1": 7.0,
    "actionable_backup_max_days_since_nondet": 7.0,
    "actionable_backup_min_evidence": 1,

    "actionable_now_cap": 3,
    "actionable_backup_cap": 2,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply actionable follow-up promotion rules."
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
            f"Missing required tables: {sorted(missing)}. Apply schema and scoring first."
        )


def fetch_queue_rows(con: sqlite3.Connection) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT
            queue_id,
            object_id,
            candid,
            report_id,
            tns_name,
            status,
            priority_bucket,
            current_score,
            best_score,
            external_classification,
            external_classification_label
        FROM followup_queue
        WHERE status NOT IN ('classified', 'classified_by_others', 'dropped')
          AND current_score IS NOT NULL
        ORDER BY current_score DESC, best_score DESC
        """
    ).fetchall()
    return rows


def fetch_latest_score_row(con: sqlite3.Connection, object_id: str, candid: str) -> sqlite3.Row | None:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT
            score_id,
            object_id,
            candid,
            score_utc,
            score_version,
            total_score,
            current_mag,
            nmtchps,
            distpsnr1,
            srmag1,
            days_since_nondet,
            score_breakdown_json
        FROM followup_score_history
        WHERE object_id = ?
          AND candid = ?
        ORDER BY score_utc DESC, score_id DESC
        LIMIT 1
        """,
        (object_id, candid),
    ).fetchone()
    return row


def parse_score_context(score_row: sqlite3.Row) -> dict[str, object]:
    payload = json.loads(score_row["score_breakdown_json"])
    obs = payload.get("observability", {})
    evidence = payload.get("evidence", {})

    return {
        "total_score": float(score_row["total_score"]),
        "current_mag": float(score_row["current_mag"]) if score_row["current_mag"] is not None else None,
        "nmtchps": int(score_row["nmtchps"]) if score_row["nmtchps"] is not None else None,
        "distpsnr1": float(score_row["distpsnr1"]) if score_row["distpsnr1"] is not None else None,
        "srmag1": float(score_row["srmag1"]) if score_row["srmag1"] is not None else None,
        "days_since_nondet": float(score_row["days_since_nondet"]) if score_row["days_since_nondet"] is not None else None,
        "observability_score": float(payload.get("components", {}).get("observability_score", 0.0)),
        "max_alt_dark_deg": float(obs["max_alt_dark_deg"]) if obs.get("max_alt_dark_deg") is not None else None,
        "hours_above_threshold_dark": float(obs.get("hours_above_threshold_dark", 0.0)),
        "effective_evidence_count": int(evidence.get("effective_evidence_count", 0)),
        "survey_evidence_epoch_count": int(evidence.get("survey_evidence_epoch_count", 0)),
        "manual_phot_evidence_count": int(evidence.get("manual_phot_evidence_count", 0)),
        "payload": payload,
    }


def srmag1_is_clean(srmag1: float | None) -> bool:
    if srmag1 is None:
        return True
    if srmag1 < 0:
        return True
    return srmag1 >= PROMOTION_PROFILE["actionable_now_min_srmag1"]


def is_actionable_now(queue_row: sqlite3.Row, ctx: dict[str, object]) -> tuple[bool, str]:
    if int(queue_row["external_classification"] or 0) == 1:
        return False, "external classification already exists"

    if ctx["total_score"] < PROMOTION_PROFILE["actionable_now_min_score"]:
        return False, "score below actionable_now threshold"

    if ctx["current_mag"] is None or ctx["current_mag"] > PROMOTION_PROFILE["actionable_now_max_mag"]:
        return False, "magnitude too faint for actionable_now"

    if ctx["observability_score"] < PROMOTION_PROFILE["actionable_now_min_obs_score"]:
        return False, "observability score too low"

    if ctx["max_alt_dark_deg"] is None or ctx["max_alt_dark_deg"] < PROMOTION_PROFILE["actionable_now_min_max_alt"]:
        return False, "max altitude too low"

    if ctx["hours_above_threshold_dark"] < PROMOTION_PROFILE["actionable_now_min_hours_above"]:
        return False, "not enough dark time above altitude threshold"

    if ctx["nmtchps"] is None or ctx["nmtchps"] > PROMOTION_PROFILE["actionable_now_max_nmtchps"]:
        return False, "field too crowded"

    if ctx["distpsnr1"] is None or ctx["distpsnr1"] < PROMOTION_PROFILE["actionable_now_min_distpsnr1"]:
        return False, "hostless separation too low"

    if not srmag1_is_clean(ctx["srmag1"]):
        return False, "host brightness too strong"

    if ctx["days_since_nondet"] is None or ctx["days_since_nondet"] > PROMOTION_PROFILE["actionable_now_max_days_since_nondet"]:
        return False, "candidate too old"

    if ctx["effective_evidence_count"] < PROMOTION_PROFILE["actionable_now_min_evidence"]:
        return False, "no post-report evidence yet"

    return True, (
        f"score={ctx['total_score']:.1f} mag={ctx['current_mag']:.3f} "
        f"obs={ctx['observability_score']:.1f} max_alt={ctx['max_alt_dark_deg']:.1f} "
        f"hours_above={ctx['hours_above_threshold_dark']:.2f} evidence={ctx['effective_evidence_count']}"
    )


def is_actionable_backup(queue_row: sqlite3.Row, ctx: dict[str, object]) -> tuple[bool, str]:
    if int(queue_row["external_classification"] or 0) == 1:
        return False, "external classification already exists"

    if ctx["total_score"] < PROMOTION_PROFILE["actionable_backup_min_score"]:
        return False, "score below actionable_backup threshold"

    if ctx["current_mag"] is None or ctx["current_mag"] > PROMOTION_PROFILE["actionable_backup_max_mag"]:
        return False, "magnitude too faint for actionable_backup"

    if ctx["observability_score"] < PROMOTION_PROFILE["actionable_backup_min_obs_score"]:
        return False, "observability score too low"

    if ctx["max_alt_dark_deg"] is None or ctx["max_alt_dark_deg"] < PROMOTION_PROFILE["actionable_backup_min_max_alt"]:
        return False, "max altitude too low"

    if ctx["hours_above_threshold_dark"] < PROMOTION_PROFILE["actionable_backup_min_hours_above"]:
        return False, "not enough dark time above altitude threshold"

    if ctx["nmtchps"] is None or ctx["nmtchps"] > PROMOTION_PROFILE["actionable_backup_max_nmtchps"]:
        return False, "field too crowded"

    if ctx["distpsnr1"] is None or ctx["distpsnr1"] < PROMOTION_PROFILE["actionable_backup_min_distpsnr1"]:
        return False, "hostless separation too low"

    if ctx["days_since_nondet"] is None or ctx["days_since_nondet"] > PROMOTION_PROFILE["actionable_backup_max_days_since_nondet"]:
        return False, "candidate too old"

    if ctx["effective_evidence_count"] < PROMOTION_PROFILE["actionable_backup_min_evidence"]:
        return False, "no post-report evidence yet"

    return True, (
        f"score={ctx['total_score']:.1f} mag={ctx['current_mag']:.3f} "
        f"obs={ctx['observability_score']:.1f} max_alt={ctx['max_alt_dark_deg']:.1f} "
        f"hours_above={ctx['hours_above_threshold_dark']:.2f} evidence={ctx['effective_evidence_count']}"
    )


def ranking_key(ctx: dict[str, object]) -> tuple:
    mag = ctx["current_mag"] if ctx["current_mag"] is not None else 99.0
    days = ctx["days_since_nondet"] if ctx["days_since_nondet"] is not None else 999.0
    nmtchps = ctx["nmtchps"] if ctx["nmtchps"] is not None else 999
    distpsnr1 = ctx["distpsnr1"] if ctx["distpsnr1"] is not None else -999.0

    return (
        -ctx["effective_evidence_count"],
        mag,
        -ctx["observability_score"],
        -ctx["total_score"],
        days,
        nmtchps,
        -distpsnr1,
    )


def update_queue_status(
    con: sqlite3.Connection,
    queue_row: sqlite3.Row,
    new_status: str,
    new_priority: str,
    promotion_triggered: int,
    action_utc: str,
    action_reason: str,
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
            queue_row["queue_id"],
        ),
    )


def insert_action(
    con: sqlite3.Connection,
    queue_row: sqlite3.Row,
    score_row: sqlite3.Row,
    ctx: dict[str, object],
    action_utc: str,
    action_type: str,
    old_status: str,
    new_status: str,
    action_reason: str,
) -> None:
    payload = {
        "current_score": ctx["total_score"],
        "current_mag": ctx["current_mag"],
        "effective_evidence_count": ctx["effective_evidence_count"],
        "survey_evidence_epoch_count": ctx["survey_evidence_epoch_count"],
        "manual_phot_evidence_count": ctx["manual_phot_evidence_count"],
        "observability_score": ctx["observability_score"],
        "max_alt_dark_deg": ctx["max_alt_dark_deg"],
        "hours_above_threshold_dark": ctx["hours_above_threshold_dark"],
        "nmtchps": ctx["nmtchps"],
        "distpsnr1": ctx["distpsnr1"],
        "srmag1": ctx["srmag1"],
        "days_since_nondet": ctx["days_since_nondet"],
        "report_id": queue_row["report_id"],
        "tns_name": queue_row["tns_name"],
        "promotion_profile": PROMOTION_PROFILE,
        "score_version": score_row["score_version"],
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
            queue_row["object_id"],
            queue_row["candid"],
            action_utc,
            "system",
            action_type,
            old_status,
            new_status,
            action_reason,
            json.dumps(payload, ensure_ascii=False),
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
        queue_rows = fetch_queue_rows(con)
        print(f"queue_rows_with_scores={len(queue_rows)}")

        action_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        enriched = []
        for queue_row in queue_rows:
            score_row = fetch_latest_score_row(con, queue_row["object_id"], queue_row["candid"])
            if score_row is None:
                continue
            ctx = parse_score_context(score_row)
            enriched.append({
                "queue_row": queue_row,
                "score_row": score_row,
                "ctx": ctx,
            })

        actionable_now_pool = []
        actionable_backup_pool = []

        for item in enriched:
            queue_row = item["queue_row"]
            ctx = item["ctx"]

            ok_now, reason_now = is_actionable_now(queue_row, ctx)
            if ok_now:
                actionable_now_pool.append((item, reason_now))
                continue

            ok_backup, reason_backup = is_actionable_backup(queue_row, ctx)
            if ok_backup:
                actionable_backup_pool.append((item, reason_backup))

        actionable_now_pool.sort(key=lambda x: ranking_key(x[0]["ctx"]))
        actionable_backup_pool.sort(key=lambda x: ranking_key(x[0]["ctx"]))

        actionable_now_selected = actionable_now_pool[:PROMOTION_PROFILE["actionable_now_cap"]]
        selected_now_ids = {
            (x[0]["queue_row"]["object_id"], x[0]["queue_row"]["candid"])
            for x in actionable_now_selected
        }

        actionable_backup_selected = []
        for item, reason in actionable_backup_pool:
            key = (item["queue_row"]["object_id"], item["queue_row"]["candid"])
            if key in selected_now_ids:
                continue
            actionable_backup_selected.append((item, reason))
            if len(actionable_backup_selected) >= PROMOTION_PROFILE["actionable_backup_cap"]:
                break

        selected_backup_ids = {
            (x[0]["queue_row"]["object_id"], x[0]["queue_row"]["candid"])
            for x in actionable_backup_selected
        }

        changed = 0
        unchanged = 0
        preview_lines = []

        for item in enriched:
            queue_row = item["queue_row"]
            score_row = item["score_row"]
            ctx = item["ctx"]

            key = (queue_row["object_id"], queue_row["candid"])
            old_status = queue_row["status"]
            old_priority = queue_row["priority_bucket"]

            if int(queue_row["external_classification"] or 0) == 1:
                new_status = "classified_by_others"
                new_priority = "normal"
                promotion_triggered = 0
                action_reason = "external classification flag is set"
            elif key in selected_now_ids:
                new_status = "actionable_now"
                new_priority = "urgent"
                promotion_triggered = 1
                action_reason = next(
                    reason for candidate, reason in actionable_now_selected
                    if (candidate["queue_row"]["object_id"], candidate["queue_row"]["candid"]) == key
                )
            elif key in selected_backup_ids:
                new_status = "actionable_backup"
                new_priority = "high"
                promotion_triggered = 1
                action_reason = next(
                    reason for candidate, reason in actionable_backup_selected
                    if (candidate["queue_row"]["object_id"], candidate["queue_row"]["candid"]) == key
                )
            elif ctx["total_score"] >= PROMOTION_PROFILE["watch_high_min"]:
                new_status = "watch_high"
                new_priority = "high"
                promotion_triggered = 0
                action_reason = f"score={ctx['total_score']:.1f} remains in scientific shortlist"
            else:
                new_status = "watch"
                new_priority = "normal"
                promotion_triggered = 0
                action_reason = f"score={ctx['total_score']:.1f} below watch_high threshold"

            will_change = (new_status != old_status) or (new_priority != old_priority)

            if will_change:
                changed += 1
                preview_lines.append(
                    f"CHANGE object_id={queue_row['object_id']} candid={queue_row['candid']} "
                    f"{old_status}/{old_priority} -> {new_status}/{new_priority} "
                    f"score={ctx['total_score']:.1f} evidence={ctx['effective_evidence_count']} "
                    f"mag={ctx['current_mag']}"
                )

                if not args.dry_run:
                    action_type = "promote" if new_status.startswith("actionable_") else "status_change"
                    update_queue_status(
                        con,
                        queue_row,
                        new_status,
                        new_priority,
                        promotion_triggered,
                        action_utc,
                        action_reason,
                    )
                    insert_action(
                        con,
                        queue_row,
                        score_row,
                        ctx,
                        action_utc,
                        action_type,
                        old_status,
                        new_status,
                        action_reason,
                    )
            else:
                unchanged += 1

        if args.dry_run:
            for line in preview_lines[:40]:
                print(line)
            print(f"actionable_now_selected={len(actionable_now_selected)}")
            print(f"actionable_backup_selected={len(actionable_backup_selected)}")
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

        print(f"actionable_now_selected={len(actionable_now_selected)}")
        print(f"actionable_backup_selected={len(actionable_backup_selected)}")
        print(f"changed={changed} unchanged={unchanged}")
        print(f"status_counts={status_rows}")
        print(f"action_counts={action_rows}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
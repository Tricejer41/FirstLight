from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Review a candidate after manual follow-up and suggest the next operational step."
    )
    parser.add_argument("--db", required=True, help="Path to follow-up SQLite DB.")
    parser.add_argument("--object-id", required=True, help="ZTF object id.")
    parser.add_argument("--candid", required=True, help="Candidate id.")
    parser.add_argument(
        "--cfg",
        default="config/remote_followup.example.yaml",
        help="Path to remote follow-up strategy config.",
    )
    parser.add_argument(
        "--apply-suggested-status",
        action="store_true",
        help="Apply the suggested queue status if the review returns one.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the review and any status change without writing.",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_scalar(value: str) -> Any:
    if value == "":
        return ""
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_simple_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    result: dict[str, Any] = {}
    current_section: dict[str, Any] | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        if not line.startswith(" "):
            if not stripped.endswith(":"):
                raise RuntimeError(f"Unsupported YAML line: {raw_line}")
            section_name = stripped[:-1].strip()
            result[section_name] = {}
            current_section = result[section_name]
            continue

        if current_section is None:
            raise RuntimeError(f"Invalid YAML nesting: {raw_line}")

        if ":" not in stripped:
            raise RuntimeError(f"Unsupported YAML key/value line: {raw_line}")

        key, value = stripped.split(":", 1)
        current_section[key.strip()] = parse_scalar(value.strip())

    return result


def safe_json_loads(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
        return {}
    except Exception:
        return {}


def safe_json_dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except Exception:
        return None
    if f <= -900:
        return None
    return f


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def fmt_float(value: float | None, digits: int = 3, missing: str = "-") -> str:
    if value is None:
        return missing
    return f"{value:.{digits}f}"


def table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table'
          AND name=?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(con: sqlite3.Connection, table_name: str) -> list[str]:
    cur = con.cursor()
    rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(r[1]) for r in rows]


def fetch_queue_row(con: sqlite3.Connection, object_id: str, candid: str) -> sqlite3.Row:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT *
        FROM followup_queue
        WHERE object_id = ?
          AND candid = ?
        LIMIT 1
        """,
        (object_id, candid),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"Candidate not found in followup_queue: object_id={object_id} candid={candid}"
        )
    return row


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


def parse_score_context(score_row: sqlite3.Row | None) -> dict[str, Any]:
    if score_row is None:
        return {
            "score_version": None,
            "total_score": None,
            "current_mag": None,
            "nmtchps": None,
            "distpsnr1": None,
            "srmag1": None,
            "days_since_nondet": None,
            "effective_freshness_days": None,
            "age_since_submission_days": None,
            "science_score": None,
            "remote_imaging_score": None,
            "remote_spectroscopy_score": None,
            "remote_bonus_score": None,
            "survey_evidence_epoch_count": 0,
            "manual_phot_evidence_count": 0,
            "effective_evidence_count": 0,
        }

    payload = safe_json_loads(score_row["score_breakdown_json"])
    components = payload.get("components", {})
    evidence = payload.get("evidence", {})
    inputs = payload.get("inputs", {})

    effective_freshness_days = safe_float(inputs.get("effective_freshness_days"))
    if effective_freshness_days is None:
        effective_freshness_days = safe_float(score_row["days_since_nondet"])

    return {
        "score_version": score_row["score_version"],
        "total_score": safe_float(score_row["total_score"]),
        "current_mag": safe_float(score_row["current_mag"]),
        "nmtchps": safe_int(score_row["nmtchps"]),
        "distpsnr1": safe_float(score_row["distpsnr1"]),
        "srmag1": safe_float(score_row["srmag1"]),
        "days_since_nondet": safe_float(score_row["days_since_nondet"]),
        "effective_freshness_days": effective_freshness_days,
        "age_since_submission_days": safe_float(inputs.get("age_since_submission_days")),
        "science_score": safe_float(components.get("science_score")),
        "remote_imaging_score": safe_float(
            components.get("remote_imaging_feasibility_score", components.get("observability_score"))
        ),
        "remote_spectroscopy_score": safe_float(
            components.get("remote_spectroscopy_feasibility_score")
        ),
        "remote_bonus_score": safe_float(components.get("remote_bonus_score")),
        "survey_evidence_epoch_count": int(evidence.get("survey_evidence_epoch_count", 0)),
        "manual_phot_evidence_count": int(evidence.get("manual_phot_evidence_count", 0)),
        "effective_evidence_count": int(evidence.get("effective_evidence_count", 0)),
    }


def fetch_manual_observation_actions(
    con: sqlite3.Connection,
    object_id: str,
    candid: str,
) -> list[dict[str, Any]]:
    if not table_exists(con, "followup_actions"):
        return []

    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT action_utc, action_type, old_status, new_status, action_reason, payload_json
        FROM followup_actions
        WHERE object_id = ?
          AND candid = ?
          AND action_type IN ('manual_observation_logged', 'manual_observation_and_status_change')
        ORDER BY action_utc DESC
        """,
        (object_id, candid),
    ).fetchall()

    parsed: list[dict[str, Any]] = []
    for row in rows:
        payload = safe_json_loads(row["payload_json"])
        parsed.append(
            {
                "action_utc": row["action_utc"],
                "action_type": row["action_type"],
                "old_status": row["old_status"],
                "new_status": row["new_status"],
                "action_reason": row["action_reason"],
                "payload": payload,
                "kind": payload.get("kind"),
                "obs_utc": payload.get("obs_utc"),
                "facility": payload.get("facility"),
                "instrument": payload.get("instrument"),
                "band": payload.get("band"),
                "mag": safe_float(payload.get("mag")),
                "mag_err": safe_float(payload.get("mag_err")),
                "limiting_mag": safe_float(payload.get("limiting_mag")),
                "snr": safe_float(payload.get("snr")),
                "exposure_s": safe_float(payload.get("exposure_s")),
                "notes": payload.get("notes"),
            }
        )
    return parsed


def summarize_manual_actions(actions: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "count_total": len(actions),
        "imaging_detection_count": 0,
        "imaging_nondetection_count": 0,
        "failed_attempt_count": 0,
        "spectroscopy_attempt_count": 0,
        "spectroscopy_success_count": 0,
        "latest_detection": None,
        "latest_nondetection": None,
        "latest_failed_attempt": None,
        "latest_spectroscopy_attempt": None,
        "latest_spectroscopy_success": None,
        "latest_any": actions[0] if actions else None,
    }

    for item in actions:
        kind = item.get("kind")

        if kind == "imaging_detection":
            summary["imaging_detection_count"] += 1
            if summary["latest_detection"] is None:
                summary["latest_detection"] = item

        elif kind == "imaging_nondetection":
            summary["imaging_nondetection_count"] += 1
            if summary["latest_nondetection"] is None:
                summary["latest_nondetection"] = item

        elif kind == "failed_attempt":
            summary["failed_attempt_count"] += 1
            if summary["latest_failed_attempt"] is None:
                summary["latest_failed_attempt"] = item

        elif kind == "spectroscopy_attempt":
            summary["spectroscopy_attempt_count"] += 1
            if summary["latest_spectroscopy_attempt"] is None:
                summary["latest_spectroscopy_attempt"] = item

        elif kind == "spectroscopy_success":
            summary["spectroscopy_success_count"] += 1
            if summary["latest_spectroscopy_success"] is None:
                summary["latest_spectroscopy_success"] = item

    return summary


def shortlist_floor(cfg: dict[str, Any]) -> float:
    return max(60.0, float(cfg["imaging"]["min_score_backup"]) - 10.0)


def decide_next_step(
    queue_row: sqlite3.Row,
    score_ctx: dict[str, Any],
    manual_summary: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    imaging = cfg["imaging"]
    spec = cfg["spectroscopy"]
    states = cfg["states"]

    current_status = str(queue_row["status"])
    total_score = safe_float(score_ctx.get("total_score"))
    current_mag = safe_float(score_ctx.get("current_mag"))
    freshness = safe_float(score_ctx.get("effective_freshness_days"))
    remote_spec_score = safe_float(score_ctx.get("remote_spectroscopy_score"))
    science_score = safe_float(score_ctx.get("science_score"))

    result = {
        "next_step": "continue_watch",
        "suggested_status": None,
        "suggested_priority_bucket": None,
        "rationale": "",
    }

    if manual_summary["spectroscopy_success_count"] >= 1:
        result["next_step"] = "prepare_dossier"
        result["suggested_status"] = None
        result["suggested_priority_bucket"] = None
        result["rationale"] = (
            "There is already a successful spectroscopy record; the next step is to prepare the dossier / classification package."
        )
        return result

    latest_detection = manual_summary["latest_detection"]
    latest_nondetection = manual_summary["latest_nondetection"]

    if latest_detection is not None:
        latest_mag = safe_float(latest_detection.get("mag"))

        if (
            latest_mag is not None
            and latest_mag <= float(spec["preferred_mag_max"])
            and (remote_spec_score is not None and remote_spec_score >= max(float(spec["min_score_ready"]) - 30.0, 50.0))
            and (science_score is not None and science_score >= 35.0)
        ):
            result["next_step"] = "prepare_spectroscopy"
            result["suggested_status"] = str(states["spectroscopy_status"])
            result["suggested_priority_bucket"] = "urgent"
            result["rationale"] = (
                f"Manual imaging detection exists (mag={latest_mag:.2f}), and the candidate is still bright enough for a realistic spectroscopy attempt."
            )
            return result

        if (
            latest_mag is not None
            and latest_mag <= float(imaging["acceptable_mag_max"])
            and (total_score is not None and total_score >= shortlist_floor(cfg))
        ):
            result["next_step"] = "repeat_imaging"
            result["suggested_status"] = str(states["primary_status"])
            result["suggested_priority_bucket"] = "urgent"
            result["rationale"] = (
                f"Manual detection exists (mag={latest_mag:.2f}), but spectroscopy is not justified yet. A second imaging epoch is the most sensible next step."
            )
            return result

        result["next_step"] = "continue_watch"
        result["suggested_status"] = (
            str(states["shortlist_status"]) if total_score is not None and total_score >= shortlist_floor(cfg) else "watch"
        )
        result["suggested_priority_bucket"] = "high" if result["suggested_status"] == str(states["shortlist_status"]) else "normal"
        result["rationale"] = (
            "There is a manual detection, but it is not strong enough to justify spectroscopy and not compelling enough for an urgent repeat-imaging escalation."
        )
        return result

    if latest_nondetection is not None:
        limmag = safe_float(latest_nondetection.get("limiting_mag"))

        if limmag is not None and current_mag is not None and limmag >= current_mag + 1.0:
            result["next_step"] = "close_case"
            result["suggested_status"] = str(states["closed_status"])
            result["suggested_priority_bucket"] = "normal"
            result["rationale"] = (
                f"Deep manual nondetection (limiting_mag={limmag:.2f}) suggests the source has faded significantly relative to the last score reference."
            )
            return result

        if limmag is not None and limmag >= float(imaging["acceptable_mag_max"]):
            result["next_step"] = "repeat_imaging"
            result["suggested_status"] = str(states["backup_status"])
            result["suggested_priority_bucket"] = "high"
            result["rationale"] = (
                f"There is a meaningful nondetection (limiting_mag={limmag:.2f}), but not yet enough evidence to close the case definitively."
            )
            return result

        result["next_step"] = "continue_watch"
        result["suggested_status"] = "watch"
        result["suggested_priority_bucket"] = "normal"
        result["rationale"] = (
            "There is a manual nondetection, but it is not deep enough to close the case and not strong enough to justify urgent follow-up."
        )
        return result

    if manual_summary["failed_attempt_count"] >= 1:
        if (
            total_score is not None
            and total_score >= float(imaging["min_score_backup"])
            and freshness is not None
            and freshness <= float(imaging["freshness_days_max"]) * 2.0
        ):
            result["next_step"] = "repeat_imaging"
            result["suggested_status"] = str(states["backup_status"])
            result["suggested_priority_bucket"] = "high"
            result["rationale"] = (
                "There was a failed attempt, but the candidate is still good enough and fresh enough to justify one more remote imaging try."
            )
            return result

        result["next_step"] = "continue_watch"
        result["suggested_status"] = current_status
        result["suggested_priority_bucket"] = queue_row["priority_bucket"]
        result["rationale"] = (
            "There was a failed attempt, but the candidate is no longer strong enough to justify an immediate retry."
        )
        return result

    result["next_step"] = "continue_watch"
    result["suggested_status"] = current_status
    result["suggested_priority_bucket"] = queue_row["priority_bucket"]
    result["rationale"] = "No manual follow-up observations are recorded yet."
    return result


def apply_status_change(
    con: sqlite3.Connection,
    queue_row: sqlite3.Row,
    new_status: str,
    new_priority_bucket: str | None,
    review_utc: str,
    rationale: str,
) -> tuple[str | None, str | None]:
    cols = set(get_table_columns(con, "followup_queue"))

    old_status = str(queue_row["status"]) if "status" in queue_row.keys() else None
    old_priority = (
        str(queue_row["priority_bucket"])
        if "priority_bucket" in queue_row.keys() and queue_row["priority_bucket"] is not None
        else None
    )

    assignments = []
    values: list[Any] = []

    if "status" in cols:
        assignments.append("status = ?")
        values.append(new_status)

    if new_priority_bucket is not None and "priority_bucket" in cols:
        assignments.append("priority_bucket = ?")
        values.append(new_priority_bucket)

    if "last_review_utc" in cols:
        assignments.append("last_review_utc = ?")
        values.append(review_utc)

    if "promotion_reason" in cols:
        assignments.append("promotion_reason = ?")
        values.append(rationale)

    if "promotion_utc" in cols:
        assignments.append("promotion_utc = ?")
        values.append(review_utc)

    if "promotion_triggered" in cols:
        assignments.append("promotion_triggered = ?")
        values.append(1)

    if not assignments:
        return old_status, old_priority

    values.append(queue_row["queue_id"])

    sql = (
        "UPDATE followup_queue SET "
        + ", ".join(assignments)
        + " WHERE queue_id = ?"
    )

    cur = con.cursor()
    cur.execute(sql, values)
    return old_status, old_priority


def insert_followup_action(
    con: sqlite3.Connection,
    queue_row: sqlite3.Row,
    review_utc: str,
    action_type: str,
    old_status: str | None,
    new_status: str | None,
    action_reason: str,
    payload: dict[str, Any],
) -> None:
    if not table_exists(con, "followup_actions"):
        return

    cols = set(get_table_columns(con, "followup_actions"))
    expected = {
        "object_id",
        "candid",
        "action_utc",
        "actor",
        "action_type",
        "old_status",
        "new_status",
        "action_reason",
        "payload_json",
    }
    if not expected.issubset(cols):
        return

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
            review_utc,
            "manual",
            action_type,
            old_status,
            new_status,
            action_reason,
            safe_json_dumps(payload),
        ),
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    cfg_path = Path(args.cfg)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    cfg = load_simple_yaml(cfg_path)

    con = sqlite3.connect(db_path)
    try:
        if not table_exists(con, "followup_queue"):
            raise RuntimeError("Missing table: followup_queue")
        if not table_exists(con, "followup_score_history"):
            raise RuntimeError("Missing table: followup_score_history")

        queue_row = fetch_queue_row(con, args.object_id, args.candid)
        score_row = fetch_latest_score_row(con, args.object_id, args.candid)
        score_ctx = parse_score_context(score_row)
        manual_actions = fetch_manual_observation_actions(con, args.object_id, args.candid)
        manual_summary = summarize_manual_actions(manual_actions)
        decision = decide_next_step(queue_row, score_ctx, manual_summary, cfg)

        print(f"object_id={args.object_id}")
        print(f"candid={args.candid}")
        print(f"current_status={queue_row['status']}")
        print(f"current_priority_bucket={queue_row['priority_bucket']}")
        print(f"current_score={fmt_float(score_ctx.get('total_score'), 1)}")
        print(f"score_version={score_ctx.get('score_version') or '-'}")
        print(f"current_mag={fmt_float(score_ctx.get('current_mag'), 3)}")
        print(f"effective_freshness_days={fmt_float(score_ctx.get('effective_freshness_days'), 3)}")
        print(f"remote_spectroscopy_score={fmt_float(score_ctx.get('remote_spectroscopy_score'), 1)}")
        print(f"manual_observation_count={manual_summary['count_total']}")
        print(f"manual_detection_count={manual_summary['imaging_detection_count']}")
        print(f"manual_nondetection_count={manual_summary['imaging_nondetection_count']}")
        print(f"failed_attempt_count={manual_summary['failed_attempt_count']}")
        print(f"spectroscopy_attempt_count={manual_summary['spectroscopy_attempt_count']}")
        print(f"spectroscopy_success_count={manual_summary['spectroscopy_success_count']}")

        latest_any = manual_summary["latest_any"]
        if latest_any is not None:
            print(
                "latest_manual_observation="
                + safe_json_dumps(
                    {
                        "kind": latest_any.get("kind"),
                        "obs_utc": latest_any.get("obs_utc"),
                        "facility": latest_any.get("facility"),
                        "instrument": latest_any.get("instrument"),
                        "band": latest_any.get("band"),
                        "mag": latest_any.get("mag"),
                        "limiting_mag": latest_any.get("limiting_mag"),
                        "snr": latest_any.get("snr"),
                        "notes": latest_any.get("notes"),
                    }
                )
            )
        else:
            print("latest_manual_observation=None")

        print(f"next_step={decision['next_step']}")
        print(f"suggested_status={decision['suggested_status']}")
        print(f"suggested_priority_bucket={decision['suggested_priority_bucket']}")
        print(f"rationale={decision['rationale']}")

        review_payload = {
            "object_id": args.object_id,
            "candid": args.candid,
            "current_status": queue_row["status"],
            "current_priority_bucket": queue_row["priority_bucket"],
            "current_score": score_ctx.get("total_score"),
            "current_mag": score_ctx.get("current_mag"),
            "effective_freshness_days": score_ctx.get("effective_freshness_days"),
            "manual_summary": {
                "count_total": manual_summary["count_total"],
                "imaging_detection_count": manual_summary["imaging_detection_count"],
                "imaging_nondetection_count": manual_summary["imaging_nondetection_count"],
                "failed_attempt_count": manual_summary["failed_attempt_count"],
                "spectroscopy_attempt_count": manual_summary["spectroscopy_attempt_count"],
                "spectroscopy_success_count": manual_summary["spectroscopy_success_count"],
            },
            "decision": decision,
        }

        if args.dry_run:
            if args.apply_suggested_status and decision["suggested_status"]:
                print(
                    "dry_run=True -> would apply status change: "
                    f"{queue_row['status']} -> {decision['suggested_status']}"
                )
            else:
                print("dry_run=True -> no DB changes written")
            return 0

        review_utc = utc_now_iso()

        old_status = str(queue_row["status"])
        new_status_for_action = old_status

        if args.apply_suggested_status and decision["suggested_status"]:
            old_status, _ = apply_status_change(
                con=con,
                queue_row=queue_row,
                new_status=str(decision["suggested_status"]),
                new_priority_bucket=decision["suggested_priority_bucket"],
                review_utc=review_utc,
                rationale=str(decision["rationale"]),
            )
            new_status_for_action = str(decision["suggested_status"])
            action_type = "manual_post_observation_status_change"
        else:
            action_type = "manual_post_observation_review"

        insert_followup_action(
            con=con,
            queue_row=queue_row,
            review_utc=review_utc,
            action_type=action_type,
            old_status=old_status,
            new_status=new_status_for_action,
            action_reason=str(decision["rationale"]),
            payload=review_payload,
        )

        con.commit()

        print("review_written=True")
        print(f"action_type={action_type}")
        print(f"queue_status_after={new_status_for_action}")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
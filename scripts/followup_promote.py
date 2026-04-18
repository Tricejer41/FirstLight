from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply remote follow-up promotion rules on the scored follow-up queue."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite DB (development DB or production follow-up mirror).",
    )
    parser.add_argument(
        "--cfg",
        default="config/remote_followup.example.yaml",
        help="Path to remote follow-up strategy config.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write changes; only print what would change.",
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


def ensure_tables_exist(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {"followup_queue", "followup_score_history", "followup_actions"}
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
            submitted_utc,
            status,
            priority_bucket,
            current_score,
            best_score,
            external_classification,
            external_classification_label
        FROM followup_queue
        WHERE status NOT IN ('classified', 'classified_by_others', 'dropped', 'closed')
          AND current_score IS NOT NULL
        ORDER BY current_score DESC, best_score DESC, submitted_utc DESC
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


def parse_score_context(score_row: sqlite3.Row) -> dict[str, Any]:
    payload = safe_json_loads(score_row["score_breakdown_json"])

    components = payload.get("components", {})
    science = payload.get("science", {})
    remote_imaging = payload.get("remote_imaging", {})
    remote_spectroscopy = payload.get("remote_spectroscopy", {})
    evidence = payload.get("evidence", {})
    inputs = payload.get("inputs", {})

    effective_freshness_days = safe_float(inputs.get("effective_freshness_days"))
    if effective_freshness_days is None:
        effective_freshness_days = safe_float(score_row["days_since_nondet"])

    return {
        "total_score": float(score_row["total_score"]),
        "current_mag": safe_float(score_row["current_mag"]),
        "nmtchps": safe_int(score_row["nmtchps"]),
        "distpsnr1": safe_float(score_row["distpsnr1"]),
        "srmag1": safe_float(score_row["srmag1"]),
        "days_since_nondet": safe_float(score_row["days_since_nondet"]),
        "effective_freshness_days": effective_freshness_days,
        "age_since_submission_days": safe_float(inputs.get("age_since_submission_days")),
        "dec_deg": safe_float(inputs.get("dec_deg")),
        "ra_deg": safe_float(inputs.get("ra_deg")),
        "science_score": float(components.get("science_score", 0.0)),
        "remote_imaging_score": float(
            components.get("remote_imaging_feasibility_score", components.get("observability_score", 0.0))
        ),
        "remote_spectroscopy_score": float(
            components.get("remote_spectroscopy_feasibility_score", 0.0)
        ),
        "remote_bonus_score": float(components.get("remote_bonus_score", 0.0)),
        "survey_evidence_epoch_count": int(evidence.get("survey_evidence_epoch_count", 0)),
        "manual_phot_evidence_count": int(evidence.get("manual_phot_evidence_count", 0)),
        "effective_evidence_count": int(evidence.get("effective_evidence_count", 0)),
        "science_breakdown": science,
        "remote_imaging_breakdown": remote_imaging,
        "remote_spectroscopy_breakdown": remote_spectroscopy,
        "payload": payload,
    }


def shortlist_floor(cfg: dict[str, Any]) -> float:
    imaging = cfg["imaging"]
    return max(60.0, float(imaging["min_score_backup"]) - 10.0)


def declination_ok_for_primary(dec_deg: float | None, cfg: dict[str, Any]) -> bool:
    if dec_deg is None:
        return True
    return dec_deg >= float(cfg["geometry_proxy"]["declination_soft_min_deg"])


def declination_ok_for_backup(dec_deg: float | None, cfg: dict[str, Any]) -> bool:
    if dec_deg is None:
        return True
    return dec_deg >= float(cfg["geometry_proxy"]["declination_hard_min_deg"])


def srmag1_is_clean(srmag1: float | None) -> bool:
    if srmag1 is None:
        return True
    if srmag1 < 0:
        return True
    return srmag1 >= 21.0


def is_shortlist_candidate(queue_row: sqlite3.Row, ctx: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    if int(queue_row["external_classification"] or 0) == 1:
        return False, "external classification already exists"

    if ctx["total_score"] < shortlist_floor(cfg):
        return False, "score below shortlist floor"

    if ctx["current_mag"] is None or ctx["current_mag"] > float(cfg["imaging"]["acceptable_mag_max"]):
        return False, "too faint for remote shortlist"

    if not declination_ok_for_backup(ctx["dec_deg"], cfg):
        return False, "declination too low for remote shortlist"

    freshness_str = (
        f"{ctx['effective_freshness_days']:.2f}"
        if ctx["effective_freshness_days"] is not None
        else "NA"
    )

    return True, (
        f"shortlist score={ctx['total_score']:.1f} mag={ctx['current_mag']:.3f} "
        f"freshness={freshness_str}"
    )


def is_actionable_now(queue_row: sqlite3.Row, ctx: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    if int(queue_row["external_classification"] or 0) == 1:
        return False, "external classification already exists"

    imaging = cfg["imaging"]

    if ctx["total_score"] < float(imaging["min_score_now"]):
        return False, "score below primary threshold"

    if ctx["current_mag"] is None or ctx["current_mag"] > float(imaging["preferred_mag_max"]):
        return False, "magnitude too faint for primary"

    if ctx["remote_imaging_score"] < 15.0:
        return False, "remote imaging feasibility too low for primary"

    if ctx["effective_freshness_days"] is None or ctx["effective_freshness_days"] > float(imaging["freshness_days_max"]) * 2.0:
        return False, "candidate too old for primary"

    if not declination_ok_for_primary(ctx["dec_deg"], cfg):
        return False, "declination below primary threshold"

    if ctx["nmtchps"] is not None and ctx["nmtchps"] > 3:
        return False, "field too crowded for primary"

    if ctx["distpsnr1"] is not None and ctx["distpsnr1"] < 7.0:
        return False, "hostless separation too small for primary"

    if not srmag1_is_clean(ctx["srmag1"]):
        return False, "host brightness too strong for primary"

    return True, (
        f"primary score={ctx['total_score']:.1f} mag={ctx['current_mag']:.3f} "
        f"freshness={ctx['effective_freshness_days']:.2f} imaging={ctx['remote_imaging_score']:.1f} "
        f"dec={ctx['dec_deg']}"
    )


def is_actionable_backup(queue_row: sqlite3.Row, ctx: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    if int(queue_row["external_classification"] or 0) == 1:
        return False, "external classification already exists"

    imaging = cfg["imaging"]

    if ctx["total_score"] < float(imaging["min_score_backup"]):
        return False, "score below backup threshold"

    if ctx["current_mag"] is None or ctx["current_mag"] > float(imaging["acceptable_mag_max"]):
        return False, "magnitude too faint for backup"

    if ctx["remote_imaging_score"] < 10.0:
        return False, "remote imaging feasibility too low for backup"

    if ctx["effective_freshness_days"] is None or ctx["effective_freshness_days"] > float(imaging["freshness_days_max"]) * 4.0:
        return False, "candidate too old for backup"

    if not declination_ok_for_backup(ctx["dec_deg"], cfg):
        return False, "declination below backup threshold"

    return True, (
        f"backup score={ctx['total_score']:.1f} mag={ctx['current_mag']:.3f} "
        f"freshness={ctx['effective_freshness_days']:.2f} imaging={ctx['remote_imaging_score']:.1f} "
        f"dec={ctx['dec_deg']}"
    )


def is_ready_spectroscopy(queue_row: sqlite3.Row, ctx: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    if int(queue_row["external_classification"] or 0) == 1:
        return False, "external classification already exists"

    spec = cfg["spectroscopy"]

    if ctx["remote_spectroscopy_score"] < float(spec["min_score_ready"]):
        return False, "spectroscopy feasibility below threshold"

    if ctx["current_mag"] is None or ctx["current_mag"] > float(spec["hard_mag_max"]):
        return False, "magnitude too faint for spectroscopy"

    if ctx["effective_evidence_count"] < int(spec["min_effective_evidence"]):
        return False, "not enough post-report evidence"

    if bool(spec["require_manual_imaging_first"]) and ctx["manual_phot_evidence_count"] < 1:
        return False, "manual imaging is required before spectroscopy"

    return True, (
        f"spectroscopy score={ctx['remote_spectroscopy_score']:.1f} "
        f"mag={ctx['current_mag']:.3f} evidence={ctx['effective_evidence_count']} "
        f"manual_phot={ctx['manual_phot_evidence_count']}"
    )


def ranking_key(item: dict[str, Any]) -> tuple[Any, ...]:
    ctx = item["ctx"]

    freshness = ctx["effective_freshness_days"]
    if freshness is None:
        freshness = 9999.0

    mag = ctx["current_mag"]
    if mag is None:
        mag = 99.0

    dec_penalty = 0.0
    if ctx["dec_deg"] is not None and ctx["dec_deg"] < 0:
        dec_penalty = abs(ctx["dec_deg"]) / 100.0

    return (
        -ctx["total_score"],
        freshness,
        mag,
        -ctx["remote_imaging_score"],
        -ctx["remote_spectroscopy_score"],
        -ctx["survey_evidence_epoch_count"],
        -ctx["manual_phot_evidence_count"],
        dec_penalty,
        str(item["queue_row"]["submitted_utc"] or ""),
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
    ctx: dict[str, Any],
    action_utc: str,
    action_type: str,
    old_status: str,
    new_status: str,
    action_reason: str,
    cfg: dict[str, Any],
) -> None:
    payload = {
        "current_score": ctx["total_score"],
        "current_mag": ctx["current_mag"],
        "effective_freshness_days": ctx["effective_freshness_days"],
        "effective_evidence_count": ctx["effective_evidence_count"],
        "survey_evidence_epoch_count": ctx["survey_evidence_epoch_count"],
        "manual_phot_evidence_count": ctx["manual_phot_evidence_count"],
        "science_score": ctx["science_score"],
        "remote_imaging_score": ctx["remote_imaging_score"],
        "remote_spectroscopy_score": ctx["remote_spectroscopy_score"],
        "nmtchps": ctx["nmtchps"],
        "distpsnr1": ctx["distpsnr1"],
        "srmag1": ctx["srmag1"],
        "dec_deg": ctx["dec_deg"],
        "report_id": queue_row["report_id"],
        "tns_name": queue_row["tns_name"],
        "score_version": score_row["score_version"],
        "ranking": cfg["ranking"],
        "imaging": cfg["imaging"],
        "spectroscopy": cfg["spectroscopy"],
        "states": cfg["states"],
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
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        ),
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    cfg_path = Path(args.cfg)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    cfg = load_simple_yaml(cfg_path)
    states = cfg["states"]
    ranking_cfg = cfg["ranking"]

    shortlist_status = str(states["shortlist_status"])
    backup_status = str(states["backup_status"])
    primary_status = str(states["primary_status"])
    spectroscopy_status = str(states["spectroscopy_status"])

    con = sqlite3.connect(db_path)
    try:
        ensure_tables_exist(con)
        queue_rows = fetch_queue_rows(con)
        print(f"queue_rows_with_scores={len(queue_rows)}")

        action_utc = utc_now_iso()

        enriched: list[dict[str, Any]] = []
        for queue_row in queue_rows:
            score_row = fetch_latest_score_row(con, queue_row["object_id"], queue_row["candid"])
            if score_row is None:
                continue
            ctx = parse_score_context(score_row)
            enriched.append(
                {
                    "queue_row": queue_row,
                    "score_row": score_row,
                    "ctx": ctx,
                }
            )

        shortlist_pool: list[tuple[dict[str, Any], str]] = []
        spectroscopy_pool: list[tuple[dict[str, Any], str]] = []

        for item in enriched:
            queue_row = item["queue_row"]
            ctx = item["ctx"]

            ok_short, reason_short = is_shortlist_candidate(queue_row, ctx, cfg)
            if ok_short:
                shortlist_pool.append((item, reason_short))

            ok_spec, reason_spec = is_ready_spectroscopy(queue_row, ctx, cfg)
            if ok_spec:
                spectroscopy_pool.append((item, reason_spec))

        shortlist_pool.sort(key=lambda x: ranking_key(x[0]))
        spectroscopy_pool.sort(key=lambda x: ranking_key(x[0]))

        shortlist_selected = shortlist_pool[: int(ranking_cfg["keep_top_watch_high"])]
        shortlist_ids = {
            (x[0]["queue_row"]["object_id"], x[0]["queue_row"]["candid"])
            for x in shortlist_selected
        }

        ready_spectroscopy_selected = []
        for item, reason in spectroscopy_pool:
            key = (item["queue_row"]["object_id"], item["queue_row"]["candid"])
            if key not in shortlist_ids:
                continue
            ready_spectroscopy_selected.append((item, reason))

        ready_spectroscopy_ids = {
            (x[0]["queue_row"]["object_id"], x[0]["queue_row"]["candid"])
            for x in ready_spectroscopy_selected
        }

        primary_pool: list[tuple[dict[str, Any], str]] = []
        backup_pool: list[tuple[dict[str, Any], str]] = []

        for item, _ in shortlist_selected:
            queue_row = item["queue_row"]
            ctx = item["ctx"]
            key = (queue_row["object_id"], queue_row["candid"])

            if key in ready_spectroscopy_ids:
                continue

            ok_primary, reason_primary = is_actionable_now(queue_row, ctx, cfg)
            if ok_primary:
                primary_pool.append((item, reason_primary))
                continue

            ok_backup, reason_backup = is_actionable_backup(queue_row, ctx, cfg)
            if ok_backup:
                backup_pool.append((item, reason_backup))

        primary_pool.sort(key=lambda x: ranking_key(x[0]))
        backup_pool.sort(key=lambda x: ranking_key(x[0]))

        actionable_now_selected = primary_pool[: int(ranking_cfg["max_daily_primary"])]
        selected_now_ids = {
            (x[0]["queue_row"]["object_id"], x[0]["queue_row"]["candid"])
            for x in actionable_now_selected
        }

        actionable_backup_selected: list[tuple[dict[str, Any], str]] = []
        for item, reason in backup_pool:
            key = (item["queue_row"]["object_id"], item["queue_row"]["candid"])
            if key in selected_now_ids:
                continue
            actionable_backup_selected.append((item, reason))
            if len(actionable_backup_selected) >= int(ranking_cfg["max_daily_backup"]):
                break

        selected_backup_ids = {
            (x[0]["queue_row"]["object_id"], x[0]["queue_row"]["candid"])
            for x in actionable_backup_selected
        }

        changed = 0
        unchanged = 0
        preview_lines: list[str] = []

        for item in enriched:
            queue_row = item["queue_row"]
            score_row = item["score_row"]
            ctx = item["ctx"]

            key = (queue_row["object_id"], queue_row["candid"])
            old_status = str(queue_row["status"])
            old_priority = str(queue_row["priority_bucket"])

            if int(queue_row["external_classification"] or 0) == 1:
                new_status = "classified_by_others"
                new_priority = "normal"
                promotion_triggered = 0
                action_reason = "external classification flag is set"

            elif key in ready_spectroscopy_ids:
                new_status = spectroscopy_status
                new_priority = "urgent"
                promotion_triggered = 1
                action_reason = next(
                    reason for candidate, reason in ready_spectroscopy_selected
                    if (candidate["queue_row"]["object_id"], candidate["queue_row"]["candid"]) == key
                )

            elif key in selected_now_ids:
                new_status = primary_status
                new_priority = "urgent"
                promotion_triggered = 1
                action_reason = next(
                    reason for candidate, reason in actionable_now_selected
                    if (candidate["queue_row"]["object_id"], candidate["queue_row"]["candid"]) == key
                )

            elif key in selected_backup_ids:
                new_status = backup_status
                new_priority = "high"
                promotion_triggered = 1
                action_reason = next(
                    reason for candidate, reason in actionable_backup_selected
                    if (candidate["queue_row"]["object_id"], candidate["queue_row"]["candid"]) == key
                )

            elif key in shortlist_ids:
                new_status = shortlist_status
                new_priority = "high"
                promotion_triggered = 0
                action_reason = f"kept in remote shortlist (score={ctx['total_score']:.1f})"

            else:
                new_status = "watch"
                new_priority = "normal"
                promotion_triggered = 0
                action_reason = f"outside remote shortlist (score={ctx['total_score']:.1f})"

            will_change = (new_status != old_status) or (new_priority != old_priority)

            if will_change:
                changed += 1
                preview_lines.append(
                    f"CHANGE object_id={queue_row['object_id']} candid={queue_row['candid']} "
                    f"{old_status}/{old_priority} -> {new_status}/{new_priority} "
                    f"score={ctx['total_score']:.1f} mag={ctx['current_mag']} "
                    f"freshness={ctx['effective_freshness_days']} imaging={ctx['remote_imaging_score']:.1f} "
                    f"spectro={ctx['remote_spectroscopy_score']:.1f} "
                    f"evidence={ctx['effective_evidence_count']}"
                )

                if not args.dry_run:
                    if new_status == spectroscopy_status:
                        action_type = "promote_spectroscopy"
                    elif new_status == primary_status:
                        action_type = "promote_primary"
                    elif new_status == backup_status:
                        action_type = "promote_backup"
                    else:
                        action_type = "status_change"

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
                        cfg,
                    )
            else:
                unchanged += 1

        if args.dry_run:
            for line in preview_lines[:50]:
                print(line)
            print(f"shortlist_selected={len(shortlist_selected)}")
            print(f"ready_spectroscopy_selected={len(ready_spectroscopy_selected)}")
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

        print(f"shortlist_selected={len(shortlist_selected)}")
        print(f"ready_spectroscopy_selected={len(ready_spectroscopy_selected)}")
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
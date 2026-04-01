# scripts/followup_score.py
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCORE_VERSION = "classifiability_v1_evidence"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute evidence-aware follow-up classifiability scores."
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
        "decisions",
        "alerts",
        "followup_observations",
    }
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('followup_queue', 'followup_score_history', 'decisions', 'alerts', 'followup_observations')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(
            f"Missing required tables: {sorted(missing)}. "
            "Apply schema/backfill first."
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
            submitted_utc,
            status,
            priority_bucket,
            external_classification,
            external_classification_label,
            current_score,
            best_score
        FROM followup_queue
        WHERE status NOT IN ('classified', 'classified_by_others', 'dropped')
        ORDER BY submitted_utc DESC
        """
    ).fetchall()
    return rows


def fetch_latest_passed_decision(con: sqlite3.Connection, object_id: str) -> sqlite3.Row | None:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT object_id, candid, topic, passed, reason, metrics_json, created_utc
        FROM decisions
        WHERE object_id = ?
          AND passed = 1
        ORDER BY created_utc DESC
        LIMIT 1
        """,
        (object_id,),
    ).fetchone()
    return row


def fetch_alert_epochs_after_submit(
    con: sqlite3.Connection,
    object_id: str,
    baseline_candid: str,
    submitted_utc: str,
    min_gap_minutes: int = 30,
) -> int:
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT created_utc
        FROM alerts
        WHERE object_id = ?
          AND candid != ?
          AND created_utc > ?
        ORDER BY created_utc ASC
        """,
        (object_id, baseline_candid, submitted_utc),
    ).fetchall()

    times = [datetime.fromisoformat(r[0]) for r in rows]
    if not times:
        return 0

    epochs = 1
    last_anchor = times[0]
    for t in times[1:]:
        if t - last_anchor >= timedelta(minutes=min_gap_minutes):
            epochs += 1
            last_anchor = t
    return epochs


def fetch_manual_phot_epochs_after_submit(
    con: sqlite3.Connection,
    object_id: str,
    submitted_utc: str,
    min_gap_minutes: int = 30,
) -> int:
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT obs_utc
        FROM followup_observations
        WHERE object_id = ?
          AND obs_type = 'photometry'
          AND obs_utc > ?
          AND (mag IS NOT NULL OR limit_mag IS NOT NULL)
        ORDER BY obs_utc ASC
        """,
        (object_id, submitted_utc),
    ).fetchall()

    times = [datetime.fromisoformat(r[0]) for r in rows]
    if not times:
        return 0

    epochs = 1
    last_anchor = times[0]
    for t in times[1:]:
        if t - last_anchor >= timedelta(minutes=min_gap_minutes):
            epochs += 1
            last_anchor = t
    return epochs


def safe_float(value: object, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: object, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def brightness_score(mag: float | None) -> float:
    if mag is None:
        return 0.0
    if mag <= 16.5:
        return 30.0
    if mag <= 17.0:
        return 26.0
    if mag <= 17.5:
        return 20.0
    if mag <= 18.0:
        return 10.0
    return 0.0


def freshness_score(days_since_nondet: float | None) -> float:
    if days_since_nondet is None:
        return 0.0
    if days_since_nondet <= 2.0:
        return 20.0
    if days_since_nondet <= 4.0:
        return 15.0
    if days_since_nondet <= 7.0:
        return 10.0
    if days_since_nondet <= 10.0:
        return 4.0
    return 0.0


def evolution_proxy_score(ndethist: int | None) -> float:
    if ndethist is None:
        return 5.0
    if ndethist <= 1:
        return 15.0
    if ndethist == 2:
        return 10.0
    if ndethist <= 4:
        return 6.0
    return 2.0


def observability_placeholder_score() -> float:
    return 7.5


def field_cleanliness_score(nmtchps: int | None) -> float:
    if nmtchps is None:
        return 0.0
    if nmtchps <= 1:
        return 10.0
    if nmtchps == 2:
        return 7.0
    if nmtchps == 3:
        return 4.0
    return 0.0


def hostless_cleanliness_score(distpsnr1: float | None, srmag1: float | None) -> float:
    sr_missing = srmag1 is None or srmag1 < 0
    if distpsnr1 is None:
        return 0.0

    if distpsnr1 >= 10.0 and (sr_missing or srmag1 >= 21.0):
        return 10.0
    if 7.0 <= distpsnr1 < 10.0 or (not sr_missing and 20.5 <= srmag1 < 21.0):
        return 7.0
    if 5.0 <= distpsnr1 < 7.0 or (not sr_missing and 20.0 <= srmag1 < 20.5):
        return 4.0
    return 0.0


def external_status_score(external_classification: int | None, tns_name: str | None) -> float:
    if external_classification == 1:
        return 0.0
    if tns_name:
        return 3.0
    return 5.0


def fetch_latest_history_meta(con: sqlite3.Connection, object_id: str, candid: str) -> tuple[str | None, str | None]:
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT score_version, score_breakdown_json
        FROM followup_score_history
        WHERE object_id = ?
          AND candid = ?
        ORDER BY score_utc DESC, score_id DESC
        LIMIT 1
        """,
        (object_id, candid),
    ).fetchone()
    if row is None:
        return None, None

    score_version = row[0]
    signature = None
    try:
        payload = json.loads(row[1])
        signature = payload.get("evidence", {}).get("evidence_signature")
    except Exception:
        signature = None
    return score_version, signature


def compute_score(queue_row: sqlite3.Row, decision_row: sqlite3.Row, survey_epochs: int, manual_phot_epochs: int) -> dict[str, object]:
    metrics = json.loads(decision_row["metrics_json"])

    mag = safe_float(metrics.get("mag"))
    fid = safe_int(metrics.get("fid"))
    days_since_nondet = safe_float(metrics.get("days_since_nondet"))
    nmtchps = safe_int(metrics.get("nmtchps"))
    distpsnr1 = safe_float(metrics.get("distpsnr1"))
    srmag1 = safe_float(metrics.get("srmag1"))
    ndethist = safe_int(metrics.get("ndethist"))

    effective_evidence_count = int(survey_epochs) + int(manual_phot_epochs)

    b_score = brightness_score(mag)
    f_score = freshness_score(days_since_nondet)
    e_score = evolution_proxy_score(ndethist)
    o_score = observability_placeholder_score()
    fc_score = field_cleanliness_score(nmtchps)
    hc_score = hostless_cleanliness_score(distpsnr1, srmag1)
    x_score = external_status_score(
        safe_int(queue_row["external_classification"], 0),
        queue_row["tns_name"],
    )

    total = b_score + f_score + e_score + o_score + fc_score + hc_score + x_score

    evidence_signature = (
        f"decision_candid={decision_row['candid']}"
        f"|decision_created_utc={decision_row['created_utc']}"
        f"|survey_epochs={survey_epochs}"
        f"|manual_phot_epochs={manual_phot_epochs}"
        f"|tns_name={queue_row['tns_name'] or ''}"
        f"|external_classification={safe_int(queue_row['external_classification'], 0)}"
    )

    breakdown = {
        "score_version": SCORE_VERSION,
        "inputs": {
            "mag": mag,
            "fid": fid,
            "days_since_nondet": days_since_nondet,
            "nmtchps": nmtchps,
            "distpsnr1": distpsnr1,
            "srmag1": srmag1,
            "ndethist": ndethist,
            "tns_name": queue_row["tns_name"],
            "external_classification": safe_int(queue_row["external_classification"], 0),
        },
        "components": {
            "brightness_score": b_score,
            "freshness_score": f_score,
            "evolution_score": e_score,
            "observability_score": o_score,
            "field_cleanliness_score": fc_score,
            "hostless_cleanliness_score": hc_score,
            "external_status_score": x_score,
        },
        "evidence": {
            "survey_evidence_epoch_count": survey_epochs,
            "manual_phot_evidence_count": manual_phot_epochs,
            "effective_evidence_count": effective_evidence_count,
            "evidence_signature": evidence_signature,
        },
        "notes": {
            "observability_mode": "placeholder_neutral",
            "evolution_mode": "proxy_from_ndethist",
            "decision_source_created_utc": decision_row["created_utc"],
        },
    }

    return {
        "mag": mag,
        "fid": fid,
        "days_since_nondet": days_since_nondet,
        "nmtchps": nmtchps,
        "distpsnr1": distpsnr1,
        "srmag1": srmag1,
        "total_score": total,
        "breakdown_json": json.dumps(breakdown, ensure_ascii=False),
        "brightness_score": b_score,
        "freshness_score": f_score,
        "evolution_score": e_score,
        "observability_score": o_score,
        "field_cleanliness_score": fc_score,
        "hostless_cleanliness_score": hc_score,
        "external_status_score": x_score,
        "survey_evidence_epoch_count": survey_epochs,
        "manual_phot_evidence_count": manual_phot_epochs,
        "effective_evidence_count": effective_evidence_count,
        "evidence_signature": evidence_signature,
    }


def insert_score_history(con: sqlite3.Connection, queue_row: sqlite3.Row, score_utc: str, score: dict[str, object]) -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO followup_score_history (
            object_id,
            candid,
            score_utc,
            score_version,
            brightness_score,
            freshness_score,
            evolution_score,
            observability_score,
            field_cleanliness_score,
            hostless_cleanliness_score,
            external_status_score,
            total_score,
            current_mag,
            current_fid,
            days_since_nondet,
            mag_slope_per_day,
            max_alt_deg,
            hours_above_35deg,
            moon_sep_deg,
            nmtchps,
            distpsnr1,
            srmag1,
            tns_name,
            score_breakdown_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            queue_row["object_id"],
            queue_row["candid"],
            score_utc,
            SCORE_VERSION,
            score["brightness_score"],
            score["freshness_score"],
            score["evolution_score"],
            score["observability_score"],
            score["field_cleanliness_score"],
            score["hostless_cleanliness_score"],
            score["external_status_score"],
            score["total_score"],
            score["mag"],
            score["fid"],
            score["days_since_nondet"],
            None,
            None,
            None,
            None,
            score["nmtchps"],
            score["distpsnr1"],
            score["srmag1"],
            queue_row["tns_name"],
            score["breakdown_json"],
        ),
    )


def update_followup_queue(con: sqlite3.Connection, queue_row: sqlite3.Row, score_utc: str, score: dict[str, object]) -> None:
    cur = con.cursor()

    old_best = cur.execute(
        """
        SELECT best_score
        FROM followup_queue
        WHERE queue_id = ?
        """,
        (queue_row["queue_id"],),
    ).fetchone()

    best_score = float(score["total_score"])
    if old_best is not None and old_best[0] is not None:
        best_score = max(float(old_best[0]), float(score["total_score"]))

    next_review_utc = (
        datetime.now(timezone.utc)
        + (timedelta(hours=6) if int(score["effective_evidence_count"]) >= 1 else timedelta(hours=12))
    ).replace(microsecond=0).isoformat()

    cur.execute(
        """
        UPDATE followup_queue
        SET current_score = ?,
            best_score = ?,
            last_score_utc = ?,
            next_review_utc = ?
        WHERE queue_id = ?
        """,
        (
            float(score["total_score"]),
            best_score,
            score_utc,
            next_review_utc,
            queue_row["queue_id"],
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
        print(f"queue_rows_found={len(queue_rows)}")

        score_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        inserted = 0
        unchanged = 0
        skipped = 0
        preview_lines: list[str] = []

        for queue_row in queue_rows:
            decision_row = fetch_latest_passed_decision(con, queue_row["object_id"])
            if decision_row is None:
                skipped += 1
                preview_lines.append(
                    f"SKIP object_id={queue_row['object_id']} candid={queue_row['candid']} no_passed_decision"
                )
                continue

            survey_epochs = fetch_alert_epochs_after_submit(
                con,
                queue_row["object_id"],
                queue_row["candid"],
                queue_row["submitted_utc"],
            )
            manual_phot_epochs = fetch_manual_phot_epochs_after_submit(
                con,
                queue_row["object_id"],
                queue_row["submitted_utc"],
            )

            score = compute_score(queue_row, decision_row, survey_epochs, manual_phot_epochs)
            last_version, last_signature = fetch_latest_history_meta(
                con, queue_row["object_id"], queue_row["candid"]
            )

            if last_version == SCORE_VERSION and last_signature == score["evidence_signature"]:
                unchanged += 1
                preview_lines.append(
                    f"UNCHANGED object_id={queue_row['object_id']} candid={queue_row['candid']} "
                    f"score={score['total_score']:.1f} evidence={score['effective_evidence_count']}"
                )
                continue

            inserted += 1
            preview_lines.append(
                f"SCORE object_id={queue_row['object_id']} candid={queue_row['candid']} "
                f"total={score['total_score']:.1f} survey_evidence={score['survey_evidence_epoch_count']} "
                f"manual_phot={score['manual_phot_evidence_count']} effective={score['effective_evidence_count']} "
                f"mag={score['mag']}"
            )

            if not args.dry_run:
                insert_score_history(con, queue_row, score_utc, score)
                update_followup_queue(con, queue_row, score_utc, score)

        if args.dry_run:
            for line in preview_lines[:25]:
                print(line)
            print(f"inserted={inserted} unchanged={unchanged} skipped={skipped}")
            print("dry_run=True -> no changes written")
            return 0

        con.commit()

        cur = con.cursor()
        hist_count = cur.execute("SELECT COUNT(*) FROM followup_score_history").fetchone()[0]
        queue_scored = cur.execute(
            "SELECT COUNT(*) FROM followup_queue WHERE current_score IS NOT NULL"
        ).fetchone()[0]

        print(f"inserted={inserted} unchanged={unchanged} skipped={skipped}")
        print(f"followup_score_history_rows={hist_count}")
        print(f"followup_queue_scored_rows={queue_scored}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
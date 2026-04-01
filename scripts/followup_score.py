# scripts/followup_score.py
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCORE_VERSION = "classifiability_v2_observability"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute evidence-aware follow-up classifiability scores with real observability."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite DB (development DB recommended).")
    parser.add_argument("--dry-run", action="store_true", help="Do not write changes; only print what would be done.")

    # Default: Banyoles area (override anytime if you prefer another observing site)
    parser.add_argument("--site-lat", type=float, default=42.12, help="Observing site latitude in degrees.")
    parser.add_argument("--site-lon", type=float, default=2.77, help="Observing site longitude in degrees (East positive).")
    parser.add_argument("--site-name", default="Banyoles", help="Observing site name for metadata only.")

    parser.add_argument("--obs-window-hours", type=float, default=12.0, help="Forward observability window in hours.")
    parser.add_argument("--step-minutes", type=int, default=20, help="Sampling step in minutes.")
    parser.add_argument("--alt-threshold-deg", type=float, default=35.0, help="Useful altitude threshold in degrees.")
    parser.add_argument("--twilight-sun-alt-deg", type=float, default=-12.0, help="Sun altitude threshold for dark-enough sky.")

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
        raise RuntimeError(f"Missing required tables: {sorted(missing)}. Apply schema/backfill first.")


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


def fetch_latest_alert_position(con: sqlite3.Connection, object_id: str) -> tuple[float | None, float | None]:
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT raw_json
        FROM alerts
        WHERE object_id = ?
        ORDER BY created_utc DESC
        LIMIT 1
        """,
        (object_id,),
    ).fetchone()

    if row is None:
        return None, None

    try:
        payload = json.loads(row[0])
        candidate = payload.get("candidate", {})
        ra = candidate.get("ra")
        dec = candidate.get("dec")
        if ra is None or dec is None:
            return None, None
        return float(ra), float(dec)
    except Exception:
        return None, None


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


def julian_date(dt: datetime) -> float:
    return dt.timestamp() / 86400.0 + 2440587.5


def normalize_angle_deg(x: float) -> float:
    return x % 360.0


def gmst_deg(dt: datetime) -> float:
    jd = julian_date(dt)
    t = (jd - 2451545.0) / 36525.0
    gmst = (
        280.46061837
        + 360.98564736629 * (jd - 2451545.0)
        + 0.000387933 * t * t
        - (t * t * t) / 38710000.0
    )
    return normalize_angle_deg(gmst)


def lst_deg(dt: datetime, lon_deg: float) -> float:
    return normalize_angle_deg(gmst_deg(dt) + lon_deg)


def alt_deg_from_radec(ra_deg: float, dec_deg: float, dt: datetime, lat_deg: float, lon_deg: float) -> float:
    local_sidereal = lst_deg(dt, lon_deg)
    hour_angle = normalize_angle_deg(local_sidereal - ra_deg)
    if hour_angle > 180.0:
        hour_angle -= 360.0

    ha_rad = math.radians(hour_angle)
    dec_rad = math.radians(dec_deg)
    lat_rad = math.radians(lat_deg)

    sin_alt = (
        math.sin(dec_rad) * math.sin(lat_rad)
        + math.cos(dec_rad) * math.cos(lat_rad) * math.cos(ha_rad)
    )
    sin_alt = max(-1.0, min(1.0, sin_alt))
    return math.degrees(math.asin(sin_alt))


def solar_radec_deg(dt: datetime) -> tuple[float, float]:
    jd = julian_date(dt)
    n = jd - 2451545.0

    l = normalize_angle_deg(280.460 + 0.9856474 * n)
    g = normalize_angle_deg(357.528 + 0.9856003 * n)

    lam = l + 1.915 * math.sin(math.radians(g)) + 0.020 * math.sin(math.radians(2 * g))
    lam = normalize_angle_deg(lam)

    eps = 23.439 - 0.0000004 * n

    lam_rad = math.radians(lam)
    eps_rad = math.radians(eps)

    ra_rad = math.atan2(math.cos(eps_rad) * math.sin(lam_rad), math.cos(lam_rad))
    dec_rad = math.asin(math.sin(eps_rad) * math.sin(lam_rad))

    ra_deg = normalize_angle_deg(math.degrees(ra_rad))
    dec_deg = math.degrees(dec_rad)
    return ra_deg, dec_deg


def compute_observability(
    ra_deg: float | None,
    dec_deg: float | None,
    now_utc: datetime,
    site_lat: float,
    site_lon: float,
    obs_window_hours: float,
    step_minutes: int,
    alt_threshold_deg: float,
    twilight_sun_alt_deg: float,
) -> dict[str, object]:
    if ra_deg is None or dec_deg is None:
        return {
            "ra_deg": ra_deg,
            "dec_deg": dec_deg,
            "max_alt_dark_deg": None,
            "hours_above_threshold_dark": 0.0,
            "dark_samples": 0,
            "observability_signature_date": now_utc.date().isoformat(),
        }

    steps = max(1, int((obs_window_hours * 60) // step_minutes))
    max_alt_dark = None
    dark_samples = 0
    above_threshold_samples = 0

    for i in range(steps + 1):
        dt = now_utc + timedelta(minutes=i * step_minutes)

        obj_alt = alt_deg_from_radec(ra_deg, dec_deg, dt, site_lat, site_lon)

        sun_ra, sun_dec = solar_radec_deg(dt)
        sun_alt = alt_deg_from_radec(sun_ra, sun_dec, dt, site_lat, site_lon)

        if sun_alt <= twilight_sun_alt_deg:
            dark_samples += 1
            if max_alt_dark is None or obj_alt > max_alt_dark:
                max_alt_dark = obj_alt
            if obj_alt >= alt_threshold_deg:
                above_threshold_samples += 1

    hours_above = above_threshold_samples * (step_minutes / 60.0)

    return {
        "ra_deg": ra_deg,
        "dec_deg": dec_deg,
        "max_alt_dark_deg": max_alt_dark,
        "hours_above_threshold_dark": round(hours_above, 3),
        "dark_samples": dark_samples,
        "observability_signature_date": now_utc.date().isoformat(),
    }


def observability_score(max_alt_dark_deg: float | None, hours_above_threshold_dark: float) -> float:
    if max_alt_dark_deg is None or hours_above_threshold_dark <= 0:
        return 0.0

    if max_alt_dark_deg >= 60.0 and hours_above_threshold_dark >= 4.0:
        return 15.0
    if max_alt_dark_deg >= 50.0 and hours_above_threshold_dark >= 2.0:
        return 12.0
    if max_alt_dark_deg >= 40.0 and hours_above_threshold_dark >= 1.0:
        return 8.0
    if max_alt_dark_deg >= 35.0 and hours_above_threshold_dark >= 0.5:
        return 5.0
    if max_alt_dark_deg >= 25.0:
        return 2.0
    return 0.0


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
        signature = payload.get("runtime", {}).get("score_signature")
    except Exception:
        signature = None
    return score_version, signature


def compute_score(queue_row: sqlite3.Row, decision_row: sqlite3.Row, survey_epochs: int, manual_phot_epochs: int, obs: dict[str, object], args: argparse.Namespace) -> dict[str, object]:
    metrics = json.loads(decision_row["metrics_json"])

    mag = safe_float(metrics.get("mag"))
    fid = safe_int(metrics.get("fid"))
    days_since_nondet = safe_float(metrics.get("days_since_nondet"))
    nmtchps = safe_int(metrics.get("nmtchps"))
    distpsnr1 = safe_float(metrics.get("distpsnr1"))
    srmag1 = safe_float(metrics.get("srmag1"))
    ndethist = safe_int(metrics.get("ndethist"))

    effective_evidence_count = int(survey_epochs) + int(manual_phot_epochs)

    max_alt_dark_deg = safe_float(obs.get("max_alt_dark_deg"))
    hours_above_threshold_dark = safe_float(obs.get("hours_above_threshold_dark"), 0.0) or 0.0

    b_score = brightness_score(mag)
    f_score = freshness_score(days_since_nondet)
    e_score = evolution_proxy_score(ndethist)
    o_score = observability_score(max_alt_dark_deg, hours_above_threshold_dark)
    fc_score = field_cleanliness_score(nmtchps)
    hc_score = hostless_cleanliness_score(distpsnr1, srmag1)
    x_score = external_status_score(
        safe_int(queue_row["external_classification"], 0),
        queue_row["tns_name"],
    )

    total = b_score + f_score + e_score + o_score + fc_score + hc_score + x_score

    score_signature = (
        f"score_version={SCORE_VERSION}"
        f"|decision_candid={decision_row['candid']}"
        f"|decision_created_utc={decision_row['created_utc']}"
        f"|survey_epochs={survey_epochs}"
        f"|manual_phot_epochs={manual_phot_epochs}"
        f"|tns_name={queue_row['tns_name'] or ''}"
        f"|external_classification={safe_int(queue_row['external_classification'], 0)}"
        f"|site_lat={args.site_lat}"
        f"|site_lon={args.site_lon}"
        f"|obs_date_utc={obs['observability_signature_date']}"
        f"|alt_threshold_deg={args.alt_threshold_deg}"
        f"|twilight_sun_alt_deg={args.twilight_sun_alt_deg}"
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
        },
        "observability": {
            "site_name": args.site_name,
            "site_lat": args.site_lat,
            "site_lon": args.site_lon,
            "obs_window_hours": args.obs_window_hours,
            "step_minutes": args.step_minutes,
            "alt_threshold_deg": args.alt_threshold_deg,
            "twilight_sun_alt_deg": args.twilight_sun_alt_deg,
            "ra_deg": obs.get("ra_deg"),
            "dec_deg": obs.get("dec_deg"),
            "max_alt_dark_deg": max_alt_dark_deg,
            "hours_above_threshold_dark": hours_above_threshold_dark,
            "dark_samples": obs.get("dark_samples"),
        },
        "runtime": {
            "decision_source_created_utc": decision_row["created_utc"],
            "score_signature": score_signature,
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
        "score_signature": score_signature,
        "max_alt_dark_deg": max_alt_dark_deg,
        "hours_above_threshold_dark": hours_above_threshold_dark,
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
            score["max_alt_dark_deg"],
            score["hours_above_threshold_dark"],
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
        now_utc = datetime.now(timezone.utc)
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

            ra_deg, dec_deg = fetch_latest_alert_position(con, queue_row["object_id"])
            obs = compute_observability(
                ra_deg=ra_deg,
                dec_deg=dec_deg,
                now_utc=now_utc,
                site_lat=args.site_lat,
                site_lon=args.site_lon,
                obs_window_hours=args.obs_window_hours,
                step_minutes=args.step_minutes,
                alt_threshold_deg=args.alt_threshold_deg,
                twilight_sun_alt_deg=args.twilight_sun_alt_deg,
            )

            score = compute_score(queue_row, decision_row, survey_epochs, manual_phot_epochs, obs, args)
            last_version, last_signature = fetch_latest_history_meta(
                con, queue_row["object_id"], queue_row["candid"]
            )

            if last_version == SCORE_VERSION and last_signature == score["score_signature"]:
                unchanged += 1
                preview_lines.append(
                    f"UNCHANGED object_id={queue_row['object_id']} candid={queue_row['candid']} "
                    f"score={score['total_score']:.1f} max_alt_dark={score['max_alt_dark_deg']} "
                    f"hours_above={score['hours_above_threshold_dark']} evidence={score['effective_evidence_count']}"
                )
                continue

            inserted += 1
            preview_lines.append(
                f"SCORE object_id={queue_row['object_id']} candid={queue_row['candid']} "
                f"total={score['total_score']:.1f} obs={score['observability_score']:.1f} "
                f"max_alt_dark={score['max_alt_dark_deg']} hours_above={score['hours_above_threshold_dark']} "
                f"evidence={score['effective_evidence_count']} mag={score['mag']}"
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
        queue_scored = cur.execute("SELECT COUNT(*) FROM followup_queue WHERE current_score IS NOT NULL").fetchone()[0]

        print(f"inserted={inserted} unchanged={unchanged} skipped={skipped}")
        print(f"followup_score_history_rows={hist_count}")
        print(f"followup_queue_scored_rows={queue_scored}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
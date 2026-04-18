from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCORE_VERSION = "classifiability_v3_remote_followup"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute remote follow-up scoring for follow-up queue."
    )
    parser.add_argument("--db", required=True, help="Path to follow-up SQLite DB.")
    parser.add_argument(
        "--cfg",
        default="config/remote_followup.example.yaml",
        help="Path to remote follow-up strategy config.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only, do not write changes.")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        text = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def age_days_since(value: str | None) -> float | None:
    dt = parse_iso_utc(value)
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    delta = now - dt
    return max(0.0, delta.total_seconds() / 86400.0)


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

        content = stripped
        if ":" not in content:
            raise RuntimeError(f"Unsupported YAML key/value line: {raw_line}")

        key, value = content.split(":", 1)
        key = key.strip()
        value = parse_scalar(value.strip())
        current_section[key] = value

    return result


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


def ensure_required_tables(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('followup_queue', 'followup_score_history', 'decisions', 'alerts')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = {"followup_queue", "followup_score_history", "decisions", "alerts"} - found
    if missing:
        raise RuntimeError(f"Missing required tables: {sorted(missing)}")


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


def get_table_columns(con: sqlite3.Connection, table_name: str) -> set[str]:
    cur = con.cursor()
    rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def get_table_info(con: sqlite3.Connection, table_name: str) -> list[tuple[Any, ...]]:
    cur = con.cursor()
    return cur.execute(f"PRAGMA table_info({table_name})").fetchall()


def fallback_for_required_column(column_name: str, column_type: str | None) -> Any:
    coltype = (column_type or "").upper()

    if "INT" in coltype or "REAL" in coltype or "FLOA" in coltype or "DOUB" in coltype or "NUM" in coltype:
        return 0

    if column_name.endswith("_score"):
        return 0.0

    return ""


def choose_col(columns: set[str], candidates: list[str]) -> str | None:
    for col in candidates:
        if col in columns:
            return col
    return None


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def round1(value: float) -> float:
    return round(value, 1)


def is_missing_mag(value: float | None) -> bool:
    return value is None or value <= -900


def normalize_mag(value: float | None) -> float | None:
    if is_missing_mag(value):
        return None
    return float(value)


def normalize_days(value: float | None) -> float | None:
    if value is None:
        return None
    if math.isnan(value):
        return None
    return float(value)


def normalize_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def get_queue_rows(con: sqlite3.Connection) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    return cur.execute(
        """
        SELECT *
        FROM followup_queue
        ORDER BY submitted_utc DESC
        """
    ).fetchall()


def get_passed_decision_rows(con: sqlite3.Connection, object_id: str) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    return cur.execute(
        """
        SELECT object_id, candid, topic, passed, reason, metrics_json, created_utc
        FROM decisions
        WHERE object_id = ?
          AND passed = 1
        ORDER BY created_utc DESC
        """,
        (object_id,),
    ).fetchall()


def get_latest_alert_ra_dec(con: sqlite3.Connection, object_id: str) -> tuple[float | None, float | None]:
    con.row_factory = sqlite3.Row
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

    if not row:
        return None, None

    payload = safe_json_loads(row["raw_json"])
    candidate = payload.get("candidate", {})
    ra = candidate.get("ra")
    dec = candidate.get("dec")

    try:
        ra = float(ra) if ra is not None else None
    except Exception:
        ra = None

    try:
        dec = float(dec) if dec is not None else None
    except Exception:
        dec = None

    return ra, dec


def get_manual_phot_evidence_count(con: sqlite3.Connection, object_id: str, submitted_utc: str | None) -> int:
    if not table_exists(con, "followup_observations"):
        return 0

    obs_cols = get_table_columns(con, "followup_observations")
    type_col = choose_col(obs_cols, ["obs_type", "observation_type"])
    utc_col = choose_col(obs_cols, ["obs_utc", "observation_utc", "observed_utc", "created_utc"])
    object_col = choose_col(obs_cols, ["object_id"])

    if not type_col or not utc_col or not object_col:
        return 0

    con.row_factory = sqlite3.Row
    cur = con.cursor()

    sql = f"""
    SELECT COUNT(*)
    FROM followup_observations
    WHERE {object_col} = ?
      AND {type_col} IN ('photometry', 'nondetection')
    """
    params: list[Any] = [object_id]

    if submitted_utc:
        sql += f" AND {utc_col} > ?"
        params.append(submitted_utc)

    row = cur.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def compute_science_score(
    mag: float | None,
    effective_freshness_days: float | None,
    nmtchps: int | None,
    srmag1: float | None,
    ndethist: int | None,
    survey_evidence_count: int,
    imaging_cfg: dict[str, Any],
) -> tuple[float, dict[str, float]]:
    preferred_mag = float(imaging_cfg["preferred_mag_max"])
    acceptable_mag = float(imaging_cfg["acceptable_mag_max"])
    freshness_days_max = float(imaging_cfg["freshness_days_max"])

    brightness = 0.0
    if mag is not None:
        if mag <= preferred_mag - 1.0:
            brightness = 25.0
        elif mag <= preferred_mag:
            brightness = 22.0
        elif mag <= acceptable_mag:
            brightness = 16.0
        elif mag <= acceptable_mag + 0.7:
            brightness = 10.0
        else:
            brightness = 4.0

    freshness = 0.0
    if effective_freshness_days is not None:
        if effective_freshness_days <= 0.10:
            freshness = 15.0
        elif effective_freshness_days <= 0.50:
            freshness = 12.0
        elif effective_freshness_days <= 1.00:
            freshness = 10.0
        elif effective_freshness_days <= freshness_days_max:
            freshness = 7.0
        elif effective_freshness_days <= 4.0:
            freshness = 3.0
        else:
            freshness = 0.0

    hostless_cleanliness = 0.0
    srmag1_norm = normalize_mag(srmag1)
    if srmag1_norm is None:
        hostless_cleanliness = 8.0
    elif srmag1_norm >= 21.5:
        hostless_cleanliness = 8.0
    elif srmag1_norm >= 21.0:
        hostless_cleanliness = 7.0
    elif srmag1_norm >= 20.5:
        hostless_cleanliness = 5.0
    elif srmag1_norm >= 20.0:
        hostless_cleanliness = 3.0
    else:
        hostless_cleanliness = 0.0

    field_cleanliness = 0.0
    if nmtchps is not None:
        if nmtchps <= 0:
            field_cleanliness = 8.0
        elif nmtchps == 1:
            field_cleanliness = 7.0
        elif nmtchps == 2:
            field_cleanliness = 5.0
        elif nmtchps == 3:
            field_cleanliness = 3.0
        elif nmtchps == 4:
            field_cleanliness = 1.0
        else:
            field_cleanliness = 0.0

    evolution = 0.0
    if ndethist is not None:
        if ndethist <= 1:
            evolution = 4.0
        elif ndethist == 2:
            evolution = 3.0
        elif ndethist == 3:
            evolution = 1.0
        else:
            evolution = 0.0

    survey_evidence_bonus = 0.0
    if survey_evidence_count >= 2:
        survey_evidence_bonus = 5.0
    elif survey_evidence_count == 1:
        survey_evidence_bonus = 3.0

    breakdown = {
        "brightness_score": brightness,
        "freshness_score": freshness,
        "hostless_cleanliness_score": hostless_cleanliness,
        "field_cleanliness_score": field_cleanliness,
        "evolution_score": evolution,
        "survey_evidence_score": survey_evidence_bonus,
    }

    total = round1(sum(breakdown.values()))
    return total, breakdown


def compute_remote_imaging_feasibility(
    mag: float | None,
    effective_freshness_days: float | None,
    dec_deg: float | None,
    survey_evidence_count: int,
    cfg: dict[str, Any],
) -> tuple[float, dict[str, float]]:
    imaging_cfg = cfg["imaging"]
    geom_cfg = cfg["geometry_proxy"]

    preferred_mag = float(imaging_cfg["preferred_mag_max"])
    acceptable_mag = float(imaging_cfg["acceptable_mag_max"])
    freshness_days_max = float(imaging_cfg["freshness_days_max"])

    brightness = 0.0
    if mag is not None:
        if mag <= preferred_mag:
            brightness = 10.0
        elif mag <= acceptable_mag:
            brightness = 7.0
        elif mag <= acceptable_mag + 0.5:
            brightness = 4.0
        else:
            brightness = 0.0

    freshness = 0.0
    if effective_freshness_days is not None:
        if effective_freshness_days <= freshness_days_max:
            freshness = 5.0
        elif effective_freshness_days <= freshness_days_max * 2.0:
            freshness = 2.0
        else:
            freshness = 0.0

    geometry = 0.0
    penalty = 0.0
    if dec_deg is None:
        geometry = 3.0
    else:
        soft_min = float(geom_cfg["declination_soft_min_deg"])
        hard_min = float(geom_cfg["declination_hard_min_deg"])

        if dec_deg >= soft_min:
            geometry = 6.0
            penalty = 0.0
        elif dec_deg >= hard_min:
            geometry = 3.0
            penalty = -4.0
        else:
            geometry = 0.0
            penalty = -10.0

    survey_activity = 0.0
    if survey_evidence_count >= 2:
        survey_activity = 4.0
    elif survey_evidence_count == 1:
        survey_activity = 2.0

    subtotal = brightness + freshness + geometry + survey_activity
    total = round1(clamp(subtotal + penalty, 0.0, 25.0))

    breakdown = {
        "brightness_score": brightness,
        "freshness_score": freshness,
        "geometry_score": geometry,
        "survey_activity_score": survey_activity,
        "declination_penalty": penalty,
    }

    return total, breakdown


def compute_remote_bonus(
    mag: float | None,
    effective_freshness_days: float | None,
    survey_evidence_count: int,
) -> tuple[float, dict[str, float]]:
    bright_bonus = 0.0
    if mag is not None and mag <= 17.2:
        bright_bonus = 4.0

    evidence_bonus = 0.0
    if survey_evidence_count >= 1:
        evidence_bonus = 3.0

    freshness_bonus = 0.0
    if effective_freshness_days is not None and effective_freshness_days <= 1.0:
        freshness_bonus = 3.0

    breakdown = {
        "bright_bonus": bright_bonus,
        "evidence_bonus": evidence_bonus,
        "freshness_bonus": freshness_bonus,
    }
    total = round1(sum(breakdown.values()))
    return total, breakdown


def compute_remote_spectroscopy_feasibility(
    mag: float | None,
    science_score: float,
    survey_evidence_count: int,
    manual_phot_evidence_count: int,
    spectroscopy_cfg: dict[str, Any],
) -> tuple[float, dict[str, float]]:
    preferred_mag = float(spectroscopy_cfg["preferred_mag_max"])
    hard_mag = float(spectroscopy_cfg["hard_mag_max"])
    require_manual_imaging_first = bool(spectroscopy_cfg["require_manual_imaging_first"])

    brightness = 0.0
    if mag is not None:
        if mag <= preferred_mag:
            brightness = 40.0
        elif mag <= hard_mag:
            brightness = 25.0
        elif mag <= hard_mag + 0.5:
            brightness = 10.0
        else:
            brightness = 0.0

    science_transfer = round1((science_score / 65.0) * 25.0)

    survey_evidence = 0.0
    if survey_evidence_count >= 2:
        survey_evidence = 15.0
    elif survey_evidence_count == 1:
        survey_evidence = 8.0

    manual_imaging = 0.0
    if require_manual_imaging_first:
        if manual_phot_evidence_count >= 2:
            manual_imaging = 20.0
        elif manual_phot_evidence_count == 1:
            manual_imaging = 12.0
        else:
            manual_imaging = 0.0
    else:
        if manual_phot_evidence_count >= 2:
            manual_imaging = 20.0
        elif manual_phot_evidence_count == 1:
            manual_imaging = 12.0
        else:
            manual_imaging = 5.0

    breakdown = {
        "brightness_score": brightness,
        "science_transfer_score": science_transfer,
        "survey_evidence_score": survey_evidence,
        "manual_imaging_score": manual_imaging,
    }

    total = round1(clamp(sum(breakdown.values()), 0.0, 100.0))
    return total, breakdown


def build_signature(
    basis_candid: str | None,
    basis_created_utc: str | None,
    mag: float | None,
    effective_freshness_days: float | None,
    ra_deg: float | None,
    dec_deg: float | None,
    survey_evidence_count: int,
    manual_phot_evidence_count: int,
) -> str:
    freshness_bucket = int(effective_freshness_days) if effective_freshness_days is not None else None
    return (
        f"basis_candid={basis_candid or ''}|"
        f"basis_created_utc={basis_created_utc or ''}|"
        f"mag={mag}|"
        f"freshness_bucket={freshness_bucket}|"
        f"ra_deg={ra_deg}|"
        f"dec_deg={dec_deg}|"
        f"survey_epochs={survey_evidence_count}|"
        f"manual_phot={manual_phot_evidence_count}"
    )


def get_latest_existing_score(con: sqlite3.Connection, object_id: str, candid: str) -> sqlite3.Row | None:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    score_id_exists = "score_id" in get_table_columns(con, "followup_score_history")

    order_clause = "score_utc DESC"
    if score_id_exists:
        order_clause += ", score_id DESC"

    row = cur.execute(
        f"""
        SELECT *
        FROM followup_score_history
        WHERE object_id = ?
          AND candid = ?
        ORDER BY {order_clause}
        LIMIT 1
        """,
        (object_id, candid),
    ).fetchone()
    return row


def update_followup_queue_scores(
    con: sqlite3.Connection,
    object_id: str,
    candid: str,
    current_score: float,
) -> None:
    cur = con.cursor()
    cur.execute(
        """
        UPDATE followup_queue
        SET current_score = ?,
            best_score = CASE
                WHEN best_score IS NULL THEN ?
                WHEN best_score < ? THEN ?
                ELSE best_score
            END
        WHERE object_id = ?
          AND candid = ?
        """,
        (current_score, current_score, current_score, current_score, object_id, candid),
    )


def insert_score_row(
    con: sqlite3.Connection,
    object_id: str,
    candid: str,
    score_utc: str,
    total_score: float,
    current_mag: float | None,
    days_since_nondet: float | None,
    nmtchps: int | None,
    distpsnr1: float | None,
    srmag1: float | None,
    score_version: str,
    score_breakdown_json: str,
    science_score: float,
    remote_imaging_score: float,
    remote_spectroscopy_score: float,
    remote_bonus_score: float,
    science_breakdown: dict[str, float],
) -> None:
    table_info = get_table_info(con, "followup_score_history")

    values_by_col: dict[str, Any] = {
        "object_id": object_id,
        "candid": candid,
        "score_utc": score_utc,
        "total_score": total_score,
        "current_mag": current_mag,
        "days_since_nondet": days_since_nondet,
        "nmtchps": nmtchps,
        "distpsnr1": distpsnr1,
        "srmag1": srmag1,
        "score_version": score_version,
        "score_breakdown_json": score_breakdown_json,
        "science_score": science_score,
        "remote_imaging_feasibility_score": remote_imaging_score,
        "remote_spectroscopy_feasibility_score": remote_spectroscopy_score,
        "remote_bonus_score": remote_bonus_score,
        "remote_priority_score": total_score,
        "brightness_score": science_breakdown.get("brightness_score", 0.0),
        "freshness_score": science_breakdown.get("freshness_score", 0.0),
        "evolution_score": science_breakdown.get("evolution_score", 0.0),
        "observability_score": remote_imaging_score,
        "field_cleanliness_score": science_breakdown.get("field_cleanliness_score", 0.0),
        "hostless_cleanliness_score": science_breakdown.get("hostless_cleanliness_score", 0.0),
        "external_status_score": science_breakdown.get("survey_evidence_score", 0.0),
    }

    insert_cols: list[str] = []
    insert_vals: list[Any] = []

    for col in table_info:
        cid, name, col_type, notnull, default_value, pk = col

        if pk:
            continue

        if name in values_by_col:
            insert_cols.append(name)
            insert_vals.append(values_by_col[name])
            continue

        if default_value is not None:
            continue

        if notnull:
            insert_cols.append(name)
            insert_vals.append(fallback_for_required_column(name, col_type))

    placeholders = ", ".join("?" for _ in insert_cols)
    cols_sql = ", ".join(insert_cols)

    sql = f"""
        INSERT INTO followup_score_history ({cols_sql})
        VALUES ({placeholders})
    """

    cur = con.cursor()
    cur.execute(sql, insert_vals)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    cfg_path = Path(args.cfg)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    cfg = load_simple_yaml(cfg_path)

    con = sqlite3.connect(db_path)
    try:
        ensure_required_tables(con)
        queue_rows = get_queue_rows(con)

        print(f"queue_rows_found={len(queue_rows)}")

        inserted = 0
        unchanged = 0
        skipped = 0

        for queue_row in queue_rows:
            object_id = queue_row["object_id"]
            queue_candid = queue_row["candid"]
            submitted_utc = queue_row["submitted_utc"]

            decision_rows = get_passed_decision_rows(con, object_id)
            if not decision_rows:
                print(f"SKIP object_id={object_id} candid={queue_candid} reason=no_passed_decision_rows")
                skipped += 1
                continue

            original_decision = None
            for dr in decision_rows:
                if str(dr["candid"]) == str(queue_candid):
                    original_decision = dr
                    break

            latest_decision = decision_rows[0]
            basis_decision = latest_decision if latest_decision else original_decision
            if basis_decision is None:
                print(f"SKIP object_id={object_id} candid={queue_candid} reason=no_basis_decision")
                skipped += 1
                continue

            basis_created_utc = basis_decision["created_utc"] if basis_decision else None
            age_since_submission_days = age_days_since(submitted_utc or basis_created_utc)

            basis_metrics = safe_json_loads(basis_decision["metrics_json"])
            if not basis_metrics:
                print(f"SKIP object_id={object_id} candid={queue_candid} reason=empty_basis_metrics")
                skipped += 1
                continue

            mag = normalize_mag(basis_metrics.get("mag"))
            days_since_nondet = normalize_days(basis_metrics.get("days_since_nondet"))
            nmtchps = normalize_int(basis_metrics.get("nmtchps"))
            distpsnr1 = normalize_mag(basis_metrics.get("distpsnr1"))
            srmag1 = normalize_mag(basis_metrics.get("srmag1"))
            ndethist = normalize_int(basis_metrics.get("ndethist"))

            freshness_candidates = [v for v in [days_since_nondet, age_since_submission_days] if v is not None]
            effective_freshness_days = max(freshness_candidates) if freshness_candidates else None

            ra_deg, dec_deg = get_latest_alert_ra_dec(con, object_id)

            survey_evidence_epoch_count = len(
                {
                    str(dr["candid"])
                    for dr in decision_rows
                    if dr["created_utc"] > submitted_utc and str(dr["candid"]) != str(queue_candid)
                }
            )

            manual_phot_evidence_count = get_manual_phot_evidence_count(con, object_id, submitted_utc)
            effective_evidence_count = survey_evidence_epoch_count

            science_score, science_breakdown = compute_science_score(
                mag=mag,
                effective_freshness_days=effective_freshness_days,
                nmtchps=nmtchps,
                srmag1=srmag1,
                ndethist=ndethist,
                survey_evidence_count=survey_evidence_epoch_count,
                imaging_cfg=cfg["imaging"],
            )

            remote_imaging_score, remote_imaging_breakdown = compute_remote_imaging_feasibility(
                mag=mag,
                effective_freshness_days=effective_freshness_days,
                dec_deg=dec_deg,
                survey_evidence_count=survey_evidence_epoch_count,
                cfg=cfg,
            )

            remote_bonus_score, remote_bonus_breakdown = compute_remote_bonus(
                mag=mag,
                effective_freshness_days=effective_freshness_days,
                survey_evidence_count=survey_evidence_epoch_count,
            )

            remote_priority_score = round1(
                clamp(science_score + remote_imaging_score + remote_bonus_score, 0.0, 100.0)
            )

            remote_spectroscopy_score, remote_spectroscopy_breakdown = compute_remote_spectroscopy_feasibility(
                mag=mag,
                science_score=science_score,
                survey_evidence_count=survey_evidence_epoch_count,
                manual_phot_evidence_count=manual_phot_evidence_count,
                spectroscopy_cfg=cfg["spectroscopy"],
            )

            signature = build_signature(
                basis_candid=str(basis_decision["candid"]) if basis_decision else None,
                basis_created_utc=basis_decision["created_utc"] if basis_decision else None,
                mag=mag,
                effective_freshness_days=effective_freshness_days,
                ra_deg=ra_deg,
                dec_deg=dec_deg,
                survey_evidence_count=survey_evidence_epoch_count,
                manual_phot_evidence_count=manual_phot_evidence_count,
            )

            payload = {
                "score_version": SCORE_VERSION,
                "signature": signature,
                "inputs": {
                    "basis_candid": str(basis_decision["candid"]) if basis_decision else None,
                    "basis_created_utc": basis_decision["created_utc"] if basis_decision else None,
                    "mag": mag,
                    "days_since_nondet": days_since_nondet,
                    "age_since_submission_days": age_since_submission_days,
                    "effective_freshness_days": effective_freshness_days,
                    "nmtchps": nmtchps,
                    "distpsnr1": distpsnr1,
                    "srmag1": srmag1,
                    "ndethist": ndethist,
                    "ra_deg": ra_deg,
                    "dec_deg": dec_deg,
                },
                "components": {
                    "science_score": science_score,
                    "remote_imaging_feasibility_score": remote_imaging_score,
                    "remote_spectroscopy_feasibility_score": remote_spectroscopy_score,
                    "remote_bonus_score": remote_bonus_score,
                    "remote_priority_score": remote_priority_score,
                    "observability_score": remote_imaging_score,
                },
                "science": science_breakdown,
                "remote_imaging": remote_imaging_breakdown,
                "remote_spectroscopy": remote_spectroscopy_breakdown,
                "remote_bonus": remote_bonus_breakdown,
                "evidence": {
                    "survey_evidence_epoch_count": survey_evidence_epoch_count,
                    "manual_phot_evidence_count": manual_phot_evidence_count,
                    "effective_evidence_count": effective_evidence_count,
                },
                "observability": {
                    "mode": "remote_followup_proxy",
                    "site_name": "remote_proxy",
                    "ra_deg": ra_deg,
                    "dec_deg": dec_deg,
                    "max_alt_dark_deg": None,
                    "hours_above_threshold_dark": None,
                },
                "notes": {
                    "local_observability_deprecated": True,
                    "selection_mode": "remote-first",
                },
            }

            payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            score_utc = utc_now_iso()

            existing = get_latest_existing_score(con, object_id, queue_candid)

            is_unchanged = False
            if existing is not None:
                existing_payload = safe_json_loads(existing["score_breakdown_json"])
                existing_signature = existing_payload.get("signature")
                existing_version = existing["score_version"]
                existing_total = float(existing["total_score"])
                if (
                    existing_version == SCORE_VERSION
                    and existing_signature == signature
                    and abs(existing_total - remote_priority_score) < 1e-9
                ):
                    is_unchanged = True

            if is_unchanged:
                update_followup_queue_scores(con, object_id, queue_candid, remote_priority_score)
                print(
                    f"UNCHANGED object_id={object_id} candid={queue_candid} "
                    f"priority={remote_priority_score} science={science_score} imaging={remote_imaging_score} "
                    f"spectro={remote_spectroscopy_score} survey_evidence={survey_evidence_epoch_count} "
                    f"manual_phot={manual_phot_evidence_count} mag={mag} dec={dec_deg} "
                    f"age_days={age_since_submission_days} effective_freshness={effective_freshness_days}"
                )
                unchanged += 1
                continue

            update_followup_queue_scores(con, object_id, queue_candid, remote_priority_score)

            if not args.dry_run:
                insert_score_row(
                    con=con,
                    object_id=object_id,
                    candid=queue_candid,
                    score_utc=score_utc,
                    total_score=remote_priority_score,
                    current_mag=mag,
                    days_since_nondet=effective_freshness_days,
                    nmtchps=nmtchps,
                    distpsnr1=distpsnr1,
                    srmag1=srmag1,
                    score_version=SCORE_VERSION,
                    score_breakdown_json=payload_json,
                    science_score=science_score,
                    remote_imaging_score=remote_imaging_score,
                    remote_spectroscopy_score=remote_spectroscopy_score,
                    remote_bonus_score=remote_bonus_score,
                    science_breakdown=science_breakdown,
                )

            print(
                f"SCORE object_id={object_id} candid={queue_candid} "
                f"priority={remote_priority_score} science={science_score} imaging={remote_imaging_score} "
                f"spectro={remote_spectroscopy_score} bonus={remote_bonus_score} "
                f"survey_evidence={survey_evidence_epoch_count} manual_phot={manual_phot_evidence_count} "
                f"mag={mag} dec={dec_deg} age_days={age_since_submission_days} "
                f"effective_freshness={effective_freshness_days}"
            )
            inserted += 1

        if args.dry_run:
            print(f"inserted={inserted} unchanged={unchanged} skipped={skipped}")
            print("dry_run=True -> no changes written")
            return 0

        con.commit()

        cur = con.cursor()
        score_history_rows = cur.execute("SELECT COUNT(*) FROM followup_score_history").fetchone()[0]
        queue_scored_rows = cur.execute(
            "SELECT COUNT(*) FROM followup_queue WHERE current_score IS NOT NULL"
        ).fetchone()[0]

        print(f"inserted={inserted} unchanged={unchanged} skipped={skipped}")
        print(f"followup_score_history_rows={score_history_rows}")
        print(f"followup_queue_scored_rows={queue_scored_rows}")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
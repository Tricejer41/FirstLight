from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare a structured follow-up dossier for a candidate."
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
        "--out-md",
        help="Optional markdown output path. Default: docs/dossiers/<object>_<candid>.md",
    )
    parser.add_argument(
        "--out-json",
        help="Optional JSON output path. Default: docs/dossiers/<object>_<candid>.json",
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


def fmt_int(value: int | None, missing: str = "-") -> str:
    if value is None:
        return missing
    return str(value)


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


def shortlist_floor(cfg: dict[str, Any]) -> float:
    return max(60.0, float(cfg["imaging"]["min_score_backup"]) - 10.0)


def srmag1_is_clean(srmag1: float | None) -> bool:
    if srmag1 is None:
        return True
    return srmag1 >= 21.0


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
    return cur.execute(
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


def fetch_score_history_tail(con: sqlite3.Connection, object_id: str, candid: str, limit: int = 10) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    return cur.execute(
        """
        SELECT
            score_utc,
            score_version,
            total_score
        FROM followup_score_history
        WHERE object_id = ?
          AND candid = ?
        ORDER BY score_utc DESC, score_id DESC
        LIMIT ?
        """,
        (object_id, candid, limit),
    ).fetchall()


def fetch_tns_report_state(con: sqlite3.Connection, queue_row: sqlite3.Row) -> sqlite3.Row | None:
    if not table_exists(con, "tns_report_state"):
        return None

    con.row_factory = sqlite3.Row
    cur = con.cursor()

    report_id = queue_row["report_id"] if "report_id" in queue_row.keys() else None
    if report_id:
        row = cur.execute(
            """
            SELECT *
            FROM tns_report_state
            WHERE report_id = ?
            LIMIT 1
            """,
            (report_id,),
        ).fetchone()
        if row is not None:
            return row

    return cur.execute(
        """
        SELECT *
        FROM tns_report_state
        WHERE object_id = ?
          AND candid = ?
        ORDER BY submitted_utc DESC
        LIMIT 1
        """,
        (queue_row["object_id"], queue_row["candid"]),
    ).fetchone()


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
            "components": {},
            "science_breakdown": {},
            "remote_imaging_breakdown": {},
            "remote_spectroscopy_breakdown": {},
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
        "components": components,
        "science_breakdown": payload.get("science", {}),
        "remote_imaging_breakdown": payload.get("remote_imaging", {}),
        "remote_spectroscopy_breakdown": payload.get("remote_spectroscopy", {}),
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


def fetch_post_observation_reviews(
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
          AND action_type IN ('manual_post_observation_review', 'manual_post_observation_status_change')
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
                "decision": payload.get("decision", {}),
            }
        )
    return parsed


def fetch_recent_actions(
    con: sqlite3.Connection,
    object_id: str,
    candid: str,
    limit: int = 12,
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
        ORDER BY action_utc DESC
        LIMIT ?
        """,
        (object_id, candid, limit),
    ).fetchall()

    parsed: list[dict[str, Any]] = []
    for row in rows:
        parsed.append(
            {
                "action_utc": row["action_utc"],
                "action_type": row["action_type"],
                "old_status": row["old_status"],
                "new_status": row["new_status"],
                "action_reason": row["action_reason"],
                "payload": safe_json_loads(row["payload_json"]),
            }
        )
    return parsed


def primary_blockers(score_ctx: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    imaging = cfg["imaging"]
    geom = cfg["geometry_proxy"]

    total_score = safe_float(score_ctx.get("total_score"))
    mag = safe_float(score_ctx.get("current_mag"))
    remote_imaging_score = safe_float(score_ctx.get("remote_imaging_score"))
    freshness = safe_float(score_ctx.get("effective_freshness_days"))
    dec_deg = None
    # dec isn't always stored directly in score_ctx in this script version

    if total_score is None or total_score < float(imaging["min_score_now"]):
        blockers.append(
            f"score {fmt_float(total_score, 1)} below primary threshold {float(imaging['min_score_now']):.1f}"
        )

    if mag is None:
        blockers.append("missing magnitude")
    elif mag > float(imaging["preferred_mag_max"]):
        blockers.append(
            f"mag {mag:.2f} fainter than preferred {float(imaging['preferred_mag_max']):.1f}"
        )

    if remote_imaging_score is None or remote_imaging_score < 15.0:
        blockers.append(
            f"remote imaging score {fmt_float(remote_imaging_score, 1)} below 15.0"
        )

    freshness_limit = float(imaging["freshness_days_max"]) * 2.0
    if freshness is None:
        blockers.append("missing effective freshness")
    elif freshness > freshness_limit:
        blockers.append(
            f"freshness {freshness:.2f} d above primary window {freshness_limit:.1f} d"
        )

    if dec_deg is not None and dec_deg < float(geom["declination_soft_min_deg"]):
        blockers.append(
            f"declination {dec_deg:.2f} below primary soft limit {float(geom['declination_soft_min_deg']):.1f}"
        )

    nmtchps = safe_int(score_ctx.get("nmtchps"))
    if nmtchps is not None and nmtchps > 3:
        blockers.append(f"crowded field nmtchps={nmtchps} > 3")

    distpsnr1 = safe_float(score_ctx.get("distpsnr1"))
    if distpsnr1 is not None and distpsnr1 < 7.0:
        blockers.append(f"distpsnr1={distpsnr1:.2f} < 7.0")

    srmag1 = safe_float(score_ctx.get("srmag1"))
    if not srmag1_is_clean(srmag1):
        blockers.append(f"srmag1={srmag1:.2f} too bright for clean primary")

    return blockers


def backup_blockers(score_ctx: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    imaging = cfg["imaging"]

    total_score = safe_float(score_ctx.get("total_score"))
    mag = safe_float(score_ctx.get("current_mag"))
    remote_imaging_score = safe_float(score_ctx.get("remote_imaging_score"))
    freshness = safe_float(score_ctx.get("effective_freshness_days"))

    if total_score is None or total_score < float(imaging["min_score_backup"]):
        blockers.append(
            f"score {fmt_float(total_score, 1)} below backup threshold {float(imaging['min_score_backup']):.1f}"
        )

    if mag is None:
        blockers.append("missing magnitude")
    elif mag > float(imaging["acceptable_mag_max"]):
        blockers.append(
            f"mag {mag:.2f} fainter than acceptable {float(imaging['acceptable_mag_max']):.1f}"
        )

    if remote_imaging_score is None or remote_imaging_score < 10.0:
        blockers.append(
            f"remote imaging score {fmt_float(remote_imaging_score, 1)} below 10.0"
        )

    freshness_limit = float(imaging["freshness_days_max"]) * 4.0
    if freshness is None:
        blockers.append("missing effective freshness")
    elif freshness > freshness_limit:
        blockers.append(
            f"freshness {freshness:.2f} d above backup window {freshness_limit:.1f} d"
        )

    return blockers


def spectroscopy_blockers(score_ctx: dict[str, Any], manual_summary: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    spec = cfg["spectroscopy"]

    remote_spec_score = safe_float(score_ctx.get("remote_spectroscopy_score"))
    mag = safe_float(score_ctx.get("current_mag"))
    effective_evidence = safe_int(score_ctx.get("effective_evidence_count"))
    manual_phot = safe_int(score_ctx.get("manual_phot_evidence_count"))

    if remote_spec_score is None or remote_spec_score < float(spec["min_score_ready"]):
        blockers.append(
            f"remote spectroscopy score {fmt_float(remote_spec_score, 1)} below threshold {float(spec['min_score_ready']):.1f}"
        )

    if mag is None:
        blockers.append("missing magnitude")
    elif mag > float(spec["hard_mag_max"]):
        blockers.append(
            f"mag {mag:.2f} fainter than spectroscopy hard max {float(spec['hard_mag_max']):.1f}"
        )

    if effective_evidence is None or effective_evidence < int(spec["min_effective_evidence"]):
        blockers.append(
            f"effective evidence {fmt_int(effective_evidence)} below required {int(spec['min_effective_evidence'])}"
        )

    if bool(spec["require_manual_imaging_first"]) and (manual_phot is None or manual_phot < 1):
        blockers.append("manual imaging missing before spectroscopy")

    if manual_summary["spectroscopy_success_count"] >= 1:
        blockers.append("spectroscopy success already recorded")

    return blockers


def classify_readiness(
    queue_row: sqlite3.Row,
    score_ctx: dict[str, Any],
    manual_summary: dict[str, Any],
    latest_review: dict[str, Any] | None,
    cfg: dict[str, Any],
) -> tuple[str, str]:
    status = str(queue_row["status"])

    if manual_summary["spectroscopy_success_count"] >= 1:
        return (
            "ready_for_classification_package",
            "A successful spectroscopy record exists; the dossier is ready to support a classification attempt/package.",
        )

    if latest_review is not None:
        decision = latest_review.get("decision", {})
        next_step = decision.get("next_step")
        if next_step == "prepare_dossier":
            return (
                "ready_for_classification_package",
                "The latest post-observation review explicitly points to dossier preparation.",
            )
        if next_step == "prepare_spectroscopy":
            return (
                "spectroscopy_candidate",
                "The latest post-observation review indicates the candidate may justify a spectroscopy attempt.",
            )

    spec_block = spectroscopy_blockers(score_ctx, manual_summary, cfg)
    if not spec_block:
        return (
            "spectroscopy_candidate",
            "Current evidence and score state suggest the source is close to a spectroscopy-ready configuration.",
        )

    if status in ("actionable_now", "actionable_backup"):
        return (
            "active_followup_candidate",
            f"Current status is {status}, so the object is still operationally active for rapid follow-up.",
        )

    if status == "watch_high":
        return (
            "scientific_shortlist",
            "The candidate remains on the scientific shortlist, but it still lacks enough evidence or freshness for the next escalation step.",
        )

    return (
        "not_ready",
        "The candidate is currently outside the useful escalation path toward classification.",
    )


def default_md_path(object_id: str, candid: str) -> Path:
    return Path("docs") / "dossiers" / f"{object_id}_{candid}.md"


def default_json_path(object_id: str, candid: str) -> Path:
    return Path("docs") / "dossiers" / f"{object_id}_{candid}.json"


def build_dossier_payload(
    object_id: str,
    candid: str,
    queue_row: sqlite3.Row,
    tns_row: sqlite3.Row | None,
    score_ctx: dict[str, Any],
    score_tail: list[sqlite3.Row],
    manual_actions: list[dict[str, Any]],
    manual_summary: dict[str, Any],
    reviews: list[dict[str, Any]],
    recent_actions: list[dict[str, Any]],
    readiness: tuple[str, str],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    latest_review = reviews[0] if reviews else None

    queue_snapshot = {k: queue_row[k] for k in queue_row.keys()}

    tns_snapshot = None
    if tns_row is not None:
        tns_snapshot = {k: tns_row[k] for k in tns_row.keys()}

    score_tail_payload = [
        {
            "score_utc": row["score_utc"],
            "score_version": row["score_version"],
            "total_score": safe_float(row["total_score"]),
        }
        for row in score_tail
    ]

    return {
        "generated_utc": utc_now_iso(),
        "candidate": {
            "object_id": object_id,
            "candid": candid,
        },
        "queue": queue_snapshot,
        "tns": tns_snapshot,
        "score_snapshot": score_ctx,
        "score_history_tail": score_tail_payload,
        "manual_observations": manual_actions,
        "manual_summary": manual_summary,
        "post_observation_reviews": reviews,
        "recent_actions": recent_actions,
        "blockers": {
            "primary": primary_blockers(score_ctx, cfg),
            "backup": backup_blockers(score_ctx, cfg),
            "spectroscopy": spectroscopy_blockers(score_ctx, manual_summary, cfg),
        },
        "readiness": {
            "level": readiness[0],
            "summary": readiness[1],
        },
        "latest_review_decision": latest_review.get("decision") if latest_review else None,
        "strategy_thresholds": {
            "shortlist_floor": shortlist_floor(cfg),
            "primary_min_score": float(cfg["imaging"]["min_score_now"]),
            "backup_min_score": float(cfg["imaging"]["min_score_backup"]),
            "preferred_mag_max": float(cfg["imaging"]["preferred_mag_max"]),
            "acceptable_mag_max": float(cfg["imaging"]["acceptable_mag_max"]),
            "spectroscopy_min_score": float(cfg["spectroscopy"]["min_score_ready"]),
            "spectroscopy_hard_mag_max": float(cfg["spectroscopy"]["hard_mag_max"]),
            "spectroscopy_min_effective_evidence": int(cfg["spectroscopy"]["min_effective_evidence"]),
        },
    }


def render_md(
    dossier: dict[str, Any],
) -> str:
    cand = dossier["candidate"]
    queue = dossier["queue"]
    tns = dossier["tns"]
    score = dossier["score_snapshot"]
    manual_summary = dossier["manual_summary"]
    reviews = dossier["post_observation_reviews"]
    recent_actions = dossier["recent_actions"]
    blockers = dossier["blockers"]
    readiness = dossier["readiness"]
    latest_review = dossier["latest_review_decision"]

    lines: list[str] = []

    lines.append(f"# Follow-up Dossier — {cand['object_id']}")
    lines.append("")
    lines.append(f"Generated UTC: `{dossier['generated_utc']}`")
    lines.append("")

    lines.append("## Identity")
    lines.append("")
    lines.append(f"- object_id: `{cand['object_id']}`")
    lines.append(f"- candid: `{cand['candid']}`")
    lines.append(f"- report_id: `{queue.get('report_id') or '-'}`")
    lines.append(f"- tns_name: `{queue.get('tns_name') or '-'}`")
    lines.append("")

    lines.append("## Queue state")
    lines.append("")
    lines.append(f"- status: `{queue.get('status')}`")
    lines.append(f"- priority_bucket: `{queue.get('priority_bucket')}`")
    lines.append(f"- current_score: `{fmt_float(safe_float(queue.get('current_score')), 1)}`")
    lines.append(f"- best_score: `{fmt_float(safe_float(queue.get('best_score')), 1)}`")
    lines.append(f"- submitted_utc: `{queue.get('submitted_utc')}`")
    lines.append("")

    lines.append("## TNS state")
    lines.append("")
    if tns is None:
        lines.append("- No `tns_report_state` row found.")
    else:
        lines.append(f"- submit_status: `{tns.get('submit_status')}`")
        lines.append(f"- reply_status: `{tns.get('reply_status')}`")
        lines.append(f"- classification_status: `{tns.get('classification_status')}`")
        lines.append(f"- public_utc: `{tns.get('public_utc') or '-'}`")
        lines.append(f"- tns_url: `{tns.get('tns_url') or '-'}`")
        lines.append(f"- certificate_url: `{tns.get('certificate_url') or '-'}`")
    lines.append("")

    lines.append("## Score snapshot")
    lines.append("")
    lines.append(f"- score_version: `{score.get('score_version') or '-'}`")
    lines.append(f"- total_score: `{fmt_float(score.get('total_score'), 1)}`")
    lines.append(f"- current_mag: `{fmt_float(score.get('current_mag'), 3)}`")
    lines.append(f"- effective_freshness_days: `{fmt_float(score.get('effective_freshness_days'), 3)}`")
    lines.append(f"- age_since_submission_days: `{fmt_float(score.get('age_since_submission_days'), 3)}`")
    lines.append(f"- science_score: `{fmt_float(score.get('science_score'), 1)}`")
    lines.append(f"- remote_imaging_score: `{fmt_float(score.get('remote_imaging_score'), 1)}`")
    lines.append(f"- remote_spectroscopy_score: `{fmt_float(score.get('remote_spectroscopy_score'), 1)}`")
    lines.append(f"- remote_bonus_score: `{fmt_float(score.get('remote_bonus_score'), 1)}`")
    lines.append(f"- nmtchps: `{fmt_int(score.get('nmtchps'))}`")
    lines.append(f"- distpsnr1: `{fmt_float(score.get('distpsnr1'), 3)}`")
    lines.append(f"- srmag1: `{fmt_float(score.get('srmag1'), 3)}`")
    lines.append(
        f"- evidence: effective=`{score.get('effective_evidence_count', 0)}` "
        f"(survey=`{score.get('survey_evidence_epoch_count', 0)}`, manual_phot=`{score.get('manual_phot_evidence_count', 0)}`)"
    )
    lines.append("")

    lines.append("## Manual follow-up summary")
    lines.append("")
    lines.append(f"- total_manual_observations: `{manual_summary['count_total']}`")
    lines.append(f"- imaging_detection_count: `{manual_summary['imaging_detection_count']}`")
    lines.append(f"- imaging_nondetection_count: `{manual_summary['imaging_nondetection_count']}`")
    lines.append(f"- failed_attempt_count: `{manual_summary['failed_attempt_count']}`")
    lines.append(f"- spectroscopy_attempt_count: `{manual_summary['spectroscopy_attempt_count']}`")
    lines.append(f"- spectroscopy_success_count: `{manual_summary['spectroscopy_success_count']}`")
    lines.append("")

    lines.append("## Readiness")
    lines.append("")
    lines.append(f"- readiness_level: `{readiness['level']}`")
    lines.append(f"- readiness_summary: {readiness['summary']}")
    lines.append("")

    lines.append("## Current blockers")
    lines.append("")
    lines.append("- primary:")
    if blockers["primary"]:
        for item in blockers["primary"]:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- backup:")
    if blockers["backup"]:
        for item in blockers["backup"]:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- spectroscopy:")
    if blockers["spectroscopy"]:
        for item in blockers["spectroscopy"]:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("")

    lines.append("## Latest post-observation review")
    lines.append("")
    if latest_review is None:
        lines.append("- No post-observation review recorded.")
    else:
        lines.append(f"- next_step: `{latest_review.get('next_step')}`")
        lines.append(f"- suggested_status: `{latest_review.get('suggested_status')}`")
        lines.append(f"- suggested_priority_bucket: `{latest_review.get('suggested_priority_bucket')}`")
        lines.append(f"- rationale: {latest_review.get('rationale')}")
    lines.append("")

    lines.append("## Manual observations detail")
    lines.append("")
    if not dossier["manual_observations"]:
        lines.append("- No manual observations recorded.")
    else:
        for idx, item in enumerate(dossier["manual_observations"], start=1):
            lines.append(f"### Observation {idx}")
            lines.append("")
            lines.append(f"- kind: `{item.get('kind')}`")
            lines.append(f"- obs_utc: `{item.get('obs_utc') or '-'}`")
            lines.append(f"- facility: `{item.get('facility') or '-'}`")
            lines.append(f"- instrument: `{item.get('instrument') or '-'}`")
            lines.append(f"- band: `{item.get('band') or '-'}`")
            lines.append(f"- mag: `{fmt_float(item.get('mag'), 3)}`")
            lines.append(f"- mag_err: `{fmt_float(item.get('mag_err'), 3)}`")
            lines.append(f"- limiting_mag: `{fmt_float(item.get('limiting_mag'), 3)}`")
            lines.append(f"- snr: `{fmt_float(item.get('snr'), 2)}`")
            lines.append(f"- exposure_s: `{fmt_float(item.get('exposure_s'), 1)}`")
            lines.append(f"- notes: {item.get('notes') or '-'}")
            lines.append("")
    lines.append("")

    lines.append("## Recent action trail")
    lines.append("")
    if not recent_actions:
        lines.append("- No follow-up actions recorded.")
    else:
        for item in recent_actions:
            lines.append(
                f"- `{item.get('action_utc')}` | `{item.get('action_type')}` | "
                f"`{item.get('old_status')}` -> `{item.get('new_status')}` | "
                f"{item.get('action_reason')}"
            )
    lines.append("")

    lines.append("## Score history tail")
    lines.append("")
    if not dossier["score_history_tail"]:
        lines.append("- No score history available.")
    else:
        for item in dossier["score_history_tail"]:
            lines.append(
                f"- `{item['score_utc']}` | `{item['score_version']}` | score=`{fmt_float(item['total_score'], 1)}`"
            )
    lines.append("")

    lines.append("## Operational recommendation")
    lines.append("")
    if readiness["level"] == "ready_for_classification_package":
        lines.append(
            "The candidate has enough structure to justify preparing a classification package / final dossier."
        )
    elif readiness["level"] == "spectroscopy_candidate":
        lines.append(
            "The candidate is not yet a classification package, but it is close enough to justify serious spectroscopy planning."
        )
    elif readiness["level"] == "active_followup_candidate":
        lines.append(
            "The candidate remains operationally live. The next gain is likely to come from another remote follow-up epoch rather than dossier preparation."
        )
    elif readiness["level"] == "scientific_shortlist":
        lines.append(
            "The candidate still belongs to the scientific shortlist, but it is not currently close to classification readiness."
        )
    else:
        lines.append(
            "The candidate is not currently close to classification. Keep it documented, but do not prioritize dossier work yet."
        )
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


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
        score_tail = fetch_score_history_tail(con, args.object_id, args.candid, limit=10)
        tns_row = fetch_tns_report_state(con, queue_row)
        manual_actions = fetch_manual_observation_actions(con, args.object_id, args.candid)
        manual_summary = summarize_manual_actions(manual_actions)
        reviews = fetch_post_observation_reviews(con, args.object_id, args.candid)
        recent_actions = fetch_recent_actions(con, args.object_id, args.candid, limit=12)
        readiness = classify_readiness(queue_row, score_ctx, manual_summary, reviews[0] if reviews else None, cfg)

        dossier = build_dossier_payload(
            object_id=args.object_id,
            candid=args.candid,
            queue_row=queue_row,
            tns_row=tns_row,
            score_ctx=score_ctx,
            score_tail=score_tail,
            manual_actions=manual_actions,
            manual_summary=manual_summary,
            reviews=reviews,
            recent_actions=recent_actions,
            readiness=readiness,
            cfg=cfg,
        )

    finally:
        con.close()

    md_text = render_md(dossier)
    print(md_text, end="")

    md_path = Path(args.out_md) if args.out_md else default_md_path(args.object_id, args.candid)
    json_path = Path(args.out_json) if args.out_json else default_json_path(args.object_id, args.candid)

    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    md_path.write_text(md_text, encoding="utf-8")
    json_path.write_text(json.dumps(dossier, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print(f"\n[written] {md_path}")
    print(f"[written] {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
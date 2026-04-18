from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATUSES = [
    "ready_spectroscopy",
    "actionable_now",
    "actionable_backup",
    "watch_high",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a human-readable remote follow-up daily report."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to follow-up SQLite DB.",
    )
    parser.add_argument(
        "--cfg",
        default="config/remote_followup.example.yaml",
        help="Path to remote follow-up strategy config.",
    )
    parser.add_argument(
        "--status",
        nargs="*",
        default=DEFAULT_STATUSES,
        help="Statuses to include as sections, in order.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of candidates per section.",
    )
    parser.add_argument(
        "--out",
        help="Optional markdown output path.",
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


def ensure_tables_exist(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {"followup_queue", "followup_score_history"}
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


def fetch_status_counts(con: sqlite3.Connection) -> list[tuple[str, int]]:
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT status, COUNT(*)
        FROM followup_queue
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


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


def fetch_last_action(con: sqlite3.Connection, object_id: str, candid: str) -> sqlite3.Row | None:
    if not table_exists(con, "followup_actions"):
        return None

    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT action_utc, action_type, old_status, new_status, action_reason
        FROM followup_actions
        WHERE object_id = ?
          AND candid = ?
        ORDER BY action_utc DESC
        LIMIT 1
        """,
        (object_id, candid),
    ).fetchone()
    return row


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
        ORDER BY
            CASE
                WHEN status='ready_spectroscopy' THEN 0
                WHEN status='actionable_now' THEN 1
                WHEN status='actionable_backup' THEN 2
                WHEN status='watch_high' THEN 3
                WHEN status='watch' THEN 4
                ELSE 5
            END,
            current_score DESC,
            best_score DESC,
            submitted_utc DESC
        """
    ).fetchall()
    return rows


def parse_score_context(score_row: sqlite3.Row) -> dict[str, Any]:
    payload = safe_json_loads(score_row["score_breakdown_json"])

    components = payload.get("components", {})
    science = payload.get("science", {})
    remote_imaging = payload.get("remote_imaging", {})
    remote_spectroscopy = payload.get("remote_spectroscopy", {})
    remote_bonus = payload.get("remote_bonus", {})
    evidence = payload.get("evidence", {})
    inputs = payload.get("inputs", {})

    effective_freshness_days = safe_float(inputs.get("effective_freshness_days"))
    if effective_freshness_days is None:
        effective_freshness_days = safe_float(score_row["days_since_nondet"])

    return {
        "score_version": score_row["score_version"],
        "total_score": safe_float(score_row["total_score"]) or 0.0,
        "current_mag": safe_float(score_row["current_mag"]),
        "nmtchps": safe_int(score_row["nmtchps"]),
        "distpsnr1": safe_float(score_row["distpsnr1"]),
        "srmag1": safe_float(score_row["srmag1"]),
        "days_since_nondet": safe_float(score_row["days_since_nondet"]),
        "effective_freshness_days": effective_freshness_days,
        "age_since_submission_days": safe_float(inputs.get("age_since_submission_days")),
        "dec_deg": safe_float(inputs.get("dec_deg")),
        "ra_deg": safe_float(inputs.get("ra_deg")),
        "science_score": safe_float(components.get("science_score")) or 0.0,
        "remote_imaging_score": safe_float(
            components.get("remote_imaging_feasibility_score", components.get("observability_score"))
        ) or 0.0,
        "remote_spectroscopy_score": safe_float(
            components.get("remote_spectroscopy_feasibility_score")
        ) or 0.0,
        "remote_bonus_score": safe_float(components.get("remote_bonus_score")) or 0.0,
        "survey_evidence_epoch_count": int(evidence.get("survey_evidence_epoch_count", 0)),
        "manual_phot_evidence_count": int(evidence.get("manual_phot_evidence_count", 0)),
        "effective_evidence_count": int(evidence.get("effective_evidence_count", 0)),
        "science_breakdown": science,
        "remote_imaging_breakdown": remote_imaging,
        "remote_spectroscopy_breakdown": remote_spectroscopy,
        "remote_bonus_breakdown": remote_bonus,
    }


def shortlist_floor(cfg: dict[str, Any]) -> float:
    return max(60.0, float(cfg["imaging"]["min_score_backup"]) - 10.0)


def srmag1_is_clean(srmag1: float | None) -> bool:
    if srmag1 is None:
        return True
    return srmag1 >= 21.0


def primary_blockers(ctx: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    imaging = cfg["imaging"]
    geom = cfg["geometry_proxy"]

    if ctx["total_score"] < float(imaging["min_score_now"]):
        blockers.append(
            f"score {ctx['total_score']:.1f} < primary threshold {float(imaging['min_score_now']):.1f}"
        )

    if ctx["current_mag"] is None:
        blockers.append("missing magnitude")
    elif ctx["current_mag"] > float(imaging["preferred_mag_max"]):
        blockers.append(
            f"mag {ctx['current_mag']:.2f} fainter than preferred {float(imaging['preferred_mag_max']):.1f}"
        )

    if ctx["remote_imaging_score"] < 15.0:
        blockers.append(
            f"remote imaging score {ctx['remote_imaging_score']:.1f} < 15.0"
        )

    freshness_limit = float(imaging["freshness_days_max"]) * 2.0
    if ctx["effective_freshness_days"] is None:
        blockers.append("missing effective freshness")
    elif ctx["effective_freshness_days"] > freshness_limit:
        blockers.append(
            f"freshness {ctx['effective_freshness_days']:.2f} d > primary window {freshness_limit:.1f} d"
        )

    soft_min = float(geom["declination_soft_min_deg"])
    if ctx["dec_deg"] is not None and ctx["dec_deg"] < soft_min:
        blockers.append(
            f"declination {ctx['dec_deg']:.2f} below primary soft limit {soft_min:.1f}"
        )

    if ctx["nmtchps"] is not None and ctx["nmtchps"] > 3:
        blockers.append(f"crowded field nmtchps={ctx['nmtchps']} > 3")

    if ctx["distpsnr1"] is not None and ctx["distpsnr1"] < 7.0:
        blockers.append(f"distpsnr1={ctx['distpsnr1']:.2f} < 7.0")

    if not srmag1_is_clean(ctx["srmag1"]):
        blockers.append(
            f"srmag1={ctx['srmag1']:.2f} too bright for clean hostless primary"
        )

    return blockers


def backup_blockers(ctx: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    imaging = cfg["imaging"]
    geom = cfg["geometry_proxy"]

    if ctx["total_score"] < float(imaging["min_score_backup"]):
        blockers.append(
            f"score {ctx['total_score']:.1f} < backup threshold {float(imaging['min_score_backup']):.1f}"
        )

    if ctx["current_mag"] is None:
        blockers.append("missing magnitude")
    elif ctx["current_mag"] > float(imaging["acceptable_mag_max"]):
        blockers.append(
            f"mag {ctx['current_mag']:.2f} fainter than acceptable {float(imaging['acceptable_mag_max']):.1f}"
        )

    if ctx["remote_imaging_score"] < 10.0:
        blockers.append(
            f"remote imaging score {ctx['remote_imaging_score']:.1f} < 10.0"
        )

    freshness_limit = float(imaging["freshness_days_max"]) * 4.0
    if ctx["effective_freshness_days"] is None:
        blockers.append("missing effective freshness")
    elif ctx["effective_freshness_days"] > freshness_limit:
        blockers.append(
            f"freshness {ctx['effective_freshness_days']:.2f} d > backup window {freshness_limit:.1f} d"
        )

    hard_min = float(geom["declination_hard_min_deg"])
    if ctx["dec_deg"] is not None and ctx["dec_deg"] < hard_min:
        blockers.append(
            f"declination {ctx['dec_deg']:.2f} below backup hard limit {hard_min:.1f}"
        )

    return blockers


def spectroscopy_blockers(ctx: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    spec = cfg["spectroscopy"]

    if ctx["remote_spectroscopy_score"] < float(spec["min_score_ready"]):
        blockers.append(
            f"remote spectroscopy score {ctx['remote_spectroscopy_score']:.1f} < threshold {float(spec['min_score_ready']):.1f}"
        )

    if ctx["current_mag"] is None:
        blockers.append("missing magnitude")
    elif ctx["current_mag"] > float(spec["hard_mag_max"]):
        blockers.append(
            f"mag {ctx['current_mag']:.2f} fainter than spectroscopy hard max {float(spec['hard_mag_max']):.1f}"
        )

    if ctx["effective_evidence_count"] < int(spec["min_effective_evidence"]):
        blockers.append(
            f"effective evidence {ctx['effective_evidence_count']} < required {int(spec['min_effective_evidence'])}"
        )

    if bool(spec["require_manual_imaging_first"]) and ctx["manual_phot_evidence_count"] < 1:
        blockers.append("manual imaging missing before spectroscopy")

    return blockers


def recommendation_for_candidate(
    queue_row: sqlite3.Row,
    ctx: dict[str, Any],
    cfg: dict[str, Any],
) -> str:
    status = str(queue_row["status"])

    if status == "ready_spectroscopy":
        return (
            "READY SPECTROSCOPY: candidato apto para intentar espectroscopía remota si hay acceso instrumental."
        )

    if status == "actionable_now":
        return (
            "ACTIONABLE NOW: mejor candidato del día para intentar imaging remoto cuanto antes."
        )

    if status == "actionable_backup":
        return (
            "ACTIONABLE BACKUP: buen candidato de reserva si el primary no se puede observar."
        )

    if status == "watch_high":
        p_block = primary_blockers(ctx, cfg)
        b_block = backup_blockers(ctx, cfg)
        s_block = spectroscopy_blockers(ctx, cfg)

        if not p_block:
            return (
                "WATCH HIGH: está muy cerca de primary, pero el estado aún no se ha promocionado manualmente."
            )

        if not b_block:
            return (
                "WATCH HIGH: científicamente bueno, pero todavía no es el backup seleccionado del día."
            )

        if ctx["effective_evidence_count"] == 0:
            return (
                "WATCH HIGH: buen candidato científico, pero aún sin evidencia nueva post-report."
            )

        if not s_block:
            return (
                "WATCH HIGH: imaging bien encaminado; si se añade una observación manual útil, puede acercarse a espectroscopía."
            )

        return (
            "WATCH HIGH: interesante, pero todavía le faltan condiciones claras para imaging o espectroscopía remotos."
        )

    if status == "watch":
        return (
            "WATCH: mantener monitorizado; actualmente está fuera de la shortlist remota útil."
        )

    if status == "classified_by_others":
        return (
            "EXTERNAL CLASSIFICATION: el caso ya no requiere seguimiento operativo interno."
        )

    return "NO RECOMMENDATION."


def build_blocker_lines(queue_row: sqlite3.Row, ctx: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    status = str(queue_row["status"])
    lines: list[str] = []

    p_block = primary_blockers(ctx, cfg)
    b_block = backup_blockers(ctx, cfg)
    s_block = spectroscopy_blockers(ctx, cfg)

    if status not in ("actionable_now", "ready_spectroscopy"):
        lines.append("primary_blockers: " + ("none" if not p_block else "; ".join(p_block)))
    if status not in ("actionable_backup", "actionable_now", "ready_spectroscopy"):
        lines.append("backup_blockers: " + ("none" if not b_block else "; ".join(b_block)))
    if status != "ready_spectroscopy":
        lines.append("spectroscopy_blockers: " + ("none" if not s_block else "; ".join(s_block)))

    return lines


def fetch_report_rows(
    con: sqlite3.Connection,
    include_statuses: list[str],
) -> list[dict[str, Any]]:
    all_queue_rows = fetch_queue_rows(con)
    filtered = [r for r in all_queue_rows if str(r["status"]) in include_statuses]

    rows: list[dict[str, Any]] = []
    for queue_row in filtered:
        score_row = fetch_latest_score_row(con, queue_row["object_id"], queue_row["candid"])
        if score_row is None:
            continue
        ctx = parse_score_context(score_row)
        last_action = fetch_last_action(con, queue_row["object_id"], queue_row["candid"])

        rows.append(
            {
                "queue_row": queue_row,
                "score_row": score_row,
                "ctx": ctx,
                "last_action": last_action,
            }
        )
    return rows


def status_sort_key(status: str) -> tuple[int, str]:
    order = {
        "ready_spectroscopy": 0,
        "actionable_now": 1,
        "actionable_backup": 2,
        "watch_high": 3,
        "watch": 4,
        "classified_by_others": 5,
    }
    return (order.get(status, 99), status)


def row_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    queue_row = item["queue_row"]
    ctx = item["ctx"]

    freshness = ctx["effective_freshness_days"]
    if freshness is None:
        freshness = 9999.0

    mag = ctx["current_mag"]
    if mag is None:
        mag = 99.0

    return (
        -ctx["total_score"],
        freshness,
        mag,
        -(ctx["remote_imaging_score"]),
        -(ctx["remote_spectroscopy_score"]),
        -(ctx["effective_evidence_count"]),
        str(queue_row["submitted_utc"] or ""),
    )


def render_section(
    status: str,
    rows: list[dict[str, Any]],
    cfg: dict[str, Any],
    limit: int,
) -> list[str]:
    title = f"## {status} ({len(rows)})"
    lines = ["", title, ""]

    if not rows:
        lines.append("No candidates in this section.")
        lines.append("")
        return lines

    rows = sorted(rows, key=row_sort_key)[:limit]

    for idx, item in enumerate(rows, start=1):
        queue_row = item["queue_row"]
        ctx = item["ctx"]
        last_action = item["last_action"]

        lines.append(f"### {idx}. {queue_row['object_id']}")
        lines.append("")
        lines.append(f"- candid: `{queue_row['candid']}`")
        lines.append(f"- report_id: `{queue_row['report_id'] or '-'}`")
        lines.append(f"- tns_name: `{queue_row['tns_name'] or '-'}`")
        lines.append(f"- submitted_utc: `{queue_row['submitted_utc']}`")
        lines.append(f"- status: `{queue_row['status']}`")
        lines.append(f"- priority_bucket: `{queue_row['priority_bucket']}`")
        lines.append(
            f"- score: `{fmt_float(ctx['total_score'], 1)}` (best: `{fmt_float(safe_float(queue_row['best_score']), 1)}`)"
        )
        lines.append(f"- score_version: `{ctx['score_version']}`")
        lines.append(f"- mag: `{fmt_float(ctx['current_mag'], 3)}`")
        lines.append(f"- effective_freshness_days: `{fmt_float(ctx['effective_freshness_days'], 3)}`")
        lines.append(f"- age_since_submission_days: `{fmt_float(ctx['age_since_submission_days'], 3)}`")
        lines.append(f"- nmtchps: `{fmt_int(ctx['nmtchps'])}`")
        lines.append(f"- distpsnr1: `{fmt_float(ctx['distpsnr1'], 3)}`")
        lines.append(f"- srmag1: `{fmt_float(ctx['srmag1'], 3)}`")
        lines.append(f"- dec_deg: `{fmt_float(ctx['dec_deg'], 3)}`")
        lines.append(f"- science_score: `{fmt_float(ctx['science_score'], 1)}`")
        lines.append(f"- remote_imaging_score: `{fmt_float(ctx['remote_imaging_score'], 1)}`")
        lines.append(f"- remote_spectroscopy_score: `{fmt_float(ctx['remote_spectroscopy_score'], 1)}`")
        lines.append(f"- remote_bonus_score: `{fmt_float(ctx['remote_bonus_score'], 1)}`")
        lines.append(
            f"- evidence: effective=`{ctx['effective_evidence_count']}` "
            f"(survey=`{ctx['survey_evidence_epoch_count']}`, manual_phot=`{ctx['manual_phot_evidence_count']}`)"
        )

        if last_action is not None:
            lines.append(
                f"- last_action: `{last_action['action_type']}` at `{last_action['action_utc']}`"
            )
            lines.append(f"- last_action_reason: {last_action['action_reason']}")

        for blocker_line in build_blocker_lines(queue_row, ctx, cfg):
            lines.append(f"- {blocker_line}")

        lines.append(f"- recommendation: {recommendation_for_candidate(queue_row, ctx, cfg)}")
        lines.append("")

    return lines


def render_report(
    db_path: Path,
    cfg: dict[str, Any],
    status_counts: list[tuple[str, int]],
    rows: list[dict[str, Any]],
    include_statuses: list[str],
    limit: int,
) -> str:
    generated_utc = utc_now_iso()

    grouped: dict[str, list[dict[str, Any]]] = {status: [] for status in include_statuses}
    for item in rows:
        grouped[str(item["queue_row"]["status"])].append(item)

    lines: list[str] = []
    lines.append("# FirstLight Follow-up Daily Report")
    lines.append("")
    lines.append(f"Generated UTC: `{generated_utc}`")
    lines.append("")
    lines.append(f"DB: `{db_path}`")
    lines.append("")
    lines.append("## Strategy thresholds")
    lines.append("")
    lines.append(f"- shortlist_floor: `{fmt_float(shortlist_floor(cfg), 1)}`")
    lines.append(f"- primary_min_score: `{fmt_float(float(cfg['imaging']['min_score_now']), 1)}`")
    lines.append(f"- backup_min_score: `{fmt_float(float(cfg['imaging']['min_score_backup']), 1)}`")
    lines.append(f"- spectroscopy_min_score: `{fmt_float(float(cfg['spectroscopy']['min_score_ready']), 1)}`")
    lines.append(
        f"- spectroscopy_min_effective_evidence: `{int(cfg['spectroscopy']['min_effective_evidence'])}`"
    )
    lines.append("")

    lines.append("## Queue status counts")
    for status, count in status_counts:
        lines.append(f"- {status}: {count}")
    lines.append("")

    summary_counts = {status: len(grouped.get(status, [])) for status in include_statuses}
    lines.append("## Decision summary")
    lines.append("")
    lines.append(
        f"- ready_spectroscopy: `{summary_counts.get('ready_spectroscopy', 0)}`"
    )
    lines.append(
        f"- actionable_now: `{summary_counts.get('actionable_now', 0)}`"
    )
    lines.append(
        f"- actionable_backup: `{summary_counts.get('actionable_backup', 0)}`"
    )
    lines.append(
        f"- watch_high: `{summary_counts.get('watch_high', 0)}`"
    )
    lines.append("")

    ordered_statuses = sorted(include_statuses, key=status_sort_key)
    for status in ordered_statuses:
        lines.extend(render_section(status, grouped.get(status, []), cfg, limit))

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
        ensure_tables_exist(con)
        status_counts = fetch_status_counts(con)
        rows = fetch_report_rows(con, args.status)

        report = render_report(
            db_path=db_path,
            cfg=cfg,
            status_counts=status_counts,
            rows=rows,
            include_statuses=args.status,
            limit=args.limit,
        )

    finally:
        con.close()

    print(report, end="")

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print(f"\n[written] {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
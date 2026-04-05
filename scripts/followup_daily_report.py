from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CandidateRow:
    object_id: str
    candid: str
    report_id: str | None
    tns_name: str | None
    status: str
    priority_bucket: str | None
    submitted_utc: str | None
    current_score: float | None
    best_score: float | None
    current_mag: float | None
    days_since_nondet: float | None
    nmtchps: int | None
    distpsnr1: float | None
    srmag1: float | None
    observability_score: float | None
    max_alt_dark_deg: float | None
    hours_above_threshold_dark: float | None
    effective_evidence_count: int
    survey_evidence_epoch_count: int
    manual_phot_evidence_count: int
    score_version: str | None
    recommendation: str
    raw_payload: dict[str, Any]


DEFAULT_STATUSES = ["actionable_now", "actionable_backup", "watch_high"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a human-readable daily follow-up shortlist report."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to follow-up SQLite DB.",
    )
    parser.add_argument(
        "--status",
        nargs="*",
        default=DEFAULT_STATUSES,
        help="Statuses to include. Default: actionable_now actionable_backup watch_high",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of rows to include.",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Optional output markdown/text file path.",
    )
    return parser.parse_args()


def ensure_tables_exist(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    required = {"followup_queue", "followup_score_history"}
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('followup_queue', 'followup_score_history')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(f"Missing required tables: {sorted(missing)}")


def get_status_counts(con: sqlite3.Connection) -> list[tuple[str, int]]:
    cur = con.cursor()
    return cur.execute(
        """
        SELECT status, COUNT(*)
        FROM followup_queue
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()


def get_latest_candidates(
    con: sqlite3.Connection,
    statuses: list[str],
    limit: int,
) -> list[CandidateRow]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    placeholders = ",".join("?" for _ in statuses)

    rows = cur.execute(
        f"""
        WITH latest_scores AS (
            SELECT
                fsh.*,
                ROW_NUMBER() OVER (
                    PARTITION BY fsh.object_id, fsh.candid
                    ORDER BY fsh.score_utc DESC, fsh.score_id DESC
                ) AS rn
            FROM followup_score_history fsh
        )
        SELECT
            fq.object_id,
            fq.candid,
            fq.report_id,
            fq.tns_name,
            fq.status,
            fq.priority_bucket,
            fq.submitted_utc,
            fq.current_score,
            fq.best_score,
            ls.score_version,
            ls.current_mag,
            ls.days_since_nondet,
            ls.nmtchps,
            ls.distpsnr1,
            ls.srmag1,
            ls.score_breakdown_json
        FROM followup_queue fq
        LEFT JOIN latest_scores ls
          ON fq.object_id = ls.object_id
         AND fq.candid = ls.candid
         AND ls.rn = 1
        WHERE fq.status IN ({placeholders})
        ORDER BY
            CASE fq.status
                WHEN 'actionable_now' THEN 0
                WHEN 'actionable_backup' THEN 1
                WHEN 'watch_high' THEN 2
                ELSE 3
            END,
            fq.current_score DESC,
            fq.submitted_utc DESC
        LIMIT ?
        """,
        (*statuses, limit),
    ).fetchall()

    result: list[CandidateRow] = []
    for row in rows:
        payload = {}
        if row["score_breakdown_json"]:
            try:
                payload = json.loads(row["score_breakdown_json"])
            except json.JSONDecodeError:
                payload = {}

        obs = payload.get("observability", {})
        evidence = payload.get("evidence", {})
        components = payload.get("components", {})

        candidate = CandidateRow(
            object_id=row["object_id"],
            candid=row["candid"],
            report_id=row["report_id"],
            tns_name=row["tns_name"],
            status=row["status"],
            priority_bucket=row["priority_bucket"],
            submitted_utc=row["submitted_utc"],
            current_score=_to_float(row["current_score"]),
            best_score=_to_float(row["best_score"]),
            current_mag=_to_float(row["current_mag"]),
            days_since_nondet=_to_float(row["days_since_nondet"]),
            nmtchps=_to_int(row["nmtchps"]),
            distpsnr1=_to_float(row["distpsnr1"]),
            srmag1=_to_float(row["srmag1"]),
            observability_score=_to_float(components.get("observability_score")),
            max_alt_dark_deg=_to_float(obs.get("max_alt_dark_deg")),
            hours_above_threshold_dark=_to_float(obs.get("hours_above_threshold_dark")),
            effective_evidence_count=_to_int(evidence.get("effective_evidence_count")) or 0,
            survey_evidence_epoch_count=_to_int(evidence.get("survey_evidence_epoch_count")) or 0,
            manual_phot_evidence_count=_to_int(evidence.get("manual_phot_evidence_count")) or 0,
            score_version=row["score_version"],
            recommendation=build_recommendation(
                status=row["status"],
                current_mag=_to_float(row["current_mag"]),
                obs_score=_to_float(components.get("observability_score")),
                evidence_count=_to_int(evidence.get("effective_evidence_count")) or 0,
            ),
            raw_payload=payload,
        )
        result.append(candidate)

    return result


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_recommendation(
    status: str,
    current_mag: float | None,
    obs_score: float | None,
    evidence_count: int,
) -> str:
    if status == "actionable_now":
        return "ACT NOW: revisar hoy, preparar ventana observacional y decidir fotometría/espectro."
    if status == "actionable_backup":
        return "BACKUP: mantener listo por si cae el actionable_now o mejora evidencia."
    if status == "watch_high":
        if evidence_count <= 0:
            return "WATCH HIGH: buen candidato científico, pero aún sin evidencia nueva post-report."
        if current_mag is not None and current_mag <= 17.5 and (obs_score or 0) >= 5:
            return "WATCH HIGH+: seguir de cerca; si mejora o entra evidencia adicional, puede escalar."
        return "WATCH HIGH: interesante, pero todavía no merece acción inmediata."
    return "WATCH: mantener en cola y reevaluar tras refresh."


def group_by_status(rows: list[CandidateRow]) -> dict[str, list[CandidateRow]]:
    grouped: dict[str, list[CandidateRow]] = {}
    for row in rows:
        grouped.setdefault(row.status, []).append(row)
    return grouped


def fmt(value: Any, digits: int = 1) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def render_report(
    db_path: Path,
    status_counts: list[tuple[str, int]],
    rows: list[CandidateRow],
    statuses_requested: list[str],
) -> str:
    grouped = group_by_status(rows)

    lines: list[str] = []
    lines.append("# FirstLight Follow-up Daily Report")
    lines.append("")
    lines.append(f"DB: `{db_path}`")
    lines.append("")
    lines.append("## Queue status counts")
    for status, count in status_counts:
        lines.append(f"- {status}: {count}")
    lines.append("")

    if not rows:
        lines.append("No rows found for requested statuses.")
        return "\n".join(lines)

    for status in statuses_requested:
        section_rows = grouped.get(status, [])
        lines.append(f"## {status} ({len(section_rows)})")
        lines.append("")
        if not section_rows:
            lines.append("No candidates in this section.")
            lines.append("")
            continue

        for idx, row in enumerate(section_rows, start=1):
            lines.append(f"### {idx}. {row.object_id}")
            lines.append("")
            lines.append(f"- candid: `{row.candid}`")
            lines.append(f"- report_id: `{row.report_id or '-'}`")
            lines.append(f"- tns_name: `{row.tns_name or '-'}`")
            lines.append(f"- submitted_utc: `{row.submitted_utc or '-'}`")
            lines.append(f"- score: `{fmt(row.current_score)}` (best: `{fmt(row.best_score)}`)")
            lines.append(f"- mag: `{fmt(row.current_mag, 3)}`")
            lines.append(f"- days_since_nondet: `{fmt(row.days_since_nondet, 3)}`")
            lines.append(f"- nmtchps: `{fmt(row.nmtchps, 0)}`")
            lines.append(f"- distpsnr1: `{fmt(row.distpsnr1, 3)}`")
            lines.append(f"- srmag1: `{fmt(row.srmag1, 3)}`")
            lines.append(f"- observability_score: `{fmt(row.observability_score)}`")
            lines.append(f"- max_alt_dark_deg: `{fmt(row.max_alt_dark_deg, 2)}`")
            lines.append(f"- hours_above_threshold_dark: `{fmt(row.hours_above_threshold_dark, 3)}`")
            lines.append(
                f"- evidence: effective=`{row.effective_evidence_count}` "
                f"(survey=`{row.survey_evidence_epoch_count}`, manual_phot=`{row.manual_phot_evidence_count}`)"
            )
            lines.append(f"- priority_bucket: `{row.priority_bucket or '-'}`")
            lines.append(f"- score_version: `{row.score_version or '-'}`")
            lines.append(f"- recommendation: {row.recommendation}")
            lines.append("")

    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    con = sqlite3.connect(db_path)
    try:
        ensure_tables_exist(con)
        status_counts = get_status_counts(con)
        rows = get_latest_candidates(con, args.status, args.limit)
        report = render_report(db_path, status_counts, rows, args.status)

        print(report)

        if args.out:
            out_path = Path(args.out)
            out_path.write_text(report, encoding="utf-8")
            print(f"\n[written] {out_path}")

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
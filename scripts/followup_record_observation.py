from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOWED_KINDS = {
    "imaging_detection",
    "imaging_nondetection",
    "failed_attempt",
    "spectroscopy_attempt",
    "spectroscopy_success",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record a manual/remote follow-up observation in followup_observations."
    )
    parser.add_argument("--db", required=True, help="Path to follow-up SQLite DB.")
    parser.add_argument("--object-id", required=True, help="ZTF object id.")
    parser.add_argument("--candid", required=True, help="Candidate id associated with the object.")
    parser.add_argument(
        "--kind",
        required=True,
        choices=sorted(ALLOWED_KINDS),
        help="Observation kind.",
    )
    parser.add_argument(
        "--obs-utc",
        help="Observation UTC timestamp in ISO format. Default: now UTC.",
    )
    parser.add_argument("--facility", default="remote", help="Facility / telescope provider.")
    parser.add_argument("--instrument", default="", help="Instrument name.")
    parser.add_argument("--band", default="", help="Band or filter name.")
    parser.add_argument("--mag", type=float, help="Measured magnitude for a detection.")
    parser.add_argument("--mag-err", type=float, help="Magnitude uncertainty.")
    parser.add_argument("--limiting-mag", type=float, help="Limiting magnitude for a nondetection.")
    parser.add_argument("--snr", type=float, help="Signal-to-noise ratio.")
    parser.add_argument("--exposure-s", type=float, help="Exposure time in seconds.")
    parser.add_argument("--observer", default="Marc Balboa Corominas", help="Observer name.")
    parser.add_argument("--notes", default="", help="Free-text notes.")
    parser.add_argument(
        "--set-status",
        help="Optional manual new status in followup_queue (example: actionable_now, watch_high, closed).",
    )
    parser.add_argument(
        "--priority-bucket",
        default=None,
        help="Optional priority bucket if --set-status is used.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the insert/update without writing.",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_obs_utc(value: str | None) -> str:
    if not value:
        return utc_now_iso()
    # Accept a trailing Z and normalize to ISO with offset if possible
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    except Exception as exc:
        raise ValueError(f"Invalid --obs-utc value: {value}") from exc


def safe_json_dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


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


def classify_kind(kind: str) -> dict[str, Any]:
    if kind == "imaging_detection":
        return {
            "observation_type": "manual_photometry",
            "observation_kind": "photometry",
            "observation_class": "manual_followup",
            "status": "success",
            "outcome": "detection",
            "is_detection": 1,
            "is_success": 1,
            "manual_followup": 1,
        }

    if kind == "imaging_nondetection":
        return {
            "observation_type": "manual_photometry",
            "observation_kind": "photometry",
            "observation_class": "manual_followup",
            "status": "success",
            "outcome": "nondetection",
            "is_detection": 0,
            "is_success": 1,
            "manual_followup": 1,
        }

    if kind == "failed_attempt":
        return {
            "observation_type": "manual_followup_attempt",
            "observation_kind": "attempt",
            "observation_class": "manual_followup",
            "status": "failed",
            "outcome": "failed_attempt",
            "is_detection": 0,
            "is_success": 0,
            "manual_followup": 1,
        }

    if kind == "spectroscopy_attempt":
        return {
            "observation_type": "manual_spectroscopy",
            "observation_kind": "spectroscopy",
            "observation_class": "manual_followup",
            "status": "attempted",
            "outcome": "spectroscopy_attempt",
            "is_detection": 0,
            "is_success": 1,
            "manual_followup": 1,
        }

    if kind == "spectroscopy_success":
        return {
            "observation_type": "manual_spectroscopy",
            "observation_kind": "spectroscopy",
            "observation_class": "manual_followup",
            "status": "success",
            "outcome": "spectroscopy_success",
            "is_detection": 0,
            "is_success": 1,
            "manual_followup": 1,
        }

    raise ValueError(f"Unsupported kind: {kind}")


def build_observation_payload(
    args: argparse.Namespace,
    queue_row: sqlite3.Row,
    obs_utc: str,
) -> dict[str, Any]:
    meta = classify_kind(args.kind)

    payload = {
        "object_id": args.object_id,
        "candid": args.candid,
        "queue_id": queue_row["queue_id"] if "queue_id" in queue_row.keys() else None,
        "report_id": queue_row["report_id"] if "report_id" in queue_row.keys() else None,
        "tns_name": queue_row["tns_name"] if "tns_name" in queue_row.keys() else None,
        "obs_utc": obs_utc,
        "observation_utc": obs_utc,
        "created_utc": obs_utc,
        "action_utc": obs_utc,
        "source": "manual_remote",
        "observer": args.observer,
        "facility": args.facility,
        "instrument": args.instrument or None,
        "band": args.band or None,
        "filter_name": args.band or None,
        "mag": args.mag,
        "mag_err": args.mag_err,
        "limiting_mag": args.limiting_mag,
        "snr": args.snr,
        "exposure_s": args.exposure_s,
        "notes": args.notes or None,
        "remarks": args.notes or None,
        "payload_json": safe_json_dumps(
            {
                "kind": args.kind,
                "observer": args.observer,
                "facility": args.facility,
                "instrument": args.instrument,
                "band": args.band,
                "mag": args.mag,
                "mag_err": args.mag_err,
                "limiting_mag": args.limiting_mag,
                "snr": args.snr,
                "exposure_s": args.exposure_s,
                "notes": args.notes,
            }
        ),
        **meta,
    }

    return payload


def insert_observation_row(
    con: sqlite3.Connection,
    payload: dict[str, Any],
) -> tuple[list[str], list[Any]]:
    cols = get_table_columns(con, "followup_observations")
    if not cols:
        raise RuntimeError("Table followup_observations exists but has no columns.")

    chosen_cols: list[str] = []
    chosen_vals: list[Any] = []

    for key, value in payload.items():
        if key in cols:
            chosen_cols.append(key)
            chosen_vals.append(value)

    required_intersection = {"object_id", "candid"} & set(chosen_cols)
    if required_intersection != {"object_id", "candid"}:
        raise RuntimeError(
            "followup_observations does not expose object_id/candid in a usable way."
        )

    placeholders = ", ".join(["?"] * len(chosen_cols))
    sql = (
        f"INSERT INTO followup_observations ({', '.join(chosen_cols)}) "
        f"VALUES ({placeholders})"
    )

    cur = con.cursor()
    cur.execute(sql, chosen_vals)
    return chosen_cols, chosen_vals


def update_queue_manual_status(
    con: sqlite3.Connection,
    queue_row: sqlite3.Row,
    new_status: str | None,
    priority_bucket: str | None,
    review_utc: str,
) -> tuple[str | None, str | None]:
    if not new_status:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE followup_queue
            SET last_review_utc = ?
            WHERE queue_id = ?
            """,
            (review_utc, queue_row["queue_id"]),
        )
        return None, None

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

    if priority_bucket is not None and "priority_bucket" in cols:
        assignments.append("priority_bucket = ?")
        values.append(priority_bucket)

    if "last_review_utc" in cols:
        assignments.append("last_review_utc = ?")
        values.append(review_utc)

    if "promotion_reason" in cols:
        assignments.append("promotion_reason = ?")
        values.append(f"manual observation recorded at {review_utc}")

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
    action_utc: str,
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
            action_utc,
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

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    obs_utc = normalize_obs_utc(args.obs_utc)

    con = sqlite3.connect(db_path)
    try:
        if not table_exists(con, "followup_queue"):
            raise RuntimeError("Missing table: followup_queue")
        if not table_exists(con, "followup_observations"):
            raise RuntimeError("Missing table: followup_observations")

        queue_row = fetch_queue_row(con, args.object_id, args.candid)

        payload = build_observation_payload(args, queue_row, obs_utc)

        if args.kind == "imaging_detection" and args.mag is None:
            print("WARNING: imaging_detection without --mag")
        if args.kind == "imaging_nondetection" and args.limiting_mag is None:
            print("WARNING: imaging_nondetection without --limiting-mag")

        print(
            f"candidate object_id={args.object_id} candid={args.candid} "
            f"current_status={queue_row['status']} current_priority={queue_row['priority_bucket']}"
        )
        print(f"obs_utc={obs_utc} kind={args.kind}")
        print(f"facility={args.facility} instrument={args.instrument or '-'} band={args.band or '-'}")
        print(
            f"mag={args.mag} mag_err={args.mag_err} limiting_mag={args.limiting_mag} "
            f"snr={args.snr} exposure_s={args.exposure_s}"
        )
        print(f"notes={args.notes or '-'}")

        if args.dry_run:
            cols = get_table_columns(con, "followup_observations")
            chosen = {k: v for k, v in payload.items() if k in cols}
            print("dry_run=True -> would insert into followup_observations:")
            for k in sorted(chosen.keys()):
                print(f"  {k}={chosen[k]!r}")

            if args.set_status:
                print(
                    f"dry_run=True -> would set followup_queue status to {args.set_status!r}"
                    + (
                        f" with priority_bucket={args.priority_bucket!r}"
                        if args.priority_bucket is not None
                        else ""
                    )
                )

            return 0

        chosen_cols, chosen_vals = insert_observation_row(con, payload)

        old_status, old_priority = update_queue_manual_status(
            con=con,
            queue_row=queue_row,
            new_status=args.set_status,
            priority_bucket=args.priority_bucket,
            review_utc=obs_utc,
        )

        action_payload = {
            "kind": args.kind,
            "obs_utc": obs_utc,
            "facility": args.facility,
            "instrument": args.instrument,
            "band": args.band,
            "mag": args.mag,
            "mag_err": args.mag_err,
            "limiting_mag": args.limiting_mag,
            "snr": args.snr,
            "exposure_s": args.exposure_s,
            "notes": args.notes,
            "set_status": args.set_status,
            "priority_bucket": args.priority_bucket,
            "inserted_columns": chosen_cols,
        }

        if args.set_status:
            action_type = "manual_observation_and_status_change"
            action_reason = (
                f"manual observation logged ({args.kind}) and status set to {args.set_status}"
            )
            new_status_for_action = args.set_status
        else:
            action_type = "manual_observation_logged"
            action_reason = f"manual observation logged ({args.kind})"
            new_status_for_action = old_status

        insert_followup_action(
            con=con,
            queue_row=queue_row,
            action_utc=obs_utc,
            action_type=action_type,
            old_status=old_status,
            new_status=new_status_for_action,
            action_reason=action_reason,
            payload=action_payload,
        )

        con.commit()

        print("observation_inserted=True")
        print(f"inserted_columns={chosen_cols}")
        print(
            f"queue_status_after={args.set_status if args.set_status else queue_row['status']}"
        )
        print(
            f"queue_priority_after={args.priority_bucket if args.priority_bucket is not None else queue_row['priority_bucket']}"
        )
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
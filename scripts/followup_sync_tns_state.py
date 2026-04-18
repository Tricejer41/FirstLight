# scripts/followup_sync_tns_state.py
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


LEGACY_TNS_NAME_RE = re.compile(r"^\d{4}[A-Za-z]{3,}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync TNS state into follow-up queue and normalize legacy AT-like identifiers."
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
        "tns_report_state",
        "followup_queue",
        "followup_actions",
    }
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name IN ('tns_report_state', 'followup_queue', 'followup_actions')
        """
    ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(
            f"Missing required tables: {sorted(missing)}. "
            "Apply schema and backfill first."
        )


def infer_tns_name_from_report_id(report_id: str | None) -> str | None:
    if not report_id:
        return None
    report_id = report_id.strip()
    if LEGACY_TNS_NAME_RE.match(report_id):
        return f"AT {report_id.lower()}"
    return None


def build_tns_url(tns_name: str | None) -> str | None:
    if not tns_name:
        return None
    parts = tns_name.strip().split()
    if len(parts) == 2 and parts[0].upper() == "AT":
        slug = parts[1]
    else:
        slug = tns_name.strip()
    return f"https://www.wis-tns.org/object/{slug}"


def fetch_report_state_rows(con: sqlite3.Connection) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT
            report_id,
            object_id,
            candid,
            submitted_utc,
            submit_status,
            tns_name,
            public_utc,
            reply_status,
            classification_status,
            tns_url,
            certificate_url,
            last_checked_utc,
            raw_reply_json
        FROM tns_report_state
        ORDER BY submitted_utc DESC
        """
    ).fetchall()
    return rows


def update_tns_report_state_if_needed(
    con: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[bool, str | None]:
    cur = con.cursor()

    inferred_tns_name = infer_tns_name_from_report_id(row["report_id"])
    current_tns_name = row["tns_name"]
    current_reply_status = row["reply_status"]
    current_tns_url = row["tns_url"]

    new_tns_name = current_tns_name
    new_reply_status = current_reply_status
    new_tns_url = current_tns_url

    reason_parts: list[str] = []

    if not current_tns_name and inferred_tns_name:
        new_tns_name = inferred_tns_name
        reason_parts.append(f"inferred tns_name from legacy report_id={row['report_id']}")

    if new_tns_name and current_reply_status == "pending":
        new_reply_status = "resolved"
        reason_parts.append("set reply_status=resolved because TNS name is known")

    if new_tns_name and not current_tns_url:
        new_tns_url = build_tns_url(new_tns_name)
        reason_parts.append("built tns_url from tns_name")

    changed = (
        new_tns_name != current_tns_name
        or new_reply_status != current_reply_status
        or new_tns_url != current_tns_url
    )

    if not changed:
        return False, None

    cur.execute(
        """
        UPDATE tns_report_state
        SET tns_name = ?,
            reply_status = ?,
            tns_url = ?,
            last_checked_utc = ?
        WHERE report_id = ?
        """,
        (
            new_tns_name,
            new_reply_status,
            new_tns_url,
            datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            row["report_id"],
        ),
    )

    return True, " | ".join(reason_parts)


def fetch_matching_queue_row(
    con: sqlite3.Connection,
    object_id: str,
    candid: str,
) -> sqlite3.Row | None:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        """
        SELECT
            queue_id,
            object_id,
            candid,
            report_id,
            tns_name,
            status,
            priority_bucket,
            external_classification,
            external_classification_label
        FROM followup_queue
        WHERE object_id = ?
          AND candid = ?
        LIMIT 1
        """,
        (object_id, candid),
    ).fetchone()
    return row


def classify_queue_update(
    queue_row: sqlite3.Row,
    state_row: sqlite3.Row,
) -> tuple[dict[str, object], str | None]:
    new_values: dict[str, object] = {}
    reasons: list[str] = []

    if state_row["report_id"] and queue_row["report_id"] != state_row["report_id"]:
        new_values["report_id"] = state_row["report_id"]
        reasons.append("synced report_id from tns_report_state")

    if state_row["tns_name"] and queue_row["tns_name"] != state_row["tns_name"]:
        new_values["tns_name"] = state_row["tns_name"]
        reasons.append("synced tns_name from tns_report_state")

    classification_status = (state_row["classification_status"] or "unknown").strip().lower()

    if classification_status in {"classified", "classified_by_others", "classified_by_me"}:
        if int(queue_row["external_classification"] or 0) != 1:
            new_values["external_classification"] = 1
            reasons.append("set external_classification=1 from classification_status")

        if queue_row["external_classification_label"] != classification_status:
            new_values["external_classification_label"] = classification_status
            reasons.append("synced external_classification_label")

        if classification_status == "classified_by_me":
            if queue_row["status"] != "classified":
                new_values["status"] = "classified"
                reasons.append("set queue status=classified")
        else:
            if queue_row["status"] != "classified_by_others":
                new_values["status"] = "classified_by_others"
                reasons.append("set queue status=classified_by_others")

    if not new_values:
        return {}, None

    return new_values, " | ".join(reasons)


def apply_queue_update(
    con: sqlite3.Connection,
    queue_row: sqlite3.Row,
    updates: dict[str, object],
) -> None:
    cur = con.cursor()

    fields = []
    values = []
    for key, value in updates.items():
        fields.append(f"{key} = ?")
        values.append(value)

    fields.append("last_review_utc = ?")
    values.append(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
    values.append(queue_row["queue_id"])

    sql = f"""
    UPDATE followup_queue
    SET {", ".join(fields)}
    WHERE queue_id = ?
    """
    cur.execute(sql, values)


def insert_action(
    con: sqlite3.Connection,
    object_id: str,
    candid: str,
    action_type: str,
    old_status: str | None,
    new_status: str | None,
    action_reason: str,
    payload: dict[str, object],
) -> None:
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
            object_id,
            candid,
            datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
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
        rows = fetch_report_state_rows(con)

        print(f"tns_report_state_rows={len(rows)}")

        report_state_updates = 0
        queue_updates = 0
        queue_status_changes = 0
        preview_lines: list[str] = []

        for state_row in rows:
            state_changed = False
            state_reason = None

            if not args.dry_run:
                state_changed, state_reason = update_tns_report_state_if_needed(con, state_row)
            else:
                inferred_name = infer_tns_name_from_report_id(state_row["report_id"])
                if (not state_row["tns_name"]) and inferred_name:
                    state_changed = True
                    state_reason = f"would infer tns_name from legacy report_id={state_row['report_id']}"

            if state_changed:
                report_state_updates += 1
                preview_lines.append(
                    f"TNS_STATE_UPDATE report_id={state_row['report_id']} object_id={state_row['object_id']} reason={state_reason}"
                )

            # Re-read state if it changed for consistent queue sync
            if not args.dry_run and state_changed:
                refreshed = con.execute(
                    """
                    SELECT
                        report_id,
                        object_id,
                        candid,
                        submitted_utc,
                        submit_status,
                        tns_name,
                        public_utc,
                        reply_status,
                        classification_status,
                        tns_url,
                        certificate_url,
                        last_checked_utc,
                        raw_reply_json
                    FROM tns_report_state
                    WHERE report_id = ?
                    """,
                    (state_row["report_id"],),
                ).fetchone()
                if refreshed is not None:
                    state_row = refreshed

            queue_row = fetch_matching_queue_row(con, state_row["object_id"], state_row["candid"])
            if queue_row is None:
                continue

            updates, queue_reason = classify_queue_update(queue_row, state_row)
            if updates:
                old_status = queue_row["status"]
                new_status = updates.get("status", old_status)

                queue_updates += 1
                if new_status != old_status:
                    queue_status_changes += 1

                preview_lines.append(
                    f"QUEUE_SYNC object_id={queue_row['object_id']} candid={queue_row['candid']} "
                    f"old_status={old_status} new_status={new_status} reason={queue_reason}"
                )

                if not args.dry_run:
                    apply_queue_update(con, queue_row, updates)
                    action_type = "status_change" if new_status != old_status else "note"
                    insert_action(
                        con,
                        queue_row["object_id"],
                        queue_row["candid"],
                        action_type,
                        old_status,
                        str(new_status),
                        queue_reason or "",
                        {
                            "report_id": state_row["report_id"],
                            "tns_name": state_row["tns_name"],
                            "reply_status": state_row["reply_status"],
                            "classification_status": state_row["classification_status"],
                            "tns_url": state_row["tns_url"],
                            "updates": updates,
                        },
                    )

        if args.dry_run:
            for line in preview_lines[:40]:
                print(line)
            print(
                f"report_state_updates={report_state_updates} "
                f"queue_updates={queue_updates} "
                f"queue_status_changes={queue_status_changes}"
            )
            print("dry_run=True -> no changes written")
            return 0

        con.commit()

        cur = con.cursor()
        count_with_tns_name = cur.execute(
            "SELECT COUNT(*) FROM tns_report_state WHERE tns_name IS NOT NULL"
        ).fetchone()[0]
        queue_with_tns_name = cur.execute(
            "SELECT COUNT(*) FROM followup_queue WHERE tns_name IS NOT NULL"
        ).fetchone()[0]

        print(
            f"report_state_updates={report_state_updates} "
            f"queue_updates={queue_updates} "
            f"queue_status_changes={queue_status_changes}"
        )
        print(f"tns_report_state_with_tns_name={count_with_tns_name}")
        print(f"followup_queue_with_tns_name={queue_with_tns_name}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
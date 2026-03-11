# src/firstlight/cli.py
"""
CLI entrypoint for FirstLight.

Usage:
  python -m firstlight tns envcheck
  python -m firstlight --env .env tns test-auth
  python -m firstlight --env .env tns submit-min
  python -m firstlight --env .env tns submit-min --print-payload
  python -m firstlight --env .env tns reply <REPORT_ID> [--raw]
  python -m firstlight --env .env tns dispatch-sandbox --db firstlight.sqlite --since-hours 24 --max-submit 3 [--dry-run] [--print-payload]
  python -m firstlight --env .env tns sweep-replies --db firstlight.sqlite --since-hours 24 --max 50

Exit codes (dispatch-sandbox):
  0  -> OK (reply_failed may be warnings)
  10 -> AUTH_FATAL (stop dispatching; fix env/api key)
  11 -> RETRYABLE/FAIL (submit failures; retry later)
  12 -> INTERNAL_ERROR
  21 -> CAP_REACHED (night can stop early if desired)
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

from firstlight.tns.client import TNSClient
from firstlight.tns.dispatch import dispatch_sandbox


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def _load_env(path: Optional[str]) -> None:
    if not path:
        return
    if load_dotenv is None:
        return
    # IMPORTANT: override=True so .env changes actually take effect.
    load_dotenv(dotenv_path=path, override=True)


def _send_enabled() -> bool:
    """Kill-switch: allow real TNS submissions only when explicitly enabled."""
    import os

    v = (os.getenv("FIRSTLIGHT_TNS_SEND", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _cmd_tns_envcheck(args: argparse.Namespace) -> int:
    _load_env(args.env)
    c = TNSClient.from_env()
    print(_safe_json(c.envcheck_dict(show_ua=args.show_ua)))
    return 0


def _cmd_tns_test_auth(args: argparse.Namespace) -> int:
    _load_env(args.env)
    c = TNSClient.from_env()
    ok, detail, _raw = c.test_auth()
    print(f"test_url: {c.test_url}")
    print(f"result: ok={ok} detail={detail}")
    return 0 if ok else 2


def _cmd_tns_submit_min(args: argparse.Namespace) -> int:
    _load_env(args.env)
    c = TNSClient.from_env()

    if args.print_payload:
        print(_safe_json(c.build_submit_min_payload()))
        return 0

    if not _send_enabled():
        print("SEND_DISABLED: set FIRSTLIGHT_TNS_SEND=1 to allow real submissions.")
        return 3

    print(f"submit_url: {c.submit_url}")
    ok, detail, report_id, _raw = c.submit_min()
    print(f"result: ok={ok} detail={detail} report_id={report_id}")
    if report_id is not None:
        print("NOTE: Run `tns reply <REPORT_ID> --raw` to see validation feedback and converge schema.")
    return 0 if ok else 2


def _cmd_tns_reply(args: argparse.Namespace) -> int:
    _load_env(args.env)
    c = TNSClient.from_env()

    print(f"reply_url: {c.reply_url}")

    ok, detail, reply_json = c.reply(report_id=args.report_id, wait_s=args.wait_s, poll_s=args.poll_s)
    print(f"result: ok={ok} detail={detail}")
    if args.raw:
        print("reply:")
        print(_safe_json(reply_json))
    return 0 if ok else 2


def _cmd_tns_sweep_replies(args: argparse.Namespace) -> int:
    """
    Fast morning sweep: checks replies for recently submitted report_id
    without blocking nights (no long polling).
    """
    _load_env(args.env)
    c = TNSClient.from_env()

    db = args.db
    since_hours = float(args.since_hours)
    max_n = int(args.max)

    cut = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).replace(microsecond=0).isoformat()

    con = sqlite3.connect(db)
    try:
        rows = con.execute(
            """
            select distinct report_id
            from tns_actions
            where created_utc>=?
              and action='submitted'
              and report_id is not null
              and trim(report_id) != ''
            order by created_utc desc
            limit ?
            """,
            (cut, max_n),
        ).fetchall()
    finally:
        con.close()

    if not rows:
        print(f"sweep-replies: no submitted report_id in last {since_hours}h")
        return 0

    fatal_auth = 0
    ok_cnt = 0
    pending_404 = 0
    other_fail = 0

    for (rid,) in rows:
        ok, detail, _reply_json = c.reply_fast(rid)
        print(f"- report_id={rid}: {detail}")

        if "fatal=auth" in (detail or ""):
            fatal_auth += 1
        elif ok:
            ok_cnt += 1
        elif "not_ready http=404" in (detail or "") or "not_ready" in (detail or ""):
            pending_404 += 1
        else:
            other_fail += 1

    print(f"done: ok={ok_cnt} pending_404={pending_404} other_fail={other_fail} fatal_auth={fatal_auth}")

    if fatal_auth:
        return 10
    if other_fail:
        return 11
    return 0


def _cmd_tns_dispatch_sandbox(args: argparse.Namespace) -> int:
    """
    Stability rules:
    - Never block the whole night waiting for replies (short wait_s; hard timeout in run_night).
    - Stop early on AUTH_FATAL (avoid spamming 401s).
    - Retry later on transient submit failures (exit code != 0, but not auth fatal).
    - Always log into tns_actions via DB.tns_log in dispatch.py.
    """
    _load_env(args.env)

    print(
        f"dispatch: db={args.db} since_hours={args.since_hours} max_submit={args.max_submit} "
        f"max_attempts={args.max_attempts} topic={args.topic!r} dry_run={args.dry_run} print_payload={args.print_payload} "
        f"skip_reply={args.skip_reply} wait_s={args.wait_s} poll_s={args.poll_s}"
    )

    try:
        res = dispatch_sandbox(
            db_path=args.db,
            since_hours=args.since_hours,
            max_submit=args.max_submit,
            max_attempts=args.max_attempts,
            dry_run=bool(args.dry_run),
            print_payload=bool(args.print_payload),
            topic=args.topic,
            skip_reply=bool(args.skip_reply),
            wait_s=int(args.wait_s),
            poll_s=int(args.poll_s),
        )
    except Exception as e:
        print(f"INTERNAL_ERROR: {e}")
        return 12

    items = res.get("items", []) or []
    for it in items:
        objid = it.get("objectId")
        candid = it.get("candid")
        topic = it.get("topic")
        ok = it.get("ok")
        detail = it.get("detail")
        rid = it.get("report_id")
        payload = it.get("payload")
        if rid is not None:
            print(f"- {objid} candid={candid} topic={topic}: ok={ok} report_id={rid} detail={detail}")
        else:
            print(f"- {objid} candid={candid} topic={topic}: ok={ok} detail={detail}")
        if payload is not None:
            print("  payload:")
            print(_safe_json(payload))

    print(
        f"done: candidates={res.get('candidates')} submitted={res.get('submitted')} "
        f"failed_submit={res.get('failed_submit')} reply_failed={res.get('reply_failed')} "
        f"submitted_existing={res.get('submitted_existing')} submitted_total={res.get('submitted_total')} "
        f"aborted_auth={res.get('aborted_auth')} aborted_transient={res.get('aborted_transient')} "
        f"aborted_send_disabled={res.get('aborted_send_disabled')} detail={res.get('detail')}"
    )

    detail_s = str(res.get("detail") or "")

    if detail_s.lower().startswith("cap reached:"):
        print("CAP_REACHED")
        return 21

    if res.get("aborted_send_disabled"):
        print("SEND_DISABLED")
        return 0

    if res.get("aborted_auth"):
        print("AUTH_FATAL")
        print("NOTE: AUTH_FATAL detected -> stop dispatching and fix .env / API key.")
        return 10

    if (res.get("failed_submit") or 0) > 0 or res.get("aborted_transient"):
        return 11

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="firstlight")
    p.add_argument("--env", default=None, help="Path to .env file (optional).")

    sub = p.add_subparsers(dest="cmd", required=True)

    # ----------------------
    # TNS group
    # ----------------------
    tns = sub.add_parser("tns", help="TNS utilities")
    tns_sub = tns.add_subparsers(dest="tns_cmd", required=True)

    p_envcheck = tns_sub.add_parser("envcheck", help="Print env/runtime configuration used by the TNS client")
    p_envcheck.add_argument("--show-ua", action="store_true", help="Include the full user agent in output")
    p_envcheck.set_defaults(func=_cmd_tns_envcheck)

    p_test = tns_sub.add_parser("test-auth", help="Hit TNS /test endpoint to verify API key (advisory)")
    p_test.set_defaults(func=_cmd_tns_test_auth)

    p_submit = tns_sub.add_parser("submit-min", help="Submit a minimal test payload (for schema convergence)")
    p_submit.add_argument("--print-payload", action="store_true", help="Print the exact payload (no network)")
    p_submit.set_defaults(func=_cmd_tns_submit_min)

    p_reply = tns_sub.add_parser("reply", help="Fetch reply/validation feedback for a report_id")
    p_reply.add_argument("report_id", help="TNS bulk report_id")
    p_reply.add_argument("--raw", action="store_true", help="Print the full reply JSON")
    p_reply.add_argument("--wait-s", type=int, default=600, help="Max time to wait for reply on 404 (seconds)")
    p_reply.add_argument("--poll-s", type=int, default=10, help="Polling interval while waiting (seconds)")
    p_reply.set_defaults(func=_cmd_tns_reply)

    p_sweep = tns_sub.add_parser("sweep-replies", help="Fast sweep: check replies for recently submitted report_id")
    p_sweep.add_argument("--db", required=True, help="Path to sqlite DB")
    p_sweep.add_argument("--since-hours", type=float, default=24.0, help="Look back window (hours)")
    p_sweep.add_argument("--max", type=int, default=50, help="Max report_id to check")
    p_sweep.set_defaults(func=_cmd_tns_sweep_replies)

    p_dispatch = tns_sub.add_parser(
        "dispatch-sandbox",
        help="Dispatch passed DB candidates to TNS (sandbox/prod depends on TNS_API_URL)",
    )
    p_dispatch.add_argument("--db", required=True, help="Path to sqlite DB (e.g. firstlight.sqlite)")
    p_dispatch.add_argument("--since-hours", type=float, default=24.0, help="Only consider passed decisions in last N hours")
    p_dispatch.add_argument("--max-submit", type=int, default=3, help="Max number of candidates to submit")
    p_dispatch.add_argument("--max-attempts", type=int, default=None, help="Guardrail: max candidates to attempt (default=max_submit*5)")
    p_dispatch.add_argument("--topic", default=None, help="Optional topic filter (e.g. n1). If omitted, any topic.")
    p_dispatch.add_argument("--dry-run", action="store_true", help="Do not submit; just print what would be done")
    p_dispatch.add_argument("--print-payload", action="store_true", help="In dry-run, also build+print the exact payload that would be submitted")
    p_dispatch.add_argument("--skip-reply", action="store_true", help="Submit only; do not poll reply (most stable)")
    p_dispatch.add_argument("--wait-s", type=int, default=60, help="Max time to wait for reply after submit (seconds)")
    p_dispatch.add_argument("--poll-s", type=int, default=5, help="Polling interval while waiting (seconds)")
    p_dispatch.set_defaults(func=_cmd_tns_dispatch_sandbox)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
"""
CLI entrypoint for FirstLight.

Usage:
  python -m firstlight tns envcheck
  python -m firstlight --env .env tns submit-min
  python -m firstlight --env .env tns reply <REPORT_ID> [--raw]
  python -m firstlight --env .env tns dispatch-sandbox --db firstlight.sqlite --since-hours 24 --max-submit 3 [--dry-run]

Notes:
- `--env .env` is handled by python-dotenv if installed; otherwise the file is ignored.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

from firstlight.storage.db import DB
from firstlight.tns.client import TNSClient


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
    load_dotenv(dotenv_path=path, override=False)


def _cmd_tns_envcheck(args: argparse.Namespace) -> int:
    _load_env(args.env)
    c = TNSClient.from_env()
    print(_safe_json(c.envcheck_dict(show_ua=args.show_ua)))
    return 0


def _cmd_tns_submit_min(args: argparse.Namespace) -> int:
    _load_env(args.env)
    c = TNSClient.from_env()

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


def _cmd_tns_dispatch_sandbox(args: argparse.Namespace) -> int:
    """
    Dispatch candidates stored in DB to TNS.

    "Sandbox" here is only a naming convention: this uses whatever `TNS_API_URL`
    is set in your env. You choose sandbox vs prod by the env.
    """
    _load_env(args.env)
    c = TNSClient.from_env()
    db = DB(args.db)

    # Defensive: these methods must exist (you added them recently).
    if not hasattr(c, "build_at_report_from_fink_payload"):
        print("ERROR: TNSClient is missing build_at_report_from_fink_payload(). Update src/firstlight/tns/client.py.")
        return 2
    if not hasattr(c, "submit_and_reply"):
        print("ERROR: TNSClient is missing submit_and_reply(). Update src/firstlight/tns/client.py.")
        return 2

    candidates = db.iter_dispatch_candidates(since_hours=args.since_hours, max_rows=args.max_submit, topic=args.topic)

    print(
        f"dispatch: db={args.db} since_hours={args.since_hours} max_submit={args.max_submit} "
        f"topic={args.topic!r} dry_run={args.dry_run}"
    )
    print(f"tns: api_base_url={c.api_base_url}")

    submitted = 0
    dry_skipped = 0
    failed = 0

    for cand in candidates:
        try:
            payload = c.build_at_report_from_fink_payload(cand.alert_json)  # type: ignore[attr-defined]
        except Exception as e:
            failed += 1
            msg = f"build_payload_failed: {e}"
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: FAIL {msg}")
            db.tns_log("skipped", cand.object_id, cand.candid, None, msg, None)
            continue

        if args.dry_run:
            dry_skipped += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: DRY_RUN (reason={cand.decision_reason})")
            continue

        ok, detail, objname, reply_json = c.submit_and_reply(payload, wait_s=args.wait_s)  # type: ignore[attr-defined]
        if ok:
            submitted += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: OK objname={objname} detail={detail}")
            db.tns_log("submitted", cand.object_id, cand.candid, objname, detail, reply_json)
        else:
            failed += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: FAIL detail={detail}")
            db.tns_log("skipped", cand.object_id, cand.candid, None, detail, reply_json)

    db.close()

    print(f"done: candidates={len(candidates)} submitted={submitted} dry_skipped={dry_skipped} failed={failed}")
    return 0 if failed == 0 else 2


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

    p_submit = tns_sub.add_parser("submit-min", help="Submit a minimal test payload (for schema convergence)")
    p_submit.set_defaults(func=_cmd_tns_submit_min)

    p_reply = tns_sub.add_parser("reply", help="Fetch reply/validation feedback for a report_id")
    p_reply.add_argument("report_id", help="TNS bulk report_id")
    p_reply.add_argument("--raw", action="store_true", help="Print the full reply JSON")
    p_reply.add_argument("--wait-s", type=int, default=600, help="Max time to wait for reply on 404 (seconds)")
    p_reply.add_argument("--poll-s", type=int, default=10, help="Polling interval while waiting (seconds)")
    p_reply.set_defaults(func=_cmd_tns_reply)

    p_dispatch = tns_sub.add_parser(
        "dispatch-sandbox",
        help="Dispatch passed DB candidates to TNS (sandbox/prod depends on TNS_API_URL)",
    )
    p_dispatch.add_argument("--db", required=True, help="Path to sqlite DB (e.g. firstlight.sqlite)")
    p_dispatch.add_argument("--since-hours", type=float, default=24.0, help="Only consider passed decisions in last N hours")
    p_dispatch.add_argument("--max-submit", type=int, default=3, help="Max number of candidates to submit")
    p_dispatch.add_argument("--topic", default=None, help="Optional topic filter (e.g. n1). If omitted, any topic.")
    p_dispatch.add_argument("--dry-run", action="store_true", help="Do not submit; just print what would be done")
    p_dispatch.add_argument("--wait-s", type=int, default=600, help="Max time to wait for reply after submit (seconds)")
    p_dispatch.set_defaults(func=_cmd_tns_dispatch_sandbox)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

"""
CLI entrypoint for FirstLight.

Usage:
  python -m firstlight tns envcheck
  python -m firstlight --env .env tns test-auth
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
    # IMPORTANT: override=True so .env changes actually take effect.
    load_dotenv(dotenv_path=path, override=True)


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

    Stability rules:
    - Never block the whole night waiting for replies.
    - Stop early on obvious auth failures (fatal=auth / http=401 on submit).
    - Always log what happened in tns_actions.
    """
    _load_env(args.env)
    c = TNSClient.from_env()
    db = DB(args.db)

    if not hasattr(c, "build_at_report_from_fink_payload"):
        print("ERROR: TNSClient is missing build_at_report_from_fink_payload(). Update src/firstlight/tns/client.py.")
        db.close()
        return 2
    if not hasattr(c, "submit_raw"):
        print("ERROR: TNSClient is missing submit_raw(). Update src/firstlight/tns/client.py.")
        db.close()
        return 2
    if not hasattr(c, "reply"):
        print("ERROR: TNSClient is missing reply(). Update src/firstlight/tns/client.py.")
        db.close()
        return 2

    print(
        f"dispatch: db={args.db} since_hours={args.since_hours} max_submit={args.max_submit} "
        f"topic={args.topic!r} dry_run={args.dry_run}"
    )
    print(f"tns: api_base_url={c.api_base_url}")

    # Preflight is advisory: some TNS deployments restrict /test.
    ok_auth, detail_auth, _raw_auth = c.test_auth()
    print(f"tns_preflight: ok={ok_auth} detail={detail_auth}")
    if not ok_auth:
        print("NOTE: /test is advisory only; continuing. Bulk submit will be the real auth check.")

    candidates = db.iter_dispatch_candidates(since_hours=args.since_hours, max_rows=args.max_submit, topic=args.topic)

    submitted = 0
    dry_skipped = 0
    failed_submit = 0
    reply_failed = 0
    aborted_auth = False

    for cand in candidates:
        # Build payload
        try:
            payload = c.build_at_report_from_fink_payload(cand.alert_json)  # type: ignore[attr-defined]
        except Exception as e:
            failed_submit += 1
            msg = f"build_payload_failed: {e}"
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: FAIL {msg}")
            db.tns_log("skipped", cand.object_id, cand.candid, None, msg, None)
            continue

        if args.dry_run:
            dry_skipped += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: DRY_RUN (reason={cand.decision_reason})")
            continue

        # Submit only (do not let reply block the pipeline)
        try:
            ok_s, detail_s, report_id, submit_json = c.submit_raw(payload)  # type: ignore[attr-defined]
        except Exception as e:
            failed_submit += 1
            msg = f"submit_exception: {e}"
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: FAIL {msg}")
            db.tns_log("failed", cand.object_id, cand.candid, None, msg, None)
            continue

        # Hard-stop on auth failures to avoid spamming 401s all night
        if ("fatal=auth" in str(detail_s)) or (" http=401" in str(detail_s)) or ("Unauthorized" in str(detail_s)):
            failed_submit += 1
            aborted_auth = True
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: FAIL AUTH detail={detail_s}")
            db.tns_log("failed_auth", cand.object_id, cand.candid, report_id, f"auth_fatal: {detail_s}", submit_json)
            break

        if not ok_s or report_id is None:
            failed_submit += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: FAIL submit detail={detail_s}")
            db.tns_log("failed", cand.object_id, cand.candid, report_id, detail_s, submit_json)
            continue

        # At this point, submit is OK -> record as submitted (even if we skip reply)
        if args.skip_reply:
            submitted += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: OK report_id={report_id} (reply skipped)")
            db.tns_log("submitted", cand.object_id, cand.candid, report_id, f"{detail_s} | reply: skipped", submit_json)
            continue

        # Reply is useful but can be flaky; keep it SHORT
        try:
            ok_r, detail_r, reply_json = c.reply(report_id=report_id, wait_s=args.wait_s, poll_s=args.poll_s)  # type: ignore[attr-defined]
        except KeyboardInterrupt:
            # Don't lose the fact that submit worked.
            print("KeyboardInterrupt during reply polling. Submit was already OK; stopping gracefully.")
            db.tns_log("submitted", cand.object_id, cand.candid, report_id, f"{detail_s} | reply: interrupted", submit_json)
            aborted_auth = True
            break
        except Exception as e:
            reply_failed += 1
            msg = f"{detail_s} | reply_exception: {e}"
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: OK submit, WARN reply_failed report_id={report_id}")
            db.tns_log("submitted", cand.object_id, cand.candid, report_id, msg, submit_json)
            continue

        if ok_r:
            submitted += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: OK report_id={report_id} detail={detail_s} | reply={detail_r}")
            db.tns_log("submitted", cand.object_id, cand.candid, report_id, f"{detail_s} | reply: {detail_r}", reply_json)
        else:
            reply_failed += 1
            print(f"- {cand.object_id} candid={cand.candid} topic={cand.topic}: OK submit, WARN reply_failed report_id={report_id} reply_detail={detail_r}")
            # Still mark as submitted: resubmitting later is usually worse (duplicate / spam).
            db.tns_log("submitted", cand.object_id, cand.candid, report_id, f"{detail_s} | reply: {detail_r}", reply_json)

    db.close()

    print(
        f"done: candidates={len(candidates)} submitted={submitted} dry_skipped={dry_skipped} "
        f"failed_submit={failed_submit} reply_failed={reply_failed}"
    )
    if aborted_auth:
        print("NOTE: Dispatch stopped early (auth fatal or interrupted). Check TNS_API_KEY / env contamination.")
    # Return non-zero only if submit failed or auth fatal; reply failures are warnings.
    return 0 if (failed_submit == 0 and not aborted_auth) else 2


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
    # Reply controls (kept short by default in practice; use skip-reply for full stability)
    p_dispatch.add_argument("--skip-reply", action="store_true", help="Do not poll reply; submit only (most stable)")
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

import argparse
from typing import Optional

from dotenv import load_dotenv

from .tns.client import TNSClient, _safe_json


def _load_env(env_path: Optional[str]) -> None:
    if env_path:
        load_dotenv(env_path, override=True)
    else:
        load_dotenv(override=False)


def main() -> None:
    parser = argparse.ArgumentParser(prog="firstlight")
    parser.add_argument("--env", help="Path to .env file (python-dotenv).", default=None)

    sub = parser.add_subparsers(dest="cmd", required=True)

    tns = sub.add_parser("tns", help="TNS Bulk API utilities")
    tns_sub = tns.add_subparsers(dest="tns_cmd", required=True)

    p_env = tns_sub.add_parser("envcheck", help="Show resolved TNS config (safe stats only).")
    p_env.add_argument("--show-ua", action="store_true", help="Print user-agent (no secrets).")

    p_submit_min = tns_sub.add_parser("submit-min", help="Submit a minimal bulk AT report skeleton (sandbox).")
    p_submit_min.add_argument("--raw", action="store_true", help="Print raw submit JSON response (no secrets).")

    p_reply = tns_sub.add_parser("reply", help="Poll bulk-report-reply for a report_id.")
    p_reply.add_argument("report_id", help="Report ID returned by submit.")
    p_reply.add_argument("--raw", action="store_true", help="Print raw reply JSON.")
    p_reply.add_argument("--wait-s", type=int, default=600, help="Max seconds to wait/poll (default: 600).")

    args = parser.parse_args()
    _load_env(args.env)

    if args.cmd != "tns":
        raise SystemExit(2)

    c = TNSClient.from_env()

    if args.tns_cmd == "envcheck":
        d = c.envcheck_dict(show_ua=bool(args.show_ua))
        print(_safe_json(d))
        return

    if args.tns_cmd == "submit-min":
        ok, detail, report_id, raw_json = c.submit_min()
        print(f"submit_url: {c.submit_url}")
        print(f"result: ok={ok} detail={detail} report_id={report_id}")
        if args.raw:
            print("submit_raw_json:")
            print(_safe_json(raw_json))
        if report_id is None:
            print("NOTE: report_id missing — inspect submit_raw_json and extraction logic.")
        else:
            print("NOTE: Run `tns reply <REPORT_ID> --raw` to see validation feedback and converge schema.")
        return

    if args.tns_cmd == "reply":
        rid = args.report_id
        print(f"reply_url: {c.reply_url}")
        ok, detail, reply_json = c.reply(report_id=rid, wait_s=int(args.wait_s))
        print(f"result: ok={ok} detail={detail}")
        if args.raw:
            print("reply:")
            print(_safe_json(reply_json))
        return

    raise SystemExit(2)


if __name__ == "__main__":
    main()

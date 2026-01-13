import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from .pipeline.runner import run_daemon
from .tns.client import TNSClient

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="firstlight", description="Firstlight N1 pipeline (Fink → TNS).")
    p.add_argument("--env", default=None, help="Path to .env file to load (recommended).")

    sp = p.add_subparsers(dest="cmd", required=True)

    run = sp.add_parser("run", help="Run the near-real-time daemon.")
    run.add_argument("--topics", nargs="+", required=True, help="Fink topics to subscribe to.")
    run.add_argument("--db", default="firstlight.sqlite", help="SQLite path (audit log).")
    run.add_argument("--config", default="config/n1.example.yaml", help="YAML config path for thresholds.")
    run.add_argument("--dry-run", action="store_true", help="Do everything except TNS submission.")
    run.add_argument("--poll-timeout", type=int, default=5, help="Poll timeout seconds.")
    run.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])

    tns = sp.add_parser("tns", help="TNS utilities.")
    tns_sp = tns.add_subparsers(dest="tns_cmd", required=True)

    tns_sp.add_parser("probe", help="Probe TNS bulk-report endpoints with current env.")
    envc = tns_sp.add_parser("envcheck", help="Print env lengths (non-secret) for debugging.")
    envc.add_argument("--show-ua", action="store_true", help="Print full TNS_USER_AGENT.")

    return p

def main():
    args = build_parser().parse_args()

    if args.env:
        load_dotenv(args.env)

    logging.basicConfig(level=getattr(logging, getattr(args, "log_level", "INFO")), format="%(asctime)s %(levelname)s %(message)s")

    if args.cmd == "run":
        run_daemon(
            topics=args.topics,
            db_path=Path(args.db),
            config_path=Path(args.config),
            dry_run=args.dry_run,
            poll_timeout=args.poll_timeout,
        )
        return

    if args.cmd == "tns":
        c = TNSClient()
        if args.tns_cmd == "probe":
            r = c.probe()
            print(f"submit_url: {r.submit_url}")
            print(f"status_url: {r.status_url}")
            print("notes:")
            for n in r.notes:
                print(f" - {n}")
            return

        if args.tns_cmd == "envcheck":
            import os
            api_key = os.getenv("TNS_API_KEY", "")
            ua = os.getenv("TNS_USER_AGENT", "")
            api_url = os.getenv("TNS_API_URL", "")
            print(f"TNS_API_URL_len: {len(api_url)}")
            print(f"TNS_API_KEY_len: {len(api_key)} dot_in_key: {'.' in api_key}")
            print(f"TNS_USER_AGENT_len: {len(ua)}")
            if args.show_ua:
                print("TNS_USER_AGENT:")
                print(ua)
            return

if __name__ == "__main__":
    main()

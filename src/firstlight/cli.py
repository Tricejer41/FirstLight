import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from .pipeline.runner import run_daemon

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="firstlight", description="Firstlight N1 pipeline (Fink → TNS).")
    p.add_argument("--env", default=".env", help="Path to .env (loaded if exists).")
    sp = p.add_subparsers(dest="cmd", required=True)

    # TNS utils
    tns = sp.add_parser("tns", help="TNS utilities (probe endpoints).")
    tns_sp = tns.add_subparsers(dest="tns_cmd", required=True)
    tns_sp.add_parser("probe", help="Probe TNS API endpoints and print what works.")

    # Daemon
    run = sp.add_parser("run", help="Run the near-real-time daemon.")
    run.add_argument("--topics", nargs="+", required=True, help="Fink topics to subscribe to.")
    run.add_argument("--db", default="firstlight.sqlite", help="SQLite path (audit log).")
    run.add_argument("--config", default="config/n1.example.yaml", help="YAML config path for thresholds.")
    run.add_argument("--dry-run", action="store_true", help="Do everything except TNS submission.")
    run.add_argument("--poll-timeout", type=int, default=5, help="Poll timeout seconds.")
    run.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return p

def main():
    p = build_parser()
    args = p.parse_args()

    # Load env variables early (bot credentials etc.)
    if args.env and Path(args.env).exists():
        load_dotenv(args.env, override=False)

    logging.basicConfig(
        level=getattr(logging, getattr(args, "log_level", "INFO")),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    if args.cmd == "tns":
        from .tns.client import TNSClient
        tns = TNSClient()
        if not tns.enabled():
            raise SystemExit("TNS not enabled. Check .env: TNS_BOT_ID, TNS_BOT_NAME, TNS_API_KEY, TNS_API_URL.")
        probe = tns.probe_endpoints()
        print("submit_url:", probe.get("submit_url"))
        print("status_url:", probe.get("status_url"))
        print("notes:")
        for n in probe.get("notes", []):
            print(" -", n)
        return

    if args.cmd == "run":
        run_daemon(
            topics=args.topics,
            db_path=Path(args.db),
            config_path=Path(args.config),
            dry_run=args.dry_run,
            poll_timeout=args.poll_timeout,
        )

if __name__ == "__main__":
    main()

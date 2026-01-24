#!/usr/bin/env python
"""
Watch/replay a flat directory of *.avro ZTF/Fink alert packets (polling).

Design goals (for running all night):
- No extra dependencies beyond fastavro + your existing repo modules.
- Idempotent across restarts (tracks processed filenames in a .seen file).
- Logs everything needed for calibration: raw alert (minimal), decision, metrics.
- Dry-run only (no TNS submit). This is for stability + filter calibration.

Typical workflow:
1) Terminal A (Fink consumer saves AVROs):
   fink_consumer --save -outdir alertDB/raw/2026-01-23 -limit 0

2) Terminal B (this watcher):
   python scripts/replay_avro_dir.py alertDB/raw/2026-01-23 --cfg config/n1.example.yaml --db firstlight.sqlite --follow --poll-s 3 --jsonl alertDB/logs/2026-01-23/replay.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import yaml
from fastavro import reader as avro_reader

# Repo modules
from firstlight.pipeline.normalize import normalize, NormalizedAlert
from firstlight.niches.n1_hostless_fast import passes_n1
from firstlight.storage.db import DB


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    except Exception:
        return json.dumps({"_repr": repr(obj)}, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def load_cfg(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def iter_avro_records(path: Path) -> Iterable[Dict[str, Any]]:
    # Each AVRO file from fink_consumer --save is usually 1 record,
    # but we handle multiple defensively.
    with path.open("rb") as fo:
        r = avro_reader(fo)
        for rec in r:
            if isinstance(rec, dict):
                yield rec


def infer_topic(rec: Dict[str, Any], fallback: str) -> str:
    # If the record includes a topic field, use it; otherwise fallback.
    for k in ("topic", "kafka_topic", "broker_topic", "brokerTopic"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return fallback


def minimal_payload_for_db(a: NormalizedAlert) -> Dict[str, Any]:
    # Store a compact subset for fast querying; raw AVRO stays on disk.
    return {
        "objectId": a.object_id,
        "candidate": {
            "candid": a.candid,
            "ra": a.ra,
            "dec": a.dec,
            "jd": a.jd,
            "fid": a.fid,
            "magpsf": a.mag,
            "sigmapsf": a.magerr,
            "diffmaglim": a.limmag,
            "drb": a.drb,
            "rb": a.rb,
            "isdiffpos": a.isdiffpos,
            "ssdistnr": a.ssdistnr,
            "distpsnr1": a.distpsnr1,
            "sgscore1": a.sgscore1,
            "srmag1": a.srmag1,
            "nmtchps": a.nmtchps,
            "ndethist": a.ndethist,
        },
        "derived": {
            "last_nondet_jd": a.last_nondet_jd,
            "last_nondet_lim": a.last_nondet_lim,
            "delta_mag_from_nondet": a.delta_mag_from_nondet,
        },
    }


def load_seen(seen_path: Path) -> Set[str]:
    if not seen_path.exists():
        return set()
    try:
        return set(x.strip() for x in seen_path.read_text(encoding="utf-8").splitlines() if x.strip())
    except Exception:
        return set()


def append_seen(seen_path: Path, fname: str) -> None:
    seen_path.parent.mkdir(parents=True, exist_ok=True)
    with seen_path.open("a", encoding="utf-8") as f:
        f.write(fname + "\n")


def write_jsonl(jsonl_path: Optional[Path], obj: Dict[str, Any]) -> None:
    if jsonl_path is None:
        return
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(safe_json(obj) + "\n")


def process_one_file(
    avro_path: Path,
    cfg: Dict[str, Any],
    db: DB,
    jsonl_path: Optional[Path],
    topic_fallback: str,
) -> Tuple[bool, str]:
    """
    Returns (ok, msg). ok=False means an exception or malformed record; file is still marked seen.
    """
    t0 = time.time()
    try:
        any_rec = False
        for rec in iter_avro_records(avro_path):
            any_rec = True
            topic = infer_topic(rec, topic_fallback)

            # Normalize (requires keys: objectId, candidate, optionally prv_candidates)
            a = normalize(rec, topic)

            # Store alert + decision
            received_utc = utc_now_iso()
            emitted_jd = a.jd

            payload_db = minimal_payload_for_db(a)
            db.add_alert(a.object_id, a.candid, topic, emitted_jd, received_utc, payload_db)

            passed, reason, metrics = passes_n1(a, cfg)
            db.add_decision(a.object_id, a.candid, topic, passed, reason, metrics)

            # JSONL line for quick tail/debug
            out = {
                "ts_utc": received_utc,
                "file": avro_path.name,
                "topic": topic,
                "object_id": a.object_id,
                "candid": a.candid,
                "jd": a.jd,
                "ra": a.ra,
                "dec": a.dec,
                "fid": a.fid,
                "passed_n1": bool(passed),
                "reason": reason,
                "metrics": metrics,
            }
            write_jsonl(jsonl_path, out)

        if not any_rec:
            return False, f"empty_avro:{avro_path.name}"

        elapsed_ms = int((time.time() - t0) * 1000)
        return True, f"ok:{avro_path.name} elapsed_ms={elapsed_ms}"

    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        err = {"ts_utc": utc_now_iso(), "file": avro_path.name, "error": repr(e), "elapsed_ms": elapsed_ms}
        write_jsonl(jsonl_path, err)
        return False, f"error:{avro_path.name} {repr(e)} elapsed_ms={elapsed_ms}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watch/replay a flat directory of *.avro ZTF/Fink packets (polling).")
    p.add_argument("avro_dir", type=str, help="Directory containing *.avro files (no subdirs).")
    p.add_argument("--cfg", type=str, required=True, help="Path to YAML config (must contain key: n1: ...).")
    p.add_argument("--db", type=str, default="firstlight.sqlite", help="SQLite DB path (default: firstlight.sqlite).")
    p.add_argument("--jsonl", type=str, default="", help="Optional JSONL log file path.")
    p.add_argument("--follow", action="store_true", help="Keep polling directory for new AVROs.")
    p.add_argument("--poll-s", type=float, default=3.0, help="Polling interval seconds when --follow.")
    p.add_argument("--seen-file", type=str, default="", help="Optional seen tracking file path. Default: <avro_dir>/.replay_seen.txt")
    p.add_argument("--topic", type=str, default="unknown", help="Fallback topic stored in DB if record has no topic field.")
    p.add_argument("--print-every", type=int, default=25, help="Print a summary line every N processed files.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    avro_dir = Path(args.avro_dir).resolve()
    if not avro_dir.exists() or not avro_dir.is_dir():
        print(f"ERROR: avro_dir not found or not a directory: {avro_dir}", file=sys.stderr)
        return 2

    cfg_path = Path(args.cfg).resolve()
    if not cfg_path.exists():
        print(f"ERROR: cfg not found: {cfg_path}", file=sys.stderr)
        return 2

    cfg = load_cfg(cfg_path)
    if not isinstance(cfg, dict) or "n1" not in cfg:
        print("ERROR: cfg must be a YAML dict with top-level key 'n1'.", file=sys.stderr)
        return 2

    db_path = Path(args.db).resolve()
    jsonl_path = Path(args.jsonl).resolve() if args.jsonl.strip() else None

    seen_path = Path(args.seen_file).resolve() if args.seen_file.strip() else (avro_dir / ".replay_seen.txt")
    seen = load_seen(seen_path)

    db = DB(db_path)
    processed = 0
    ok_n = 0
    fail_n = 0
    pass_n1 = 0

    print(f"avro_dir: {avro_dir}")
    print(f"cfg:      {cfg_path}")
    print(f"db:       {db_path}")
    print(f"jsonl:    {jsonl_path if jsonl_path else '(disabled)'}")
    print(f"seen:     {seen_path} (loaded {len(seen)} entries)")
    print(f"follow:   {bool(args.follow)} poll_s={args.poll_s}")
    print("----")

    def list_new_files() -> List[Path]:
        files = sorted(avro_dir.glob("*.avro"), key=lambda p: p.name)
        return [p for p in files if p.name not in seen]

    try:
        while True:
            new_files = list_new_files()
            if not new_files and not args.follow:
                break

            for fp in new_files:
                processed += 1
                ok, msg = process_one_file(fp, cfg, db, jsonl_path, args.topic)
                # Mark as seen regardless of ok, to avoid infinite crash loops.
                seen.add(fp.name)
                append_seen(seen_path, fp.name)

                if ok:
                    ok_n += 1
                else:
                    fail_n += 1

                # Cheap way to count passes: read last jsonl line is messy; instead re-evaluate quickly:
                # We already logged decision in DB; for tonight it's fine to approximate using cfg again
                # by opening record once more only if you want. We keep it simple: do nothing here.
                # You'll see pass rate in SQLite queries later.

                if processed % max(1, int(args.print_every)) == 0:
                    print(f"[{utc_now_iso()}] processed={processed} ok={ok_n} fail={fail_n} last={msg}")

            if args.follow:
                time.sleep(max(0.2, float(args.poll_s)))
            else:
                break

    except KeyboardInterrupt:
        print("\nInterrupted. Closing DB.")
    finally:
        db.close()

    print(f"done: processed={processed} ok={ok_n} fail={fail_n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

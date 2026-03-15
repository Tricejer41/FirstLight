"""Utility: run N1 filter + vetting on a folder of saved .avro alerts (no Kafka).

Example:
  python scripts/run_once_from_avro_dir.py alertDB --config config/n1.example.yaml
"""
from __future__ import annotations

import argparse
from pathlib import Path
import json
import yaml
import io

from fastavro import reader

from firstlight.pipeline.normalize import normalize
from firstlight.niches.n1_hostless_fast import passes_n1

def load_one_avro(path: Path) -> dict:
    with open(path, "rb") as f:
        r = reader(f)
        for rec in r:
            return rec
    raise RuntimeError(f"No record in {path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", type=str)
    ap.add_argument("--config", default="config/n1.example.yaml")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))

    for p in sorted(Path(args.dir).glob("*.avro")):
        alert = load_one_avro(p)
        topic = alert.get("topic", "unknown")  # saved avro might not include topic
        na = normalize(alert, topic)
        passed, reason, metrics = passes_n1(na, cfg)
        if passed:
            print("PASS", na.object_id, na.candid, json.dumps(metrics, ensure_ascii=False))
        else:
            print("FAIL", na.object_id, na.candid, reason)

if __name__ == "__main__":
    main()

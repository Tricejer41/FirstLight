#!/usr/bin/env python
from __future__ import annotations

import argparse
import base64
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import yaml
from fastavro import reader

from firstlight.niches.n1_hostless_fast import passes_n1
from firstlight.pipeline.normalize import normalize
from firstlight.storage.db import DB


# -------------------------
# Config
# -------------------------

def load_cfg(cfg_path: str) -> Dict[str, Any]:
    """
    IMPORTANT:
    passes_n1() espera un dict con clave "n1" (cfg["n1"]).
    Por tanto aquí devolvemos el YAML completo, no la subsección.
    """
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg if isinstance(cfg, dict) else {}


# -------------------------
# AVRO utils
# -------------------------

def iter_avro_records(avro_path: Path) -> Iterable[Dict[str, Any]]:
    with avro_path.open("rb") as fo:
        for rec in reader(fo):
            if isinstance(rec, dict):
                yield rec


def topic_from_filename(_: Path) -> str:
    return "fink"


# -------------------------
# Seen tracking
# -------------------------

def load_seen(seen_path: Path) -> Set[str]:
    if not seen_path.exists():
        return set()
    return set(
        x.strip()
        for x in seen_path.read_text(encoding="utf-8").splitlines()
        if x.strip()
    )


def save_seen(seen_path: Path, seen: Set[str]) -> None:
    seen_path.write_text("\n".join(sorted(seen)) + "\n", encoding="utf-8")


# -------------------------
# JSON-safe conversion (bytes-safe)
# -------------------------

def _is_numpy_scalar(x: Any) -> bool:
    try:
        import numpy as np  # type: ignore
        return isinstance(x, np.generic)
    except Exception:
        return False


def make_json_safe(x: Any) -> Any:
    if x is None or isinstance(x, (bool, int, float, str)):
        return x

    if _is_numpy_scalar(x):
        try:
            return x.item()
        except Exception:
            return str(x)

    if isinstance(x, (bytes, bytearray)):
        return {"__bytes_b64__": base64.b64encode(bytes(x)).decode("ascii")}

    if isinstance(x, memoryview):
        return {"__bytes_b64__": base64.b64encode(x.tobytes()).decode("ascii")}

    if isinstance(x, dict):
        out: Dict[str, Any] = {}
        for k, v in x.items():
            ks = k if isinstance(k, str) else str(k)
            out[ks] = make_json_safe(v)
        return out

    if isinstance(x, (list, tuple)):
        return [make_json_safe(v) for v in x]

    return str(x)


# -------------------------
# Core processing
# -------------------------

def process_file(avro_path: Path, cfg: Dict[str, Any], db: DB) -> Tuple[bool, str, Dict[str, Any]]:
    last = (False, "no_records", {})

    for alert in iter_avro_records(avro_path):
        topic = topic_from_filename(avro_path)
        na = normalize(alert, topic)

        # cfg completo (con "n1")
        passed, reason, metrics = passes_n1(na, cfg)

        safe_alert = make_json_safe(alert)

        db.add_alert(
            object_id=na.object_id,
            candid=na.candid,
            topic=na.topic,
            raw_json=safe_alert,
        )
        db.add_decision(
            object_id=na.object_id,
            candid=na.candid,
            topic=na.topic,
            passed=passed,
            reason=reason,
            metrics=metrics,
        )

        last = (bool(passed), str(reason), metrics if isinstance(metrics, dict) else {})

    return last


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("raw_dir", help="Directorio donde fink_consumer guarda los .avro")
    ap.add_argument("--cfg", required=True, help="Ruta a config yaml (ej: config/n1.example.yaml)")
    ap.add_argument("--db", required=True, help="Ruta al sqlite (ej: firstlight.sqlite)")
    ap.add_argument("--follow", action="store_true", help="Seguir vigilando el directorio")
    ap.add_argument("--poll-s", type=float, default=2.0)
    ap.add_argument("--jsonl", default=None, help="Ruta a jsonl para auditoría (ok/errores)")
    ap.add_argument("--print-every", type=int, default=200)
    ap.add_argument("--reset-seen", action="store_true", help="Borra .replay_seen.txt antes de empezar")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir).resolve()
    cfg_path = str(Path(args.cfg).resolve())
    db_path = str(Path(args.db).resolve())
    jsonl_path = Path(args.jsonl).resolve() if args.jsonl else None

    if not raw_dir.exists():
        print(f"[replay] ERROR: raw_dir no existe: {raw_dir}", file=sys.stderr)
        return 2

    cfg = load_cfg(cfg_path)

    # sanity check: esto evita volver a perder tiempo
    if "n1" not in cfg or not isinstance(cfg.get("n1"), dict):
        print(f"[replay] ERROR: el cfg NO contiene sección 'n1:' válida. cfg_path={cfg_path}", file=sys.stderr)
        return 2

    seen_path = raw_dir / ".replay_seen.txt"
    if args.reset_seen and seen_path.exists():
        try:
            seen_path.unlink()
        except Exception:
            pass

    seen = load_seen(seen_path)

    db = DB(db_path)

    processed = 0
    ok = 0
    failed = 0

    try:
        while True:
            avros = sorted(raw_dir.glob("*.avro"))
            for avro in avros:
                key = avro.name
                if key in seen:
                    continue

                processed += 1
                try:
                    passed, reason, metrics = process_file(avro, cfg, db)
                    ok += 1
                    evt = {
                        "file": avro.name,
                        "ok": True,
                        "passed": bool(passed),
                        "reason": str(reason),
                        "metrics": metrics,
                    }
                except Exception as e:
                    failed += 1
                    tb = traceback.format_exc()
                    print(f"[replay] ERROR file={avro.name} {type(e).__name__}: {e}", file=sys.stderr)
                    print(tb, file=sys.stderr)
                    evt = {
                        "file": avro.name,
                        "ok": False,
                        "error": f"{type(e).__name__}: {e}",
                        "traceback": tb,
                    }

                if jsonl_path:
                    append_jsonl(jsonl_path, evt)

                seen.add(key)

                if processed % max(1, int(args.print_every)) == 0:
                    print(
                        f"[replay] processed={processed} ok={ok} failed={failed} seen={len(seen)} dir={raw_dir}",
                        flush=True,
                    )

            save_seen(seen_path, seen)

            if not args.follow:
                break

            time.sleep(max(0.2, float(args.poll_s)))

    finally:
        db.close()

    print(f"[replay] DONE processed={processed} ok={ok} failed={failed} dir={raw_dir}", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

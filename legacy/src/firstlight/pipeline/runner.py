from __future__ import annotations

import time
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

import yaml
import yaml as _yaml  # for reading ~/.finkclient/credentials.yml

from fink_client.consumer import AlertConsumer

from ..storage.db import DB
from ..pipeline.normalize import normalize
from ..niches.n1_hostless_fast import passes_n1
from ..tns.fink_resolver import ztf_to_tns
from ..tns.client import TNSClient
from ..utils.time import now_utc, jd_to_datetime_utc
from ..utils.fits_min import quick_stamp_metrics

logger = logging.getLogger("firstlight")

def _load_cfg(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _load_fink_credentials() -> Dict[str, Any]:
    """Load ~/.finkclient/credentials.yml as plain YAML dict."""
    cred_path = Path.home() / ".finkclient" / "credentials.yml"
    if not cred_path.exists():
        raise FileNotFoundError(f"Missing {cred_path}. Run fink_client_register first.")
    return _yaml.safe_load(cred_path.read_text(encoding="utf-8"))

def run_daemon(
    topics: List[str],
    db_path: Path,
    config_path: Path,
    dry_run: bool,
    poll_timeout: int,
):
    cfg = _load_cfg(config_path)
    db = DB(db_path)
    tns = TNSClient()

    # Probe TNS endpoints once at startup (only if we intend to submit).
    submit_url = None
    status_url = None
    if not dry_run and tns.enabled():
        try:
            probe = tns.probe_endpoints()
            submit_url = probe.get("submit_url")
            status_url = probe.get("status_url")
            if submit_url:
                logger.info("TNS endpoint OK: %s", submit_url)
            else:
                logger.error(
                    "TNS endpoint probe failed. Will NOT submit to TNS (effectively dry-run). Notes=%s",
                    probe.get("notes"),
                )
        except Exception as e:
            logger.error("TNS probe exception: %s. Will NOT submit to TNS.", e)

    fink_cfg = _load_fink_credentials()

    # IMPORTANT: per Fink docs, do not set password if it is null in the credential file.
    # We pass exactly what is in credentials.yml.
    consumer = AlertConsumer(topics, fink_cfg)

    logger.info("Started. topics=%s dry_run=%s db=%s", topics, dry_run, db_path)

    while True:
        topic, alert, key = consumer.poll(poll_timeout)
        if topic is None:
            continue

        t0 = now_utc()
        try:
            na = normalize(topic, alert)
        except Exception as e:
            logger.exception("Normalize failed: %s", e)
            continue

        object_id = na.object_id
        candid = na.candid

        # quick image/shape sanity (fast stamps metrics)
        stamp_metrics = quick_stamp_metrics(alert)

        ok_n1, reason, metrics = passes_n1(na, alert, cfg, extra_metrics=stamp_metrics)

        db.alert_log(
            object_id=object_id,
            candid=candid,
            topic=na.topic,
            ra=na.ra,
            dec=na.dec,
            jd=na.jd,
            fid=na.fid,
            mag=na.mag,
            magerr=na.magerr,
            limmag=na.limmag,
            drb=na.drb,
            rb=na.rb,
            distpsnr1=na.distpsnr1,
            srmag1=na.srmag1,
            distnr=na.distnr,
            sgscore1=na.sgscore1,
            ndethist=na.ndethist,
            nmtchps=na.nmtchps,
            isdiffpos=na.isdiffpos,
            has_g_minus_r=na.has_g_minus_r,
            g_minus_r=na.g_minus_r,
            reason=reason,
            metrics=metrics,
            stamp_metrics=stamp_metrics,
            t_received=t0,
        )

        if not ok_n1:
            continue

        # Anti-duplicate layer 0: never re-submit the same ZTF objectId
        if db.was_submitted_or_skipped(object_id):
            db.tns_log(object_id, candid, action="check", outcome="skip", detail="already_submitted_or_checked_in_db")
            continue

        # Anti-duplicate layer 1: fast ZTF→TNS reverse resolver
        r = ztf_to_tns(object_id)
        if r is not None:
            db.tns_log(object_id, candid, action="check", outcome="skip", detail=f"resolver_found_tns={r}")
            continue
        db.tns_log(object_id, candid, action="check", outcome="ok", detail="resolver_no_match")

        # Submission (or dry-run)
        if dry_run or not submit_url:
            db.tns_log(
                object_id,
                candid,
                action="submit",
                outcome="skip",
                detail="dry_run" if dry_run else "tns_endpoint_unknown_probe_failed",
            )
            logger.info("CANDIDATE PASS (not submitted): %s %s mag=%.3f jd=%.5f", object_id, candid, na.mag, na.jd)
            continue

        # Build a minimal AT report payload (best-effort). If TNS rejects, the response will include missing fields.
        try:
            disc_dt = jd_to_datetime_utc(float(na.jd)).isoformat().replace("+00:00", "Z")
        except Exception:
            disc_dt = now_utc().isoformat().replace("+00:00", "Z")

        fid_map = {1: "g", 2: "r", 3: "i"}
        filt = fid_map.get(int(na.fid), str(na.fid))

        at_json = build_minimal_at_report(
            objname=na.object_id,
            ra_deg=float(na.ra),
            dec_deg=float(na.dec),
            discovery_utc_iso=disc_dt,
            mag=float(na.mag),
            filt=filt,
            instrument="ZTF",
            observer="Fink/ZTF",
            reporter_name=tns.cfg.reporter_name,
            reporter_email=tns.cfg.reporter_email,
            reporter_institution=tns.cfg.reporter_institution,
        )

        ok, detail = tns.submit_at_report(at_json, submit_url=submit_url)
        db.tns_log(object_id, candid, action="submit", outcome="ok" if ok else "error", detail=detail)

        if ok:
            logger.info("SUBMITTED: %s %s", object_id, candid)
        else:
            logger.error("SUBMIT FAILED: %s %s — %s", object_id, candid, detail)

        # be polite with CPU; Kafka poll dominates anyway
        time.sleep(0.01)

from __future__ import annotations

from dataclasses import asdict
from typing import Dict, Any, Tuple
import math

from ..pipeline.normalize import NormalizedAlert

def passes_n1(a: NormalizedAlert, cfg: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """N1 niche: hostless-ish, very likely real, early, with a recent non-detection.

    IMPORTANT:
    - This is not "best science". It's tuned to maximize *early* + *new* candidates
      with minimal junk, under low-latency constraints.
    """
    c = cfg["n1"]

    # 1) Real/Bogus gate
    drb_ok = (a.drb is not None and a.drb >= float(c["drb_min"]))
    rb_ok = (a.drb is None and a.rb is not None and a.rb >= float(c["rb_fallback_min"]))
    if not (drb_ok or rb_ok):
        return False, "rb_fail", {"drb": a.drb, "rb": a.rb}

    # 2) Positive subtraction (avoid negative residual artifacts)
    if c.get("require_positive_diff", True) and a.isdiffpos not in ("t", "1", True):
        return False, "isdiffpos_fail", {"isdiffpos": a.isdiffpos}

    # 3) Avoid Solar System objects (if value exists)
    if a.ssdistnr is not None and a.ssdistnr != -999:
        if a.ssdistnr < float(c["min_ssdistnr_arcsec"]):
            return False, "sso_match", {"ssdistnr": a.ssdistnr}

    # 4) Hostless-ish heuristic (PS1 match far or faint)
    # NOTE: hostless topics from Fink already bias toward this, but we re-check.
    if a.distpsnr1 is not None and a.distpsnr1 != -999:
        if a.distpsnr1 < float(c["min_distpsnr1_arcsec"]):
            return False, "ps1_too_close", {"distpsnr1": a.distpsnr1}
    if a.srmag1 is not None and a.srmag1 != -999:
        if a.srmag1 < float(c["min_ps1_mag"]):
            return False, "ps1_too_bright", {"srmag1": a.srmag1}

    # 5) Not too crowded
    if a.nmtchps is not None and a.nmtchps > int(c["max_nmtchps"]):
        return False, "crowded_field", {"nmtchps": a.nmtchps}

    # 6) Early history
    if a.ndethist is not None and a.ndethist > int(c["max_ndethist"]):
        return False, "too_many_detections", {"ndethist": a.ndethist}

    # 7) Recent non-detection and brightness jump
    if a.last_nondet_jd is None or a.last_nondet_lim is None or a.delta_mag_from_nondet is None:
        return False, "no_recent_nondet", {"last_nondet_jd": a.last_nondet_jd}
    days = (a.jd - a.last_nondet_jd)
    if days < 0:
        return False, "nondet_in_future", {"days": days}
    if days > float(c["max_days_since_nondet"]):
        return False, "nondet_too_old", {"days": days}

    if a.delta_mag_from_nondet < float(c["min_delta_mag_from_nondet"]):
        return False, "delta_mag_small", {"delta_mag": a.delta_mag_from_nondet}

    metrics = {
        "object_id": a.object_id,
        "candid": a.candid,
        "topic": a.topic,
        "jd": a.jd,
        "mag": a.mag,
        "limmag": a.limmag,
        "delta_mag_from_nondet": a.delta_mag_from_nondet,
        "days_since_nondet": days,
        "drb": a.drb,
        "rb": a.rb,
        "distpsnr1": a.distpsnr1,
        "srmag1": a.srmag1,
        "nmtchps": a.nmtchps,
        "ndethist": a.ndethist,
    }
    return True, "pass", metrics

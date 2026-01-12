from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

@dataclass(frozen=True)
class NormalizedAlert:
    object_id: str
    candid: str
    topic: str
    ra: float
    dec: float
    jd: float
    fid: int
    mag: float
    magerr: float
    limmag: float
    drb: Optional[float]
    rb: Optional[float]
    isdiffpos: Optional[str]
    ssdistnr: Optional[float]
    distpsnr1: Optional[float]
    sgscore1: Optional[float]
    srmag1: Optional[float]
    nmtchps: Optional[int]
    ndethist: Optional[int]
    last_nondet_jd: Optional[float]
    last_nondet_lim: Optional[float]
    delta_mag_from_nondet: Optional[float]
    raw: Dict[str, Any]

def _last_nondet(prv_candidates: List[Dict[str, Any]], current_jd: float) -> Optional[Dict[str, Any]]:
    nondets = [
        p for p in prv_candidates
        if p.get("candid") is None and p.get("jd") is not None and float(p["jd"]) < float(current_jd)
    ]
    if not nondets:
        return None
    return max(nondets, key=lambda p: float(p["jd"]))

def normalize(alert: Dict[str, Any], topic: str) -> NormalizedAlert:
    c = alert["candidate"]
    object_id = alert["objectId"]
    candid = str(c.get("candid", ""))

    prv = alert.get("prv_candidates", []) or []
    nd = _last_nondet(prv, float(c["jd"]))
    last_nondet_jd = float(nd["jd"]) if nd and nd.get("jd") is not None else None
    last_nondet_lim = float(nd["diffmaglim"]) if nd and nd.get("diffmaglim") is not None else None
    delta = None
    if last_nondet_lim is not None and c.get("magpsf") is not None:
        delta = float(last_nondet_lim) - float(c["magpsf"])

    return NormalizedAlert(
        object_id=object_id,
        candid=candid,
        topic=topic,
        ra=float(c["ra"]),
        dec=float(c["dec"]),
        jd=float(c["jd"]),
        fid=int(c.get("fid", 0)),
        mag=float(c.get("magpsf", float("nan"))),
        magerr=float(c.get("sigmapsf", float("nan"))),
        limmag=float(c.get("diffmaglim", float("nan"))),
        drb=(float(c["drb"]) if c.get("drb") is not None else None),
        rb=(float(c["rb"]) if c.get("rb") is not None else None),
        isdiffpos=(c.get("isdiffpos")),
        ssdistnr=(float(c["ssdistnr"]) if c.get("ssdistnr") is not None else None),
        distpsnr1=(float(c["distpsnr1"]) if c.get("distpsnr1") is not None else None),
        sgscore1=(float(c["sgscore1"]) if c.get("sgscore1") is not None else None),
        srmag1=(float(c["srmag1"]) if c.get("srmag1") is not None else None),
        nmtchps=(int(c["nmtchps"]) if c.get("nmtchps") is not None else None),
        ndethist=(int(c["ndethist"]) if c.get("ndethist") is not None else None),
        last_nondet_jd=last_nondet_jd,
        last_nondet_lim=last_nondet_lim,
        delta_mag_from_nondet=delta,
        raw=alert
    )

from __future__ import annotations

from typing import Optional, Dict, Any
import requests

FINK_RESOLVER_ENDPOINT = "https://api.ztf.fink-portal.org/api/v1/resolver"

def ztf_to_tns(object_id: str, timeout_s: int = 5) -> Optional[Dict[str, Any]]:
    """Query Fink portal resolver to check if a ZTF objectId already has a TNS counterpart.

    Returns:
        None if no counterpart (or error), else dict with response payload.
    """
    payload = {
        "resolver": "tns",
        "reverse": True,
        "name": object_id,
    }
    try:
        r = requests.post(FINK_RESOLVER_ENDPOINT, json=payload, timeout=timeout_s)
        r.raise_for_status()
        # response is JSON; could be empty list/object
        out = r.json()
        return out if out else None
    except Exception:
        return None

from __future__ import annotations

import datetime as _dt

_JD_UNIX_EPOCH = 2440587.5  # JD at Unix epoch 1970-01-01 00:00:00 UTC

def jd_to_datetime_utc(jd: float) -> _dt.datetime:
    """Convert Julian Date to datetime (UTC). No leap-second handling."""
    unix = (jd - _JD_UNIX_EPOCH) * 86400.0
    return _dt.datetime.fromtimestamp(unix, tz=_dt.timezone.utc)

def now_utc() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.timezone.utc)

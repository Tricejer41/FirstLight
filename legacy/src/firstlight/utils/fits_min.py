from __future__ import annotations

import gzip
import io
import struct
from dataclasses import dataclass
from typing import Tuple, Optional

import numpy as np

@dataclass(frozen=True)
class FitsImage:
    data: np.ndarray
    header: dict

def _read_header_block(buf: bytes, offset: int) -> Tuple[dict, int]:
    header = {}
    pos = offset
    while True:
        card = buf[pos:pos+80].decode("ascii", errors="ignore")
        pos += 80
        key = card[:8].strip()
        if key == "END":
            break
        if "=" in card[8:10]:
            # KEYWORD = value / comment
            raw = card[10:].split("/")[0].strip()
            val = raw
            if raw.startswith("'") and raw.endswith("'"):
                val = raw.strip("'")
            else:
                # int/float/bool
                if raw in ("T", "F"):
                    val = (raw == "T")
                else:
                    try:
                        if "." in raw or "E" in raw or "e" in raw:
                            val = float(raw)
                        else:
                            val = int(raw)
                    except Exception:
                        val = raw
            header[key] = val

    # headers are padded to 2880 bytes
    hdr_len = pos - offset
    pad = (2880 - (hdr_len % 2880)) % 2880
    pos += pad
    return header, pos

def read_gz_fits_image(stamp_data_gz: bytes) -> FitsImage:
    """Read a gzipped FITS primary image HDU into numpy (minimal implementation)."""
    raw = gzip.decompress(stamp_data_gz)
    header, pos = _read_header_block(raw, 0)

    bitpix = int(header.get("BITPIX"))
    naxis = int(header.get("NAXIS", 0))
    if naxis != 2:
        raise ValueError(f"Only 2D images supported, got NAXIS={naxis}")

    nx = int(header.get("NAXIS1"))
    ny = int(header.get("NAXIS2"))

    # FITS is big-endian
    dtype_map = {
        8:  ">u1",
        16: ">i2",
        32: ">i4",
        -32: ">f4",
        -64: ">f8",
    }
    if bitpix not in dtype_map:
        raise ValueError(f"Unsupported BITPIX={bitpix}")

    dtype = np.dtype(dtype_map[bitpix])
    n = nx * ny
    data = np.frombuffer(raw, dtype=dtype, count=n, offset=pos).astype("float32", copy=False)
    data = data.reshape((ny, nx))
    return FitsImage(data=data, header=header)

def quick_stamp_metrics(stamp_data_gz: bytes) -> dict:
    """Cheap, fast metrics to reject obvious junk."""
    img = read_gz_fits_image(stamp_data_gz).data
    # Robust stats
    med = float(np.nanmedian(img))
    mad = float(np.nanmedian(np.abs(img - med))) + 1e-6
    peak = float(np.nanmax(img))
    trough = float(np.nanmin(img))
    snr_like = (peak - med) / (1.4826 * mad)
    return {
        "stamp_med": med,
        "stamp_mad": mad,
        "stamp_peak": peak,
        "stamp_trough": trough,
        "stamp_snr_like": float(snr_like),
        "stamp_shape": [int(img.shape[0]), int(img.shape[1])],
    }

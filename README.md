# Firstlight N1 — low-latency “first report” pipeline (Fink → TNS)

This repo is a **template** to run a near-real-time pipeline:
Fink livestream → normalize → N1 filter → minimal vetting → anti-duplicate checks → (optional) TNS report → logging/auditing.

It is intentionally **small and hackable**. The only hard dependency is `fink-client`.
TNS submission is provided as a pluggable module (stub by default) because credentials + endpoint details vary.

## Quick start (Windows / PowerShell)

1) Create & activate venv
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) Make sure you can poll from Fink (you already tested):
```powershell
fink_consumer --display -limit 1
```

3) (Recommended) Reset offsets to start *from now* (avoid 4 days backlog):
```powershell
fink_consumer --display -start_at latest -limit 1
```

4) Run the pipeline in **dry-run** (no TNS submission)
```powershell
python -m firstlight run --topics fink_new_hostless_ztf fink_intra_night_hostless_ztf fink_inter_night_hostless_ztf --dry-run
```

5) When ready, configure TNS credentials (see `config/tns.example.env`) and run:
```powershell
python -m firstlight run --topics fink_new_hostless_ztf fink_intra_night_hostless_ztf fink_inter_night_hostless_ztf
```

## Where things live

- `src/firstlight/ingest/fink_stream.py` consumes alerts with `fink_client.consumer.AlertConsumer`.
- `src/firstlight/niches/n1_hostless_fast.py` is **the N1 filter** (edit thresholds here).
- `src/firstlight/tns/` contains anti-duplicate checks + TNS submission stub.
- `firstlight.sqlite` (created on first run) stores audit logs + decisions.

## Notes

- Cutouts are stored in the alert packet as gzipped FITS bytes. We do not depend on `astropy`.
- Anti-duplicate check uses:
  - your own DB (never re-send the same objectId), and
  - Fink Portal resolver (ZTF → TNS reverse lookup) when available.


## TNS endpoint probe

Run:

```bash
python -m firstlight --env .env tns probe
```

If `submit_url` is None, keep `--dry-run` and extend endpoint candidates in `src/firstlight/tns/client.py`.

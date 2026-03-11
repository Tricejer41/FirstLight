Patch files (overwrite in your repo):

- src/firstlight/tns/client.py
- src/firstlight/tns/dispatch.py
- src/firstlight/storage/db.py
- src/firstlight/cli.py
- config/tns.example.env

Key behavior changes:
- No hardcoded reporter fallback.
- Real submits require TNS_SEND_ENABLED=1.
- dispatch-sandbox enforces cap based on DB "submitted" actions (successful submits) over since-hours window.
- New CLI options to print/dump payloads without network.
- dispatch-sandbox now accepts --timeout-s (sets TNS_TIMEOUT_S).

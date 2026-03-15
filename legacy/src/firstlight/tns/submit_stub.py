from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from typing import Any, Dict, Optional

from dotenv import load_dotenv
import requests

from .client import TnsClient


def _load_env(env_path: str) -> None:
    # Respect your rule: do not assume PowerShell exported env vars.
    load_dotenv(env_path, override=True)


def _get_required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _safe_cfg_snapshot() -> Dict[str, Any]:
    api_url = os.getenv("TNS_API_URL", "").strip()
    api_key = os.getenv("TNS_API_KEY", "").strip()
    ua = os.getenv("TNS_USER_AGENT", "").strip()
    bot_id = os.getenv("TNS_BOT_ID", "").strip()
    bot_name = os.getenv("TNS_BOT_NAME", "").strip()

    def mask(s: str) -> Dict[str, Any]:
        return {
            "len": len(s or ""),
            "has_dot": ("." in (s or "")),
            "prefix": (s[:3] + "…") if s else "",
            "suffix": ("…" + s[-3:]) if s else "",
        }

    return {
        "TNS_API_URL": api_url,
        "TNS_API_KEY": mask(api_key),
        "TNS_USER_AGENT_len": len(ua or ""),
        "TNS_BOT_ID_set": bool(bot_id),
        "TNS_BOT_NAME_set": bool(bot_name),
    }


def envcheck(env_path: str, show_ua: bool = False) -> int:
    _load_env(env_path)
    snap = _safe_cfg_snapshot()
    print(json.dumps(snap, indent=2))
    if show_ua:
        ua = os.getenv("TNS_USER_AGENT", "").strip()
        # UA is not a secret per se, but keep it on-demand only.
        print("\nUSER_AGENT:\n" + ua)
    return 0


def _make_client() -> TnsClient:
    api_url = _get_required("TNS_API_URL").rstrip("/")
    api_key = _get_required("TNS_API_KEY")
    ua = _get_required("TNS_USER_AGENT")
    return TnsClient(api_base_url=api_url, api_key=api_key, user_agent=ua, timeout_s=30.0)


def submit_min(env_path: str) -> int:
    """
    Minimal 'submit' for sandbox connectivity: tries to obtain a report_id with a very small payload.

    Important:
      - This is NOT your science payload.
      - This exists only to validate submit/reply plumbing end-to-end.
    """
    _load_env(env_path)
    client = _make_client()

    url = f"{client.api_base_url}/set/bulk-report"

    # Minimal payload attempt (designed to be "schema-light").
    # If your server requires full schema, your existing submit-min already works; keep using it.
    payload = {
        "api_key": client.api_key,
        "data": {
            # try empty report list; many bulk systems accept and return report_id with a reply describing issues.
            "report": []
        },
    }

    files = {
        "data": ("data", json.dumps(payload), "application/json"),
    }

    t0 = time.perf_counter()
    resp = requests.post(
        url,
        headers={"User-Agent": client.user_agent, "Accept": "application/json"},
        files=files,
        timeout=30.0,
    )
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    try:
        j = resp.json()
    except Exception:
        j = None

    print(f"submit_url: {url}")
    print(f"result: http={resp.status_code} elapsed_ms={elapsed_ms}")
    if isinstance(j, dict):
        print(f"id_code: {j.get('id_code')}")
        print(f"id_message: {j.get('id_message')}")
        if "report_id" in j:
            print(f"report_id: {j.get('report_id')}")
    else:
        print("non_json_body_preview:", (resp.text or "")[:500])

    # keep exit code strict: HTTP 200 is expected
    return 0 if resp.status_code == 200 else 2


def reply(env_path: str, report_id: str) -> int:
    """
    P0: robust reply retriever. Tries multiple encodings to eliminate 401 caused by parsing mismatch.
    """
    _load_env(env_path)
    client = _make_client()

    res = client.get_bulk_report_reply(report_id=str(report_id))

    print("reply_url:", f"{client.api_base_url}/get/bulk-report-reply")
    print("client_identity:", json.dumps(client.debug_identity(), indent=2))

    att = res.attempt
    print("\nreply_attempt:")
    print(json.dumps(asdict(att), indent=2))

    if res.raw_json is not None:
        # Do NOT print secrets; api_key is never echoed by our client.
        print("\nreply_json (truncated view):")
        # Print a truncated JSON view to keep logs readable
        txt = json.dumps(res.raw_json, ensure_ascii=False)
        print(txt[:2000] + ("…" if len(txt) > 2000 else ""))
    else:
        if att.body_preview:
            print("\nreply_body_preview:")
            print(att.body_preview)

    # Success definition: we got something useful without 401 loops.
    if res.ok:
        return 0
    # If still 401, keep it explicit
    if att.http_status == 401 or att.id_message == "Unauthorized":
        return 3
    return 4


def probe(env_path: str) -> int:
    """
    Quick diagnostics:
      - prints env snapshot
      - runs a reply attempt with a clearly invalid report_id to see how auth/parsing behaves
    """
    _load_env(env_path)
    print("env_snapshot:", json.dumps(_safe_cfg_snapshot(), indent=2))

    # Use a fake report_id. If we still get 401, it's parsing/auth.
    fake_id = "0"
    print("\n--- probing reply with report_id=0 ---")
    return reply(env_path, fake_id)

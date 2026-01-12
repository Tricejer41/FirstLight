from __future__ import annotations

import os
from typing import Dict, Any, Tuple

class TNSSubmitter:
    """Submission stub.

    Why stub?
    - The official TNS docs are behind a site that sometimes blocks automated fetches.
    - Teams use different wrappers (direct bulk API vs helper libs).
    - This module is the seam: implement/replace with your preferred TNS client.

    What you need to provide (usually via BOT creation):
    - TNS_BOT_ID, TNS_BOT_NAME, TNS_API_KEY
    """

    def __init__(self):
        self.bot_id = os.getenv("TNS_BOT_ID", "").strip()
        self.bot_name = os.getenv("TNS_BOT_NAME", "").strip()
        self.api_key = os.getenv("TNS_API_KEY", "").strip()

    def enabled(self) -> bool:
        return bool(self.bot_id and self.bot_name and self.api_key)

    def submit_at_report(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.enabled():
            return False, "TNS submission disabled (missing env vars). See config/tns.example.env"
        # TODO: Implement real submission.
        # Suggested approach: use a maintained wrapper (or implement the bulk API call).
        return False, "TNS submission not implemented in stub."

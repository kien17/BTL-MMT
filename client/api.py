from __future__ import annotations

import json
from pathlib import Path


def parse_json_body(body: bytes) -> dict:
    if not body:
        return {}
    value = json.loads(body.decode("utf-8"))
    return value if isinstance(value, dict) else {}


def sanitize_filename(name: str) -> str:
    candidate = Path(name).name.strip()
    return candidate or "upload.bin"

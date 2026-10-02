from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_catalog = json.loads((Path(__file__).resolve().parent / "locale.json").read_text())
language = os.environ.get("ME_LANGUAGE", "en")


def tr(template: str, **params: Any) -> str:
    return _catalog.get(language, {}).get(template, template).format(**params)

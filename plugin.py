from __future__ import annotations

import os
import platform
import sys
from pathlib import Path
from typing import Any
from localization import language

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "core" / "bridge" / "sdk" / "python"))
from core_bridge import Context, run


async def start(ctx: Context) -> None:
    async def info(method: str, _params: Any) -> Any:
        if method != "info":
            raise ValueError(f"unknown method {method}")
        return {"name": ctx.plugin_id, "version": "b1-Rc6", "generation": ctx.generation,
                "python": platform.python_version(), "pid": os.getpid(), "language": language}

    await ctx.provide("me.init.info", info)


run(start)

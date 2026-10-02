from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import localization
from localization import tr

VERSION = "b1-Rc4"
_output_lock = threading.Lock()


def emit(event: dict[str, Any], protocol: bool) -> None:
    with _output_lock:
        try:
            if protocol:
                print(json.dumps(event, ensure_ascii=False), flush=True)
            elif event.get("type") == "log":
                print(f'[{event["level"].upper()}] {event["source"]}  {event["message"]}', flush=True)
            elif event.get("type") == "ready":
                for plugin in event["snapshot"]["plugins"]:
                    print(f'  {plugin["id"]:<24} {plugin["state"]}', flush=True)
        except BrokenPipeError:
            # 显示端关闭后仍继续回收宿主
            pass


def log(level: str, message: str, protocol: bool) -> None:
    emit({"type": "log", "time": datetime.now(timezone.utc).isoformat(), "level": level,
          "source": "me.init", "message": message}, protocol)


def build_host(root: Path, protocol: bool) -> Path:
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(tr("Go 1.26+ is required to build me.init"))
    version = subprocess.check_output([go, "version"], text=True).strip()
    digest = hashlib.sha256(f"{version}:{platform.system()}:{platform.machine()}".encode())
    files = [root / "core" / "go.mod", root / "me.init" / "host" / "go.mod"]
    for directory in (root / "core", root / "me.init" / "host"):
        files.extend(path for path in directory.rglob("*.go")
                     if not path.name.endswith("_test.go") and "examples" not in path.parts)
    for path in sorted(files):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    cache_root = Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
    cache = cache_root / "testbot" / "me.init" / digest.hexdigest()[:24]
    binary = cache / ("host.exe" if os.name == "nt" else "host")
    if binary.is_file():
        log("info", tr("using cached runtime host"), protocol)
        return binary
    cache.mkdir(parents=True, exist_ok=True)
    log("info", tr("building runtime host"), protocol)
    fd, temporary = tempfile.mkstemp(prefix="host-", dir=cache)
    os.close(fd)
    try:
        child = subprocess.Popen([go, "build", "-o", temporary, "."], cwd=root / "me.init" / "host",
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        def interrupted(_signal: int, _frame: Any) -> None:
            raise InterruptedError(tr("host build interrupted"))
        previous = {number: signal.signal(number, interrupted) for number in (signal.SIGINT, signal.SIGTERM)}
        try:
            _, stderr = child.communicate(timeout=120)
        finally:
            if child.poll() is None:
                kill_descendants(child.pid)
                child.kill()
                child.communicate()
            for number, handler in previous.items():
                signal.signal(number, handler)
        if child.returncode:
            raise RuntimeError(stderr.strip() or tr("host build failed"))
        os.replace(temporary, binary)
    finally:
        Path(temporary).unlink(missing_ok=True)
    log("ok", tr("runtime host built"), protocol)
    return binary


def kill_descendants(pid: int) -> None:
    # bridge进程有自己的进程组，强制退出时不能只杀宿主
    if sys.platform != "linux":
        return
    children: dict[int, list[int]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            children.setdefault(int(fields[1]), []).append(int(entry.name))
        except (OSError, ValueError, IndexError):
            continue
    def visit(parent: int) -> None:
        for child in children.get(parent, []):
            visit(child)
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
    visit(pid)


def supervise(binary: Path, root: Path, config: Path, protocol: bool) -> int:
    env = os.environ.copy()
    env.update(ME_NODE=env.get("ME_NODE") or shutil.which("node") or "node",
               ME_PYTHON=sys.executable, PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    child = subprocess.Popen([str(binary), "--root", str(root), "--config", str(config)],
                             stdin=sys.stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             env=env, text=True, encoding="utf-8", errors="replace", bufsize=1)
    stopping = threading.Event()

    def stop(_signal: int, _frame: Any) -> None:
        if not stopping.is_set():
            stopping.set()
            if child.poll() is None:
                child.terminate()

    def deadline() -> None:
        stopping.wait()
        try:
            child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            log("error", tr("shutdown timed out; external cleanup is unconfirmed"), protocol)
            kill_descendants(child.pid)
            child.kill()

    def stderr() -> None:
        assert child.stderr is not None
        for line in child.stderr:
            log("error", line.rstrip(), protocol)

    def stdout() -> None:
        assert child.stdout is not None
        for line in child.stdout:
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError("expected object")
                emit(event, protocol)
            except (ValueError, TypeError):
                log("error", tr("invalid host output"), protocol)

    previous = {number: signal.signal(number, stop) for number in (signal.SIGINT, signal.SIGTERM)}
    threading.Thread(target=deadline, daemon=True).start()
    output_thread = threading.Thread(target=stdout)
    error_thread = threading.Thread(target=stderr)
    output_thread.start()
    error_thread.start()
    try:
        code = child.wait()
    finally:
        if child.poll() is None:
            stop(signal.SIGTERM, None)
            child.wait()
        stopping.set()
        output_thread.join()
        error_thread.join()
        for number, handler in previous.items():
            signal.signal(number, handler)
    if code != 0:
        log("error", tr("runtime host exited ({code})", code=code), protocol)
    return code if code >= 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=tr("Initialize and supervise the Testbot runtime"))
    parser.add_argument("--language", choices=("zh", "en"), default=os.environ.get("ME_LANGUAGE", "en"))
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--protocol", action="store_true", help=tr("emit JSON Lines for me.CLI"))
    args = parser.parse_args()
    localization.language = args.language
    os.environ["ME_LANGUAGE"] = args.language
    root = args.root.resolve()
    config = (args.config or root / "me.init" / "plugins.json").resolve()
    log("info", tr("Starting me.init {version}", version=VERSION), args.protocol)
    try:
        if not config.is_file():
            raise RuntimeError(tr("config not found: {path}", path=config))
        binary = build_host(root, args.protocol)
        return supervise(binary, root, config, args.protocol)
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        log("error", str(error), args.protocol)
        return 1


if __name__ == "__main__":
    sys.exit(main())

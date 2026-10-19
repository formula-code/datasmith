#!/usr/bin/env python3
"""Host-side semaphore limiting concurrent LSV measure steps (timing is memory-bandwidth sensitive).

Trials POST /acquire?sid=X before measuring and /release?sid=X after; a trial runs ungated when the gate is down,
and a lease TTL frees the slots of dead holders. Defaults env-overridable via MEASURE_GATE_{PORT,SLOTS,TTL,ACQUIRE_WAIT}.
With --secret-file, acquire and release also need &token=<label>.<HMAC-SHA256(secret, label)>; the token is
put only in the verifier's environment, so the agent cannot hold or free slots.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import hmac
import json
import os
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DEFAULT_HOST = os.environ.get("MEASURE_GATE_HOST", "0.0.0.0")  # noqa: S104
DEFAULT_PORT = int(os.environ.get("MEASURE_GATE_PORT", "8266"))
DEFAULT_SLOTS = int(os.environ.get("MEASURE_GATE_SLOTS", "12"))
DEFAULT_TTL = float(os.environ.get("MEASURE_GATE_TTL", "2400"))
DEFAULT_ACQUIRE_WAIT = float(os.environ.get("MEASURE_GATE_ACQUIRE_WAIT", "7200"))
DEFAULT_SECRET_FILE = os.environ.get("MEASURE_GATE_SECRET_FILE", "")

SLOTS = DEFAULT_SLOTS
TTL = DEFAULT_TTL
ACQUIRE_WAIT = DEFAULT_ACQUIRE_WAIT
SECRET: bytes | None = None

_lock = threading.Lock()
_held: dict[str, float] = {}  # sid -> lease expiry
_timeouts: list[dict] = []  # acquires that gave up; the caller measures ungated
_sem = threading.Semaphore(SLOTS)


def _reap() -> None:
    now = time.time()
    with _lock:
        for sid in [s for s, exp in _held.items() if exp <= now]:
            del _held[sid]
            _sem.release()


def _acquire(sid: str) -> bool:
    _reap()
    start = time.time()
    if _sem.acquire(timeout=ACQUIRE_WAIT):
        with _lock:
            _held[sid] = time.time() + TTL
        return True
    with _lock:
        _timeouts.append({"sid": sid, "requested_at": start, "timed_out_at": time.time()})
    return False


def _release(sid: str) -> bool:
    with _lock:
        if sid in _held:
            _held.pop(sid, None)
            _sem.release()
            return True
    return False


def token_for(secret: bytes, label: str) -> str:
    return f"{label}.{hmac.new(secret, label.encode(), hashlib.sha256).hexdigest()}"


def token_ok(token: str) -> bool:
    if SECRET is None:
        return True
    label, _, _ = token.rpartition(".")
    return bool(label) and hmac.compare_digest(token_for(SECRET, label), token)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        with contextlib.suppress(OSError):
            self.wfile.write(body)

    def _client_gone(self) -> bool:
        try:
            if not select.select([self.connection], [], [], 0)[0]:
                return False
            return self.connection.recv(1, socket.MSG_PEEK) == b""
        except OSError:
            return True

    def _query(self, key: str, default: str) -> str:
        return (parse_qs(urlparse(self.path).query).get(key) or [default])[0]

    def do_GET(self):
        p = urlparse(self.path).path
        if p in ("/health", "/"):
            with _lock:
                self._send(
                    200,
                    {
                        "slots": SLOTS,
                        "ttl": TTL,
                        "acquire_wait": ACQUIRE_WAIT,
                        "in_use": len(_held),
                        "held": list(_held.keys()),
                        "n_timeouts": len(_timeouts),
                    },
                )
        elif p == "/timeouts":
            with _lock:
                self._send(200, {"timeouts": list(_timeouts)})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        p = urlparse(self.path).path
        sid = self._query("sid", "anon")
        if p in ("/acquire", "/release") and not token_ok(self._query("token", "")):
            self._send(403, {"error": "bad token", "sid": sid})
        elif p == "/acquire":
            acquired = _acquire(sid)
            # A client that gave up while waiting (timeout, killed trial) would otherwise keep this slot until the TTL.
            if acquired and self._client_gone():
                _release(sid)
                return
            self._send(200, {"acquired": acquired, "sid": sid})
        elif p == "/release":
            self._send(200, {"released": _release(sid), "sid": sid})
        else:
            self._send(404, {"error": "not found"})


def _reaper_loop():
    while True:
        time.sleep(10)
        _reap()


def main() -> None:
    global SLOTS, TTL, ACQUIRE_WAIT, SECRET, _sem
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=DEFAULT_HOST, help="comma separated addresses")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--slots", type=int, default=DEFAULT_SLOTS)
    ap.add_argument("--ttl", type=float, default=DEFAULT_TTL)
    ap.add_argument("--acquire-wait", type=float, default=DEFAULT_ACQUIRE_WAIT)
    ap.add_argument("--secret-file", default=DEFAULT_SECRET_FILE, help="require tokens made with this secret")
    a = ap.parse_args()
    SLOTS, TTL, ACQUIRE_WAIT = a.slots, a.ttl, a.acquire_wait
    SECRET = Path(a.secret_file).read_bytes().strip() if a.secret_file else None
    _sem = threading.Semaphore(SLOTS)
    threading.Thread(target=_reaper_loop, daemon=True).start()
    servers = [ThreadingHTTPServer((host, a.port), Handler) for host in a.host.split(",")]
    for srv in servers[1:]:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(
        f"[measure_gate] up on {a.host}:{a.port} slots={SLOTS} ttl={TTL}s acquire_wait={ACQUIRE_WAIT}s tokens={SECRET is not None}",
        flush=True,
    )
    servers[0].serve_forever()


if __name__ == "__main__":
    main()

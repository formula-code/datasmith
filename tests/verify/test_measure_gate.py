import http.client
import json
import threading
import time
from contextlib import suppress
from http.server import ThreadingHTTPServer
from unittest import mock

from datasmith.verify.services import measure_gate


def serve():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), measure_gate.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def post(srv, path):
    c = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
    c.request("POST", path)
    r = c.getresponse()
    return r.status, json.loads(r.read())


def test_acquire_timeout_is_recorded():
    with (
        mock.patch.object(measure_gate, "_sem") as sem,
        mock.patch.object(measure_gate, "ACQUIRE_WAIT", 0),
        mock.patch.object(measure_gate, "_timeouts", []) as timeouts,
    ):
        sem.acquire.return_value = False
        assert measure_gate._acquire("s1") is False
        assert [t["sid"] for t in timeouts] == ["s1"]


def test_a_client_that_gave_up_does_not_keep_the_slot():
    with (
        mock.patch.object(measure_gate, "_sem", threading.Semaphore(1)),
        mock.patch.object(measure_gate, "_held", {}),
        mock.patch.object(measure_gate, "ACQUIRE_WAIT", 30),
        mock.patch.object(measure_gate, "TTL", 600),
    ):
        srv = serve()
        assert measure_gate._acquire("holder")
        waiter = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=0.5)
        waiter.request("POST", "/acquire?sid=gone")
        with suppress(OSError):
            waiter.getresponse()
        waiter.close()
        measure_gate._release("holder")
        time.sleep(0.5)
        srv.shutdown()
        assert "gone" not in measure_gate._held


def test_gate_with_a_secret_needs_a_token():
    with (
        mock.patch.object(measure_gate, "_sem", threading.Semaphore(1)),
        mock.patch.object(measure_gate, "_held", {}),
        mock.patch.object(measure_gate, "SECRET", b"s3"),
    ):
        srv = serve()
        good = measure_gate.token_for(b"s3", "job-1")
        assert post(srv, "/acquire?sid=a")[0] == 403
        assert post(srv, f"/acquire?sid=a&token={good[:-1]}x")[0] == 403
        assert post(srv, f"/acquire?sid=a&token={good}") == (200, {"acquired": True, "sid": "a"})
        assert post(srv, "/release?sid=a")[0] == 403 and "a" in measure_gate._held
        assert post(srv, f"/release?sid=a&token={good}")[1]["released"]
        srv.shutdown()

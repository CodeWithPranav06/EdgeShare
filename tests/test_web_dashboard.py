"""Web dashboard: HTTP API against a real coordinator and worker, plus static checks of
the page (the 3D itself is checked by eye in a browser)."""
import io
import json
import re
import socket
import time
import urllib.error
import urllib.request

import pytest
from PIL import Image

from compute.coordinator import TaskCoordinator
from gui.web_server import PAGE, start_web_dashboard
from tests.test_networking import FakeWorker, Server, free_port, make_png, wait_for


def get(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.status, response.headers, response.read()


def post(url, data):
    request = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


@pytest.fixture
def web(tmp_path):
    with Server() as server:
        http, url = start_web_dashboard(server.coordinator, server.port, port=free_port(),
                                        host="127.0.0.1", out_dir=tmp_path / "out")
        assert http is not None, url
        base = "http://127.0.0.1:%d" % http.server_address[1]
        yield server, base
        http.shutdown()
        http.server_close()


def test_page_and_state_are_served(web):
    server, base = web
    status, headers, body = get(base + "/")
    assert status == 200 and headers["Content-Type"].startswith("text/html")
    assert b"<title>EdgeShare" in body and b"three@0.160.0" in body

    worker = FakeWorker(server.port, "mac")
    worker.start()
    assert wait_for(lambda: "mac" in server.registry)
    status, _, body = get(base + "/api/state")
    state = json.loads(body)
    worker.kill()
    assert status == 200
    assert [w["worker_id"] for w in state["workers"]] == ["mac"]
    assert state["coordinator"]["port"] == server.port
    assert state["counts"]["total"] == 0 and state["uploads"] == {}
    assert state["events"][0][1] == "worker mac registered"


def test_browser_upload_is_resized_by_a_worker_and_downloadable(web, tmp_path):
    server, base = web
    worker = FakeWorker(server.port, "W1")
    worker.start()
    assert wait_for(lambda: "W1" in server.registry)

    image = make_png(tmp_path / "holiday.png", 100, 60)
    status, reply = post(base + "/api/submit?name=holiday.png&size=48", image.read_bytes())
    assert status == 202
    task_id = reply["task_id"]

    def finished():
        upload = json.loads(get(base + "/api/state")[2])["uploads"].get(task_id, {})
        return upload.get("done")
    assert wait_for(finished, 15)
    state = json.loads(get(base + "/api/state")[2])
    worker.kill()

    upload = state["uploads"][task_id]
    assert upload["ok"] and upload["worker_id"] == "W1" and upload["name"] == "holiday.png"
    assert state["tasks"][0]["task_id"] == task_id and state["tasks"][0]["state"] == "completed"
    status, headers, body = get(base + "/api/result/" + task_id)
    assert status == 200 and headers["Content-Type"] == "image/png"
    assert "holiday_resized.png" in headers["Content-Disposition"]
    with Image.open(io.BytesIO(body)) as result:
        assert result.size == (48, 48)
    assert (tmp_path / "out" / "holiday_resized.png").read_bytes() == body


def test_upload_name_cannot_escape_output_folder(web, tmp_path):
    server, base = web
    worker = FakeWorker(server.port, "W1")
    worker.start()
    assert wait_for(lambda: "W1" in server.registry)
    image = make_png(tmp_path / "a.png")
    status, reply = post(base + "/api/submit?name=..%2F..%2Fevil.png&size=8", image.read_bytes())
    assert status == 202
    assert wait_for(lambda: json.loads(get(base + "/api/state")[2])["uploads"][reply["task_id"]]["done"], 15)
    worker.kill()
    assert (tmp_path / "out" / "evil_resized.png").exists()
    assert not (tmp_path / "evil_resized.png").exists()


@pytest.mark.parametrize("query,body,code", [
    ("name=a.png&size=0", b"x", 400),
    ("name=a.png&size=99999", b"x", 400),
    ("name=a.png&size=abc", b"x", 400),
    ("name=a.png&size=64", b"", 413),
])
def test_bad_uploads_are_rejected(web, query, body, code):
    _, base = web
    status, reply = post(base + "/api/submit?" + query, body)
    assert status == code and reply["error"]


def test_unknown_paths_and_results_are_404(web):
    _, base = web
    for path in ("/nope", "/api/result/web-missing", "/api/result/../../config.py"):
        with pytest.raises(urllib.error.HTTPError) as err:
            get(base + path)
        assert err.value.code == 404
    assert get(base + "/favicon.ico")[0] == 204


def test_busy_port_reports_instead_of_crashing():
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        server, message = start_web_dashboard(TaskCoordinator(), 5000, port=taken.getsockname()[1],
                                              host="127.0.0.1")
    assert server is None and "not started" in message


# ---- static checks of the page ---------------------------------------------------------------

def test_page_element_ids_used_by_the_script_exist():
    html = PAGE.read_text(encoding="utf-8")
    defined = set(re.findall(r'\bid="([^"]+)"', html))
    used = set(re.findall(r"\$\('([A-Za-z0-9_-]+)'\)", html))
    assert used and used <= defined, used - defined
    for prefix, count in (("slide-", 4), ("dash-fill-", 4)):
        assert {"%s%d" % (prefix, n) for n in range(1, count + 1)} <= defined
    assert {"panel-2", "panel-3", "panel-4"} <= defined


def test_page_inserts_server_text_only_through_escaping():
    html = PAGE.read_text(encoding="utf-8")
    # every ${...} that prints a worker / task / upload / event field goes through esc()
    risky = re.findall(r"\$\{(?:w|t|u|e)\.(?:worker_id|reason|name|error|task_id|text|state)\}", html)
    assert risky == []

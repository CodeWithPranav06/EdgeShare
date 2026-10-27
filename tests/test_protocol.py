import socket
import threading

from networking.protocol import recv_msg, send_msg


def test_roundtrip_with_payload():
    a, b = socket.socketpair()
    payload = b"x" * 100_000
    t = threading.Thread(target=send_msg, args=(a, {"type": "TASK", "task_id": 1}, payload))
    t.start()
    header, data = recv_msg(b)
    t.join()
    assert header["type"] == "TASK" and header["task_id"] == 1
    assert data == payload

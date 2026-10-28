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


def test_file_transfer_with_checksum(tmp_path):
    import pytest
    from networking.protocol import recv_file, send_file
    src = tmp_path / "pic.bin"
    src.write_bytes(b"abc" * 50_000)
    a, b = socket.socketpair()
    t = threading.Thread(target=send_file, args=(a, str(src)))
    t.start()
    out = recv_file(b, str(tmp_path / "recv"))
    t.join()
    assert open(out, "rb").read() == src.read_bytes()

import hashlib
import random
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from vdb_client import VDBProtocolError, connect


def random_port() -> int:
    return 56000 + random.randint(0, 3000)


def server_binary_path() -> Path:
    name = "vdb.exe" if __import__("sys").platform == "win32" else "vdb"
    return Path(__file__).resolve().parents[2] / "build" / name


def wait_for_server(host: str, port: int, proc: subprocess.Popen, timeout_sec: float = 8.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if proc.poll() is not None:
            stderr = ""
            if proc.stderr is not None:
                try:
                    stderr = proc.stderr.read().decode("utf-8", errors="replace")
                except Exception:
                    stderr = ""
            raise RuntimeError(f"Server exited early with code {proc.returncode}: {stderr}")
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("Timed out waiting for server startup")


class RawProtocolConnection:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5.0)
        self.file = self.sock.makefile("rwb", buffering=0)

    def read_line(self) -> str:
        raw = self.file.readline()
        if not raw:
            raise RuntimeError("Connection closed by server")
        return raw.decode("utf-8", errors="replace").rstrip("\r\n")

    def write_line(self, line: str) -> None:
        self.file.write((line + "\n").encode("utf-8"))

    def close(self) -> None:
        try:
            self.file.close()
        finally:
            self.sock.close()


class TestPythonClientAuthParity(unittest.TestCase):
    host = "127.0.0.1"

    def setUp(self) -> None:
        self.port = random_port()
        exe = server_binary_path()
        self.server = subprocess.Popen(
            [str(exe), "--server", "--host", self.host, "--port", str(self.port), "--auth-mode", "password"],
            cwd=str(Path(__file__).resolve().parents[2]),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        wait_for_server(self.host, self.port, self.server)

    def tearDown(self) -> None:
        if self.server.poll() is None:
            self.server.kill()
            try:
                self.server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.server.kill()
        if self.server.stderr is not None:
            self.server.stderr.close()

    def test_auth_required_and_permission_denied(self) -> None:
        with connect(host=self.host, port=self.port) as unauth:
            cur = unauth.cursor()
            with self.assertRaises(VDBProtocolError) as auth_required_ctx:
                cur.execute("SELECT 1;")
            self.assertIn("auth_required", str(auth_required_ctx.exception))

        with connect(host=self.host, port=self.port, user="admin", password="admin") as admin:
            cur = admin.cursor()
            cur.execute("CREATE USER py_bob IDENTIFIED BY 'bobpw';")
            cur.execute("CREATE TABLE py_auth_t(id INT);")
            cur.execute("INSERT INTO py_auth_t VALUES (1);")

        with connect(host=self.host, port=self.port, user="py_bob", password="bobpw") as bob:
            cur = bob.cursor()
            with self.assertRaises(VDBProtocolError) as denied_ctx:
                cur.execute("SELECT id FROM py_auth_t;")
            self.assertIn("permission_denied", str(denied_ctx.exception))

    def test_auth_failed_on_invalid_password(self) -> None:
        with self.assertRaises(VDBProtocolError) as ctx:
            connect(host=self.host, port=self.port, user="admin", password="wrong-password")
        self.assertIn("auth_failed", str(ctx.exception))

    def test_auth_nonce_expired_after_stale_proof_reuse(self) -> None:
        raw = RawProtocolConnection(self.host, self.port)
        try:
            self.assertEqual(raw.read_line(), "HELLO VDB")
            self.assertTrue(raw.read_line().startswith("SESSION "))

            raw.write_line("AUTH_START admin")
            self.assertTrue(raw.read_line().startswith("AUTH_CHALLENGE "))

            raw.write_line("AUTH_PROOF deadbeef")
            self.assertEqual(raw.read_line(), "AUTH_ERROR auth_failed")

            raw.write_line("AUTH_PROOF deadbeef")
            self.assertEqual(raw.read_line(), "AUTH_ERROR auth_nonce_expired")
        finally:
            raw.close()

    def test_auth_locked_after_repeated_failed_proofs(self) -> None:
        raw = RawProtocolConnection(self.host, self.port)
        try:
            self.assertEqual(raw.read_line(), "HELLO VDB")
            self.assertTrue(raw.read_line().startswith("SESSION "))

            for _ in range(3):
                raw.write_line("AUTH_START admin")
                self.assertTrue(raw.read_line().startswith("AUTH_CHALLENGE "))
                raw.write_line("AUTH_PROOF deadbeef")
                self.assertEqual(raw.read_line(), "AUTH_ERROR auth_failed")

            raw.write_line("AUTH_START admin")
            self.assertEqual(raw.read_line(), "AUTH_ERROR auth_locked")
        finally:
            raw.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

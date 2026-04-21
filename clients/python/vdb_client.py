import socket
from typing import List, Optional, Tuple


class VDBProtocolError(RuntimeError):
    pass


class Cursor:
    def __init__(self, conn: "Connection"):
        self._conn = conn
        self._last_status: Optional[str] = None
        self._last_output: List[str] = []

    def execute(self, sql: str) -> "Cursor":
        status, output = self._conn._execute(sql)
        self._last_status = status
        self._last_output = output
        if status != "OK":
            raise VDBProtocolError("SQL execution failed: " + "\n".join(output))
        return self

    def fetchall(self) -> List[str]:
        # v1 protocol returns textual engine output; parsing into rows is future work.
        return list(self._last_output)


class Connection:
    def __init__(self, host: str = "127.0.0.1", port: int = 54330, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._file = self._sock.makefile("rwb", buffering=0)
        self.session = self._read_handshake()

    def _readline(self) -> str:
        raw = self._file.readline()
        if not raw:
            raise VDBProtocolError("Connection closed by server")
        return raw.decode("utf-8", errors="replace").rstrip("\r\n")

    def _writeline(self, line: str) -> None:
        data = (line + "\n").encode("utf-8")
        self._file.write(data)

    def _read_handshake(self) -> str:
        hello = self._readline()
        session = self._readline()
        if hello != "HELLO VDB" or not session.startswith("SESSION "):
            raise VDBProtocolError(f"Unexpected handshake: {hello!r}, {session!r}")
        return session[len("SESSION "):]

    def ping(self) -> bool:
        self._writeline("PING")
        return self._readline() == "PONG"

    def cursor(self) -> Cursor:
        return Cursor(self)

    def _execute(self, sql: str) -> Tuple[str, List[str]]:
        text = sql.strip()
        if not text:
            raise ValueError("SQL text must be non-empty")

        # Keep parity with server expectation: statements terminate with ';'
        if not text.endswith(";"):
            text += ";"

        for line in text.splitlines():
            if line.strip():
                self._writeline(line)
                maybe = self._readline()
                if maybe == "CONTINUE":
                    continue
                if maybe in ("OK", "ERROR"):
                    return maybe, self._read_until_end()
                raise VDBProtocolError(f"Unexpected response line: {maybe!r}")

        # If server never emitted status, fetch next expected line.
        status = self._readline()
        if status not in ("OK", "ERROR"):
            raise VDBProtocolError(f"Expected status, got: {status!r}")
        return status, self._read_until_end()

    def _read_until_end(self) -> List[str]:
        lines: List[str] = []
        while True:
            line = self._readline()
            if line == "END":
                return lines
            lines.append(line)

    def close(self) -> None:
        try:
            self._writeline("QUIT")
            _ = self._readline()
        except Exception:
            pass
        finally:
            try:
                self._file.close()
            finally:
                self._sock.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def connect(host: str = "127.0.0.1", port: int = 54330, timeout: float = 10.0) -> Connection:
    return Connection(host=host, port=port, timeout=timeout)

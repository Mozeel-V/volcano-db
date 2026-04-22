const net = require("net");

class VDBProtocolError extends Error {
  constructor(message) {
    super(message);
    this.name = "VDBProtocolError";
  }
}

function splitColumns(line) {
  return String(line || "")
    .trimEnd()
    .split(/\s{2,}/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function parseTextTable(lines) {
  if (!Array.isArray(lines) || lines.length < 3) {
    return null;
  }

  const rowCountIdx = lines.findIndex((line) => /^\(\d+ rows\)$/.test(String(line || "").trim()));
  if (rowCountIdx < 0 || rowCountIdx < 2) {
    return null;
  }

  const headerLine = lines[0];
  const separatorLine = lines[1];
  if (!/^[-\s]+$/.test(String(separatorLine || ""))) {
    return null;
  }

  const columns = splitColumns(headerLine);
  if (columns.length === 0) {
    return null;
  }

  const rowLines = lines.slice(2, rowCountIdx);
  const rows = rowLines
    .filter((line) => String(line || "").trim().length > 0)
    .map((line) => {
      const values = splitColumns(line);
      const row = {};
      for (let i = 0; i < columns.length; i += 1) {
        row[columns[i]] = values[i] ?? "";
      }
      return row;
    });

  const countMatch = String(lines[rowCountIdx]).trim().match(/^\((\d+) rows\)$/);
  const rowCount = countMatch ? Number(countMatch[1]) : rows.length;

  return {
    columns,
    rows,
    rowCount,
  };
}

class Connection {
  constructor(socket, host, port) {
    this._socket = socket;
    this.host = host;
    this.port = port;
    this.session = null;
    this._queue = [];
    this._waiters = [];
    this._buffer = "";

    this._socket.on("data", (chunk) => this._onData(chunk));
    this._socket.on("error", (err) => this._rejectAll(err));
    this._socket.on("close", () => this._rejectAll(new VDBProtocolError("Connection closed")));
  }

  static connect({ host = "127.0.0.1", port = 54330, timeoutMs = 10000 } = {}) {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host, port });
      const timeout = setTimeout(() => {
        socket.destroy();
        reject(new Error("Connection timeout"));
      }, timeoutMs);

      socket.once("error", (err) => {
        clearTimeout(timeout);
        reject(err);
      });

      socket.once("connect", async () => {
        clearTimeout(timeout);
        try {
          const conn = new Connection(socket, host, port);
          const hello = await conn._readLine();
          const session = await conn._readLine();
          if (hello !== "HELLO VDB" || !session.startsWith("SESSION ")) {
            throw new VDBProtocolError(`Unexpected handshake: ${hello}, ${session}`);
          }
          conn.session = session.slice("SESSION ".length);
          resolve(conn);
        } catch (err) {
          socket.destroy();
          reject(err);
        }
      });
    });
  }

  _onData(chunk) {
    this._buffer += chunk.toString("utf8");
    let idx = this._buffer.indexOf("\n");
    while (idx >= 0) {
      let line = this._buffer.slice(0, idx);
      this._buffer = this._buffer.slice(idx + 1);
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      if (this._waiters.length > 0) {
        const waiter = this._waiters.shift();
        waiter.resolve(line);
      } else {
        this._queue.push(line);
      }
      idx = this._buffer.indexOf("\n");
    }
  }

  _rejectAll(err) {
    while (this._waiters.length > 0) {
      const waiter = this._waiters.shift();
      waiter.reject(err);
    }
  }

  _readLine() {
    if (this._queue.length > 0) {
      return Promise.resolve(this._queue.shift());
    }
    return new Promise((resolve, reject) => {
      this._waiters.push({ resolve, reject });
    });
  }

  _writeLine(line) {
    return new Promise((resolve, reject) => {
      this._socket.write(`${line}\n`, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async ping() {
    await this._writeLine("PING");
    return (await this._readLine()) === "PONG";
  }

  cursor() {
    return new Cursor(this);
  }

  async execute(sql) {
    const text = String(sql || "").trim();
    if (!text) {
      throw new Error("SQL text must be non-empty");
    }

    const normalized = text.endsWith(";") ? text : `${text};`;
    const lines = normalized.split(/\r?\n/).filter((l) => l.trim().length > 0);

    for (const line of lines) {
      await this._writeLine(line);
      const maybe = await this._readLine();
      if (maybe === "CONTINUE") {
        continue;
      }
      if (maybe === "OK" || maybe === "ERROR") {
        return {
          status: maybe,
          output: await this._readUntilEnd(),
        };
      }
      throw new VDBProtocolError(`Unexpected response line: ${maybe}`);
    }

    const status = await this._readLine();
    if (status !== "OK" && status !== "ERROR") {
      throw new VDBProtocolError(`Expected status, got: ${status}`);
    }

    return {
      status,
      output: await this._readUntilEnd(),
    };
  }

  async _readUntilEnd() {
    const lines = [];
    while (true) {
      const line = await this._readLine();
      if (line === "END") {
        return lines;
      }
      lines.push(line);
    }
  }

  async close() {
    try {
      await this._writeLine("QUIT");
      await this._readLine();
    } catch (_) {
      // Ignore close path errors.
    } finally {
      this._socket.end();
      this._socket.destroy();
    }
  }
}

class Cursor {
  constructor(conn) {
    this._conn = conn;
    this._lastStatus = null;
    this._lastOutput = [];
    this._lastTable = null;
  }

  async execute(sql) {
    const result = await this._conn.execute(sql);
    this._lastStatus = result.status;
    this._lastOutput = result.output;
    this._lastTable = parseTextTable(result.output);
    if (this._lastStatus !== "OK") {
      throw new VDBProtocolError(`SQL execution failed: ${this._lastOutput.join("\\n")}`);
    }
    return this;
  }

  fetchAll() {
    return [...this._lastOutput];
  }

  fetchTable() {
    if (!this._lastTable) return null;
    return {
      columns: [...this._lastTable.columns],
      rows: this._lastTable.rows.map((row) => ({ ...row })),
      rowCount: this._lastTable.rowCount,
    };
  }

  fetchRows() {
    const table = this.fetchTable();
    return table ? table.rows : [];
  }
}

module.exports = {
  connect: (opts) => Connection.connect(opts),
  Connection,
  Cursor,
  VDBProtocolError,
};

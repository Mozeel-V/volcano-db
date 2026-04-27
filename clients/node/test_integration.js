const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const fs = require("node:fs");
const { spawn } = require("node:child_process");
const net = require("node:net");
const { connect } = require("./vdb_client");

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomPort() {
  return 55000 + Math.floor(Math.random() * 4000);
}

function serverBinaryPath() {
  const name = process.platform === "win32" ? "vdb.exe" : "vdb";
  return path.resolve(__dirname, "..", "..", "build", name);
}

async function startServer(host, port) {
  return startServerWithOptions({ host, port });
}

async function startServerWithOptions({ host, port, authMode = null }) {
  const exe = serverBinaryPath();
  if (!fs.existsSync(exe)) {
    throw new Error(`Server binary not found at: ${exe}`);
  }
  const args = ["--server", "--host", host, "--port", String(port)];
  if (authMode) {
    args.push("--auth-mode", authMode);
  }
  const child = spawn(exe, args, {
    cwd: path.resolve(__dirname, "..", ".."),
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderr = "";
  let stdout = "";

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString("utf8");
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString("utf8");
  });

  for (let i = 0; i < 40; i += 1) {
    if (child.exitCode !== null) {
      throw new Error(
        `Server exited early (code=${child.exitCode})\n` +
        `exe=${exe}\nargs=${args.join(" ")}\n` +
        `stdout=${stdout}\nstderr=${stderr}`,
      );
    }
    const reachable = await new Promise((resolve) => {
      const probe = net.createConnection({ host, port });
      probe.on("connect", () => {
        probe.destroy();
        resolve(true);
      });
      probe.on("error", () => resolve(false));
    });
    if (reachable) {
      return child;
    }
    await delay(100);
  }

  child.kill();
  throw new Error("Timed out waiting for server start");
}

async function stopServer(child) {
  if (!child || child.exitCode !== null) return;
  child.kill();
  await delay(200);
  if (child.exitCode === null) {
    child.kill("SIGKILL");
  }
}

function createRawConnection(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    socket.once("error", reject);
    socket.once("connect", () => {
      socket.removeListener("error", reject);
      let buffer = "";
      const queue = [];
      const waiters = [];

      socket.on("data", (chunk) => {
        buffer += chunk.toString("utf8");
        let idx = buffer.indexOf("\n");
        while (idx >= 0) {
          let line = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 1);
          if (line.endsWith("\r")) {
            line = line.slice(0, -1);
          }
          if (waiters.length > 0) {
            waiters.shift()(line);
          } else {
            queue.push(line);
          }
          idx = buffer.indexOf("\n");
        }
      });

      const readLine = () => {
        if (queue.length > 0) {
          return Promise.resolve(queue.shift());
        }
        return new Promise((res) => waiters.push(res));
      };

      const writeLine = (line) => new Promise((res, rej) => {
        socket.write(`${line}\n`, (err) => (err ? rej(err) : res()));
      });

      resolve({
        socket,
        readLine,
        writeLine,
        close: () => {
          socket.end();
          socket.destroy();
        },
      });
    });
  });
}

test("Node client integration: ping and table parsing", async () => {
  const host = "127.0.0.1";
  const port = randomPort();
  const server = await startServer(host, port);

  try {
    const conn = await connect({ host, port });
    try {
      assert.equal(await conn.ping(), true);

      const tableName = `node_it_${Date.now()}`;
      const cur = conn.cursor();
      await cur.execute(`CREATE TABLE ${tableName} (id INT);`);
      await cur.execute(`INSERT INTO ${tableName} VALUES (21);`);
      await cur.execute(`SELECT id FROM ${tableName};`);

      const table = cur.fetchTable();
      assert.ok(table, "Expected parsed table output");
      assert.deepEqual(table.columns, ["id"]);
      assert.equal(table.rowCount, 1);
      assert.deepEqual(cur.fetchRows(), [{ id: "21" }]);
    } finally {
      await conn.close();
    }
  } finally {
    await stopServer(server);
  }
});

test("Node client integration: malformed SQL raises protocol error", async () => {
  const host = "127.0.0.1";
  const port = randomPort();
  const server = await startServer(host, port);

  try {
    const conn = await connect({ host, port });
    try {
      const cur = conn.cursor();
      await assert.rejects(async () => {
        await cur.execute("SELECT FROM broken_sql;");
      }, /SQL execution failed/);
    } finally {
      await conn.close();
    }
  } finally {
    await stopServer(server);
  }
});

test("Node client auth parity: auth_required and permission_denied", async () => {
  const host = "127.0.0.1";
  const port = randomPort();
  const server = await startServerWithOptions({ host, port, authMode: "password" });

  try {
    const unauth = await connect({ host, port });
    try {
      const cur = unauth.cursor();
      await assert.rejects(async () => {
        await cur.execute("SELECT 1;");
      }, /auth_required/);
    } finally {
      await unauth.close();
    }

    const admin = await connect({ host, port, user: "admin", password: "admin" });
    const tableName = `node_auth_${Date.now()}`;
    try {
      const cur = admin.cursor();
      await cur.execute("CREATE USER node_bob IDENTIFIED BY 'bobpw';");
      await cur.execute(`CREATE TABLE ${tableName} (id INT);`);
      await cur.execute(`INSERT INTO ${tableName} VALUES (1);`);
    } finally {
      await admin.close();
    }

    const bob = await connect({ host, port, user: "node_bob", password: "bobpw" });
    try {
      const cur = bob.cursor();
      await assert.rejects(async () => {
        await cur.execute(`SELECT id FROM ${tableName};`);
      }, /permission_denied/);
    } finally {
      await bob.close();
    }
  } finally {
    await stopServer(server);
  }
});

test("Node client auth parity: auth_failed on invalid password", async () => {
  const host = "127.0.0.1";
  const port = randomPort();
  const server = await startServerWithOptions({ host, port, authMode: "password" });

  try {
    await assert.rejects(async () => {
      await connect({ host, port, user: "admin", password: "wrong-password" });
    }, /auth_failed/);
  } finally {
    await stopServer(server);
  }
});

test("Node client auth parity: auth_nonce_expired after stale proof reuse", async () => {
  const host = "127.0.0.1";
  const port = randomPort();
  const server = await startServerWithOptions({ host, port, authMode: "password" });

  try {
    const raw = await createRawConnection(host, port);
    try {
      assert.equal(await raw.readLine(), "HELLO VDB");
      assert.match(await raw.readLine(), /^SESSION\s+/);

      await raw.writeLine("AUTH_START admin");
      assert.match(await raw.readLine(), /^AUTH_CHALLENGE\s+/);

      await raw.writeLine("AUTH_PROOF deadbeef");
      assert.equal(await raw.readLine(), "AUTH_ERROR auth_failed");

      await raw.writeLine("AUTH_PROOF deadbeef");
      assert.equal(await raw.readLine(), "AUTH_ERROR auth_nonce_expired");
    } finally {
      raw.close();
    }
  } finally {
    await stopServer(server);
  }
});

test("Node client auth parity: auth_locked after repeated failed proofs", async () => {
  const host = "127.0.0.1";
  const port = randomPort();
  const server = await startServerWithOptions({ host, port, authMode: "password" });

  try {
    const raw = await createRawConnection(host, port);
    try {
      assert.equal(await raw.readLine(), "HELLO VDB");
      assert.match(await raw.readLine(), /^SESSION\s+/);

      for (let i = 0; i < 3; i += 1) {
        await raw.writeLine("AUTH_START admin");
        assert.match(await raw.readLine(), /^AUTH_CHALLENGE\s+/);
        await raw.writeLine("AUTH_PROOF deadbeef");
        assert.equal(await raw.readLine(), "AUTH_ERROR auth_failed");
      }

      await raw.writeLine("AUTH_START admin");
      assert.equal(await raw.readLine(), "AUTH_ERROR auth_locked");
    } finally {
      raw.close();
    }
  } finally {
    await stopServer(server);
  }
});

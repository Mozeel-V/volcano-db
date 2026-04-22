const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
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
  const exe = serverBinaryPath();
  const child = spawn(exe, ["--server", "--host", host, "--port", String(port)], {
    cwd: path.resolve(__dirname, "..", ".."),
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderr = "";

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString("utf8");
  });

  for (let i = 0; i < 40; i += 1) {
    if (child.exitCode !== null) {
      throw new Error(`Server exited early (code=${child.exitCode}): ${stderr}`);
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

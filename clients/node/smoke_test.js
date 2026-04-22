const { connect } = require("./vdb_client");

function parseArgs(argv) {
  let host = "127.0.0.1";
  let port = 54330;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--host" && i + 1 < argv.length) {
      host = argv[i + 1];
      i += 1;
    } else if (arg === "--port" && i + 1 < argv.length) {
      port = Number(argv[i + 1]);
      i += 1;
    }
  }
  return { host, port };
}

async function main() {
  const { host, port } = parseArgs(process.argv.slice(2));
  const conn = await connect({ host, port });
  try {
    console.log(`Connected. Session=${conn.session}`);
    const ok = await conn.ping();
    console.log(`PING: ${ok ? "OK" : "FAIL"}`);

    const cur = conn.cursor();
    await cur.execute("CREATE TABLE smoke_node (id INT);");
    await cur.execute("INSERT INTO smoke_node VALUES (11);");
    await cur.execute("SELECT id FROM smoke_node;");

    console.log("SQL output:");
    const table = cur.fetchTable();
    if (table) {
      console.log(`Columns: ${table.columns.join(", ")}`);
      console.log(`Rows: ${table.rowCount}`);
      for (const row of table.rows) {
        console.log(JSON.stringify(row));
      }
    } else {
      for (const line of cur.fetchAll()) {
        console.log(line);
      }
    }

    console.log("Node smoke test completed");
  } finally {
    await conn.close();
  }
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});

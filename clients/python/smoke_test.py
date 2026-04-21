import argparse
from vdb_client import connect


def main() -> int:
    parser = argparse.ArgumentParser(description="VolcanoDB native protocol smoke test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=54330)
    args = parser.parse_args()

    with connect(host=args.host, port=args.port) as conn:
        print(f"Connected. Session={conn.session}")
        ok = conn.ping()
        print(f"PING: {'OK' if ok else 'FAIL'}")

        cur = conn.cursor()
        cur.execute("CREATE TABLE smoke (id INT);")
        cur.execute("INSERT INTO smoke VALUES (1);")
        cur.execute("SELECT id FROM smoke;")
        print("SQL output:")
        for line in cur.fetchall():
            print(line)

    print("Smoke test completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

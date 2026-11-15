"""用已有 MySQL 的两个 schema 启动本机 Master 和两个 Region。"""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=3306)
    parser.add_argument("--db-user", default="routing_demo")
    parser.add_argument("--db-a", default="region_a")
    parser.add_argument("--db-b", default="region_b")
    parser.add_argument("--master-port", type=int, default=5000)
    args = parser.parse_args()
    if args.db_a.casefold() == args.db_b.casefold():
        parser.error("两个副本需要使用不同 schema")
    if not os.environ.get("MYSQL_PASSWORD"):
        parser.error("请先设置 MYSQL_PASSWORD 环境变量")

    project = Path(__file__).resolve().parent
    master_url = f"http://127.0.0.1:{args.master_port}"
    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",
        "MYSQL_HOST": args.db_host,
        "MYSQL_PORT": str(args.db_port),
        "MYSQL_USER": args.db_user,
    }
    processes = {}
    try:
        processes["Master"] = subprocess.Popen(
            [sys.executable, "master.py", "--port", str(args.master_port)],
            cwd=project,
            env=env,
        )
        for name, port, database in (
            ("region-a", 5001, args.db_a),
            ("region-b", 5002, args.db_b),
        ):
            processes[name] = subprocess.Popen(
                [
                    sys.executable, "region_server.py",
                    "--name", name, "--host", "127.0.0.1", "--port", str(port),
                    "--advertise", f"http://127.0.0.1:{port}",
                    "--master", master_url,
                ],
                cwd=project,
                env={**env, "MYSQL_DATABASE": database},
            )
        while True:
            for name, process in processes.items():
                code = process.poll()
                if code is not None:
                    print(f"{name} 已退出，状态码 {code}", file=sys.stderr)
                    return 1
            time.sleep(0.25)
    except KeyboardInterrupt:
        return 0
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
        for process in processes.values():
            process.wait()


if __name__ == "__main__":
    sys.exit(main())

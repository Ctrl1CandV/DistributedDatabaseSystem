"""在本机启动多个 Raft 节点，每个节点使用独立的 MySQL schema。"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--base-port", type=int, default=5001)
    parser.add_argument("--cluster-id", default="classroom-local")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=3306)
    parser.add_argument("--db-user", default="ddb_demo")
    parser.add_argument("--db-prefix", default="ddb_node")
    args = parser.parse_args()
    if not 1 <= args.nodes or not 1 <= args.base_port <= 65536 - args.nodes:
        parser.error("节点数必须为正数，且所有 HTTP 端口必须在 1 到 65535 之间")
    if not 1 <= args.db_port <= 65535:
        parser.error("MySQL 端口必须在 1 到 65535 之间")
    if not re.fullmatch(r"[A-Za-z0-9_]+", args.db_prefix):
        parser.error("数据库前缀只能包含英文字母、数字和下划线")
    if not os.environ.get("DDB_MYSQL_PASSWORD"):
        parser.error("请先设置 DDB_MYSQL_PASSWORD")

    project = Path(__file__).resolve().parent
    members = {
        f"n{i + 1}": f"http://127.0.0.1:{args.base_port + i}" for i in range(args.nodes)
    }
    children = {}
    with TemporaryDirectory(prefix="ddb-local-") as folder:
        try:
            for i, node_id in enumerate(members):
                database = f"{args.db_prefix}_{i + 1}"
                config = Path(folder) / (node_id + ".json")
                config.write_text(json.dumps({
                    "cluster_id": args.cluster_id,
                    "node_id": node_id,
                    "bind_host": "127.0.0.1",
                    "bind_port": args.base_port + i,
                    "members": members,
                    "mysql": {
                        "host": args.db_host, "port": args.db_port, "database": database,
                        "user": args.db_user, "password_env": "DDB_MYSQL_PASSWORD",
                    },
                }), encoding="utf-8")
                print(f"{node_id}: {members[node_id]} -> {database}", flush=True)
                children[node_id] = subprocess.Popen(
                    [sys.executable, "-m", "ddb", "serve", "--config", str(config)], cwd=project,
                )
            # Keep surviving nodes running when a child fails during a demo.
            reported = set()
            while len(reported) < len(children):
                for node_id, process in children.items():
                    code = process.poll()
                    if code is not None and node_id not in reported:
                        print(f"{node_id} 已退出（{code}）", file=sys.stderr, flush=True)
                        reported.add(node_id)
                time.sleep(0.25)
            return 1
        except KeyboardInterrupt:
            return 0
        finally:
            for process in children.values():
                if process.poll() is None:
                    process.terminate()
            for process in children.values():
                process.wait()


if __name__ == "__main__":
    sys.exit(main())

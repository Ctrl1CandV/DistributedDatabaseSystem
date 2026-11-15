"""分布式 MySQL 学生数据演示客户端。"""

import argparse
import json
import sys

import requests


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", default="http://127.0.0.1:5000", help="Master HTTP 地址")
    parser.add_argument("--timeout", type=float, default=30, help="请求超时秒数，默认 30")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="查看节点和待同步状态")
    commands.add_parser("compare", help="读取并对比两套 MySQL 的数据")
    sync = commands.add_parser("sync", help="用指定源节点的数据全量同步另一节点")
    sync.add_argument("--source", required=True, help="compare 后选定的源节点 ID")
    read = commands.add_parser("read", help="按学号读取记录")
    read.add_argument("student_id", type=int, help="学号")
    upsert = commands.add_parser("upsert", help="新增或更新记录")
    upsert.add_argument("student_id", type=int, help="学号")
    upsert.add_argument("name", help="学生姓名")
    upsert.add_argument("score", type=int, help="分数")
    delete = commands.add_parser("delete", help="按学号删除记录")
    delete.add_argument("student_id", type=int, help="学号")
    for mutation in (upsert, delete):
        mutation.add_argument("--policy", choices=("all", "available"), help="默认沿用 Master 配置")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not 0 < args.timeout < float("inf"):
        parser.error("timeout 必须是有限正数")
    payload = None
    if args.command in ("status", "compare"):
        path = f"/api/{args.command}"
    elif args.command == "read":
        path = f"/api/students/{args.student_id}"
    elif args.command == "sync":
        path, payload = "/api/sync", {"source": args.source}
    else:
        path = "/api/mutation"
        payload = {"action": args.command, "student_id": args.student_id}
        if args.command == "upsert":
            payload.update(name=args.name, score=args.score)
        if args.policy is not None:
            payload["policy"] = args.policy

    try:
        with requests.Session() as session:
            session.trust_env = False
            response = session.request(
                "GET" if payload is None else "POST", args.master.rstrip("/") + path,
                json=payload, timeout=args.timeout, allow_redirects=False,
            )
    except requests.RequestException as error:
        print(f"未获得 Master 响应：{error}", file=sys.stderr)
        return 1

    try:
        result = response.json()
    except ValueError:
        print(f"Master 未返回 JSON（HTTP {response.status_code}）", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if response.ok and isinstance(result, dict) and result.get("signal") == "success" else 1


if __name__ == "__main__":
    sys.exit(main())

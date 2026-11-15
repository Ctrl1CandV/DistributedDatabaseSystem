"""管理节点心跳、学生数据双写及手动补同步。"""

import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from time import monotonic
from urllib.parse import urlsplit

import requests
from flask import Flask, jsonify, request


DEFAULT_REGIONS = ("region-a", "region-b")
STATE_FILE = Path(__file__).with_name("master_state.json")
MAX_BIGINT = (1 << 63) - 1


def _read_metadata(path, regions):
    if not path.exists():
        return {
            "version": 0, "versions": dict.fromkeys(regions, 0),
            "stale": [], "blocked": False, "pending": None,
        }
    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state, dict) or set(state.get("versions", {})) != set(regions):
        raise ValueError("Master 状态文件与配置的两个 Region 不匹配")
    return state


def _write_metadata(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(state, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def _address(value):
    if not isinstance(value, str):
        raise ValueError("address 必须是 HTTP URL")
    url = urlsplit(value)
    if (url.scheme not in ("http", "https") or not url.hostname or url.username is not None
            or url.password is not None or url.port == 0 or url.path not in ("", "/")
            or url.query or url.fragment):
        raise ValueError("address 格式为 http://主机:端口")
    return value.rstrip("/")


def _valid_id(student_id):
    if type(student_id) is not int or not 1 <= student_id <= MAX_BIGINT:
        raise ValueError("student_id 必须是 1 到 9223372036854775807 之间的整数")


def create_app(
    json_path=STATE_FILE, default_policy="all", regions=DEFAULT_REGIONS,
    heartbeat_timeout=5, request_timeout=2,
):
    app = Flask(__name__)
    metadata_path = Path(json_path)
    state = _read_metadata(metadata_path, regions)
    requests_lock = Lock()
    heartbeat_lock = Lock()
    heartbeats = {}

    def persist(**changes):
        updated = {**state, **changes}
        _write_metadata(metadata_path, updated)
        state.update(updated)

    def result(message, status=200, **data):
        return jsonify(signal="success" if status < 400 else "fail", message=message, **data), status

    def json_body():
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return body

    def live_nodes():
        now = monotonic()
        with heartbeat_lock:
            return {
                name: node["address"] for name, node in heartbeats.items()
                if now - node["received"] <= heartbeat_timeout and node["db_ready"]
            }

    def replica_request(address, path, body=None):
        try:
            with requests.Session() as session:
                session.trust_env = False
                response = session.request(
                    "GET" if body is None else "POST", address + path, json=body,
                    timeout=request_timeout, allow_redirects=False,
                )
            data = response.json()
        except requests.RequestException as error:
            return False, {"error": str(error)}
        if not isinstance(data, dict):
            return False, {"error": "Region 未返回 JSON 对象"}
        return 200 <= response.status_code < 300, data

    def is_blocked():
        return state["blocked"] or state["pending"] is not None

    def blocked_result():
        return result("存在未确认的写入或同步，请 compare 后执行 sync", 503,
                      blocked=True, pending=state["pending"], stale=state["stale"])

    def collect_replicas(online):
        replicas = {}
        for name in regions:
            if name not in online:
                replicas[name] = {"error": "Region 未就绪"}
                continue
            ok, data = replica_request(online[name], "/internal/students")
            if ok and isinstance(data.get("records"), list) and type(data.get("applied_version")) is int:
                replicas[name] = {"records": data["records"], "applied_version": data["applied_version"]}
            else:
                replicas[name] = {"error": data.get("error", "Region 未返回完整快照")}
        return replicas

    @app.errorhandler(ValueError)
    def invalid_input(error):
        return result(str(error), 400)

    @app.errorhandler(OSError)
    def metadata_error(error):
        app.logger.error("Master 状态文件写入失败: %s", error)
        return result("Master 状态文件写入失败，请检查磁盘与文件权限", 503)

    @app.post("/internal/heartbeat")
    def heartbeat():
        body = json_body()
        name = body.get("node_id")
        if name not in regions:
            return result("未配置的 Region", 404)
        address = _address(body.get("address"))
        if type(body.get("db_ready")) is not bool:
            raise ValueError("db_ready 必须是布尔值")
        with heartbeat_lock:
            existing = heartbeats.get(name)
            now = monotonic()
            if (existing and existing["address"] != address
                    and now - existing["received"] <= heartbeat_timeout):
                return result("同名 Region 已在另一地址注册", 409)
            heartbeats[name] = {
                "address": address, "db_ready": body["db_ready"],
                "reported_version": body.get("applied_version"), "received": now,
            }
        return result("已记录心跳")

    @app.get("/api/status")
    def status():
        with requests_lock, heartbeat_lock:
            now = monotonic()
            nodes = {}
            for name in regions:
                node = heartbeats.get(name, {})
                online = bool(node) and now - node["received"] <= heartbeat_timeout
                nodes[name] = {
                    "address": node.get("address"),
                    "online": online,
                    "db_ready": online and node["db_ready"],
                    "reported_version": node.get("reported_version"),
                    "expected_version": state["versions"][name],
                    "needs_sync": name in state["stale"],
                }
            return result(
                "Region 状态", nodes=nodes, default_policy=default_policy,
                blocked=is_blocked(), pending=state["pending"], stale=state["stale"],
                version=state["version"],
            )

    @app.get("/api/students/<int:student_id>")
    def read(student_id):
        _valid_id(student_id)
        with requests_lock:
            if is_blocked():
                return blocked_result()
            stale = set(state["stale"])
            for name, address in live_nodes().items():
                if name in stale:
                    continue
                ok, data = replica_request(address, f"/internal/students/{student_id}")
                if not ok or "record" not in data:
                    continue
                if data.get("applied_version") != state["versions"][name]:
                    stale.add(name)
                    persist(stale=sorted(stale))
                    continue
                status_code = 200 if data["record"] is not None else 404
                return result("已读取" if status_code == 200 else "记录不存在", status_code,
                              record=data["record"], node_id=name, applied_version=data["applied_version"])
            return result("没有可读的最新副本", 503, stale=sorted(stale))

    @app.post("/api/mutation")
    def mutate():
        body = json_body()
        action = body.get("action")
        if action not in ("upsert", "delete"):
            raise ValueError("action 必须是 upsert 或 delete")
        student_id = body.get("student_id")
        _valid_id(student_id)
        policy = body.get("policy", default_policy)
        if policy not in ("all", "available"):
            raise ValueError("policy 只能是 all 或 available")
        operation = {"action": action, "student_id": student_id}
        if action == "upsert":
            name, score = body.get("name"), body.get("score")
            if not isinstance(name, str) or not name.strip() or len(name) > 100:
                raise ValueError("name 必须是 1 到 100 字符的非空姓名")
            name.encode("utf-8")
            if type(score) is not int or not 0 <= score <= 100:
                raise ValueError("score 必须是 0 到 100 之间的整数")
            operation.update(name=name, score=score)

        with requests_lock:
            if is_blocked():
                return blocked_result()
            online = live_nodes()
            stale = set(state["stale"])
            candidates = [name for name in regions if name in online and name not in stale]
            skipped = [name for name in regions if name not in candidates]
            report = {"policy": policy, "confirmed": [], "failed": {}, "unknown": {}, "skipped": skipped}
            if not candidates or (policy == "all" and skipped):
                return result("可用副本不足，未发送写入", 503, **report,
                              blocked=False, degraded=False, stale=state["stale"])

            version = state["version"] + 1
            if version > MAX_BIGINT:
                return result("操作版本已耗尽", 503)
            operation.update(
                version=version, updated_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(
                    timespec="microseconds",
                ),
            )
            persist(version=version, pending={"action": action, "student_id": student_id, "version": version})
            versions = state["versions"].copy()
            confirmed, failed, unknown = report["confirmed"], report["failed"], report["unknown"]
            for name in candidates:
                ok, data = replica_request(
                    online[name], "/internal/mutation", {**operation, "expected_version": versions[name]},
                )
                if ok and data.get("committed") is True:
                    confirmed.append(name)
                    versions[name] = version
                elif data.get("committed") is False:
                    failed[name] = data.get("error", "Region 拒绝写入")
                    observed = data.get("applied_version")
                    if observed is not None and observed != versions[name]:
                        stale.add(name)
                else:
                    unknown[name] = data.get("error", "未取得提交确认")

            success = len(confirmed) == len(regions) or (policy == "available" and bool(confirmed))
            blocked = not success and bool(confirmed or unknown)
            if confirmed:
                stale.update(set(regions) - set(confirmed))
            stale.update(unknown)
            persist(versions=versions, stale=sorted(stale), blocked=blocked, pending=None)
            message = "写入已达到确认条件" if success else "未达到写入确认条件"
            return result(message, 200 if success else 503, **report, version=version, blocked=blocked,
                          degraded=success and len(confirmed) < len(regions), stale=state["stale"])

    @app.get("/api/compare")
    def compare():
        with requests_lock:
            replicas = collect_replicas(live_nodes())
            snapshots = [replica["records"] for replica in replicas.values() if "records" in replica]
            equal = snapshots[0] == snapshots[1] if len(snapshots) == len(regions) else None
            return result("副本数据对比", replicas=replicas, equal=equal,
                          blocked=is_blocked(), stale=state["stale"])

    @app.post("/api/sync")
    def sync():
        source = json_body().get("source")
        if source not in regions:
            raise ValueError("source 必须是已配置的 Region")
        target = next(name for name in regions if name != source)
        with requests_lock:
            online = live_nodes()
            if len(online) != len(regions):
                return result("同步需要两个 Region 和各自的 MySQL 均在线", 503)
            before = collect_replicas(online)
            if any("error" in replica for replica in before.values()):
                return result("无法读取两个副本，未开始同步", 503, replicas=before)
            version = max(state["version"], *(replica["applied_version"] for replica in before.values())) + 1
            if version > MAX_BIGINT:
                return result("操作版本已耗尽", 503)
            persist(version=version, pending={"action": "sync", "source": source, "version": version})

            # 源端先在事务中推进版本并取快照，目标端以同版本事务替换。
            # 晚到的旧请求无法在任一副本的同步事务之后提交。
            ok, snapshot = replica_request(
                online[source], "/internal/snapshot",
                {"version": version, "expected_version": before[source]["applied_version"]},
            )
            if (not ok or snapshot.get("committed") is not True
                    or snapshot.get("applied_version") != version or not isinstance(snapshot.get("records"), list)):
                return result("源端快照未确认，业务操作已暂停", 503, blocked=True,
                              source=source, error=snapshot.get("error"))
            ok, replaced = replica_request(
                online[target], "/internal/replace", {"version": version, "records": snapshot["records"]},
            )
            if not ok or replaced.get("committed") is not True:
                return result("目标端同步未确认，业务操作已暂停", 503, blocked=True,
                              target=target, error=replaced.get("error"))
            after = collect_replicas(online)
            expected = {"applied_version": version, "records": snapshot["records"]}
            if any(replica != expected for replica in after.values()):
                return result("同步后校验失败，业务操作已暂停", 503, blocked=True, replicas=after)
            persist(versions=dict.fromkeys(regions, version), stale=[], blocked=False, pending=None)
            return result("同步完成，两副本已对齐", source=source, target=target,
                          rows=len(snapshot["records"]), version=version, blocked=False, stale=[])

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--policy", choices=("all", "available"), default="all")
    parser.add_argument("--metadata", default=str(STATE_FILE))
    parser.add_argument("--heartbeat-timeout", type=float, default=5)
    parser.add_argument("--request-timeout", type=float, default=2)
    parser.add_argument("--regions", nargs=2, default=DEFAULT_REGIONS)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port 必须在 1 到 65535 之间")
    if len(set(args.regions)) != 2:
        parser.error("必须指定两个不同的 Region 名称")
    if any(not math.isfinite(value) or value <= 0 for value in (args.heartbeat_timeout, args.request_timeout)):
        parser.error("超时参数必须是有限正数")
    try:
        app = create_app(args.metadata, args.policy, args.regions, args.heartbeat_timeout, args.request_timeout)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()

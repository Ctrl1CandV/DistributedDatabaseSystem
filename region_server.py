"""运行 Region 数据服务，并向 Master 汇报本地 MySQL 状态。"""

import argparse
import logging
import math
import re
from datetime import datetime
from threading import Event, Thread
from urllib.parse import urlsplit

import mysql.connector
import requests
from flask import Flask, jsonify, request

from storage import CommitUnknown, RegionStore, VersionConflict


LOGGER = logging.getLogger(__name__)
MAX_BIGINT = (1 << 63) - 1


def _integer(body, field, minimum=1, maximum=MAX_BIGINT):
    value = body.get(field)
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field} 必须是 {minimum} 到 {maximum} 之间的整数")
    return value


def _student_record(body, student_id, version):
    name = body.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > 100:
        raise ValueError("name 必须是 1 到 100 字符的非空姓名")
    name.encode("utf-8")
    score = _integer(body, "score", 0, 100)
    timestamp = body.get("updated_at")
    if not isinstance(timestamp, str):
        raise ValueError("updated_at 必须是 ISO 8601 时间")
    timestamp = datetime.fromisoformat(timestamp)
    if timestamp.tzinfo is not None or timestamp.year < 1000:
        raise ValueError("updated_at 必须是 MySQL DATETIME 范围内的 UTC 时间，不带时区后缀")
    return student_id, name, score, version, timestamp


def _body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        raise ValueError("请求体必须是 JSON 对象")
    return body


def _database_status(store):
    try:
        version = store.ready_version()
    except mysql.connector.Error:
        return {"db_ready": False, "applied_version": None}
    return {"db_ready": True, "applied_version": version}


def create_app(store):
    app = Flask(__name__)

    @app.errorhandler(ValueError)
    def bad_input(error):
        return jsonify(committed=False, error=str(error)), 400

    @app.errorhandler(VersionConflict)
    def conflict(error):
        return jsonify(committed=False, error=str(error), applied_version=error.applied_version), 409

    @app.errorhandler(CommitUnknown)
    def uncertain(error):
        return jsonify(committed=None, error=str(error)), 503

    @app.errorhandler(mysql.connector.Error)
    def database_error(error):
        app.logger.warning("MySQL 操作失败: %s", error)
        return jsonify(committed=False, error="MySQL 操作失败，事务未提交"), 503

    @app.get("/health")
    def health():
        status = _database_status(store)
        return jsonify(status), 200 if status["db_ready"] else 503

    @app.get("/internal/students")
    def all_students():
        version, records = store.read()
        return jsonify(records=records, applied_version=version)

    @app.get("/internal/students/<int:student_id>")
    def student(student_id):
        _integer({"student_id": student_id}, "student_id")
        version, records = store.read(student_id)
        return jsonify(record=records[0] if records else None, applied_version=version)

    @app.post("/internal/mutation")
    def mutate():
        body = _body()
        version = _integer(body, "version")
        expected = _integer(body, "expected_version", 0)
        student_id = _integer(body, "student_id")
        action = body.get("action")
        if action == "upsert":
            store.upsert(_student_record(body, student_id, version), version, expected)
        elif action == "delete":
            store.delete(student_id, version, expected)
        else:
            raise ValueError("action 必须是 upsert 或 delete")
        return jsonify(committed=True)

    @app.post("/internal/snapshot")
    def snapshot():
        body = _body()
        version = _integer(body, "version")
        expected = _integer(body, "expected_version", 0) if "expected_version" in body else None
        records = store.fenced_snapshot(version, expected)
        return jsonify(committed=True, records=records, applied_version=version)

    @app.post("/internal/replace")
    def replace():
        body = _body()
        version = _integer(body, "version")
        if not isinstance(body.get("records"), list):
            raise ValueError("records 必须是记录数组")
        records = []
        for record in body["records"]:
            if not isinstance(record, dict):
                raise ValueError("每条记录必须是 JSON 对象")
            student_id = _integer(record, "student_id")
            record_version = _integer(record, "version", maximum=version)
            records.append(_student_record(record, student_id, record_version))
        store.replace(records, version)
        return jsonify(committed=True)

    return app


def _send_heartbeats(store, master, node_id, address, interval, stop):
    last_error = None
    with requests.Session() as session:
        session.trust_env = False
        while not stop.is_set():
            payload = {"node_id": node_id, "address": address}
            payload.update(_database_status(store))
            try:
                response = session.post(f"{master}/internal/heartbeat", json=payload, timeout=1)
                response.raise_for_status()
                last_error = None
            except requests.RequestException as exc:
                error = str(exc)
                if error != last_error:
                    LOGGER.warning("心跳上报失败: %s", error)
                    last_error = error
            stop.wait(interval)


def _http_address(value):
    address = urlsplit(value)
    if (address.scheme not in ("http", "https") or not address.hostname or address.username
            or address.password or address.path not in ("", "/") or address.query or address.fragment):
        raise ValueError("地址格式为 http://主机:端口")
    if address.port == 0:
        raise ValueError("地址的端口必须大于 0")
    return value.rstrip("/")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--advertise", help="Master 能访问的 Region URL")
    parser.add_argument("--master", default="http://127.0.0.1:5000")
    parser.add_argument("--heartbeat-interval", type=float, default=1)
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.name) or args.name in {".", ".."}:
        parser.error("节点名只能包含字母、数字、下划线、短横线和点，且不能为 . 或 ..")
    if not 1 <= args.port <= 65535:
        parser.error("port 必须在 1 到 65535 之间")
    if not math.isfinite(args.heartbeat_interval) or args.heartbeat_interval <= 0:
        parser.error("heartbeat-interval 必须是有限正数")
    if not args.advertise and args.host in ("0.0.0.0", "::"):
        parser.error("通配地址监听需要设置 --advertise")

    try:
        address = _http_address(args.advertise or f"http://{args.host}:{args.port}")
        master = _http_address(args.master)
        store = RegionStore.from_env()
    except ValueError as exc:
        parser.error(str(exc))

    stop = Event()
    thread = Thread(
        target=_send_heartbeats,
        args=(store, master, args.name, address, args.heartbeat_interval, stop),
        daemon=True,
    )
    thread.start()
    try:
        create_app(store).run(host=args.host, port=args.port, debug=False, use_reloader=False)
    finally:
        stop.set()


if __name__ == "__main__":
    main()

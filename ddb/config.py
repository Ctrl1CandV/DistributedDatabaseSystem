"""加载并校验单节点配置。"""

import hashlib
import json
import math
import os
import re
from pathlib import Path
from urllib.parse import urlsplit


def _identifier(value, field):
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 128:
        raise ValueError(f"{field} 必须是 1 到 128 字符的非空字符串")
    return value


def _member_id(value, field):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value):
        raise ValueError(f"{field} 只能包含 1 到 128 个英文字母、数字、下划线、点或短横线")
    return value


def _url(value):
    if not isinstance(value, str):
        raise ValueError("成员地址必须是 HTTP URL")
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError as error:
        raise ValueError("成员地址端口无效") from error
    if (parts.scheme not in ("http", "https") or not parts.hostname
            or parts.username is not None or parts.password is not None
            or port is None or not 1 <= port <= 65535
            or parts.path not in ("", "/") or parts.query or parts.fragment):
        raise ValueError("成员地址格式应为 http://主机:端口")
    return value.rstrip("/")


def _positive(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{field} 必须是有限正数")
    return float(value)


def load_config(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"读取配置失败: {error}") from error
    if not isinstance(data, dict):
        raise ValueError("配置必须是 JSON 对象")
    cluster_id = _member_id(data.get("cluster_id"), "cluster_id")
    node_id = _member_id(data.get("node_id"), "node_id")
    bind_host = _identifier(data.get("bind_host"), "bind_host")
    bind_port = data.get("bind_port")
    if type(bind_port) is not int or not 1 <= bind_port <= 65535:
        raise ValueError("bind_port 必须在 1 到 65535 之间")
    members = data.get("members")
    if not isinstance(members, dict) or not members:
        raise ValueError("members 必须是非空对象")
    members = {_member_id(key, "成员 ID"): _url(value) for key, value in members.items()}
    if node_id not in members:
        raise ValueError("members 必须包含本节点 node_id")
    if len(set(members.values())) != len(members):
        raise ValueError("成员 URL 必须互不相同")
    mysql = data.get("mysql")
    if not isinstance(mysql, dict):
        raise ValueError("mysql 必须是对象")
    mysql = dict(mysql)
    for field in ("host", "database", "user", "password_env"):
        _identifier(mysql.get(field), f"mysql.{field}")
    if type(mysql.get("port")) is not int or not 1 <= mysql["port"] <= 65535:
        raise ValueError("mysql.port 必须在 1 到 65535 之间")
    raft = data.get("raft", {})
    if not isinstance(raft, dict):
        raise ValueError("raft 必须是对象")
    defaults = {"heartbeat_interval": .15, "election_min": .55, "election_max": .9,
                "operation_timeout": 4.0, "rpc_timeout": .4}
    pre_vote = raft.get("pre_vote", True)
    if type(pre_vote) is not bool:
        raise ValueError("raft.pre_vote 必须是布尔值")
    raft = {key: _positive(raft.get(key, default), f"raft.{key}") for key, default in defaults.items()}
    raft["pre_vote"] = pre_vote
    if not raft["heartbeat_interval"] < raft["election_min"] < raft["election_max"]:
        raise ValueError("必须满足 heartbeat_interval < election_min < election_max")
    digest = hashlib.sha256(json.dumps(members, sort_keys=True, ensure_ascii=False,
                                        separators=(",", ":")).encode("utf-8")).hexdigest()
    return {"cluster_id": cluster_id, "node_id": node_id, "bind_host": bind_host,
            "bind_port": bind_port, "members": members, "membership_digest": digest,
            "mysql": mysql, "raft": raft}


def mysql_connection(config):
    mysql = config["mysql"]
    password = os.environ.get(mysql["password_env"])
    if not password:
        raise ValueError(f"请设置 MySQL 密码环境变量 {mysql['password_env']}")
    return {key: mysql[key] for key in ("host", "port", "database", "user")} | {"password": password}

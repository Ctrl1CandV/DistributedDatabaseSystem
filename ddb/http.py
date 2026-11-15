"""HTTP transport and public / internal node endpoints."""

from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

from .raft import RaftError

MAX_BIGINT = (1 << 63) - 1


class HttpTransport:
    def __init__(self, config):
        self.config = config

    def call(self, node_id, rpc, payload):
        if node_id not in self.config['members'] or rpc not in ('pre_vote', 'vote', 'append'):
            raise OSError('未知节点或 RPC')
        headers = {'X-DDB-Cluster': self.config['cluster_id'],
                   'X-DDB-Membership': self.config['membership_digest'],
                   'X-DDB-Sender': self.config['node_id']}
        try:
            with requests.Session() as session:
                session.trust_env = False
                response = session.post(self.config['members'][node_id] + '/internal/' + rpc,
                                        json=payload, headers=headers,
                                        timeout=self.config['raft']['rpc_timeout'], allow_redirects=False)
                if not 200 <= response.status_code < 300:
                    raise ValueError(f'RPC HTTP {response.status_code}')
                result = response.json()
            if not isinstance(result, dict):
                raise ValueError('RPC 返回值不是对象')
            term = result.get('term')
            key = 'vote_granted' if rpc in ('pre_vote', 'vote') else 'success'
            if type(term) is not int or not 0 <= term <= MAX_BIGINT or type(result.get(key)) is not bool:
                raise ValueError('RPC 返回值字段无效')
            if rpc == 'pre_vote' and (type(result.get('campaign')) is not int
                                      or not 0 <= result['campaign'] <= MAX_BIGINT):
                raise ValueError('RPC 预投票轮次无效')
            for field in ('match_index', 'conflict_index'):
                if field in result and (type(result[field]) is not int or not 0 <= result[field] <= MAX_BIGINT):
                    raise ValueError('RPC 返回的索引无效')
            return result
        except (requests.RequestException, ValueError) as error:
            raise OSError(f'RPC {rpc} 请求失败: {error}') from error


def _student_id(value):
    if type(value) is not int or not 1 <= value <= MAX_BIGINT:
        raise ValueError('student_id 必须是正的 64 位整数')


def _mutation(body):
    if not isinstance(body, dict):
        raise ValueError('请求体必须是 JSON 对象')
    action = body.get('action')
    if action not in ('upsert', 'delete'):
        raise ValueError('action 必须是 upsert 或 delete')
    student_id = body.get('student_id')
    _student_id(student_id)
    command = {'action': action, 'student_id': student_id}
    if action == 'upsert':
        name, score = body.get('name'), body.get('score')
        if not isinstance(name, str) or not name.strip() or len(name) > 100:
            raise ValueError('name 必须是 1 到 100 字符的非空姓名')
        try:
            name.encode('utf-8')
        except UnicodeEncodeError as error:
            raise ValueError('name 不是有效 Unicode 文本') from error
        if type(score) is not int or not 0 <= score <= 100:
            raise ValueError('score 必须是 0 到 100 之间的整数')
        command.update(name=name, score=score,
                       updated_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec='microseconds'))
    return command


def _validate_rpc(rpc, payload, sender):
    if not isinstance(payload, dict):
        raise ValueError('RPC 请求体必须是 JSON 对象')
    identity_key = 'candidate_id' if rpc in ('pre_vote', 'vote') else 'leader_id'
    if payload.get(identity_key) != sender:
        raise ValueError('RPC 发送者与节点身份不匹配')
    numeric = ('term', 'last_log_index', 'last_log_term') if rpc in ('pre_vote', 'vote') else (
        'term', 'prev_log_index', 'prev_log_term', 'leader_commit')
    if any(type(payload.get(key)) is not int or not 0 <= payload[key] <= MAX_BIGINT for key in numeric):
        raise ValueError('RPC 任期和索引必须是非负整数')
    if rpc == 'pre_vote' and (type(payload.get('campaign')) is not int
                              or not 0 <= payload['campaign'] <= MAX_BIGINT):
        raise ValueError('RPC 预投票轮次无效')
    if rpc == 'append':
        entries = payload.get('entries')
        if not isinstance(entries, list) or any(
            not isinstance(entry, dict) or type(entry.get('term')) is not int
            or not 0 <= entry['term'] <= MAX_BIGINT or not isinstance(entry.get('command'), dict)
            for entry in entries
        ):
            raise ValueError('RPC 日志格式无效')


def create_app(node, config):
    app = Flask(__name__)

    def leader_url(leader_id):
        return config['members'].get(leader_id)

    @app.errorhandler(ValueError)
    def invalid_input(error):
        return jsonify(signal='fail', message=str(error)), 400

    @app.errorhandler(RaftError)
    def raft_error(error):
        return jsonify(signal='fail', message=str(error), leader_id=error.leader_id,
                       leader_url=leader_url(error.leader_id)), error.status

    @app.get('/api/status')
    def status():
        data = node.status()
        return jsonify(signal='success', **{**data, 'leader_url': leader_url(data.get('leader_id'))})

    @app.get('/api/students/<int:student_id>')
    def read(student_id):
        _student_id(student_id)
        mode = request.args.get('mode', 'linearizable')
        if mode not in ('linearizable', 'stale'):
            raise ValueError('mode 只能是 linearizable 或 stale')
        return jsonify(signal='success', **node.read(student_id, mode))

    @app.post('/api/mutation')
    def mutation():
        command = _mutation(request.get_json(silent=True))
        return jsonify(signal='success', **node.mutate(command))

    @app.post('/internal/<rpc>')
    def internal(rpc):
        if rpc not in ('pre_vote', 'vote', 'append'):
            return jsonify(error='未知 RPC'), 404
        if (request.headers.get('X-DDB-Cluster') != config['cluster_id']
                or request.headers.get('X-DDB-Membership') != config['membership_digest']):
            return jsonify(error='集群身份或成员配置不匹配'), 409
        sender = request.headers.get('X-DDB-Sender')
        if sender not in config['members'] or sender == config['node_id']:
            return jsonify(error='未知 RPC 发送者'), 403
        payload = request.get_json(silent=True)
        _validate_rpc(rpc, payload, sender)
        return jsonify(getattr(node, rpc)(payload))

    return app

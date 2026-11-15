"""Command-line interface for serving and accessing a database node."""

import argparse
import json
import logging
import math
import sys

import requests

from .config import load_config, mysql_connection


def build_parser():
    parser = argparse.ArgumentParser(description='分布式学生数据库')
    parser.add_argument('--server', default='http://127.0.0.1:5001', help='要访问的节点 URL')
    parser.add_argument('--timeout', type=float, default=15, help='客户端请求超时（秒）')
    commands = parser.add_subparsers(dest='command', required=True)
    serve = commands.add_parser('serve', help='启动数据库节点')
    serve.add_argument('--config', required=True, help='节点 JSON 配置文件')
    commands.add_parser('status', help='查看节点状态')
    read = commands.add_parser('read', help='按学号读取')
    read.add_argument('student_id', type=int)
    read.add_argument('--mode', choices=('linearizable', 'stale'), default='linearizable')
    upsert = commands.add_parser('upsert', help='插入或更新')
    upsert.add_argument('student_id', type=int)
    upsert.add_argument('name')
    upsert.add_argument('score', type=int)
    delete = commands.add_parser('delete', help='删除')
    delete.add_argument('student_id', type=int)
    return parser


def serve(config_path):
    try:
        config = load_config(config_path)
        connection = mysql_connection(config)
    except ValueError as error:
        print(f'配置错误：{error}', file=sys.stderr)
        return 1
    try:
        from .http import HttpTransport, create_app
        from .raft import Node
        from .storage import MySQLStore
    except ImportError as error:
        print(f'依赖加载失败：{error}；请运行 python -m pip install -r requirements.txt', file=sys.stderr)
        return 1
    try:
        store = MySQLStore(connection)
        store.bind_identity(config['node_id'], config['cluster_id'], config['membership_digest'])
        raft = config['raft']
        node = Node(config['node_id'], config['members'], store, HttpTransport(config),
                    heartbeat_interval=raft['heartbeat_interval'], election_min=raft['election_min'],
                    election_max=raft['election_max'], operation_timeout=raft['operation_timeout'],
                    pre_vote=raft['pre_vote'])
        node.start()
        if not node.status()['healthy']:
            node.stop()
            raise OSError('无法加载已绑定的 Raft 存储')
    except (OSError, ValueError) as error:
        print(f'节点启动失败：{error}。请检查 MySQL 连接、账户权限、初始化脚本及节点身份。', file=sys.stderr)
        return 1
    try:
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        print(f"节点 {config['node_id']} 正在监听 {config['bind_host']}:{config['bind_port']}", flush=True)
        create_app(node, config).run(host=config['bind_host'], port=config['bind_port'],
                                     debug=False, threaded=True, use_reloader=False)
    finally:
        node.stop()
    return 0


def client(args):
    if args.command == 'status':
        path, payload, params = '/api/status', None, None
    elif args.command == 'read':
        path, payload, params = f'/api/students/{args.student_id}', None, {'mode': args.mode}
    else:
        path, params = '/api/mutation', None
        payload = {'action': args.command, 'student_id': args.student_id}
        if args.command == 'upsert':
            payload.update(name=args.name, score=args.score)
    try:
        with requests.Session() as session:
            session.trust_env = False
            response = session.request('GET' if payload is None else 'POST', args.server.rstrip('/') + path,
                                       params=params, json=payload, timeout=args.timeout, allow_redirects=False)
        data = response.json()
    except (requests.RequestException, ValueError) as error:
        print(f'请求未获得可解析的节点响应：{error}。写入结果可能未确认，请先查询再决定后续操作。', file=sys.stderr)
        return 1
    print(json.dumps(data, ensure_ascii=False, indent=2))
    if not response.ok or not isinstance(data, dict) or data.get('signal') != 'success':
        if isinstance(data, dict) and data.get('leader_url'):
            print(f"可用 --server {data['leader_url']} 指向已知领导者；写入未自动重试。", file=sys.stderr)
        return 1
    return 0


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('--timeout 必须是有限正数')
    return serve(args.config) if args.command == 'serve' else client(args)

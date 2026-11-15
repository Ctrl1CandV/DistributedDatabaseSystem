"""Storage failure boundary tests; real database behavior needs integration tests."""
import os
import subprocess
import sys
import unittest


# Run isolated so an optional stand-in connector cannot affect other test modules.
_SETUP = r'''
import sys, types
try:
    import mysql.connector
except ModuleNotFoundError:
    mysql = types.ModuleType("mysql")
    connector = types.ModuleType("mysql.connector")
    class ConnectorError(Exception): pass
    connector.Error = ConnectorError
    connector.connect = lambda **kwargs: None
    mysql.connector = connector
    sys.modules["mysql"] = mysql
    sys.modules["mysql.connector"] = connector
from unittest.mock import patch
from ddb.storage import MySQLStore
import mysql.connector
'''


class MySQLFailureTests(unittest.TestCase):
    def run_script(self, body):
        result = subprocess.run([sys.executable, "-c", _SETUP + body], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_connection_failure_is_oserror(self):
        self.run_script(r'''
store = MySQLStore({"host":"127.0.0.1", "database":"test", "user":"test", "password":"test"})
with patch.object(mysql.connector, "connect", side_effect=mysql.connector.Error("offline")):
    try:
        store.bind_identity("n1", "cluster", "members")
    except OSError:
        pass
    else:
        raise AssertionError("connection failure must raise OSError")
''')

    def test_store_cannot_be_used_before_binding(self):
        self.run_script(r'''
try:
    MySQLStore({}).load()
except OSError:
    pass
else:
    raise AssertionError("unbound store must fail")
''')

    def test_commit_failure_is_reported_as_unknown(self):
        self.run_script(r'''
class Cursor:
    def execute(self, *args): pass
    def fetchone(self): return {"node_id":"n1", "cluster_id":"cluster", "membership_digest":"members"}
class Connection:
    def cursor(self, **kwargs): return Cursor()
    def commit(self): raise mysql.connector.Error("connection lost while committing")
    def rollback(self): pass
    def close(self): pass
with patch.object(mysql.connector, "connect", return_value=Connection()):
    try:
        MySQLStore({}).bind_identity("n1", "cluster", "members")
    except OSError:
        pass
    else:
        raise AssertionError("uncertain commit must raise OSError")
''')


@unittest.skipUnless(os.environ.get('DDB_TEST_MYSQL_PORT'),
                     'set DDB_TEST_MYSQL_PORT to an isolated MySQL instance')
class MySQLIntegrationTests(unittest.TestCase):
    """Creates and removes only a fresh, randomly named schema per test."""

    def setUp(self):
        import os
        import uuid
        from pathlib import Path
        import mysql.connector
        from ddb.storage import MySQLStore
        self.store_class = MySQLStore
        self.connector = mysql.connector
        self.schema = 'raft_storage_qa_' + uuid.uuid4().hex[:16]
        self.base = dict(host=os.environ.get('DDB_TEST_MYSQL_HOST', '127.0.0.1'),
                         port=int(os.environ['DDB_TEST_MYSQL_PORT']),
                         user=os.environ.get('DDB_TEST_MYSQL_USER', 'root'),
                         password=os.environ.get('DDB_TEST_MYSQL_PASSWORD', ''))
        self.admin = self.connector.connect(**self.base, autocommit=True)
        self.addCleanup(self.admin.close)
        self.admin.cursor().execute(f'CREATE DATABASE `{self.schema}` CHARACTER SET utf8mb4')
        self.addCleanup(self.drop_schema)
        self.config = {**self.base, 'database': self.schema}
        connection = self.connector.connect(**self.config)
        try:
            cursor = connection.cursor()
            sql = (Path(__file__).resolve().parents[1] / 'sql' / 'init.sql').read_text(encoding='utf-8-sig')
            for statement in sql.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            connection.commit()
        finally:
            connection.close()
        self.store = self.store_class(self.config)
        self.store.bind_identity('n1', 'cluster', 'membership')

    def drop_schema(self):
        self.admin.cursor().execute(f'DROP DATABASE `{self.schema}`')

    @staticmethod
    def entry(term, score):
        return {'term': term, 'command': {'action': 'upsert', 'student_id': 1,
                'name': 'Alice', 'score': score, 'updated_at': '2026-11-15T10:20:30.000000'}}

    def test_restart_load_and_replace_uncommitted_tail(self):
        self.store.save_hard_state(2, 'n1')
        self.store.append_entries(0, [self.entry(1, 90), {'term': 1, 'command': {'action': 'noop'}}])
        self.store.commit_through(1)
        restarted = self.store_class(self.config)
        restarted.bind_identity('n1', 'cluster', 'membership')
        self.assertEqual(restarted.load(), {'term': 2, 'voted_for': 'n1', 'commit_index': 1,
                                           'log': [self.entry(1, 90), {'term': 1, 'command': {'action': 'noop'}}]})
        restarted.append_entries(1, [self.entry(2, 99)])
        restarted.commit_through(2)
        record = restarted.read_student(1)
        self.assertEqual((record['score'], record['version']), (99, 2))
        with self.assertRaises(ValueError):
            restarted.append_entries(1, [])
        self.assertEqual(restarted.load()['commit_index'], 2)

    def test_failed_commit_rolls_back_all_student_changes(self):
        self.store.append_entries(0, [self.entry(1, 90)])
        self.store.commit_through(1)
        self.store.append_entries(1, [self.entry(2, 99), {'term': 2, 'command': {'action': 'bad'}}])
        with self.assertRaises(OSError):
            self.store.commit_through(3)
        self.assertEqual(self.store.read_student(1)['score'], 90)
        self.assertEqual(self.store.load()['commit_index'], 1)
        self.store.append_entries(2, [{'term': 2, 'command': {'action': 'delete', 'student_id': 1}}])
        self.store.commit_through(3)
        self.assertIsNone(self.store.read_student(1))

    def test_node_and_membership_must_match(self):
        for node, cluster, membership in [('n2', 'cluster', 'membership'),
                                           ('n1', 'another', 'membership'),
                                           ('n1', 'cluster', 'another')]:
            with self.subTest(node=node, cluster=cluster, membership=membership):
                with self.assertRaises(OSError):
                    self.store_class(self.config).bind_identity(node, cluster, membership)

    def test_health_fails_if_hard_state_is_missing(self):
        from unittest.mock import patch
        self.store.check_health()
        with patch.object(self.connector, 'connect', side_effect=self.connector.Error('offline')):
            with self.assertRaises(OSError):
                self.store.check_health()
        connection = self.connector.connect(**self.config, autocommit=True)
        try:
            connection.cursor().execute('DELETE FROM raft_hard_state WHERE singleton=1')
        finally:
            connection.close()
        with self.assertRaises(OSError):
            self.store.check_health()

    def test_old_database_missing_identity_fails_with_migration_message(self):
        connection = self.connector.connect(**self.config, autocommit=True)
        try:
            connection.cursor().execute('DROP TABLE raft_identity')
        finally:
            connection.close()
        with self.assertRaisesRegex(OSError, 'old database is incompatible'):
            self.store_class(self.config).bind_identity('n1', 'cluster', 'membership')

    def test_vote_cannot_change_within_same_term(self):
        self.store.save_hard_state(1, 'n1')
        with self.assertRaises(ValueError):
            self.store.save_hard_state(1, 'n2')
        self.assertEqual(self.store.load()['voted_for'], 'n1')
        self.store.save_hard_state(2, None)
        self.assertIsNone(self.store.load()['voted_for'])


if __name__ == "__main__":
    unittest.main()

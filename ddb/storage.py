"""Durable Raft log and committed student state in one MySQL schema.

Initialize a *new, dedicated* database with sql/init.sql. Bind it to exactly
one node and configuration before loading or changing any data.
"""
import json
from contextlib import contextmanager, suppress
from datetime import datetime

import mysql.connector


_MAX_INDEX = (1 << 63) - 1


def _index(value, name):
    if type(value) is not int or not 0 <= value <= _MAX_INDEX:
        raise ValueError(f"{name} must be a non-negative signed 64-bit integer")
    return value


def _decode_command(value):
    try:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        command = json.loads(value) if isinstance(value, str) else value
        if not isinstance(command, dict):
            raise ValueError("not an object")
        return command
    except (ValueError, UnicodeError) as exc:
        raise OSError("Invalid command in persisted Raft log") from exc


class MySQLStore:
    def __init__(self, config):
        self._config = dict(config)
        self._bound = False

    @contextmanager
    def _connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                **self._config, autocommit=False, charset="utf8mb4",
                connection_timeout=2, read_timeout=5, write_timeout=5,
            )
            yield connection
        except (mysql.connector.Error, OSError) as exc:
            if connection is not None:
                with suppress(mysql.connector.Error, OSError):
                    connection.rollback()
            if isinstance(exc, OSError):
                raise
            # A commit exception does not prove the server rolled back. The
            # caller must stop serving and reload from disk before proceeding.
            raise OSError(f"MySQL storage operation failed or commit is uncertain: {exc}") from exc
        finally:
            if connection is not None:
                with suppress(mysql.connector.Error, OSError):
                    connection.close()

    def _require_bound(self):
        if not self._bound:
            raise OSError("MySQL storage must be bound to a node and cluster before use")

    def check_health(self):
        """Check the bound database using a new connection, without mutating it."""
        self._require_bound()
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT current_term, commit_index FROM raft_hard_state WHERE singleton=1")
            if cursor.fetchone() is None:
                raise OSError("Raft hard state missing")

    def bind_identity(self, node_id, cluster_id, membership_digest):
        values = (node_id, cluster_id, membership_digest)
        if any(not isinstance(value, str) or not value or len(value) > 255 for value in values):
            raise ValueError("node_id, cluster_id and membership_digest must be non-empty strings of at most 255 characters")
        self._bound = False
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute("SELECT node_id, cluster_id, membership_digest FROM raft_identity WHERE singleton = 1 FOR UPDATE")
            except mysql.connector.Error as exc:
                if getattr(exc, "errno", None) == 1146:
                    raise OSError("Raft schema missing: the old database is incompatible; initialize a new database with sql/init.sql") from exc
                raise
            row = cursor.fetchone()
            if row is None:
                raise OSError("Raft storage schema is missing identity; initialize a new empty database with sql/init.sql")
            actual = (row["node_id"], row["cluster_id"], row["membership_digest"])
            if actual == (None, None, None):
                # Binding never silently adopts data from a prior or partial setup.
                cursor.execute("SELECT current_term, voted_for, commit_index FROM raft_hard_state WHERE singleton = 1 FOR UPDATE")
                state = cursor.fetchone()
                cursor.execute("SELECT COUNT(*) AS count FROM raft_log")
                logs = cursor.fetchone()["count"]
                cursor.execute("SELECT COUNT(*) AS count FROM students")
                students = cursor.fetchone()["count"]
                if state is None or state["current_term"] != 0 or state["voted_for"] is not None or state["commit_index"] != 0 or logs or students:
                    raise OSError("Unbound Raft database is not empty; initialize a new dedicated database")
                cursor.execute("UPDATE raft_identity SET node_id=%s, cluster_id=%s, membership_digest=%s WHERE singleton=1", values)
            elif actual != values:
                raise OSError("Raft database belongs to a different node, cluster or membership")
            connection.commit()
        self._bound = True

    def load(self):
        self._require_bound()
        with self._connection() as connection:
            connection.start_transaction(consistent_snapshot=True, isolation_level="REPEATABLE READ", readonly=True)
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT current_term, voted_for, commit_index FROM raft_hard_state WHERE singleton = 1")
            state = cursor.fetchone()
            if state is None:
                raise OSError("Raft hard state missing; initialize a new database with sql/init.sql")
            cursor.execute("SELECT log_index, term, command FROM raft_log ORDER BY log_index")
            rows = cursor.fetchall()
            log = []
            for expected, row in enumerate(rows, 1):
                if row["log_index"] != expected or type(row["term"]) is not int or row["term"] < 0:
                    raise OSError("Raft log is corrupt or contains an index gap")
                log.append({"term": row["term"], "command": _decode_command(row["command"])})
            if type(state["current_term"]) is not int or state["current_term"] < 0 or type(state["commit_index"]) is not int or not 0 <= state["commit_index"] <= len(log):
                raise OSError("Invalid persisted Raft hard state")
            return {"term": state["current_term"], "voted_for": state["voted_for"],
                    "commit_index": state["commit_index"], "log": log}

    def save_hard_state(self, term, voted_for):
        self._require_bound()
        _index(term, "term")
        if voted_for is not None and (not isinstance(voted_for, str) or not voted_for or len(voted_for) > 255):
            raise ValueError("voted_for must be None or a non-empty string of at most 255 characters")
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT current_term, voted_for FROM raft_hard_state WHERE singleton=1 FOR UPDATE")
            state = cursor.fetchone()
            if state is None:
                raise OSError("Raft hard state missing")
            if term < state["current_term"]:
                raise ValueError("Raft term cannot decrease")
            if term == state["current_term"] and state["voted_for"] is not None and voted_for != state["voted_for"]:
                raise ValueError("cannot change a vote within the same term")
            cursor.execute("UPDATE raft_hard_state SET current_term=%s, voted_for=%s WHERE singleton=1", (term, voted_for))
            connection.commit()

    def append_entries(self, after, entries):
        self._require_bound()
        _index(after, "after")
        if not isinstance(entries, list) or len(entries) > _MAX_INDEX - after:
            raise ValueError("entries must be a list with valid indexes")
        encoded = []
        for offset, entry in enumerate(entries, 1):
            if not isinstance(entry, dict) or not isinstance(entry.get("command"), dict):
                raise ValueError("each log entry must contain a command object")
            term = _index(entry.get("term"), "entry term")
            encoded.append((after + offset, term, json.dumps(entry["command"], ensure_ascii=False, allow_nan=False)))
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT commit_index FROM raft_hard_state WHERE singleton=1 FOR UPDATE")
            state = cursor.fetchone()
            if state is None:
                raise OSError("Raft hard state missing")
            cursor.execute("SELECT COALESCE(MAX(log_index), 0) AS last_index FROM raft_log")
            last = cursor.fetchone()["last_index"]
            if after > last:
                raise ValueError("cannot append after a missing log index")
            if after < state["commit_index"]:
                raise ValueError("cannot truncate committed entries")
            cursor.execute("DELETE FROM raft_log WHERE log_index > %s", (after,))
            if encoded:
                cursor.executemany("INSERT INTO raft_log (log_index, term, command) VALUES (%s, %s, %s)", encoded)
            connection.commit()

    def commit_through(self, index):
        self._require_bound()
        _index(index, "index")
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT commit_index FROM raft_hard_state WHERE singleton=1 FOR UPDATE")
            state = cursor.fetchone()
            if state is None:
                raise OSError("Raft hard state missing")
            current = state["commit_index"]
            if index < current:
                raise ValueError("commit index cannot decrease")
            cursor.execute("SELECT log_index, command FROM raft_log WHERE log_index > %s AND log_index <= %s ORDER BY log_index", (current, index))
            rows = cursor.fetchall()
            if len(rows) != index - current or any(row["log_index"] != current + offset for offset, row in enumerate(rows, 1)):
                raise ValueError("cannot commit missing log entries")
            for row in rows:
                command = _decode_command(row["command"])
                action = command.get("action")
                if action == "noop":
                    continue
                student_id = command.get("student_id")
                if type(student_id) is not int or not 1 <= student_id <= _MAX_INDEX:
                    raise OSError("invalid student_id in committed command")
                if action == "delete":
                    cursor.execute("DELETE FROM students WHERE student_id=%s", (student_id,))
                elif action == "upsert":
                    name, score, stamp = command.get("name"), command.get("score"), command.get("updated_at")
                    if not isinstance(name, str) or not name.strip() or len(name) > 100 or type(score) is not int or not 0 <= score <= 100 or not isinstance(stamp, str):
                        raise OSError("invalid upsert command in Raft log")
                    try:
                        timestamp = datetime.fromisoformat(stamp)
                        if timestamp.tzinfo is not None or timestamp.year < 1000:
                            raise ValueError("timestamp must be UTC without an offset")
                    except ValueError as exc:
                        raise OSError("invalid timestamp in Raft log") from exc
                    cursor.execute("""INSERT INTO students (student_id, name, score, version, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE name=VALUES(name), score=VALUES(score),
                        version=VALUES(version), updated_at=VALUES(updated_at)""",
                        (student_id, name, score, row["log_index"], timestamp))
                else:
                    raise OSError("unknown command in Raft log")
            cursor.execute("UPDATE raft_hard_state SET commit_index=%s WHERE singleton=1", (index,))
            connection.commit()

    def read_student(self, student_id):
        self._require_bound()
        if type(student_id) is not int or not 1 <= student_id <= _MAX_INDEX:
            raise ValueError("student_id must be a positive signed 64-bit integer")
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT student_id, name, score, version, updated_at FROM students WHERE student_id=%s", (student_id,))
            row = cursor.fetchone()
            if row is not None:
                row["updated_at"] = row["updated_at"].isoformat(timespec="microseconds")
            return row

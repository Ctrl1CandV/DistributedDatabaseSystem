"""单个 Region 的 MySQL 事务与副本版本。"""

import os
from contextlib import contextmanager, suppress

import mysql.connector


class VersionConflict(Exception):
    def __init__(self, applied_version):
        super().__init__("副本版本不符合本次操作预期")
        self.applied_version = applied_version


class CommitUnknown(Exception):
    pass


def _applied_version(cursor, for_update=False):
    query = "SELECT applied_version FROM replica_state WHERE singleton = 1"
    cursor.execute(query + " FOR UPDATE" if for_update else query)
    row = cursor.fetchone()
    if row is None:
        raise mysql.connector.Error("replica_state 缺少初始记录")
    return row["applied_version"]


def _read_students(cursor, student_id=None):
    columns = "student_id, name, score, version, updated_at"
    if student_id is None:
        cursor.execute(f"SELECT {columns} FROM students ORDER BY student_id")
    else:
        cursor.execute(f"SELECT {columns} FROM students WHERE student_id = %s", (student_id,))
    rows = cursor.fetchall()
    for row in rows:
        row["updated_at"] = row["updated_at"].isoformat(timespec="microseconds")
    return rows


class RegionStore:
    def __init__(self, **connection_args):
        self.connection_args = connection_args

    @classmethod
    def from_env(cls):
        required = ("MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD")
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise ValueError(f"请设置环境变量 {', '.join(missing)}")
        port = int(os.environ.get("MYSQL_PORT", "3306"))
        if not 1 <= port <= 65535:
            raise ValueError("MYSQL_PORT 必须在 1 到 65535 之间")
        return cls(
            host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
            port=port,
            database=os.environ["MYSQL_DATABASE"],
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
        )

    @contextmanager
    def _connection(self):
        connection = mysql.connector.connect(
            **self.connection_args,
            autocommit=False,
            charset="utf8mb4",
            connection_timeout=1,
            read_timeout=2,
            write_timeout=2,
        )
        try:
            yield connection
        finally:
            # 断开连接会回滚尚未提交的 InnoDB 事务。
            with suppress(mysql.connector.Error):
                connection.close()

    def ready_version(self):
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT student_id FROM students LIMIT 1")
            cursor.fetchall()
            return _applied_version(cursor)

    def read(self, student_id=None):
        with self._connection() as connection:
            connection.start_transaction(
                consistent_snapshot=True, isolation_level="REPEATABLE READ", readonly=True,
            )
            cursor = connection.cursor(dictionary=True)
            version = _applied_version(cursor)
            return version, _read_students(cursor, student_id)

    @contextmanager
    def _advance_version(self, version, expected_version=None):
        with self._connection() as connection:
            cursor = connection.cursor(dictionary=True)
            current = _applied_version(cursor, for_update=True)
            if version <= current or (expected_version is not None and current != expected_version):
                raise VersionConflict(current)
            yield cursor
            cursor.execute(
                "UPDATE replica_state SET applied_version = %s WHERE singleton = 1", (version,),
            )
            try:
                connection.commit()
            except mysql.connector.Error as exc:
                raise CommitUnknown("MySQL 提交结果暂时无法确认") from exc

    def upsert(self, record, version, expected_version):
        with self._advance_version(version, expected_version) as cursor:
            cursor.execute(
                """INSERT INTO students (student_id, name, score, version, updated_at)
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE name = %s, score = %s, version = %s, updated_at = %s""",
                record + record[1:],
            )

    def delete(self, student_id, version, expected_version):
        with self._advance_version(version, expected_version) as cursor:
            cursor.execute("DELETE FROM students WHERE student_id = %s", (student_id,))

    def fenced_snapshot(self, version, expected_version=None):
        with self._advance_version(version, expected_version) as cursor:
            return _read_students(cursor)

    def replace(self, records, version):
        with self._advance_version(version) as cursor:
            cursor.execute("DELETE FROM students")
            if records:
                cursor.executemany(
                    """INSERT INTO students (student_id, name, score, version, updated_at)
                       VALUES (%s, %s, %s, %s, %s)""",
                    records,
                )

-- Raft v1 schema. Run only on a new, empty dedicated database. This is not an
-- in-place migration: an existing students table makes initialization fail.
CREATE TABLE students (
    student_id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    score INT NOT NULL,
    version BIGINT NOT NULL,
    updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE raft_identity (
    singleton TINYINT NOT NULL PRIMARY KEY CHECK (singleton = 1),
    node_id VARCHAR(255) NULL,
    cluster_id VARCHAR(255) NULL,
    membership_digest VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE raft_hard_state (
    singleton TINYINT NOT NULL PRIMARY KEY CHECK (singleton = 1),
    current_term BIGINT NOT NULL,
    voted_for VARCHAR(255) NULL,
    commit_index BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE raft_log (
    log_index BIGINT NOT NULL PRIMARY KEY,
    term BIGINT NOT NULL,
    command JSON NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO raft_identity (singleton, node_id, cluster_id, membership_digest)
VALUES (1, NULL, NULL, NULL);
INSERT INTO raft_hard_state (singleton, current_term, voted_for, commit_index)
VALUES (1, 0, NULL, 0);

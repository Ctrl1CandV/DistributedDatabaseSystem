CREATE TABLE IF NOT EXISTS students (
    student_id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    score INT NOT NULL,
    version BIGINT NOT NULL,
    updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS replica_state (
    singleton TINYINT NOT NULL PRIMARY KEY CHECK (singleton = 1),
    applied_version BIGINT NOT NULL
) ENGINE=InnoDB;

INSERT INTO replica_state (singleton, applied_version) VALUES (1, 0)
    ON DUPLICATE KEY UPDATE singleton = 1;

CREATE TABLE IF NOT EXISTS datasets
(
    id           INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name         VARCHAR(100) UNIQUE NOT NULL,
    total_rows   INT,
    size         BIGINT              NOT NULL,
    created      TIMESTAMP           NOT NULL,
    last_updated TIMESTAMP
)
CREATE TABLE IF NOT EXISTS labels
(
    id         INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    dataset_id INT REFERENCES datasets (id) NOT NULL,
    name       VARCHAR(100) UNIQUE          NOT NULL
)
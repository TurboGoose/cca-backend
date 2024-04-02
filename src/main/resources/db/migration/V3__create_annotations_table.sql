CREATE TABLE IF NOT EXISTS annotations
(
    row_num  BIGINT                     NOT NULL,
    label_id INT REFERENCES labels (id) NOT NULL,
    PRIMARY KEY (row_num, label_id)
)
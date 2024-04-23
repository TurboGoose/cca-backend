CREATE TABLE IF NOT EXISTS annotations
(
    row_num  BIGINT NOT NULL,
    label_id INT    NOT NULL REFERENCES labels (id) ON DELETE CASCADE,
    PRIMARY KEY (row_num, label_id)
)
insert into datasets(total_rows, created, last_updated, size, name) values (5, now(), null, 100, 'dataset');
insert into labels(dataset_id, name) values (1, 'label1'), (1, 'label2');
insert into annotations(label_id, row_num) VALUES (1, 1), (2, 1), (2, 3);
insert into storages(dataset_id, mode, status, storage_id) values (1, 'PRIMARY', 'READY', 'd4233fc7-5b38-4f93-b937-bc13bfec48de')
-- select 1;
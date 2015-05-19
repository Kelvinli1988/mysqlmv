use mysqlmv;
call create_mv('test', 'test_log_view', 1);

set @mview_id = 1;
call mysqlmv.raise('error message');

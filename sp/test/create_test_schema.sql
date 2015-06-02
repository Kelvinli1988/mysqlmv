use test;
create table test_log
(
  id SERIAL,
  fname varchar(128),
  lname varchar(128),
  PRIMARY KEY (id)
);
create view test_log_view AS
  select t1.fname, t2.lname from test_log t1 join test_log t2 on t1.id = t2.id;

use mysqlmv;
call create_mv('test', 'test_log_view', 1);

call mysqlmv.create_mv('test', 'student_score', 1);
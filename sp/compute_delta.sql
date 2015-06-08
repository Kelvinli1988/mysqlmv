DELIMITER //

DROP PROCEDURE IF EXISTS  mysqlmv.compute_delta//
CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.compute_delta(
  param_group_id bigint,
  mv_id bigint
)
DETERMINISTIC
NO SQL
SQL SECURITY DEFINER
BEGIN
  DECLARE mv_sql, sql_select, sql_from, sql_where varchar(4000);
  set sql_select = generate_select(param_group_id, mv_id);
  set sql_from = generate_from(param_group_id, mv_id);
  set mv_sql = concat('select ', sql_select,
               'from ', sql_from);
  return mv_sql;
END //

DELIMITER ;
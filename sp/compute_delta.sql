DELIMITER //

DROP PROCEDURE IF EXISTS  mysqlmv.compute_delta//
CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.compute_delta(
  param_group_id bigint,
  mv_id bigint,
  from_ts long,
  to_ts long,
  is_add tinyint
)
BEGIN
  DECLARE mv_sql, sql_select, sql_from, sql_where varchar(4000);
  DECLARE from_count, from_pc, is_base_table int;

  set sql_select = generate_select(param_group_id, mv_id);
  set sql_from = generate_from(param_group_id, mv_id);
  set mv_sql = concat('select ', sql_select,
               'from ', sql_from);
  select count(*) into from_count from mview_expression where mview_id = mv_id;
  set from_pc = 1;
  while from_pc <= from_count DO
    # 1. check whether this table is a base table.
    # if it is do computation, else continue.
    select pl.is_base_table
    into is_base_table
    from parameter_list pl
    where pl.group_id = param_group_id and pl.table_order = from_pc;
    if is_base_table = 1 THEN
      DECLARE new_group_id bigint;
      call allocate_parameters(param_group_id, mv_id, 1, from_ts, to_ts, new_group_id);
      DECLARE delta_sql varchar(4000);
      set delta_sql = generate_sql(new_group_id, mv_id);
      # got ts.
      # exec.
      # prepare new parameter.
      # deallocate old parameter.
      # recursive call.
      
    END IF ;
    set from_pc = from_pc + 1;
  END WHILE;

END //

DROP FUNCTION IF EXISTS mysqlmv.has_base_table//
CREATE DEFINER=mysqlmv@localhost FUNCTION mysqlmv.has_base_table(
  IN param_group_id bigint
)
RETURNS int
BEGIN
  DECLARE table_count TINYINT;
  select count(*)
  into bool_ret
  from parameter_list
  where group_id = param_group_id;
  return bool_ret;
END//

DROP PROCEDURE IF EXISTS mysqlmv.deallocate_parameter//
CREATE DEFINER=mysqlmv@localhost PROCEDURE  mysqlmv.deallocate_parameters(
  IN param_group_id BIGINT
)
BEGIN
  delete from parameter_list where group_id = param_group_id;
END//

DROP PROCEDURE IF EXISTS mysqlmv.allocate_parameter//
CREATE DEFINER=mysqlmv@localhost PROCEDURE  mysqlmv.allocate_parameters(
  IN param_group_id bigint,
  IN mv_id bigint,
  IN seq int, # begin from 1.
  IN in_from_ts bigint,
  IN in_to_ts bigint,
  OUT ret_group_id bigint
)
BEGIN
  call allocate_param_group_id(ret_group_id);
  IF param_group_id = -1 THEN
    insert into parameter_list
    select NULL, ret_group_id, table_owner, table_name, -1, -1, expr_order, 1, now()
    from mview_expression
    where mview_id = mv_id and type = 'from'
    order by expr_order;
  ELSE
    # if table_order < seq,
    # old = old
    # if table_owner > seq,
    # old = new.
    # if table_owner = seq,
    # use the origin one.
    insert into parameter_list
    select NULL, ret_group_id, table_owner, table_name, from_ts, in_to_ts, table_order, is_base_table, now()
    from parameter_list
    where group_id = param_group_id and table_order < seq;

    insert into parameter_list
    select NULL, ret_group_id, table_owner, table_name, from_ts, to_ts, table_order, 0, now()
    from parameter_list
    where group_id = param_group_id and table_order = seq;

    insert into parameter_list
    select NULL, ret_group_id, table_owner, table_name, to_ts, in_to_ts, table_order, is_base_table, now()
    from parameter_list
    where group_id = param_group_id and table_order > seq;
  END IF ;
END//

DROP PROCEDURE IF EXISTS mysqlmv.allocate_param_group_id;
CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.allocate_param_group_id(
  OUT group_id bigint
)
BEGIN
  insert into parameter_group (create_datetime) values(now());
  select last_insert_id() into group_id;
END//

DELIMITER ;
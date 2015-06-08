DELIMITER //

DROP FUNCTION IF EXISTS  mysqlmv.generate_sql//
CREATE DEFINER=mysqlmv@localhost FUNCTION mysqlmv.generate_sql(
  param_group_id bigint,
  mv_id bigint
)
RETURNS varchar(4000)
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

DROP FUNCTION IF EXISTS  mysqlmv.generate_select//
CREATE DEFINER=mysqlmv@localhost FUNCTION mysqlmv.generate_select(
  param_id bigint,
  mv_id bigint
)
RETURNS varchar(4000)
DETERMINISTIC
NO SQL
SQL SECURITY DEFINER
BEGIN
  DECLARE sql_from varchar(4000);
  select group_concat(expression order by expr_order asc)
  into sql_from
  from mview_expression
  where type = 'select' and mview_id = mv_id;

  return sql_from;
END //

DROP FUNCTION IF EXISTS  mysqlmv.generate_from//
CREATE DEFINER=mysqlmv@localhost FUNCTION mysqlmv.generate_from(
  param_group_id bigint,
  mv_id bigint
)
RETURNS varchar(4000)
DETERMINISTIC
NO SQL
SQL SECURITY DEFINER
BEGIN
  DECLARE sql_from varchar(4000);
  DECLARE join_count int;
  DECLARE root_expr_id bigint;
  select count(1) into join_count
  from mview_expression
  where type = 'join' and mview_id = mv_id;

  IF join_count = 0 THEN
    select id
    into root_expr_id
    from mview_expression
    where type = 'select' and mview_id = mv_id;
    call generate_fromtable_sql(param_group_id, mv_id, sql_from);
  ELSE
    select id into root_expr_id
    from mview_expression
    where type = 'join' and mview_id = mv_id
    order by expr_order desc
    limit 1;
    call generate_join_sql(param_group_id, root_expr_id, sql_from);
  END IF;

  return sql_from;
END //

DROP PROCEDURE IF EXISTS mysqlmv.generate_fromtable_sql//
CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.generate_fromtable_sql(
  IN param_group_id bigint,
  IN expr_id BIGINT,
  OUT from_sql varchar(4000)
)
BEGIN
  select
    IF(isnull(pl.from),
       concat(expr.table_owner, '.',  expr.table_name, ' ', expr.table_alias),
       concat('\`mysqlmv\`.', expr.table_name,' ', expr.table_alias)
    ) AS tf
  into from_sql
  from mview_expression expr left join parameter_list pl on expr.expr_order = pl.table_order
    -- -1 means there is no parameter at all.
  where expr.id = expr_id and (pl.group_id = param_group_id or param_group_id = -1);
END//

DROP PROCEDURE IF EXISTS mysqlmv.generate_join_sql//
CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.generate_join_sql(
  IN param_group_id bigint,
  IN expr_id BIGINT,
  OUT join_sql varchar(4000)
)
BEGIN
  DECLARE sql_join, j_left, j_right varchar(4000);
  DECLARE left_id, right_id int;
  DECLARE left_type, right_type, j_type varchar(128);
  DECLARE join_cond varchar(1024);
  -- get left join clause.
  select join_left, expression, join_type
  into left_id, join_cond, j_type
  from mview_expression where id = expr_id;
  -- get left type.
  select type
  into left_type
  from mview_expression
  where id = left_id;
  IF left_type = 'join' THEN
    call generate_join_sql(param_group_id, left_id, j_left);
  ELSEIF left_type = 'from' THEN
    call generate_fromtable_sql(param_group_id, left_id, j_left);
  END IF;
  -- get right join clause
  select join_right
  into right_id
  from mview_expression where id = expr_id;
  select type
  into right_type
  from mview_expression
  where id = right_id;
  IF right_type = 'join' THEN
    call generate_join_sql(param_group_id, right_id, j_right);
  ELSEIF right_type = 'from' THEN
    call generate_fromtable_sql(param_group_id, right_id, j_right);
  END IF;
  set join_sql = concat(j_left, ' ', j_type, ' ', j_right, ' ON ', join_cond);
END//

DROP FUNCTION IF EXISTS  mysqlmv.generate_where//
CREATE DEFINER=mysqlmv@localhost FUNCTION mysqlmv.generate_where(
  param_group_id bigint,
  mv_id bigint
)
RETURNS varchar(4000)
DETERMINISTIC
NO SQL
  SQL SECURITY DEFINER
  BEGIN
    DECLARE sql_where varchar(4000);
    return sql_where;
  END //

DELIMITER ;

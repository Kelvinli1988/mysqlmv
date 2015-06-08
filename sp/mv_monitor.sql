DELIMITER //

DROP PROCEDURE IF EXISTS  mysqlmv.mv_monitor//

CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.mv_monitor(
  mv_id bigint,
  status int
)
BEGIN
  -- check mv_status.
  DECLARE mv_status int;
  DECLARE drop_view_sql, create_table_sql varchar(4000);
  set mv_status = get_mv_status(mv_id);
  if mv_status = -2 THEN
    -- the setup in java side is done.
    -- set the status as INIT_LOAD_READY.
    call update_mv_status(mv_id, 2);
  #ELSEIF mv_status = -1 THEN
    -- error happened the setup in java side
  #  ;
  END IF;

  -- DO INIT_LOAD.
  -- drop original view;
  select concat('drop view ', mview_schema, '.', mview_name),
         concat('create table ', mview_schema, '.', mview_name, 'as ', mview_definition)
  into drop_view_sql, create_table_sql
  from mview
  where mview_id = mv_id;
  SELECT drop_view_sql, create_table_sql;

  PREPARE drop_view_stmt FROM drop_view_sql;
  EXECUTE drop_view_stmt;
  DEALLOCATE PREPARE drop_view_stmt;
  PREPARE create_table_sql FROM drop_view_sql;
  EXECUTE create_table_sql;
  DEALLOCATE PREPARE create_table_sql;
  # update the status of the materialized view.
  call update_mv_status(mv_id, 3);

END//

DELIMITER ;

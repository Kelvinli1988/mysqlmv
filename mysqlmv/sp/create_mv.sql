DELIMITER //

DROP PROCEDURE IF EXISTS  mysqlmv.create_mv//

CREATE DEFINER=root@localhost PROCEDURE mysqlmv.create_mv(
  schema_name TEXT,
  view_name TEXT,
  refresh_mode TINYINT
)
BEGIN
  DECLARE v_ret INT DEFAULT 0;
  DECLARE view_def TEXT;
#   DECLARE r_mode_enum ENUM();
  -- 1. get view definition
  select view_definition
  into view_def
  from `information_schema`.`views` sv
  where sv.table_schema = schema_name and sv.table_name = view_name;
  select view_def;
  -- 2. create mview
  -- 2.1 check refresh_type
  START TRANSACTION;
  insert into `mview`(  mview_id,       mview_schema,   mview_name,         mview_setup_finished,
                        mview_enabled,  is_complete,    mview_definition,   create_datetime)
  values(               null,           schema_name,    view_name,        0,
                        0,              refresh_mode,   view_def,         now());
  COMMIT;

END//

DELIMITER ;

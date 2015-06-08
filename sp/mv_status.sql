DELIMITER //

DROP PROCEDURE IF EXISTS  mysqlmv.update_mv_status//

CREATE DEFINER=mysqlmv@localhost PROCEDURE mysqlmv.update_mv_status(
  mv_id bigint,
  status int
)
BEGIN
  START TRANSACTION;
    update mview set mview_status = status where mview_id = mv_id;
  COMMIT;
END//

DROP FUNCTION IF EXISTS  mysqlmv.update_mv_status//
CREATE DEFINER=mysqlmv@localhost FUNCTION mysqlmv.get_mv_status(
  mv_id bigint
)
RETURNS int
DETERMINISTIC
NO SQL
SQL SECURITY DEFINER
BEGIN
  DECLARE mv_stat int;
  select mview_status into mv_stat from mview where mview_id=mv_id;
  SET @MV_STATUS = mv_stat;
  return (mv_stat);
END //

DELIMITER ;

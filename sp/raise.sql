DELIMITER //

DROP PROCEDURE IF EXISTS  mysqlmv.`raise`//

CREATE DEFINER=`root`@`localhost` PROCEDURE mysqlmv.`raise`(
  IN errortext TEXT CHARACTER SET UTF8
)
  BEGIN
    SET @sql = CONCAT('INSERT INTO mysqlmv.`mview_event_log` (`mview_id`, `message`) values(1, ', errortext, ')');
    SELECT @sql;
    PREPARE raise_stmt FROM @sql;
    EXECUTE raise_stmt;
    DEALLOCATE PREPARE raise_stmt;
  END ;
//

DELIMITER ;
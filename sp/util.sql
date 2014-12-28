DELIMITER //

DROP PROCEDURE IF EXISTS mysqlmv.util_concat //

CREATE DEFINER=root@localhost PROCEDURE mysqlmv.util_concat(
  sql_text TEXT,
  seperator CHAR
)
BEGIN
  PREPARE selectStmt from sql_text;
  DECLARE sourceCursor CURSOR FOR selectStmt;

END //

DELIMITER ;
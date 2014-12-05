package org.mysqlmv.etp.dao;


import org.mysqlmv.common.util.db.DBUtil;
import org.mysqlmv.common.util.db.QueryCallBack;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.mv.MaterializedView;
import org.mysqlmv.etp.scanner.MysqlMVConstant;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mysqlmv.common.util.db.DBUtil.*;

/**
 * Created by Kelvin Li on 12/5/2014 10:41 AM.
 */
public class EtpDao {
    public static List<MaterializedView> findUninitializedMV() {
        return executeInPrepareStmt(new QueryCallBack<List<MaterializedView>>() {
            @Override
            public String getSql() {
                return "select * from mview where mview_setup_finished = 0 and mview_name is not null";
            }

            @Override
            public List<MaterializedView> doInCallback(PreparedStatement pstmt) throws SQLException {
                rs = pstmt.executeQuery();
                List<MaterializedView> mvList = new ArrayList<MaterializedView>();
                while (rs.next()) {
                    MaterializedView mv = new MaterializedView();
                    mv.setName(rs.getString("mview_name"));
                    mv.setOriginalSchema(rs.getString("mview_schema"));
                    mv.setDefStr(rs.getString("mview_definition"));
                    mv.setId(rs.getInt("mview_id"));
                    mvList.add(mv);
                }
                return mvList;
            }
        });
    }


    public static List<ToiEntry> findUnsetupToiEntry() {
        return executeInPrepareStmt(new QueryCallBack<List<ToiEntry>>() {
            @Override
            public String getSql() {
                return "select distinct schema_name, table_name from mview_toi where setup_finished = 0";
            }

            @Override
            public List<ToiEntry> doInCallback(PreparedStatement pstmt) throws SQLException {
                ResultSet rs = pstmt.getResultSet();
                List<ToiEntry> entryList = new ArrayList<ToiEntry>();
                while (rs.next()) {
                    entryList.add(new ToiEntry(rs.getString("schema_name"),
                            rs.getString("table_name")));
                }
                return entryList;
            }
        });
    }

    public static boolean isTableExisted(final String tableName) {
        return executeInPrepareStmt(new QueryCallBack<Boolean>() {
            @Override
            public String getSql() {
                return "select * from `information_schema`.`tables` where table_schema = 'mysqlmv' and table_name = ?";
            }

            @Override
            public Boolean doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setString(1, tableName);
                rs = pstmt.executeQuery();
                return rs.next();
            }
        });
    }

    public static void updateToiSetup(final String schema, final String table) {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "update mview_toi set setup_finished = 1 where schema_name = ? and table_name = ? and setup_finished = 0";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setString(1, schema);
                pstmt.setString(2, table);
                pstmt.executeUpdate();
                return null;
            }
        });
    }

    public static void createToiTable(final String schema, final String table) {
        DBUtil.executeInPrepareStmt(new QueryCallBack<Object>() {
            @Override
            public String getSql() {
                return String.format(MysqlMVConstant.CREATE_TOI_TEMPLATE, schema, table);
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.execute();
                return null;
            }
        });
    }

    public static void addMviewToiDef(final int mviewId, final String schema,
                                      final String table, final String alias) {
        DBUtil.executeInPrepareStmt(new QueryCallBack<Object>() {
            @Override
            public String getSql() {
                return "insert into mview_toi(mview_toi_id, mview_id, schema_name, table_name, alias, create_datetime, last_update_datetime) " +
                        "values(null, ?, ?, ?, ?, now(), now())";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setInt(1, mviewId);
                pstmt.setString(2, schema);
                pstmt.setString(3, table);
                pstmt.setString(4, alias);
                return null;
            }
        });
    }

    public static void updateMVDef(final MaterializedView mv) {

        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "update mview set mview_setup_finished = 1 where mview_id = ?";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setInt(1, mv.getId());
                pstmt.execute();
                return null;
            }
        });
    }

    public static Map<ToiEntry, ToiValue> findToiContext() {
        return DBUtil.executeInPrepareStmt(new QueryCallBack<Map<ToiEntry, ToiValue>>() {
            @Override
            public String getSql() {
                return "select mview_toi_id, mview_id, schema_name, table_name " +
                        "from mview_toi where setup_finished = 1 order by schema_name, table_name";
            }

            @Override
            public Map<ToiEntry, ToiValue> doInCallback(PreparedStatement pstmt) throws SQLException {
                Map<ToiEntry, ToiValue> ret = new HashMap<ToiEntry, ToiValue>();
                pstmt.execute();
                rs = pstmt.getResultSet();
                while(rs.next()) {
                    String schema = rs.getString("schema_name");
                    String table = rs.getString("table_name");
                    int mview_toi_id = rs.getInt("mview_toi_id");
                    int mview_id = rs.getInt("mview_id");
                    ret.put(new ToiEntry(schema, table), new ToiValue(mview_toi_id, mview_id));
//                    ToiContext.addToiEntry(new ToiEntry(schema, table), new ToiValue(mview_toi_id, mview_id));
                }
                return ret;
            }
        });
    }
}
